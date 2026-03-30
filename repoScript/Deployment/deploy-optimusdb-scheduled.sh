
#!/bin/bash

# OptimusDB K3s - Scheduled Deployment Script
# Runs at: 08:00 AM daily
# Purpose: Deploy OptimusDB cluster for working hours

set -e

LOG_FILE="/var/log/optimusdb/deploy-$(date +%Y%m%d-%H%M%S).log"
PUBLIC_IP="193.225.250.240"

# Model file on the server — copied into every pod after deployment
MODEL_SRC="/opt/iccs/libs/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"

# Create log directory
sudo mkdir -p /var/log/optimusdb
sudo chown ubuntu:ubuntu /var/log/optimusdb

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "OptimusDB K3s - Scheduled Deployment START"
log "=========================================="

# Check if manifest exists
if [ ! -f "/opt/iccs/manifests/optimusddc-k3s-manifest.yaml" ]; then
    log "ERROR: Manifest file not found!"
    log "Expected: /opt/iccs/manifests/optimusddc-k3s-manifest.yaml"
    exit 1
fi

# Check model file exists and is not empty
if [ ! -f "$MODEL_SRC" ]; then
    log "ERROR: Model file not found at $MODEL_SRC"
    exit 1
fi
MODEL_SIZE=$(stat -c%s "$MODEL_SRC" 2>/dev/null || stat -f%z "$MODEL_SRC" 2>/dev/null)
if [ "$MODEL_SIZE" -lt 100000000 ]; then
    log "ERROR: Model file at $MODEL_SRC is too small (${MODEL_SIZE} bytes) — expected ~668 MB"
    exit 1
fi
log "INFO: Model file OK ($(du -sh $MODEL_SRC | cut -f1))"

# Check if GHCR PAT file exists — fail fast before deploying anything
# One-time setup:
#   sudo mkdir -p /opt/iccs/secrets
#   echo 'YOUR_GITHUB_PAT' | sudo tee /opt/iccs/secrets/ghcr-pat.txt
#   sudo chmod 600 /opt/iccs/secrets/ghcr-pat.txt
GHCR_PAT_FILE="/opt/iccs/secrets/ghcr-pat.txt"
if [ ! -f "$GHCR_PAT_FILE" ]; then
    log "ERROR: GHCR PAT file not found at $GHCR_PAT_FILE"
    log "       Run once to create it:"
    log "       sudo mkdir -p /opt/iccs/secrets"
    log "       echo 'YOUR_GITHUB_PAT' | sudo tee $GHCR_PAT_FILE"
    log "       sudo chmod 600 $GHCR_PAT_FILE"
    exit 1
fi

# ── Helper: ensure ghcr-secret exists in the namespace ───────────────────────
ensure_ghcr_secret() {
    if kubectl get secret ghcr-secret -n optimusddc &>/dev/null; then
        log "INFO: ghcr-secret already exists — skipping creation"
    else
        log "INFO: ghcr-secret missing — creating now..."
        GHCR_PAT=$(cat "$GHCR_PAT_FILE")
        kubectl create secret docker-registry ghcr-secret \
            --docker-server=ghcr.io \
            --docker-username=georgegeorgakakos \
            --docker-password="$GHCR_PAT" \
            --docker-email=george.georgakakos@gmail.com \
            -n optimusddc >> "$LOG_FILE" 2>&1
        if [ $? -ne 0 ]; then
            log "ERROR: Failed to create ghcr-secret — catalogsearch will not start!"
            exit 1
        fi
        log "SUCCESS: ghcr-secret created"
    fi
}
# ─────────────────────────────────────────────────────────────────────────────

# ── Helper: copy model into all running optimusdb pods ───────────────────────
# Runs on every deploy path (fresh and already-deployed) because the Docker
# image may have been built with a 0-byte model file on the Windows host.
# The copy is skipped for pods that already have the correct file size.
copy_model_to_pods() {
    log "INFO: Copying TinyLlama model into optimusdb pods..."

    PODS=$(kubectl get pods -n optimusddc -l app=optimusdb \
        --field-selector=status.phase=Running \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)

    if [ -z "$PODS" ]; then
        log "WARNING: No running optimusdb pods found — skipping model copy"
        return
    fi

    for POD in $PODS; do
        # Check existing model size inside pod to avoid unnecessary copy
        EXISTING=$(kubectl exec -n optimusddc "$POD" -- \
            stat -c%s /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf 2>/dev/null || echo "0")

        if [ "$EXISTING" -ge 100000000 ] 2>/dev/null; then
            log "INFO: $POD already has a valid model (${EXISTING} bytes) — skipping"
        else
            log "INFO: Copying model to $POD (existing size: ${EXISTING} bytes)..."
            if kubectl cp "$MODEL_SRC" \
                "optimusddc/${POD}:/models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf" \
                >> "$LOG_FILE" 2>&1; then
                log "SUCCESS: Model copied to $POD"
            else
                log "WARNING: Failed to copy model to $POD — will retry on next run"
                continue
            fi

            # Restart tinyllama so it picks up the new model file
            # '|| true' prevents set -e from aborting if supervisord is still initialising
            kubectl exec -n optimusddc "$POD" -- \
                supervisorctl restart tinyllama >> "$LOG_FILE" 2>&1 || true
            log "INFO: tinyllama restart signalled in $POD"
        fi
    done
}
# ─────────────────────────────────────────────────────────────────────────────

# Check if already deployed
if kubectl get namespace optimusddc &>/dev/null; then
    log "WARNING: Namespace 'optimusddc' already exists"
    log "INFO: Checking if ghcr-secret is present (may have been lost on prior run)..."

    # FIX: Always verify ghcr-secret even when namespace exists.
    # If the namespace was pre-existing but the secret is missing
    # (e.g. script ran before secret was created, or namespace was
    # partially recreated), catalogsearch will be stuck forever.
    ensure_ghcr_secret

    # If catalogsearch is stuck in ContainerCreating, restart it now
    # so it can pick up the newly created secret.
    CS_REASON=$(kubectl get pod -n optimusddc \
        -l app=catalogsearch \
        -o jsonpath='{.items[0].status.containerStatuses[0].state.waiting.reason}' 2>/dev/null || echo "")
    CS_STATUS=$(kubectl get pod -n optimusddc \
        -l app=catalogsearch \
        -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "")

    if [ "$CS_REASON" = "ContainerCreating" ] || [ "$CS_STATUS" = "Pending" ]; then
        log "WARNING: catalogsearch is stuck ($CS_REASON) — restarting deployment..."
        kubectl rollout restart deployment/catalogsearch -n optimusddc >> "$LOG_FILE" 2>&1
        log "INFO: catalogsearch deployment restarted"
    fi

    RUNNING_PODS=$(kubectl get pods -n optimusddc \
        --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    log "INFO: $RUNNING_PODS pods currently running"

    # Always copy model — runs even on the already-deployed path
    copy_model_to_pods

    log "=========================================="
    log "OptimusDB K3s - Scheduled Deployment COMPLETE (existing cluster)"
    log "=========================================="
    exit 0
fi

# ── Fresh deployment ──────────────────────────────────────────────────────────
log "INFO: Deploying OptimusDB cluster..."
kubectl apply -f /opt/iccs/manifests/optimusddc-k3s-manifest.yaml >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    log "ERROR: Deployment failed!"
    exit 1
fi

# Create ghcr-secret immediately after namespace + deployments are created
ensure_ghcr_secret

# Wait for optimusdb pods to be running before copying model
log "INFO: Waiting for optimusdb pods to be running (max 3 minutes)..."
for i in $(seq 1 18); do
    RUNNING=$(kubectl get pods -n optimusddc -l app=optimusdb \
        --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    if [ "$RUNNING" -ge 3 ]; then
        log "INFO: $RUNNING optimusdb pods are running"
        break
    fi
    log "INFO: Waiting... ($RUNNING/3 pods running, attempt $i/18)"
    sleep 10
done

# Copy model into pods NOW — before waiting for readiness
# This ensures tinyllama can load the model during the readiness window
copy_model_to_pods

# Wait for all pods to be ready
log "INFO: Waiting for all pods to be ready (max 5 minutes)..."
kubectl wait --for=condition=ready pod --all -n optimusddc --timeout=300s >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    log "WARNING: Pods did not become ready within timeout"
    log "INFO: Current pod status:"
    kubectl get pods -n optimusddc >> "$LOG_FILE" 2>&1
else
    log "SUCCESS: All pods are ready"
fi

# Wait for all other services
log "INFO: Waiting for all other services..."
sleep 30

# Get pod status
log "INFO: Final pod status:"
kubectl get pods -n optimusddc -o wide >> "$LOG_FILE" 2>&1

# Test endpoints
log "INFO: Testing endpoints..."
ENDPOINTS_OK=0
for path in optimusdb1 optimusdb2 optimusdb3; do
    response=$(curl -s -o /dev/null -w "%{http_code}" \
        http://$PUBLIC_IP/${path}/swarmkb/agent/status 2>/dev/null)
    if [ "$response" = "200" ]; then
        log "SUCCESS: /${path} is responding (HTTP $response)"
        ((ENDPOINTS_OK++))
    else
        log "WARNING: /${path} not responding (HTTP $response)"
    fi
done

log "INFO: $ENDPOINTS_OK/3 endpoints are healthy"

# Summary
log "=========================================="
log "Deployment Summary:"
log "  Deployed at: $(date)"
log "  Endpoints:"
log "    - http://$PUBLIC_IP/optimusdb1/swarmkb/"
log "    - http://$PUBLIC_IP/optimusdb2/swarmkb/"
log "    - http://$PUBLIC_IP/optimusdb3/swarmkb/"
log "  Healthy endpoints: $ENDPOINTS_OK/3"
log "  Log file: $LOG_FILE"
log "=========================================="
log "OptimusDB K3s - Scheduled Deployment COMPLETE"
log "=========================================="
