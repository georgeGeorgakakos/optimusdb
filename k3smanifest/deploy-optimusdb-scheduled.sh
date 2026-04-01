#!/bin/bash

# OptimusDB K3s - Scheduled Deployment Script
# Runs at: 08:00 AM daily
# Purpose: Deploy OptimusDB cluster for working hours
#
# Changes from previous version:
#   - kubectl cp replaced with cat-pipe to prevent silent truncation of large files
#   - supervisorctl uses full group-qualified name (optimusdb-suite:tinyllama)
#   - embedding endpoint readiness wait added after every model copy + restart
#   - supervisorctl start used instead of restart when process is in FATAL state
#   - post-deploy embedding health check added before declaring deployment complete

set -e

LOG_FILE="/var/log/optimusdb/deploy-$(date +%Y%m%d-%H%M%S).log"
PUBLIC_IP="193.225.250.240"

# Model file on the server — streamed into every pod after deployment
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

# ── Pre-flight checks ─────────────────────────────────────────────────────────

if [ ! -f "/opt/iccs/manifests/optimusddc-k3s-manifest.yaml" ]; then
    log "ERROR: Manifest file not found!"
    log "Expected: /opt/iccs/manifests/optimusddc-k3s-manifest.yaml"
    exit 1
fi

if [ ! -f "$MODEL_SRC" ]; then
    log "ERROR: Model file not found at $MODEL_SRC"
    exit 1
fi
MODEL_SIZE=$(stat -c%s "$MODEL_SRC" 2>/dev/null || stat -f%z "$MODEL_SRC" 2>/dev/null)
if [ "$MODEL_SIZE" -lt 600000000 ]; then
    log "ERROR: Model file at $MODEL_SRC is too small (${MODEL_SIZE} bytes) — expected ~638 MB"
    exit 1
fi
log "INFO: Model file OK ($(du -sh $MODEL_SRC | cut -f1))"

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

# ── Helper: stream model into a single pod and wait for embedding to be live ──
#
# Uses cat-pipe instead of kubectl cp.
# kubectl cp silently truncates large files (produces ~37 MB from a 668 MB source
# with exit code 0 and no error). The cat-pipe streams the file through stdin,
# which is reliable for large binary files.
#
# After copying, supervisorctl start/restart uses the full group-qualified name
# (optimusdb-suite:tinyllama) which supervisord requires when programs belong
# to a group. Using the bare name "tinyllama" returns "no such process".
#
# After the restart signal we poll the /embedding endpoint until it responds
# with a float vector, confirming the model loaded successfully. Without this
# wait the deployment script may declare success while tinyllama is still
# loading the 668 MB model (typically takes 10–30 s on CPU).
copy_model_to_pod() {
    local POD="$1"

    EXISTING=$(kubectl exec -n optimusddc "$POD" -- \
        stat -c%s /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf 2>/dev/null || echo "0")

    if [ "$EXISTING" -ge 600000000 ] 2>/dev/null; then
        log "INFO: $POD already has a valid model (${EXISTING} bytes) — skipping copy"
    else
        log "INFO: Streaming model to $POD (existing size: ${EXISTING} bytes)..."

        # FIX: cat-pipe instead of kubectl cp — prevents silent truncation
        if cat "$MODEL_SRC" | kubectl exec -i -n optimusddc "$POD" -- \
            sh -c 'cat > /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf' >> "$LOG_FILE" 2>&1; then

            WRITTEN=$(kubectl exec -n optimusddc "$POD" -- \
                stat -c%s /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf 2>/dev/null || echo "0")

            if [ "$WRITTEN" -lt 600000000 ]; then
                log "ERROR: Model copy to $POD still truncated (${WRITTEN} bytes) — skipping restart"
                return 1
            fi
            log "SUCCESS: Model streamed to $POD (${WRITTEN} bytes)"
        else
            log "WARNING: Failed to stream model to $POD — will retry on next run"
            return 1
        fi
    fi

    # Start or restart tinyllama using the group-qualified name.
    # supervisord registers programs as "group:program" when a [group] stanza
    # is present — using the bare name "tinyllama" returns "no such process".
    # We check current state first: if FATAL use 'start', otherwise 'restart'.
    TSTATE=$(kubectl exec -n optimusddc "$POD" -- \
        supervisorctl status optimusdb-suite:tinyllama 2>/dev/null | awk '{print $2}' || echo "UNKNOWN")

    if [ "$TSTATE" = "RUNNING" ] && [ "$EXISTING" -ge 600000000 ] 2>/dev/null; then
        log "INFO: $POD tinyllama already RUNNING with valid model — skipping restart"
    elif [ "$TSTATE" = "FATAL" ] || [ "$TSTATE" = "STOPPED" ] || [ "$TSTATE" = "EXITED" ]; then
        log "INFO: $POD tinyllama is $TSTATE — using 'start' to recover from terminal state"
        kubectl exec -n optimusddc "$POD" -- \
            supervisorctl start optimusdb-suite:tinyllama >> "$LOG_FILE" 2>&1 || true
    else
        log "INFO: $POD tinyllama is $TSTATE — restarting to load new model"
        kubectl exec -n optimusddc "$POD" -- \
            supervisorctl restart optimusdb-suite:tinyllama >> "$LOG_FILE" 2>&1 || true
    fi

    # Poll embedding endpoint until it returns a float vector (max 90 s).
    # This confirms the model loaded successfully — not just that the process started.
    log "INFO: Waiting for $POD embedding endpoint to be ready (max 90s)..."
    for i in $(seq 1 18); do
        EMBED=$(kubectl exec -n optimusddc "$POD" -- \
            curl -s -X POST http://127.0.0.1:8080/embedding \
            -H "Content-Type: application/json" \
            -d '{"content":"test"}' 2>/dev/null || echo "")

        if echo "$EMBED" | grep -q '"embedding":\['; then
            log "SUCCESS: $POD embedding endpoint is live (attempt $i/18)"
            return 0
        fi

        log "INFO: $POD embedding not ready yet (attempt $i/18) — waiting 5s..."
        sleep 5
    done

    log "WARNING: $POD embedding endpoint did not become ready within 90s"
    log "         Check: kubectl exec -n optimusddc $POD -- cat /var/log/supervisor/tinyllama_info.log"
    return 1
}

# ── Helper: copy model into all running optimusdb pods ───────────────────────
copy_model_to_pods() {
    log "INFO: Copying TinyLlama model into optimusdb pods..."

    PODS=$(kubectl get pods -n optimusddc -l app=optimusdb \
        --field-selector=status.phase=Running \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)

    if [ -z "$PODS" ]; then
        log "WARNING: No running optimusdb pods found — skipping model copy"
        return
    fi

    EMBED_OK=0
    for POD in $PODS; do
        if copy_model_to_pod "$POD"; then
            EMBED_OK=$((EMBED_OK + 1))
        fi
    done

    POD_COUNT=$(echo $PODS | wc -w)
    log "INFO: Embedding ready on $EMBED_OK/$POD_COUNT pods"
}

# ── Helper: verify embedding endpoint is live across all nodes (external) ────
verify_embedding_endpoints() {
    log "INFO: Verifying embedding endpoints via external HTTP..."
    EMBED_OK=0
    for path in optimusdb1 optimusdb2 optimusdb3; do
        RESULT=$(curl -s -X POST \
            "http://$PUBLIC_IP/${path}/swarmkb/embedding" \
            -H "Content-Type: application/json" \
            -d '{"content":"solar farm Greece"}' 2>/dev/null || echo "")

        # Also try internal port directly in case Traefik doesn't proxy /embedding
        if ! echo "$RESULT" | grep -q '"embedding":\['; then
            POD=$(kubectl get pods -n optimusddc -l "app=${path}" \
                -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [ -n "$POD" ]; then
                RESULT=$(kubectl exec -n optimusddc "$POD" -- \
                    curl -s -X POST http://127.0.0.1:8080/embedding \
                    -H "Content-Type: application/json" \
                    -d '{"content":"test"}' 2>/dev/null || echo "")
            fi
        fi

        if echo "$RESULT" | grep -q '"embedding":\['; then
            log "SUCCESS: ${path} embedding endpoint responding"
            EMBED_OK=$((EMBED_OK + 1))
        else
            log "WARNING: ${path} embedding endpoint not responding"
        fi
    done
    log "INFO: Embedding healthy on $EMBED_OK/3 nodes"
}

# ── Already-deployed path ─────────────────────────────────────────────────────
if kubectl get namespace optimusddc &>/dev/null; then
    log "WARNING: Namespace 'optimusddc' already exists"
    log "INFO: Checking if ghcr-secret is present (may have been lost on prior run)..."
    ensure_ghcr_secret

    # If catalogsearch is stuck in ContainerCreating, restart it now
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

    copy_model_to_pods
    verify_embedding_endpoints

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

# Stream model into pods and wait for embedding readiness before declaring ready
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

log "INFO: Waiting for all other services..."
sleep 30

log "INFO: Final pod status:"
kubectl get pods -n optimusddc -o wide >> "$LOG_FILE" 2>&1

# Test HTTP endpoints
log "INFO: Testing HTTP endpoints..."
ENDPOINTS_OK=0
for path in optimusdb1 optimusdb2 optimusdb3; do
    response=$(curl -s -o /dev/null -w "%{http_code}" \
        http://$PUBLIC_IP/${path}/swarmkb/agent/status 2>/dev/null)
    if [ "$response" = "200" ]; then
        log "SUCCESS: /${path} is responding (HTTP $response)"
        ENDPOINTS_OK=$((ENDPOINTS_OK + 1))
    else
        log "WARNING: /${path} not responding (HTTP $response)"
    fi
done
log "INFO: $ENDPOINTS_OK/3 HTTP endpoints are healthy"

# Verify embedding endpoints are live
verify_embedding_endpoints

# Summary
log "=========================================="
log "Deployment Summary:"
log "  Deployed at: $(date)"
log "  Endpoints:"
log "    - http://$PUBLIC_IP/optimusdb1/swarmkb/"
log "    - http://$PUBLIC_IP/optimusdb2/swarmkb/"
log "    - http://$PUBLIC_IP/optimusdb3/swarmkb/"
log "  Healthy HTTP endpoints: $ENDPOINTS_OK/3"
log "  Log file: $LOG_FILE"
log "=========================================="
log "OptimusDB K3s - Scheduled Deployment COMPLETE"
log "=========================================="
