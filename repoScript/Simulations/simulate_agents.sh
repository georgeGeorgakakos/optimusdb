#!/bin/bash
# ==============================================================================
# OptimusDB Agent Simulation Script
# Adds agents 4 and 5 to the optimusddc cluster, then removes them
# Derived from the existing 3-node manifest (namespace: optimusddc)
# ==============================================================================

set -euo pipefail

# ─── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Colour

# ─── Timings (seconds) ────────────────────────────────────────────────────────
OBSERVE_AFTER_ADD=60      # How long to pause so you can see it in the GUI
OBSERVE_AFTER_BOTH=120    # Pause with both nodes 4+5 running
BETWEEN_REMOVALS=60       # Pause between removing node 5 and node 4

# ─── Helpers ──────────────────────────────────────────────────────────────────
log()     { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $*"; }
success() { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓ $*${NC}"; }
warn()    { echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠ $*${NC}"; }
error()   { echo -e "${RED}[$(date '+%H:%M:%S')] ✗ $*${NC}" >&2; }
banner()  { echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════${NC}"; echo -e "${BOLD}${CYAN}  $*${NC}"; echo -e "${BOLD}${CYAN}══════════════════════════════════════════${NC}\n"; }

NS="optimusddc"
IMAGE="ghcr.io/georgegeorgakakos/optimusdb:latest"

# ─── Wait for pod ─────────────────────────────────────────────────────────────
wait_for_node() {
  local node=$1
  local label="app=optimusdb,node=${node}"
  log "Waiting for optimusdb${node} pod to be Running..."
  kubectl wait pod \
    -n "${NS}" \
    -l "${label}" \
    --for=condition=Ready \
    --timeout=180s && \
    success "optimusdb${node} is Ready" || \
    warn "optimusdb${node} did not become Ready within 180s – check: kubectl -n ${NS} get pods -l ${label}"
}

# ─── Countdown ────────────────────────────────────────────────────────────────
countdown() {
  local seconds=$1
  local msg=$2
  echo -e "${YELLOW}⏳ ${msg} – ${seconds}s observation window...${NC}"
  for ((i=seconds; i>0; i--)); do
    printf "\r${YELLOW}   %3ds remaining...${NC}" "$i"
    sleep 1
  done
  echo ""
}

# ─── Create one OptimusDB agent ───────────────────────────────────────────────
create_agent() {
  local N=$1
  local API_NP=$((30000 + N))    # e.g. 30004
  local P2P_NP=$((30010 + N))    # e.g. 30014
  local GW_NP=$((30020 + N))     # e.g. 30024

  banner "Adding OptimusDB Node ${N}  (API :${API_NP} | P2P :${P2P_NP} | GW :${GW_NP})"

  # ── PersistentVolumeClaim ──────────────────────────────────────────────────
  log "Creating PVC optimusdb${N}-data..."
  kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: optimusdb${N}-data
  namespace: ${NS}
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: local-path
  resources:
    requests:
      storage: 5Gi
EOF

  # ── ClusterIP Service ──────────────────────────────────────────────────────
  log "Creating ClusterIP Service optimusdb${N}..."
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: optimusdb${N}
  namespace: ${NS}
spec:
  type: ClusterIP
  selector:
    app: optimusdb
    node: "${N}"
  ports:
    - name: api
      port: 8089
      targetPort: 8089
EOF

  # ── NodePort Service ───────────────────────────────────────────────────────
  log "Creating NodePort Service optimusdb${N}-nodeport..."
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: optimusdb${N}-nodeport
  namespace: ${NS}
  labels:
    app: optimusdb
    node: "${N}"
spec:
  type: NodePort
  selector:
    app: optimusdb
    node: "${N}"
  ports:
    - name: api
      port: 8089
      targetPort: 8089
      nodePort: ${API_NP}
      protocol: TCP
    - name: p2p
      port: 4001
      targetPort: 4001
      nodePort: ${P2P_NP}
      protocol: TCP
    - name: gateway
      port: 5001
      targetPort: 5001
      nodePort: ${GW_NP}
      protocol: TCP
EOF

  # ── Traefik Middleware (strip prefix) ──────────────────────────────────────
  log "Creating Traefik Middleware optimusdb${N}-stripprefix..."
  kubectl apply -f - <<EOF
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: optimusdb${N}-stripprefix
  namespace: ${NS}
spec:
  stripPrefix:
    prefixes:
      - /optimusdb${N}
EOF

  # ── Traefik IngressRoute (node-specific) ───────────────────────────────────
  log "Creating Traefik IngressRoute optimusdb-node-${N}..."
  kubectl apply -f - <<EOF
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: optimusdb-node-${N}
  namespace: ${NS}
spec:
  entryPoints:
    - web
  routes:
    - match: PathPrefix(\`/optimusdb${N}\`)
      kind: Rule
      priority: 5
      middlewares:
        - name: optimusdb${N}-stripprefix
      services:
        - name: optimusdb${N}
          port: 8089
EOF

  # ── Deployment ─────────────────────────────────────────────────────────────
  log "Creating Deployment optimusdb${N}..."
  kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: optimusdb${N}
  namespace: ${NS}
  labels:
    app: optimusdb
    node: "${N}"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: optimusdb
      node: "${N}"
  template:
    metadata:
      labels:
        app: optimusdb
        node: "${N}"
    spec:
      imagePullSecrets:
        - name: ghcr-secret
      containers:
        - name: optimusdb
          image: ${IMAGE}
          imagePullPolicy: Always
          ports:
            - name: api
              containerPort: 8089
              protocol: TCP
            - name: p2p
              containerPort: 4001
              protocol: TCP
            - name: gateway
              containerPort: 5001
              protocol: TCP
          env:
            - name: POD_NAME
              value: "optimusdb${N}"
            - name: NODE_ID
              value: "${N}"
            - name: NODE_NAME
              value: "optimusdb${N}"
            - name: OPTIMUSDB_API_PORT
              valueFrom:
                configMapKeyRef:
                  name: optimusdb-config
                  key: OPTIMUSDB_API_PORT
            - name: OPTIMUSDB_P2P_PORT
              valueFrom:
                configMapKeyRef:
                  name: optimusdb-config
                  key: OPTIMUSDB_P2P_PORT
            - name: OPTIMUSDB_GATEWAY_PORT
              valueFrom:
                configMapKeyRef:
                  name: optimusdb-config
                  key: OPTIMUSDB_GATEWAY_PORT
            - name: OPTIMUSDB_LOG_LEVEL
              valueFrom:
                configMapKeyRef:
                  name: optimusdb-config
                  key: OPTIMUSDB_LOG_LEVEL
            - name: OPTIMUSDB_CONTEXT
              valueFrom:
                configMapKeyRef:
                  name: optimusdb-config
                  key: OPTIMUSDB_CONTEXT
            - name: OPTIMUSDB_BENCHMARK
              valueFrom:
                configMapKeyRef:
                  name: optimusdb-config
                  key: OPTIMUSDB_BENCHMARK
            # EMS integration via SSH tunnel (ActiveMQ STOMP)
            - name: HOST_IP
              valueFrom: { fieldRef: { fieldPath: status.hostIP } }
            - name: EMS_SERVICE_NAME
              valueFrom: { fieldRef: { fieldPath: status.hostIP } }
            - name: EMS_STOMP_PORT
              value: "61610"
            - name: EMS_TOPIC
              value: "/topic/>"
            - name: MQ_USER
              value: "aaa"
            - name: MQ_PASS
              value: "111"
            - name: MQ_CLIENT_ID
              value: "\$(POD_NAME)"
          startupProbe:
            tcpSocket:
              port: 8089
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          readinessProbe:
            tcpSocket:
              port: 8089
            initialDelaySeconds: 10
            periodSeconds: 5
          livenessProbe:
            tcpSocket:
              port: 8089
            initialDelaySeconds: 30
            periodSeconds: 10
          volumeMounts:
            - name: data
              mountPath: /var/lib/optimusdb
          resources:
            limits:
              memory: "768Mi"
              cpu: "500m"
            requests:
              memory: "384Mi"
              cpu: "100m"
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: optimusdb${N}-data
EOF

  wait_for_node "${N}"
  success "Agent ${N} is live."
}

# ─── Remove one OptimusDB agent ───────────────────────────────────────────────
remove_agent() {
  local N=$1
  banner "Removing OptimusDB Node ${N}"

  log "Deleting Deployment optimusdb${N}..."
  kubectl delete deployment  "optimusdb${N}"            -n "${NS}" --ignore-not-found

  log "Deleting IngressRoute optimusdb-node-${N}..."
  kubectl delete ingressroute "optimusdb-node-${N}"     -n "${NS}" --ignore-not-found

  log "Deleting Middleware optimusdb${N}-stripprefix..."
  kubectl delete middleware  "optimusdb${N}-stripprefix" -n "${NS}" --ignore-not-found

  log "Deleting NodePort Service optimusdb${N}-nodeport..."
  kubectl delete service     "optimusdb${N}-nodeport"   -n "${NS}" --ignore-not-found

  log "Deleting ClusterIP Service optimusdb${N}..."
  kubectl delete service     "optimusdb${N}"            -n "${NS}" --ignore-not-found

  log "Deleting PVC optimusdb${N}-data..."
  kubectl delete pvc         "optimusdb${N}-data"       -n "${NS}" --ignore-not-found

  success "Agent ${N} removed."
}

# ─── Show current cluster state ───────────────────────────────────────────────
show_status() {
  echo ""
  log "Current OptimusDB pods in namespace '${NS}':"
  kubectl get pods -n "${NS}" -l app=optimusdb \
    -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName,READY:.status.containerStatuses[0].ready' \
    2>/dev/null || kubectl get pods -n "${NS}" -l app=optimusdb
  echo ""
}

# ==============================================================================
# MAIN SIMULATION
# ==============================================================================

banner "OptimusDB Agent Simulation  (namespace: ${NS})"
echo -e "  ${BOLD}Observation window after each add : ${OBSERVE_AFTER_ADD}s${NC}"
echo -e "  ${BOLD}Observation window with both nodes: ${OBSERVE_AFTER_BOTH}s${NC}"
echo -e "  ${BOLD}Pause between removals            : ${BETWEEN_REMOVALS}s${NC}"
echo ""

# ── Step 1: Verify existing 3 nodes are healthy ─────────────────────────────
log "Verifying existing 3-node cluster..."
show_status

# ── Step 2: Add Agent 4 ──────────────────────────────────────────────────────
create_agent 4
show_status
countdown ${OBSERVE_AFTER_ADD} "Agent 4 is running – observe it in the OptimusDDC GUI"

# ── Step 3: Add Agent 5 ──────────────────────────────────────────────────────
create_agent 5
show_status
countdown ${OBSERVE_AFTER_BOTH} "Both agents 4 and 5 are running – observe them in the GUI"

# ── Step 4: Remove Agent 5 ───────────────────────────────────────────────────
remove_agent 5
show_status
countdown ${BETWEEN_REMOVALS} "Agent 5 removed – confirm it disappeared from the GUI"

# ── Step 5: Remove Agent 4 ───────────────────────────────────────────────────
remove_agent 4
show_status

banner "Simulation Complete"
log "Cluster is back to the original 3-node configuration."
show_status