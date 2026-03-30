#!/bin/bash

# OptimusDB K3s - Scheduled Undeployment Script
# Runs at: 00:00 (midnight) daily
# Purpose: Undeploy OptimusDB cluster to save resources

set -e

LOG_FILE="/var/log/optimusdb/undeploy-$(date +%Y%m%d-%H%M%S).log"

# Create log directory
sudo mkdir -p /var/log/optimusdb
sudo chown ubuntu:ubuntu /var/log/optimusdb

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "OptimusDB K3s - Scheduled Undeployment START"
log "=========================================="

# Check if namespace exists
if ! kubectl get namespace optimusddc &>/dev/null; then
    log "INFO: Namespace 'optimusddc' does not exist"
    log "Nothing to undeploy (already stopped)"
    exit 0
fi

# Get current pod count
RUNNING_PODS=$(kubectl get pods -n optimusddc --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
log "INFO: Currently $RUNNING_PODS pods running"

# Delete the namespace (this removes everything)
log "INFO: Deleting namespace 'optimusddc'..."
kubectl delete namespace optimusddc >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    log "ERROR: Failed to delete namespace"
    exit 1
fi

# Wait for namespace deletion
log "INFO: Waiting for namespace deletion (max 2 minutes)..."
timeout=120
elapsed=0
while kubectl get namespace optimusddc &>/dev/null; do
    if [ $elapsed -ge $timeout ]; then
        log "WARNING: Namespace deletion timeout after ${timeout}s"
        break
    fi
    sleep 5
    ((elapsed+=5))
    log "INFO: Still deleting... (${elapsed}s elapsed)"
done

if kubectl get namespace optimusddc &>/dev/null; then
    log "WARNING: Namespace still exists after timeout"
    log "INFO: You may need to manually cleanup finalizers"
else
    log "SUCCESS: Namespace deleted successfully"
fi

# Verify no pods are running
REMAINING_PODS=$(kubectl get pods -n optimusddc --no-headers 2>/dev/null | wc -l)
if [ "$REMAINING_PODS" -eq 0 ]; then
    log "SUCCESS: All pods removed"
else
    log "WARNING: $REMAINING_PODS pods still exist"
fi

# Summary
log "=========================================="
log "Undeployment Summary:"
log "  Undeployed at: $(date)"
log "  Pods removed: $RUNNING_PODS"
log "  Log file: $LOG_FILE"
log "=========================================="
log "OptimusDB K3s - Scheduled Undeployment COMPLETE"
log "=========================================="

# Optional: Send notification
# echo "OptimusDB undeployed successfully at $(date)" | mail -s "OptimusDB Undeployment Success" your-email@example.com

exit 0