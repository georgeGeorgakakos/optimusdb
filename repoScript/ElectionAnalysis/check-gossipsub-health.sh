#!/bin/bash
# ============================================================================
# GossipSub Mesh Health Check Script
# Save as: check-gossipsub-health.sh
# Usage: ./check-gossipsub-health.sh
# ============================================================================

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║         GossipSub Mesh Health Check for OptimusDB            ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

# ============================================================================
# Test 1: GossipSub Initialization
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TEST 1: Checking GossipSub Initialization"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

INIT_COUNT=$(grep -r "GossipSub created with.*mesh" node-*/logs/latest.log 2>/dev/null | wc -l)
echo "Nodes with mesh initialization: $INIT_COUNT/8"

if [ $INIT_COUNT -eq 8 ]; then
    echo -e "${GREEN}✅ PASS: All nodes initialized GossipSub mesh${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
elif [ $INIT_COUNT -gt 0 ]; then
    echo -e "${YELLOW}⚠️  WARNING: Only $INIT_COUNT nodes have mesh initialization${NC}"
    WARN_COUNT=$((WARN_COUNT + 1))
else
    echo -e "${RED}❌ FAIL: No nodes have mesh initialization${NC}"
    echo "   → Check if new code is deployed"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Show which nodes initialized
if [ $INIT_COUNT -gt 0 ]; then
    grep -r "GossipSub created with.*mesh" node-*/logs/latest.log 2>/dev/null | sed 's/:.*//' | while read node; do
        echo "   ✓ $node"
    done
fi
echo ""

# ============================================================================
# Test 2: Mesh Formation (GRAFT messages)
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TEST 2: Checking Mesh Formation (GRAFT messages)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

GRAFT_COUNT=$(grep -ri "graft" node-*/logs/latest.log 2>/dev/null | grep -v "OpportunisticGraft" | wc -l)
echo "Total GRAFT messages detected: $GRAFT_COUNT"

if [ $GRAFT_COUNT -gt 15 ]; then
    echo -e "${GREEN}✅ PASS: Mesh formation is active${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
elif [ $GRAFT_COUNT -gt 5 ]; then
    echo -e "${YELLOW}⚠️  WARNING: Low GRAFT activity ($GRAFT_COUNT messages)${NC}"
    echo "   → Mesh may still be forming, wait 60 seconds"
    WARN_COUNT=$((WARN_COUNT + 1))
else
    echo -e "${RED}❌ FAIL: No mesh formation detected${NC}"
    echo "   → Enable debug logging: export GOLOG_LOG_LEVEL=debug"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi
echo ""

# ============================================================================
# Test 3: Coordinator Election (CRITICAL!)
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TEST 3: Checking Coordinator Election (MOST IMPORTANT!)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

COORDINATOR_COUNT=$(grep -r "Elected as coordinator" node-*/logs/latest.log 2>/dev/null | wc -l)
echo "Coordinators elected: $COORDINATOR_COUNT"

if [ $COORDINATOR_COUNT -eq 1 ]; then
    echo -e "${GREEN}✅ PASS: Single coordinator elected (NO SPLIT-BRAIN!)${NC}"
    COORDINATOR=$(grep -r "Elected as coordinator" node-*/logs/latest.log 2>/dev/null | cut -d'/' -f1)
    echo "   → Coordinator: $COORDINATOR"
    PASS_COUNT=$((PASS_COUNT + 1))
elif [ $COORDINATOR_COUNT -eq 0 ]; then
    echo -e "${YELLOW}⚠️  WARNING: No coordinator elected yet${NC}"
    echo "   → Election may still be in progress, wait 30 seconds"
    WARN_COUNT=$((WARN_COUNT + 1))
else
    echo -e "${RED}❌ FAIL: SPLIT-BRAIN DETECTED! ($COORDINATOR_COUNT coordinators)${NC}"
    echo "   Multiple coordinators found:"
    grep -r "Elected as coordinator" node-*/logs/latest.log 2>/dev/null
    echo "   → This is the problem you're trying to fix!"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi
echo ""

# ============================================================================
# Test 4: Peer Connectivity
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TEST 4: Checking Peer Connectivity"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

LOW_PEER_COUNT=0
for i in {1..8}; do
    if [ -f "node-$i/logs/latest.log" ]; then
        PEER_COUNT=$(grep "Connected to peer\|Discovered peer" node-$i/logs/latest.log 2>/dev/null | wc -l)
        if [ $PEER_COUNT -ge 3 ]; then
            echo -e "   ${GREEN}✓${NC} node-$i: $PEER_COUNT peers"
        elif [ $PEER_COUNT -gt 0 ]; then
            echo -e "   ${YELLOW}⚠${NC} node-$i: $PEER_COUNT peers (low)"
            LOW_PEER_COUNT=$((LOW_PEER_COUNT + 1))
        else
            echo -e "   ${RED}✗${NC} node-$i: $PEER_COUNT peers (isolated!)"
            LOW_PEER_COUNT=$((LOW_PEER_COUNT + 1))
        fi
    else
        echo -e "   ${RED}✗${NC} node-$i: Log file not found"
        LOW_PEER_COUNT=$((LOW_PEER_COUNT + 1))
    fi
done

if [ $LOW_PEER_COUNT -eq 0 ]; then
    echo -e "${GREEN}✅ PASS: All nodes have good peer connectivity${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
elif [ $LOW_PEER_COUNT -le 2 ]; then
    echo -e "${YELLOW}⚠️  WARNING: $LOW_PEER_COUNT nodes have low peer count${NC}"
    WARN_COUNT=$((WARN_COUNT + 1))
else
    echo -e "${RED}❌ FAIL: $LOW_PEER_COUNT nodes have connectivity issues${NC}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi
echo ""

# ============================================================================
# Test 5: Message Propagation
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TEST 5: Checking Message Propagation"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

MSG_COUNT=$(grep -r "Received.*from peer" node-*/logs/latest.log 2>/dev/null | wc -l)
echo "Total messages received from peers: $MSG_COUNT"

if [ $MSG_COUNT -gt 30 ]; then
    echo -e "${GREEN}✅ PASS: Messages are propagating across network${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
elif [ $MSG_COUNT -gt 10 ]; then
    echo -e "${YELLOW}⚠️  WARNING: Low message activity ($MSG_COUNT messages)${NC}"
    WARN_COUNT=$((WARN_COUNT + 1))
else
    echo -e "${RED}❌ FAIL: Very low or no message propagation${NC}"
    echo "   → Messages may not be reaching other nodes"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi
echo ""

# ============================================================================
# Test 6: Discovery PubSub (Optional)
# ============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TEST 6: Checking Discovery PubSub (Optional)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

DISCOVERY_PUBSUB=$(grep -r "Enabling PubSub-based discovery" node-*/logs/latest.log 2>/dev/null | wc -l)

if [ $DISCOVERY_PUBSUB -gt 0 ]; then
    echo "PubSub discovery is ENABLED on $DISCOVERY_PUBSUB nodes"
    DISCOVERY_MESH=$(grep -r "DISCOVERY.*GossipSub created" node-*/logs/latest.log 2>/dev/null | wc -l)
    if [ $DISCOVERY_MESH -gt 0 ]; then
        echo -e "${GREEN}✅ Discovery mesh initialized on $DISCOVERY_MESH nodes${NC}"
    else
        echo -e "${YELLOW}⚠️  Discovery enabled but mesh not initialized${NC}"
    fi
else
    echo -e "ℹ️  PubSub discovery is DISABLED (this is normal)"
    echo "   Most setups use mDNS discovery only"
fi
echo ""

# ============================================================================
# FINAL SUMMARY
# ============================================================================
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                        FINAL SUMMARY                          ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

TOTAL_TESTS=$((PASS_COUNT + FAIL_COUNT + WARN_COUNT))
echo "Tests Run: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASS_COUNT${NC}"
echo -e "${YELLOW}Warnings: $WARN_COUNT${NC}"
echo -e "${RED}Failed: $FAIL_COUNT${NC}"
echo ""

if [ $FAIL_COUNT -eq 0 ] && [ $PASS_COUNT -ge 4 ]; then
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║  ✅ ALL CRITICAL TESTS PASSED!                               ║"
    echo "║  Your GossipSub mesh is working correctly!                   ║"
    echo "║  Split-brain problem is SOLVED! 🎉                           ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    exit 0
elif [ $FAIL_COUNT -eq 0 ] && [ $WARN_COUNT -gt 0 ]; then
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║  ⚠️  TESTS PASSED WITH WARNINGS                              ║"
    echo "║  System is functional but may need attention                 ║"
    echo "║  Review warnings above                                       ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    exit 0
else
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║  ❌ SOME TESTS FAILED                                        ║"
    echo "║  Review failed tests above and apply fixes                   ║"
    echo "║  Most critical: Coordinator election test                    ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    exit 1
fi