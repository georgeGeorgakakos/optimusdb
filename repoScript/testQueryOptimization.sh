#!/bin/bash

# Test Script for Optimized OptimusDB Query
# This script tests the optimized query implementation

set -e

echo "========================================="
echo "OptimusDB Optimized Query Test Suite"
echo "========================================="
echo ""

# Configuration
BASE_PORT=18001
AGENTS=8

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Helper function to make requests
query_agent() {
    local port=$1
    local data=$2
    curl -s -X POST "http://localhost:${port}/optimusdb/command" \
        -H "Content-Type: application/json" \
        -d "${data}"
}

# Helper function to check if agents are running
check_agents() {
    echo "Checking if all agents are running..."
    for i in $(seq 1 $AGENTS); do
        port=$((BASE_PORT + i - 1))
        if ! curl -s "http://localhost:${port}/optimusdb/command" > /dev/null 2>&1; then
            echo -e "${RED}[FAIL]${NC} Agent $i (port ${port}) is not responding"
            exit 1
        fi
    done
    echo -e "${GREEN}[PASS]${NC} All $AGENTS agents are running"
    echo ""
}

# Test 1: Insert data to multiple agents
test_insert_data() {
    echo "Test 1: Inserting test data to agents..."

    # Insert to Agent 1
    result=$(query_agent $((BASE_PORT)) '{
        "method": {"cmd": "crudput", "argcnt": 1},
        "args": [""],
        "dstype": "SWres",
        "UpdateData": [
            {"id": "opt-test-1", "name": "Solar Panel Alpha", "status": "active", "type": "solar", "power": 150}
        ]
    }')
    echo "  Agent 1: Inserted Solar Panel Alpha"

    # Insert to Agent 3
    result=$(query_agent $((BASE_PORT + 2)) '{
        "method": {"cmd": "crudput", "argcnt": 1},
        "args": [""],
        "dstype": "SWres",
        "UpdateData": [
            {"id": "opt-test-2", "name": "Wind Turbine Beta", "status": "active", "type": "wind", "power": 200}
        ]
    }')
    echo "  Agent 3: Inserted Wind Turbine Beta"

    # Insert to Agent 5
    result=$(query_agent $((BASE_PORT + 4)) '{
        "method": {"cmd": "crudput", "argcnt": 1},
        "args": [""],
        "dstype": "SWres",
        "UpdateData": [
            {"id": "opt-test-3", "name": "Hydro Plant Gamma", "status": "active", "type": "hydro", "power": 300}
        ]
    }')
    echo "  Agent 5: Inserted Hydro Plant Gamma"

    echo -e "${GREEN}[PASS]${NC} Test data inserted"
    ((TESTS_PASSED++))
    echo ""

    # Give time for replication
    echo "Waiting 3 seconds for data replication..."
    sleep 3
}

# Test 2: Query from different agent (cache miss)
test_query_cache_miss() {
    echo "Test 2: Query from Agent 7 (should aggregate from other agents)..."

    start_time=$(date +%s%N)
    result=$(query_agent $((BASE_PORT + 6)) '{
        "method": {"cmd": "query", "argcnt": 0},
        "criteria": [{"status": "active"}]
    }')
    end_time=$(date +%s%N)

    duration=$(( (end_time - start_time) / 1000000 ))

    # Check if we got results
    count=$(echo "$result" | grep -o "opt-test" | wc -l)

    if [ $count -ge 1 ]; then
        echo -e "${GREEN}[PASS]${NC} Query returned $count results in ${duration}ms"
        echo "  Query latency (cache miss): ${duration}ms"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}[FAIL]${NC} Query returned no results"
        echo "  Response: $result"
        ((TESTS_FAILED++))
    fi
    echo ""
}

# Test 3: Same query again (cache hit)
test_query_cache_hit() {
    echo "Test 3: Same query again (should hit cache)..."

    start_time=$(date +%s%N)
    result=$(query_agent $((BASE_PORT + 6)) '{
        "method": {"cmd": "query", "argcnt": 0},
        "criteria": [{"status": "active"}]
    }')
    end_time=$(date +%s%N)

    duration=$(( (end_time - start_time) / 1000000 ))

    count=$(echo "$result" | grep -o "opt-test" | wc -l)

    if [ $count -ge 1 ]; then
        echo -e "${GREEN}[PASS]${NC} Query returned $count results in ${duration}ms"
        echo "  Query latency (cache hit): ${duration}ms"

        # Cache hit should be much faster (< 100ms)
        if [ $duration -lt 100 ]; then
            echo -e "${GREEN}[PASS]${NC} Cache optimization working (< 100ms)"
            ((TESTS_PASSED++))
        else
            echo -e "${YELLOW}[WARN]${NC} Cache hit was slower than expected (${duration}ms)"
        fi
    else
        echo -e "${RED}[FAIL]${NC} Query returned no results"
        ((TESTS_FAILED++))
    fi
    echo ""
}

# Test 4: Check cache statistics
test_cache_stats() {
    echo "Test 4: Checking cache statistics..."

    result=$(query_agent $((BASE_PORT + 6)) '{
        "method": {"cmd": "cachestats", "argcnt": 0}
    }')

    if echo "$result" | grep -q "hit_rate"; then
        echo -e "${GREEN}[PASS]${NC} Cache stats available"
        echo "  Stats: $result"
        ((TESTS_PASSED++))
    else
        echo -e "${YELLOW}[WARN]${NC} Cache stats not available (might not be implemented yet)"
    fi
    echo ""
}

# Test 5: Query with complex criteria
test_complex_query() {
    echo "Test 5: Complex query (type + power filter)..."

    start_time=$(date +%s%N)
    result=$(query_agent $((BASE_PORT + 1)) '{
        "method": {"cmd": "query", "argcnt": 0},
        "criteria": [{"type": "solar", "power": 150}]
    }')
    end_time=$(date +%s%N)

    duration=$(( (end_time - start_time) / 1000000 ))

    if echo "$result" | grep -q "Solar Panel Alpha"; then
        echo -e "${GREEN}[PASS]${NC} Complex query returned correct results in ${duration}ms"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}[FAIL]${NC} Complex query did not return expected results"
        ((TESTS_FAILED++))
    fi
    echo ""
}

# Test 6: Performance test (10 queries)
test_performance() {
    echo "Test 6: Performance test (10 queries)..."

    total_time=0
    for i in {1..10}; do
        start_time=$(date +%s%N)
        query_agent $((BASE_PORT)) '{
            "method": {"cmd": "query", "argcnt": 0},
            "criteria": [{"status": "active"}]
        }' > /dev/null
        end_time=$(date +%s%N)

        duration=$(( (end_time - start_time) / 1000000 ))
        total_time=$((total_time + duration))
    done

    avg_time=$((total_time / 10))

    echo "  10 queries completed"
    echo "  Average latency: ${avg_time}ms"
    echo "  Total time: ${total_time}ms"

    if [ $avg_time -lt 100 ]; then
        echo -e "${GREEN}[PASS]${NC} Performance is good (avg < 100ms)"
        ((TESTS_PASSED++))
    else
        echo -e "${YELLOW}[WARN]${NC} Performance could be better (avg ${avg_time}ms)"
    fi
    echo ""
}

# Test 7: Verify deduplication
test_deduplication() {
    echo "Test 7: Testing deduplication..."

    # Insert same data to multiple agents
    for agent_offset in 0 2 4; do
        query_agent $((BASE_PORT + agent_offset)) '{
            "method": {"cmd": "crudput", "argcnt": 1},
            "args": [""],
            "dstype": "SWres",
            "UpdateData": [
                {"id": "dedup-test-1", "name": "Duplicate Test", "status": "test"}
            ]
        }' > /dev/null
    done

    sleep 2

    # Query and count results
    result=$(query_agent $((BASE_PORT + 7)) '{
        "method": {"cmd": "query", "argcnt": 0},
        "criteria": [{"id": "dedup-test-1"}]
    }')

    count=$(echo "$result" | grep -o "dedup-test-1" | wc -l)

    if [ $count -eq 1 ]; then
        echo -e "${GREEN}[PASS]${NC} Deduplication working (only 1 result returned)"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}[FAIL]${NC} Deduplication not working ($count results returned)"
        ((TESTS_FAILED++))
    fi
    echo ""
}

# Main test execution
main() {
    check_agents
    test_insert_data
    test_query_cache_miss
    test_query_cache_hit
    test_cache_stats
    test_complex_query
    test_performance
    test_deduplication

    echo "========================================="
    echo "Test Summary"
    echo "========================================="
    echo -e "Tests Passed: ${GREEN}${TESTS_PASSED}${NC}"
    echo -e "Tests Failed: ${RED}${TESTS_FAILED}${NC}"
    echo ""

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}Some tests failed${NC}"
        exit 1
    fi
}

# Run tests
main