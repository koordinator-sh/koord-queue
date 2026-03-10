#!/bin/bash

# Test for Intelligent Queue Strategy
# Expected scheduling order:
# 1. pi-high-1 (priority=5, highest priority in high queue)
# 2. pi-high-2 (priority=5, second in high queue) 
# 3. pi-low-1  (priority=3, highest in low-priority queue)
# 4. pi-low-2  (priority=3, middle in low-priority queue)

set -e

if [[ "$1" == "cleanup" ]]; then
  kubectl delete -f ./intelligent.queue.yaml
  kubectl delete -f ./namespace.yaml
  exit 0
fi

echo "=== Setting up Intelligent Queue E2E Test ==="

# Apply resources
echo "Applying Intelligent Queue and ElasticQuotaTree..."
kubectl apply -f ./intelligent.queue.yaml

echo "Applying Namespace..."
kubectl apply -f ./namespace.yaml

sleep 3

echo "Applying test jobs..."
kubectl apply -f ./intelligent.jobs.yaml

sleep 5

echo ""
echo "=== Checking scheduling order ==="

# Function to get queue unit position
get_position() {
  local job_name=$1
  kubectl get queues -n kube-queue intelligent-queue -o json | \
    jq -r --arg name "$job_name" ' .status.queueItemDetails.active[] | select(.name==$name) | .position // 0'
}

# Function to check if job is dequeued
is_dequeued() {
  local job_name=$1
  local phase=$(kubectl get queueunits "$job_name" -n test-group -o json 2>/dev/null | jq -r '.status.phase // "NotFound"')
  [[ "$phase" == "Dequeued" ]] && echo "true" || echo "false"
}

# Wait a bit for queue to process
sleep 10

# Check positions
echo "Checking queue positions..."
pos_high1=$(get_position "pi-high-1")
pos_high2=$(get_position "pi-high-2")
pos_low1=$(get_position "pi-low-1")
pos_low2=$(get_position "pi-low-2")

echo "high-prio-job-1 (priority=5): position=$pos_high1"
echo "high-prio-job-2 (priority=5): position=$pos_high2"
echo "low-prio-job-1  (priority=3): position=$pos_low1"
echo "low-prio-job-2  (priority=3): position=$pos_low2"

echo ""
echo "=== Verification ==="

# Verify high priority jobs are in priority order (positions 1, 2, 3)
if [[ "$pos_high1" == "1" ]] && [[ "$pos_high2" == "2" ]]; then
  echo "✓ High priority jobs (>=4) are in priority+time order: PASS"
  high_prio_pass=true
else
  echo "✗ High priority jobs (>=4) are NOT in priority+time order: FAIL"
  echo "  Expected: high-prio-job-1=1, high-prio-job-2=2"
  echo "  Actual:   high-prio-job-1=$pos_high1, high-prio-job-2=$pos_high2"
  high_prio_pass=false
fi

# Verify low priority jobs are in priority order (positions 4, 5, 6)
# low-prio-job-2 (prio=3) should be position 4
# low-prio-job-3 (prio=2) should be position 5
# low-prio-job-1 (prio=1) should be position 6
if [[ "$pos_low1" == "3" ]] && [[ "$pos_low2" == "4" ]]; then
  echo "✓ Low priority jobs (<4) are in priority order: PASS"
  low_prio_pass=true
else
  echo "✗ Low priority jobs (<4) are NOT in priority order: FAIL"
  echo "  Expected: low-prio-job-1=3, low-prio-job-2=4"
  echo "  Actual:   low-prio-job-1=$pos_low1, low-prio-job-2=$pos_low2"
  low_prio_pass=false
fi

echo ""

# Overall result
if [[ "$high_prio_pass" == "true" ]] && [[ "$low_prio_pass" == "true" ]]; then
  echo "=== TEST PASSED ✓ ==="
  echo "Intelligent Queue Strategy works correctly:"
  echo "  - High priority tasks (>=threshold) use priority+time ordering"
  echo "  - Low priority tasks (<threshold) use priority+time ordering"
else
  echo "=== TEST FAILED ✗ ==="
  exit 1
fi


echo ""
echo "=== Additional Verification ==="

# 1. Check that all jobs are not scheduled (still in Enqueued state)
echo "1. Checking that all jobs are not scheduled (still in Enqueued state)..."

all_enqueued=true
for job in pi-high-1 pi-high-2 pi-low-1 pi-low-2; do
  phase=$(kubectl get queueunits "$job" -n test-group -o json 2>/dev/null | jq -r '.status.phase // ""')
  if [[ "$phase" != "Enqueued" && "$phase" != "" ]]; then
    echo "✗ Job $job is not in Enqueued state, phase: $phase"
    all_enqueued=false
  else
    echo "✓ Job $job is in Enqueued state"
  fi
done

if [[ "$all_enqueued" == "true" ]]; then
  echo "✓ All jobs are in Enqueued state: PASS"
else
  echo "✗ Some jobs are not in Enqueued state: FAIL"
fi

echo ""

# 2. Delete pi-high-1, wait for a while, and verify that all jobs except pi-low-1 are in Dequeued state
echo "2. Deleting pi-high-1 and verifying other jobs transition to Dequeued state..."

kubectl delete job pi-high-1 -n test-group

# Wait for queue to process the deletion
echo "Waiting for queue to process deletion..."
sleep 10

all_dequeued_except_low1=true
for job in pi-high-2 pi-low-2; do
  phase=$(kubectl get queueunits "$job" -n test-group -o json 2>/dev/null | jq -r '.status.phase // "NotFound"')
  if [[ "$phase" != "Dequeued" && "$phase" != "Running" && "$phase" != "Succeed" ]]; then
    echo "✗ Job $job is not in Dequeued state, phase: $phase"
    all_dequeued_except_low1=false
  else
    echo "✓ Job $job is in Dequeued state"
  fi
done

# Check that pi-low-1 is NOT in Dequeued state
phase_low1=$(kubectl get queueunits "pi-low-1" -n test-group -o json 2>/dev/null | jq -r '.status.phase // "NotFound"')
if [[ "$phase_low1" == "Dequeued" ]]; then
  echo "✗ Job pi-low-1 is in Dequeued state, but it should not be"
  all_dequeued_except_low1=false
else
  echo "✓ Job pi-low-1 is NOT in Dequeued state as expected"
fi

if [[ "$all_dequeued_except_low1" == "true" ]]; then
  echo "✓ All jobs except pi-low-1 are in Dequeued state after pi-high-1 deletion: PASS"
else
  echo "✗ Verification failed: not all jobs except pi-low-1 are in Dequeued state: FAIL"
fi

echo ""
echo "=== FINAL RESULT ==="
if [[ "$all_enqueued" == "true" ]] && [[ "$all_dequeued_except_low1" == "true" ]]; then
  echo "✓ All additional verifications passed"
  exit 0
else
  echo "✗ Some additional verifications failed"
  exit 1
fi
