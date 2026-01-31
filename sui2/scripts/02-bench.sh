#!/bin/bash

# 02-bench.sh - Complete FDP benchmark orchestration
#
# Usage:
#   FDP_MODE=0 ./02-bench.sh   # Baseline (all data in p0)
#   FDP_MODE=1 ./02-bench.sh   # FDP mode (data segregated by PID)
#
#   WORKERS=32 DURATION=300 ./02-bench.sh  # Custom worker count and duration
#   BLOB_SIZE_KB=200 BATCH_COUNT=50        # 10MB per transaction
#
# This script:
#   1. Sets up FDP storage
#   2. Starts Sui validator with FDP-aware placement
#   3. Starts the epoch watcher (FDP_MODE=1 only)
#   4. Starts the storage monitor
#   5. Funds the benchmark account
#   6. Publishes the Move contract
#   7. Runs the benchmark workload

set -e

FDP_MODE="${FDP_MODE:-0}"
SUI_DISABLE_GAS="${SUI_DISABLE_GAS:-1}"  # Default: gas disabled for max throughput
MOUNT_POINT="${MOUNT_POINT:-$HOME/f2fs_fdp_mount}"
# Auto-detect project root from script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/log}"

# Export for child scripts
export SUI_DISABLE_GAS

# ============================================================
# Helper functions
# ============================================================
log() { echo "[$(date '+%H:%M:%S')] $1"; }
die() { log "ERROR: $1"; exit 1; }

# # Flag to track if we should cleanup node on exit
# CLEANUP_NODE_ON_EXIT=0
# 
# cleanup() {
#     local exit_code=$?
# 
#     # Kill background monitor processes
#     [ -n "$MONITOR_PID" ] && kill $MONITOR_PID 2>/dev/null || true
#     [ -n "$WATCHER_PID" ] && kill $WATCHER_PID 2>/dev/null || true
# 
#     # Only kill node if benchmark ran (CLEANUP_NODE_ON_EXIT=1) or on error
#     if [ "$CLEANUP_NODE_ON_EXIT" -eq 1 ] || [ $exit_code -ne 0 ]; then
#         log "Cleaning up (exit_code=$exit_code)..."
#         [ -n "$NODE_PID" ] && kill $NODE_PID 2>/dev/null || true
#         pkill -f "sui-node" 2>/dev/null || true
#         pkill -f "sui start" 2>/dev/null || true
#         pkill -f "fdp-epoch-watcher" 2>/dev/null || true
#         log "Cleanup complete"
#     fi
# }
# 
# trap cleanup EXIT

# ============================================================
# Create results directory
# ============================================================
mkdir -p "$RESULTS_DIR"
log "Results will be saved to: $RESULTS_DIR"

# Save run configuration
cat > "$RESULTS_DIR/config.txt" << EOF
FDP_MODE=$FDP_MODE
SUI_DISABLE_GAS=$SUI_DISABLE_GAS
MOUNT_POINT=$MOUNT_POINT
BLOB_SIZE_KB=${BLOB_SIZE_KB:-200}
BATCH_COUNT=${BATCH_COUNT:-10}
PARALLEL=${PARALLEL:-32}
DURATION_SECONDS=${DURATION_SECONDS:-0}
START_TIME=$(date -Iseconds)
HOSTNAME=$(hostname)
KERNEL=$(uname -r)
EOF

# ============================================================
# Step 1: Start Sui node with FDP placement
# ============================================================
log ""
log "╔═══════════════════════════════════════════════════════════════╗"
log "║  SUI FDP BENCHMARK - Mode: $([ $FDP_MODE -eq 1 ] && echo 'FDP (multi-PID)' || echo 'Baseline (single PID)') "
log "║  Gas Disabled: $([ \"$SUI_DISABLE_GAS\" = \"1\" ] && echo 'YES (max throughput)' || echo 'NO (normal gas fees)') "
log "╚═══════════════════════════════════════════════════════════════╝"
log ""

log "[1/7] Starting Sui node with FDP storage..."
RESULTS_DIR="$RESULTS_DIR" "$SCRIPT_DIR/start-node.sh" 2>&1 | tee "$RESULTS_DIR/startup.log"

# start-node.sh exits when node is ready (or fails), no additional wait needed
log "✓ Node startup complete"

# ============================================================
# Step 2: Start epoch watcher (FDP mode only)
# ============================================================
log ""
log "[2/7] Starting epoch watcher..."

if [ "$FDP_MODE" -eq 1 ]; then
    CONFIG_DIR="$MOUNT_POINT/p0/sui_node" "$SCRIPT_DIR/fdp-epoch-watcher.sh" watch > "$RESULTS_DIR/epoch_watcher.log" 2>&1 &
    WATCHER_PID=$!
    log "✓ Epoch watcher started (PID: $WATCHER_PID)"
else
    log "  Skipped (FDP_MODE=0)"
fi

# ============================================================
# Step 3: Start storage monitor
# ============================================================
log ""
log "[3/7] Starting storage monitor..."

# Set SUI_CONFIG_DIR early (needed by fund-account.sh and benchmark)
SUI_CONFIG_DIR="${SUI_CONFIG_DIR:-$MOUNT_POINT/p0/sui_node}"
export SUI_CONFIG_DIR

FDP_MODE=$FDP_MODE "$SCRIPT_DIR/monitor-bloat.sh" "$MOUNT_POINT/p0/sui_node" "$RESULTS_DIR/bloat.csv" > "$RESULTS_DIR/monitor.log" 2>&1 &
MONITOR_PID=$!
log "✓ Storage monitor started (PID: $MONITOR_PID)"

# ============================================================
# Step 4: Fund the benchmark account
# ============================================================
log ""
log "[4/7] Funding benchmark account..."

if [ -f "$SCRIPT_DIR/fund-account.sh" ]; then
    SUI_CONFIG_DIR="$SUI_CONFIG_DIR" "$SCRIPT_DIR/fund-account.sh" 2>&1 | tee "$RESULTS_DIR/funding.log" || true
    log "✓ Account funded"
else
    log "  Skipped (fund-account.sh not found)"
fi

# ============================================================
# Step 5: Publish Move contract
# ============================================================
log ""
log "[5/7] Publishing Move contract..."
    
# Clear any cached package ID to force re-publish with correct chain ID
rm -f "$SUI_CONFIG_DIR/.package_id" 2>/dev/null || true

# First, verify node is still running
log "Checking if node is responding..."
if ! curl -s --max-time 5 http://127.0.0.1:9000 -d '{"jsonrpc":"2.0","id":1,"method":"sui_getLatestCheckpointSequenceNumber"}' -H 'Content-Type: application/json' >/dev/null 2>&1; then
    log "ERROR: Node RPC is not responding"
    exit 1
fi
log "✓ Node RPC is responding"

MOVE_DIR="$PROJECT_ROOT/move/bloat_storage"
if [ -d "$MOVE_DIR" ]; then
    log "Publishing contract from: $MOVE_DIR"
    cd "$MOVE_DIR"
    
    # Clear any cached publication files to avoid chain ID conflicts
    rm -f Pub.localnet.toml Pub.testnet.toml 2>/dev/null || true
    
    # Publish using test-publish for local network
    log "Running: sui client test-publish --json"
    set +e  # Temporarily disable exit on error
    PUBLISH_OUTPUT=$(SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui client test-publish --build-env localnet --json)
    PUBLISH_EXIT_CODE=$?
    set -e  # Re-enable exit on error
    
    log "Publish exit code: $PUBLISH_EXIT_CODE"
    
    if [ $PUBLISH_EXIT_CODE -eq 0 ] && echo "$PUBLISH_OUTPUT" | jq -e '.effects.V2.status == "Success"' >/dev/null 2>&1; then
        PACKAGE_ID=$(echo "$PUBLISH_OUTPUT" | jq -r '.changed_objects[] | select(.objectType == "package") | .objectId')
        echo "$PACKAGE_ID" > "$SUI_CONFIG_DIR/.package_id"
        log "✓ Contract published: $PACKAGE_ID"
    else
        log "ERROR: Failed to publish contract (exit code: $PUBLISH_EXIT_CODE)"
        log "Publish output:"
        echo "$PUBLISH_OUTPUT" | head -20
        exit 1
    fi
else
    log "ERROR: Move directory not found: $MOVE_DIR"
    exit 1
fi

# # ============================================================
# # Step 5: Run benchmark workload
# # ============================================================
# log ""
# log "[6/7] Starting benchmark workload..."
# log ""

# log "================================================================"
# log " BENCHMARK RUNNING"
# log "================================================================"
# log ""
# log " Sui Config:      $SUI_CONFIG_DIR"
# log " FDP Mode:        $FDP_MODE"
# log " Results Dir:     $RESULTS_DIR"
# log ""
# log " Monitor storage: tail -f $RESULTS_DIR/bloat.csv"
# log " Node logs:       tail -f $RESULTS_DIR/01_sui_node.log"
# log ""

# BENCH_SCRIPT="$PROJECT_ROOT/server/max-device-write-bench.sh"
# log "Using device-write focused benchmark: $BENCH_SCRIPT"

# if [ -n "$BENCH_SCRIPT" ] && [ -f "$BENCH_SCRIPT" ]; then
#     log "Found benchmark script: $BENCH_SCRIPT"
    
#     # Export config for the benchmark
#     # Note: sui-benchmark.sh handles chain ID injection into Move.toml automatically
#     export SUI_CONFIG_DIR="$SUI_CONFIG_DIR"
#     export MOVE_DIR="$PROJECT_ROOT/move/bloat_storage"
    
#     # Benchmark parameters (batch mode: 10 blobs x 200KB = 2MB per tx)
#     export BLOB_SIZE_KB="${BLOB_SIZE_KB:-200}"       # Max 200KB per blob (Move limit)
#     export BATCH_COUNT="${BATCH_COUNT:-10}"          # 10 blobs per tx = 2MB per tx
#     export PARALLEL="${PARALLEL:-32}"                # 32 workers for ~1.3 GB/min
#     export DURATION_SECONDS="${DURATION_SECONDS:-0}" # 0 = run until Ctrl+C
    
#     log ""
#     log "Benchmark settings:"
#     log "  BLOB_SIZE_KB=$BLOB_SIZE_KB (max per blob)"
#     log "  BATCH_COUNT=$BATCH_COUNT blobs/tx ($(($BLOB_SIZE_KB * $BATCH_COUNT))KB per tx)"
#     log "  PARALLEL=$PARALLEL workers"
#     log "  DURATION_SECONDS=$DURATION_SECONDS (0=infinite)"
#     log "  MOVE_DIR=$MOVE_DIR"
#     log ""
    
#     # Start the benchmark
#     "$BENCH_SCRIPT" 2>&1 | tee "$RESULTS_DIR/benchmark.log"
    
# # Fall back to Node.js server if available
# elif [ -d "$PROJECT_ROOT/server" ] && [ -f "$PROJECT_ROOT/server/package.json" ]; then
#     log "Found Node.js benchmark server at $PROJECT_ROOT/server"
#     cd "$PROJECT_ROOT/server"
    
#     # Install dependencies if needed
#     if [ ! -d "node_modules" ]; then
#         log "Installing npm dependencies..."
#         npm install 2>&1 | tee "$RESULTS_DIR/npm_install.log"
#     fi
    
#     # Export config dir for the benchmark
#     export SUI_CONFIG_DIR="$SUI_CONFIG_DIR"
#     export BLOB_SIZE_KB="${BLOB_SIZE_KB:-100}"
#     export BATCH_SIZE="${BATCH_SIZE:-5}"
#     export STRATEGY="${STRATEGY:-mixed}"
    
#     log ""
#     log "Benchmark settings:"
#     log "  BLOB_SIZE_KB=$BLOB_SIZE_KB"
#     log "  BATCH_SIZE=$BATCH_SIZE"
#     log "  STRATEGY=$STRATEGY"
#     log ""
    
#     # Start the benchmark
#     npm start 2>&1 | tee "$RESULTS_DIR/benchmark.log"
# else
#     log ""
#     log "⚠️  No benchmark server found at $PROJECT_ROOT/server"
#     log ""
#     log "To create it, run:"
#     log "  mkdir -p $PROJECT_ROOT/server"
#     log "  # ... add package.json and src files"
#     log ""
#     log "Or run your benchmark manually:"
#     log "  - Use sui client commands"
#     log "  - Run transaction scripts"
#     log "  - Execute Move contracts"
#     log ""
#     log "Press Ctrl+C to stop the node and save results"
    
#     # Wait indefinitely
#     while true; do
#         sleep 60
        
#         # Print periodic status
#         if [ "$FDP_MODE" -eq 1 ]; then
#             log ""
#             log "=== FDP Status ==="
#             for pid in 0 1 2 3 4 5 6 7; do
#                 size=$(du -sh "$MOUNT_POINT/p$pid" 2>/dev/null | cut -f1 || echo "0")
#                 printf "  p%d: %s\n" $pid "$size"
#             done
#         else
#             total=$(du -sh "$MOUNT_POINT" 2>/dev/null | cut -f1 || echo "0")
#             log "Total storage: $total"
#         fi
#     done
# fi
