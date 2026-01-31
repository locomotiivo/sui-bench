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
#   5. Runs the benchmark workload

set -e

FDP_MODE="${FDP_MODE:-0}"
SUI_DISABLE_GAS="${SUI_DISABLE_GAS:-1}"  # Default: gas disabled for max throughput
MOUNT_POINT="${MOUNT_POINT:-$HOME/f2fs_fdp_mount}"
# Auto-detect project root from script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="${RESULTS_DIR:-$PROJECT_ROOT/log}"

# Export for child scripts
export SUI_DISABLE_GAS

# ============================================================
# Helper functions
# ============================================================
log() { echo "[$(date '+%H:%M:%S')] $1"; }
die() { log "ERROR: $1"; exit 1; }

# Flag to track if we should cleanup node on exit
CLEANUP_NODE_ON_EXIT=0

cleanup() {
    local exit_code=$?
    
    # Kill background monitor processes
    [ -n "$MONITOR_PID" ] && kill $MONITOR_PID 2>/dev/null || true
    [ -n "$WATCHER_PID" ] && kill $WATCHER_PID 2>/dev/null || true
    
    # Only kill node if benchmark ran (CLEANUP_NODE_ON_EXIT=1) or on error
    if [ "$CLEANUP_NODE_ON_EXIT" -eq 1 ] || [ $exit_code -ne 0 ]; then
        log "Cleaning up (exit_code=$exit_code)..."
        [ -n "$NODE_PID" ] && kill $NODE_PID 2>/dev/null || true
        pkill -f "sui-node" 2>/dev/null || true
        pkill -f "sui start" 2>/dev/null || true
        pkill -f "fdp-epoch-watcher" 2>/dev/null || true
        log "Cleanup complete"
    fi
}

trap cleanup EXIT

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

log "[1/5] Starting Sui node with FDP storage..."
RESULTS_DIR="$RESULTS_DIR" "$SCRIPT_DIR/start-node.sh" 2>&1 | tee "$RESULTS_DIR/startup.log"

# start-node.sh exits when node is ready (or fails), no additional wait needed
log "✓ Node startup complete"

# ============================================================
# Step 2: Start epoch watcher (FDP mode only)
# ============================================================
log ""
log "[2/5] Starting epoch watcher..."

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
log "[3/5] Starting storage monitor..."

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
log "[4/5] Funding benchmark account..."

if [ -f "$SCRIPT_DIR/fund-account.sh" ]; then
    "$SCRIPT_DIR/fund-account.sh" 2>&1 | tee "$RESULTS_DIR/funding.log" || true
    log "✓ Account funded"
else
    log "  Skipped (fund-account.sh not found)"
fi

# ============================================================
# Step 5: Run benchmark workload
# ============================================================
log ""
log "[5/5] Starting benchmark workload..."
log ""

log "================================================================"
log " BENCHMARK RUNNING"
log "================================================================"
log ""
log " Sui Config:      $SUI_CONFIG_DIR"
log " FDP Mode:        $FDP_MODE"
log " Results Dir:     $RESULTS_DIR"
log ""
log " Monitor storage: tail -f $RESULTS_DIR/bloat.csv"
log " Node logs:       tail -f $RESULTS_DIR/01_sui_node.log"
log ""

BENCH_SCRIPT="$PROJECT_ROOT/server/max-device-write-bench.sh"
log "Using device-write focused benchmark: $BENCH_SCRIPT"

if [ -n "$BENCH_SCRIPT" ] && [ -f "$BENCH_SCRIPT" ]; then
    log "Found benchmark script: $BENCH_SCRIPT"
    
    # Export config for the benchmark
    export MOVE_DIR="$PROJECT_ROOT/move/bloat_storage"
    
    # Benchmark parameters (batch mode: 50 blobs x 200KB = 10MB per tx for max device writes)
    export BLOB_SIZE_KB="${BLOB_SIZE_KB:-200}"       # Max 200KB per blob (Move limit)
    export BATCH_COUNT="${BATCH_COUNT:-50}"          # 50 blobs per tx = 10MB per tx
    export PARALLEL="${PARALLEL:-32}"                # 32 workers
    export WORKERS="${WORKERS:-$PARALLEL}"           # Alias for max-device-write-bench.sh
    export DURATION="${DURATION:-300}"               # 5 minutes default
    export DURATION_SECONDS="${DURATION_SECONDS:-$DURATION}"
    
    log ""
    log "Benchmark settings:"
    log "  BLOB_SIZE_KB=$BLOB_SIZE_KB (max per blob)"
    log "  BATCH_COUNT=$BATCH_COUNT blobs/tx ($(($BLOB_SIZE_KB * $BATCH_COUNT / 1024))MB per tx)"
    log "  WORKERS=$WORKERS"
    log "  DURATION=$DURATION seconds"
    log "  MOVE_DIR=$MOVE_DIR"
    log ""
    
    # ============================================================
    # Publish Move contract if not already published
    # ============================================================
    PACKAGE_ID_FILE="$SUI_CONFIG_DIR/.package_id"
    if [ ! -f "$PACKAGE_ID_FILE" ] || [ ! -s "$PACKAGE_ID_FILE" ]; then
        log "Publishing Move contract..."
        
        # Verify MOVE_DIR exists
        if [ ! -d "$MOVE_DIR" ]; then
            log "  ERROR: Move directory not found: $MOVE_DIR"
            exit 1
        fi
        
        # Get chain ID and update Move.toml
        CHAIN_ID=$(curl -s http://127.0.0.1:9000 -d '{"jsonrpc":"2.0","id":1,"method":"sui_getChainIdentifier"}' -H 'Content-Type: application/json' | grep -oP '"result"\s*:\s*"\K[^"]+' || echo "")
        if [ -n "$CHAIN_ID" ] && [ -f "$MOVE_DIR/Move.toml" ]; then
            log "  Chain ID: $CHAIN_ID"
            sed -i "s/published-at = \"0x[^\"]*\"/published-at = \"0x0\"/" "$MOVE_DIR/Move.toml" 2>/dev/null || true
            # Remove any existing [addresses] section chain-specific entries
            sed -i '/^\[addresses\]/,/^\[/{/chain_id/d}' "$MOVE_DIR/Move.toml" 2>/dev/null || true
            
            # Add [environments] section for localnet if not present
            if ! grep -q "^\[environments\]" "$MOVE_DIR/Move.toml"; then
                echo "" >> "$MOVE_DIR/Move.toml"
                echo "[environments]" >> "$MOVE_DIR/Move.toml"
            fi
            # Update or add localnet environment with current chain ID
            if grep -q "^localnet\s*=" "$MOVE_DIR/Move.toml"; then
                sed -i "s/^localnet\s*=.*/localnet = \"$CHAIN_ID\"/" "$MOVE_DIR/Move.toml"
            else
                echo "localnet = \"$CHAIN_ID\"" >> "$MOVE_DIR/Move.toml"
            fi
            log "  Updated Move.toml with localnet environment"
        fi
        
        # Remove old ephemeral publication file (has stale chain ID)
        rm -f "$MOVE_DIR/Pub.localnet.toml" 2>/dev/null || true
        
        # Publish the contract using test-publish (creates ephemeral publication)
        SAVED_DIR="$(pwd)"
        cd "$MOVE_DIR" || { log "  ERROR: Cannot cd to $MOVE_DIR"; exit 1; }
        
        log "  Running: sui client test-publish --build-env localnet --gas-budget 500000000 --json"
        set +e
        PUBLISH_OUTPUT=$(sui client test-publish --build-env localnet --gas-budget 500000000 --json 2>&1)
        PUBLISH_EXIT=$?
        set -e
        cd "$SAVED_DIR"
        
        if [ $PUBLISH_EXIT -ne 0 ]; then
            log "  ERROR: sui client publish failed (exit $PUBLISH_EXIT)"
            log "  Output: $PUBLISH_OUTPUT"
            exit 1
        fi
        
        # Extract package ID - first try from JSON output
        PACKAGE_ID=$(echo "$PUBLISH_OUTPUT" | grep -oP '"packageId"\s*:\s*"\K0x[a-fA-F0-9]+' | head -1)
        
        if [ -z "$PACKAGE_ID" ]; then
            # Try alternative JSON extraction
            PACKAGE_ID=$(echo "$PUBLISH_OUTPUT" | grep -oP '"objectId"\s*:\s*"\K0x[a-fA-F0-9]+' | head -1)
        fi
        
        if [ -z "$PACKAGE_ID" ]; then
            # Extract from Pub.localnet.toml which test-publish creates
            if [ -f "$MOVE_DIR/Pub.localnet.toml" ]; then
                PACKAGE_ID=$(grep "^published-at" "$MOVE_DIR/Pub.localnet.toml" | grep -oP '0x[a-fA-F0-9]+' | head -1)
                log "  Extracted package ID from Pub.localnet.toml"
            fi
        fi
        
        if [ -n "$PACKAGE_ID" ]; then
            echo "$PACKAGE_ID" > "$PACKAGE_ID_FILE"
            log "  ✓ Package published: $PACKAGE_ID"
        else
            log "  ERROR: Failed to extract package ID"
            log "  Output: $PUBLISH_OUTPUT"
            if [ -f "$MOVE_DIR/Pub.localnet.toml" ]; then
                log "  Pub.localnet.toml:"
                cat "$MOVE_DIR/Pub.localnet.toml"
            fi
            exit 1
        fi
        cd - > /dev/null
    else
        PACKAGE_ID=$(cat "$PACKAGE_ID_FILE")
        log "Using existing package: $PACKAGE_ID"
    fi
    export PACKAGE_ID
    
    # Record initial FDP/device stats
    if [ -f "$SCRIPT_DIR/fdp-stats.sh" ]; then
        log "Recording initial FDP stats..."
        STATS_DIR="$RESULTS_DIR" "$SCRIPT_DIR/fdp-stats.sh" start 2>&1 | tee "$RESULTS_DIR/fdp_stats_start.log"
    fi
    
    # Mark that we're starting the benchmark - cleanup node on exit from this point
    CLEANUP_NODE_ON_EXIT=1
    
    # Start the benchmark
    BENCH_START_TIME=$(date +%s)
    "$BENCH_SCRIPT" 2>&1 | tee "$RESULTS_DIR/benchmark.log"
    BENCH_END_TIME=$(date +%s)
    
    # Record final FDP/device stats and calculate WAF
    if [ -f "$SCRIPT_DIR/fdp-stats.sh" ]; then
        log ""
        log "Recording final FDP stats..."
        STATS_DIR="$RESULTS_DIR" "$SCRIPT_DIR/fdp-stats.sh" stop 2>&1 | tee "$RESULTS_DIR/fdp_stats_final.log"
    fi
    
    # Print final summary
    BENCH_DURATION=$((BENCH_END_TIME - BENCH_START_TIME))
    log ""
    log "════════════════════════════════════════════════════════════════"
    log "  BENCHMARK SUMMARY"
    log "════════════════════════════════════════════════════════════════"
    log "  Total Duration: ${BENCH_DURATION}s"
    log "  FDP Mode:       $FDP_MODE"
    log "  Results Dir:    $RESULTS_DIR"
    if [ -f "$RESULTS_DIR/results.txt" ]; then
        log ""
        log "  Device Stats (from $RESULTS_DIR/results.txt):"
        cat "$RESULTS_DIR/results.txt" | while read line; do
            log "    $line"
        done
    fi
    log "════════════════════════════════════════════════════════════════"
    
# Fall back to Node.js server if available
elif [ -d "$PROJECT_ROOT/server" ] && [ -f "$PROJECT_ROOT/server/package.json" ]; then
    log "Found Node.js benchmark server at $PROJECT_ROOT/server"
    cd "$PROJECT_ROOT/server"
    
    # Install dependencies if needed
    if [ ! -d "node_modules" ]; then
        log "Installing npm dependencies..."
        npm install 2>&1 | tee "$RESULTS_DIR/npm_install.log"
    fi
    
    # Export config dir for the benchmark
    export SUI_CONFIG_DIR="$SUI_CONFIG_DIR"
    export BLOB_SIZE_KB="${BLOB_SIZE_KB:-100}"
    export BATCH_SIZE="${BATCH_SIZE:-5}"
    export STRATEGY="${STRATEGY:-mixed}"
    
    log ""
    log "Benchmark settings:"
    log "  BLOB_SIZE_KB=$BLOB_SIZE_KB"
    log "  BATCH_SIZE=$BATCH_SIZE"
    log "  STRATEGY=$STRATEGY"
    log ""
    
    # Start the benchmark
    npm start 2>&1 | tee "$RESULTS_DIR/benchmark.log"
else
    log ""
    log "⚠️  No benchmark server found at $PROJECT_ROOT/server"
    log ""
    log "To create it, run:"
    log "  mkdir -p $PROJECT_ROOT/server"
    log "  # ... add package.json and src files"
    log ""
    log "Or run your benchmark manually:"
    log "  - Use sui client commands"
    log "  - Run transaction scripts"
    log "  - Execute Move contracts"
    log ""
    log "Press Ctrl+C to stop the node and save results"
    
    # Wait indefinitely
    while true; do
        sleep 60
        
        # Print periodic status
        if [ "$FDP_MODE" -eq 1 ]; then
            log ""
            log "=== FDP Status ==="
            for pid in 0 1 2 3 4 5 6 7; do
                size=$(du -sh "$MOUNT_POINT/p$pid" 2>/dev/null | cut -f1 || echo "0")
                printf "  p%d: %s\n" $pid "$size"
            done
        else
            total=$(du -sh "$MOUNT_POINT" 2>/dev/null | cut -f1 || echo "0")
            log "Total storage: $total"
        fi
    done
fi

