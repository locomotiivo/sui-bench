#!/bin/bash
#
# FDP Benchmark Orchestrator - SDK-based High-Throughput I/O Benchmark
#
# This script orchestrates the FDP benchmark using the Rust SDK-based load generator
# instead of spawning multiple `sui client` CLI processes.
#
# Key improvements over the previous approach:
# 1. Direct SDK transaction submission (no CLI process spawning overhead)
# 2. Async connection pooling to the SUI node  
# 3. Proper memory management with configurable concurrency limits
# 4. Mixed CREATE/UPDATE workload for hot/cold data segregation testing
#
# Usage:
#   ./bench.sh                    # Run both FDP and non-FDP benchmarks
#   NOFDP=yes ./bench.sh          # Run only non-FDP benchmark
#   NONFDP=yes ./bench.sh         # Run only FDP benchmark
#

set -e

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$SCRIPT_DIR/.."
FDP_TOOLS_DIR="/home/femu/fdp-scripts/f2fs-tools-fdp"
FDP_STATS="$FDP_TOOLS_DIR/fdp_stats"
MOUNT_POINT="/home/femu/f2fs_fdp_mount"
NVME_DEVICE="nvme0n1"
NVME_DEV_PATH="/dev/$NVME_DEVICE"
MOVE_DIR="$BENCH_DIR/move/io_churn"

# Benchmark binary
BENCH_BINARY="$BENCH_DIR/target/release/fdp-sui-bench"

# ═══════════════════════════════════════════════════════════════════════════════
# BENCHMARK PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════════

# Duration and throughput - TUNED FOR HIGH I/O
DURATION="${DURATION:-5400}"                   # 90 minutes default
WORKERS="${WORKERS:-32}"                      # More concurrent workers
BATCH_SIZE="${BATCH_SIZE:-100}"               # More objects per transaction
MAX_INFLIGHT="${MAX_INFLIGHT:-500}"           # More concurrent transactions
TARGET_TPS="${TARGET_TPS:-0}"                 # 0 = unlimited

# Workload mix - HEAVY UPDATES to cause more compaction/overwrites
CREATE_PCT="${CREATE_PCT:-10}"                # 10% CREATE = 90% updates (more rewrites)
SEED_OBJECTS="${SEED_OBJECTS:-500}"           # More seed objects to update

# Use 4KB LargeBlob objects instead of MicroCounters (40x more I/O per object)
USE_BLOBS="${USE_BLOBS:-no}"

# Gas - high budget for large batches
GAS_BUDGET="${GAS_BUDGET:-2000000000}"

# Control flags
NOFDP="${NOFDP:-no}"                          # Skip FDP benchmark
NONFDP="${NONFDP:-no}"                        # Skip non-FDP benchmark

# HOST access for FEMU stats
HOST_IP="${HOST_IP:-10.0.2.2}"
HOST_USER="${HOST_USER:-hajin}"
HOST_FEMU_LOG="${HOST_FEMU_LOG:-/home/hajin/femu-scripts/run-fdp.log}"
USE_HOST_STATS="${USE_HOST_STATS:-yes}"

# Output
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/results}"
LOG_DIR="$RESULTS_DIR/logs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

log_info()    { echo -e "${BLUE}[INFO]${NC} $1" >&2; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1" >&2; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1" >&2; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1" >&2; }

log_section() {
    echo "" >&2
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}" >&2
    echo -e "${BLUE}  $1${NC}" >&2
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}" >&2
}

# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

get_disk_stats() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $6, $10}' /proc/diskstats 2>/dev/null || echo "0 0"
}

get_device_sectors() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats 2>/dev/null || echo 0
}

format_bytes() {
    local bytes=$1
    if [ "$bytes" -ge 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1073741824" | bc) GB"
    elif [ "$bytes" -ge 1048576 ]; then
        echo "$(echo "scale=2; $bytes / 1048576" | bc) MB"
    elif [ "$bytes" -ge 1024 ]; then
        echo "$(echo "scale=2; $bytes / 1024" | bc) KB"
    else
        echo "$bytes B"
    fi
}

check_sui_node() {
    curl -s http://127.0.0.1:9000 \
        -H 'Content-Type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"sui_getChainIdentifier","params":[]}' \
        2>/dev/null | grep -q "result"
}

stop_sui_node() {
    log_info "Stopping SUI node..."
    pkill -15 -x "sui" 2>/dev/null || true
    pkill -15 -f "sui-node" 2>/dev/null || true
    pkill -15 -f "sui start" 2>/dev/null || true
    sleep 3
    pkill -9 -x "sui" 2>/dev/null || true
    pkill -9 -f "sui-node" 2>/dev/null || true
    pkill -9 -f "sui start" 2>/dev/null || true
    sleep 2
}

cleanup_benchmark() {
    log_info "Cleaning up benchmark..."
    pkill -9 -f "fdp-sui-bench" 2>/dev/null || true
    stop_sui_node
    
    if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
        sync
        sleep 2
        sudo umount "$MOUNT_POINT" 2>/dev/null || sudo umount -l "$MOUNT_POINT" 2>/dev/null || true
    fi
    
    log_success "Cleanup complete"
}

unmount_f2fs() {
    log_info "Unmounting F2FS..."
    cleanup_benchmark
}

mount_f2fs() {
    log_info "Mounting F2FS with FDP..."
    
    mkdir -p "$MOUNT_POINT"
    sudo "$FDP_STATS" "$NVME_DEV_PATH" --reset >/dev/null 2>&1 || true
    sudo "$FDP_TOOLS_DIR"/mkfs/mkfs.f2fs -f -O lost_found "$NVME_DEV_PATH"
    sudo "$FDP_TOOLS_DIR/fdp_f2fs_mount" 8
    sudo chmod -R 777 "$MOUNT_POINT"
    
    # Clean up any leftover data from previous runs
    rm -rf "$MOUNT_POINT/account_state" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/ledger" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/p0/*" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/p1/*" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/p2/*" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/p3/*" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/p4/*" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/p5/*" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/p6/*" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/p7/*" 2>/dev/null || true
    
    log_success "F2FS mounted at $MOUNT_POINT"
}

start_sui_node() {
    local use_fdp="${1:-false}"
    # Config goes in p7 (metadata PID) for proper FDP placement
    local config_dir="$MOUNT_POINT/p7/sui_config"
    
    log_info "Starting SUI node (FDP=$use_fdp)..."
    
    # Disable SUI_DISABLE_GAS for now - it causes execution issues
    # The faucet provides enough gas for benchmarking
    unset SUI_DISABLE_GAS
    
    # ═══════════════════════════════════════════════════════════════════════════
    # AGGRESSIVE ROCKSDB COMPACTION SETTINGS
    # These settings force more frequent flushes and compactions to stress the SSD
    # and trigger garbage collection in FEMU
    # ═══════════════════════════════════════════════════════════════════════════
    export MAX_WRITE_BUFFER_SIZE_MB=16          # 16MB vs 256MB default (16x smaller)
    export MAX_WRITE_BUFFER_NUMBER=2            # 2 vs 6 default (3x fewer buffers)
    export L0_NUM_FILES_COMPACTION_TRIGGER=2    # 2 vs 4 default (2x more frequent compaction)
    export TARGET_FILE_SIZE_BASE_MB=8           # 8MB vs 128MB default (16x smaller files)
    export DB_PARALLELISM=8                     # Use all 8 CPU cores for compaction
    
    log_info "RocksDB tuning: write_buffer=${MAX_WRITE_BUFFER_SIZE_MB}MB, L0_trigger=${L0_NUM_FILES_COMPACTION_TRIGGER}, file_size=${TARGET_FILE_SIZE_BASE_MB}MB"
    
    if [ "$use_fdp" = "true" ]; then
        # Enable WAL-Semantic FDP - instance IDs now ensure unique WAL/SST paths
        export SUI_FDP_WAL_SEMANTIC=1
        export SUI_FDP_BASE_PATH="$MOUNT_POINT"
        export SUI_FDP_HOT_SIZE_MB=64
        unset SUI_FDP_ENABLED SUI_FDP_MODE SUI_FDP_SEMANTIC
        log_info "FDP env: SUI_FDP_WAL_SEMANTIC=1, BASE_PATH=$MOUNT_POINT"
        log_info "  PID 0: ALL WAL (consolidated)"
        log_info "  PID 1-2: authority_db SST (hot/cold)"
        log_info "  PID 3-4: consensus_db SST (hot/cold)"
        log_info "  PID 5-6: fullnode_db SST (hot/cold)"
        log_info "  PID 7: Config + metadata"
    else
        unset SUI_FDP_ENABLED SUI_FDP_BASE_PATH SUI_FDP_MODE SUI_FDP_SEMANTIC SUI_FDP_WAL_SEMANTIC
        log_info "FDP env: DISABLED (standard RocksDB)"
    fi
    
    mkdir -p "$config_dir"
    sui genesis -f --working-dir "$config_dir"
    
    SUI_CONFIG_DIR="$config_dir" sui start --network.config "$config_dir" \
        --fullnode-rpc-port 9000 --with-faucet > "$LOG_DIR/sui_node.log" 2>&1 &
    
    log_info "Waiting for SUI node..."
    local max_wait=120 waited=0
    while ! check_sui_node && [ $waited -lt $max_wait ]; do
        sleep 2
        waited=$((waited + 2))
    done
    
    if check_sui_node; then
        log_success "SUI node started"
        return 0
    else
        log_error "SUI node failed to start"
        return 1
    fi
}

publish_contract() {
    local config_dir="${1:-$MOUNT_POINT/p0/sui_node}"
    log_info "Publishing io_churn contract..."
    
    export SUI_CONFIG_DIR="$config_dir"
    
    # Get active address and request gas
    local address=$(sui client active-address)
    log_info "Active address: $address"
    
    curl -s --location --request POST 'http://127.0.0.1:9123/gas' \
      --header 'Content-Type: application/json' \
      --data-raw "{\"FixedAmountRequest\": {\"recipient\": \"$address\"}}" >/dev/null 2>&1 || true
    sleep 2
    
    # Publish contract
    cd "$MOVE_DIR"
    rm -f Pub.localnet.toml Pub.testnet.toml 2>/dev/null || true
    
    local raw_output=$(sui client test-publish --build-env localnet --json 2>&1)
    local result=$(echo "$raw_output" | sed -n '/^{/,$p')
    
    if echo "$result" | jq -e '.effects.V2.status == "Success"' >/dev/null 2>&1; then
        local package_id=$(echo "$result" | jq -r '.changed_objects[] | select(.objectType == "package") | .objectId')
        
        if [ -n "$package_id" ] && [ "$package_id" != "null" ]; then
            echo "$package_id" > "$config_dir/.package_id"
            log_success "Contract published: $package_id"
            echo "$package_id"
            return 0
        fi
    fi
    
    log_error "Failed to publish contract"
    log_error "Output: ${raw_output:0:500}"
    return 1
}

read_femu_stats() {
    local sum_written=0
    local copied=0
    local waf="N/A"
    
    if [ "$USE_HOST_STATS" != "yes" ]; then
        echo "0 0 N/A"
        return
    fi
    
    if [ -x "$FDP_STATS" ]; then
        timeout 10 sudo "$FDP_STATS" "$NVME_DEV_PATH" --read-only >/dev/null 2>&1 || true
        sleep 2
    fi
    
    local femu_output=""
    femu_output=$(timeout 15 ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no \
        "${HOST_USER}@${HOST_IP}" \
        "tail -300 '$HOST_FEMU_LOG' 2>/dev/null | tr -d '\0' | grep -E 'Host written|GC copied|WAF:'" 2>/dev/null || true)
    
    if [ -n "$femu_output" ]; then
        sum_written=$(echo "$femu_output" | grep -oE 'Host written: [0-9]+' | tail -1 | grep -oE '[0-9]+')
        copied=$(echo "$femu_output" | grep -oE 'GC copied: [0-9]+' | tail -1 | grep -oE '[0-9]+')
        waf=$(echo "$femu_output" | grep -oE 'WAF: [0-9]+\.[0-9]+' | tail -1 | grep -oE '[0-9]+\.[0-9]+')
    fi
    
    sum_written=${sum_written:-0}
    copied=${copied:-0}
    
    if [ -z "$waf" ] || [ "$waf" = "N/A" ]; then
        if [ "$sum_written" -gt 0 ] 2>/dev/null; then
            waf=$(echo "scale=4; 1 + $copied / $sum_written" | bc)
        else
            waf="N/A"
        fi
    fi
    
    echo "$sum_written $copied $waf"
}

# ═══════════════════════════════════════════════════════════════════════════════
# BUILD BENCHMARK BINARY
# ═══════════════════════════════════════════════════════════════════════════════

build_benchmark() {
    log_info "Building SDK benchmark binary..."
    
    cd "$BENCH_DIR"
    
    if [ ! -f "Cargo.toml" ]; then
        log_error "Cargo.toml not found in $BENCH_DIR"
        return 1
    fi
    
    cargo build --release 2>&1 | tail -20
    
    if [ -f "$BENCH_BINARY" ]; then
        log_success "Benchmark binary built: $BENCH_BINARY"
        return 0
    else
        log_error "Failed to build benchmark binary"
        return 1
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# RUN BENCHMARK
# ═══════════════════════════════════════════════════════════════════════════════

run_benchmark() {
    local mode="$1"
    local run_dir="$2"
    local config_dir="$MOUNT_POINT/p7/sui_config"
    
    log_section "SDK BENCHMARK: $mode"
    
    local start_time=$(date +%s)
    
    # Get package ID
    local package_id=$(cat "$config_dir/.package_id" 2>/dev/null || echo "")
    if [ -z "$package_id" ]; then
        log_error "No package ID found"
        return 1
    fi
    
    log_info "Package ID: $package_id"
    
    # Record initial disk stats
    local initial_sectors=$(get_device_sectors)
    
    # Reset FEMU stats
    sudo "$FDP_STATS" "$NVME_DEV_PATH" --reset >/dev/null 2>&1 || true
    
    # Record benchmark info
    {
        echo "benchmark_mode=$mode"
        echo "start_time=$(date -Iseconds)"
        echo "start_sectors=$initial_sectors"
        echo "duration=$DURATION"
        echo "workers=$WORKERS"
        echo "batch_size=$BATCH_SIZE"
        echo "max_inflight=$MAX_INFLIGHT"
        echo "create_pct=$CREATE_PCT"
        echo "seed_objects=$SEED_OBJECTS"
    } > "$run_dir/benchmark_info.txt"
    
    log_info "Starting SDK benchmark..."
    log_info "  Duration:     ${DURATION}s"
    log_info "  Workers:      $WORKERS"
    log_info "  Batch Size:   $BATCH_SIZE"
    log_info "  Max Inflight: $MAX_INFLIGHT"
    log_info "  Create %:     $CREATE_PCT"
    log_info "  Use Blobs:    $USE_BLOBS"
    
    # Build the benchmark command
    local bench_cmd=(
        "$BENCH_BINARY"
        --rpc-url "http://127.0.0.1:9000"
        --package-id "$package_id"
        --duration "$DURATION"
        --workers "$WORKERS"
        --batch-size "$BATCH_SIZE"
        --max-inflight "$MAX_INFLIGHT"
        --create-pct "$CREATE_PCT"
        --seed-objects "$SEED_OBJECTS"
        --gas-budget "$GAS_BUDGET"
        --stats-interval 30
        --output "$run_dir/bench_results.json"
    )
    
    # Add --use-blobs flag if enabled
    if [ "$USE_BLOBS" = "yes" ]; then
        bench_cmd+=(--use-blobs)
    fi
    
    # Run the benchmark
    "${bench_cmd[@]}" 2>&1 | tee "$run_dir/bench.log"
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    # Collect final stats
    local final_sectors=$(get_device_sectors)
    local delta_sectors=$((final_sectors - initial_sectors))
    local delta_bytes=$((delta_sectors * 512))
    
    # Get FEMU stats
    local femu_stats=$(read_femu_stats)
    local sum_written=$(echo "$femu_stats" | awk '{print $1}')
    local copied=$(echo "$femu_stats" | awk '{print $2}')
    local waf=$(echo "$femu_stats" | awk '{print $3}')
    
    # Parse benchmark results
    local tx_success=0
    local objects_created=0
    local objects_updated=0
    if [ -f "$run_dir/bench_results.json" ]; then
        tx_success=$(jq -r '.tx_success // 0' "$run_dir/bench_results.json")
        objects_created=$(jq -r '.objects_created // 0' "$run_dir/bench_results.json")
        objects_updated=$(jq -r '.objects_updated // 0' "$run_dir/bench_results.json")
    fi
    
    local tps=0
    if [ $total_duration -gt 0 ]; then
        tps=$((tx_success / total_duration))
    fi
    
    # Append final stats
    {
        echo ""
        echo "end_time=$(date -Iseconds)"
        echo "total_duration=$total_duration"
        echo "final_sectors=$final_sectors"
        echo "delta_sectors=$delta_sectors"
        echo "delta_bytes=$delta_bytes"
        echo "sum_wpp_written=$sum_written"
        echo "copied=$copied"
        echo "final_waf=$waf"
        echo "tx_success=$tx_success"
        echo "objects_created=$objects_created"
        echo "objects_updated=$objects_updated"
        echo "tps=$tps"
    } >> "$run_dir/benchmark_info.txt"
    
    # Generate summary
    {
        echo "═══════════════════════════════════════════════════════════════════════════════"
        echo "  SDK Benchmark Summary: $mode"
        echo "═══════════════════════════════════════════════════════════════════════════════"
        echo ""
        echo "Configuration:"
        echo "  Duration:      ${total_duration}s"
        echo "  Workers:       $WORKERS"
        echo "  Batch Size:    $BATCH_SIZE objects/tx"
        echo "  Create %:      $CREATE_PCT%"
        echo ""
        echo "Transaction Performance:"
        echo "  Total TXs:         $tx_success"
        echo "  Objects Created:   $objects_created"
        echo "  Objects Updated:   $objects_updated"
        echo "  TPS:               $tps tx/sec"
        echo ""
        echo "Disk I/O:"
        echo "  Sectors Written:   $delta_sectors"
        echo "  Bytes Written:     $(format_bytes $delta_bytes)"
        echo ""
        echo "FEMU GC Statistics:"
        echo "  Host Written:      $sum_written pages"
        echo "  GC Copied:         $copied pages"
        echo "  WAF:               $waf"
        echo "═══════════════════════════════════════════════════════════════════════════════"
    } | tee "$run_dir/summary.txt"
    
    log_success "Benchmark $mode complete: WAF=$waf, TPS=$tps"
}

run_fdp_disabled_benchmark() {
    log_section "NON-FDP SDK BENCHMARK"
    local run_dir="$RESULTS_DIR/nfdp"
    local config_dir="$MOUNT_POINT/p7/sui_config"
    mkdir -p "$run_dir"
    
    stop_sui_node
    unmount_f2fs
    mount_f2fs
    start_sui_node "false"
    publish_contract "$config_dir"
    
    run_benchmark "nofdp" "$run_dir"
    
    stop_sui_node
    unmount_f2fs
}

run_fdp_enabled_benchmark() {
    log_section "FDP-ENABLED SDK BENCHMARK"
    local run_dir="$RESULTS_DIR/fdp"
    local config_dir="$MOUNT_POINT/p7/sui_config"
    mkdir -p "$run_dir"
    
    stop_sui_node
    unmount_f2fs
    mount_f2fs
    start_sui_node "true"
    publish_contract "$config_dir"
    
    run_benchmark "fdp" "$run_dir"
    
    stop_sui_node
    unmount_f2fs
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

main() {
    mkdir -p "$RESULTS_DIR" "$LOG_DIR"
    
    log_section "FDP SDK Benchmark for SUI Blockchain"
    log_info ""
    log_info "╔═══════════════════════════════════════════════════════════════════════╗"
    log_info "║  SDK-BASED BENCHMARK: Direct Transaction Submission                   ║"
    log_info "╠═══════════════════════════════════════════════════════════════════════╣"
    log_info "║  No CLI overhead - uses SUI SDK directly                              ║"
    log_info "║  Async connection pooling for high throughput                         ║"
    log_info "║  Proper concurrency control with semaphores                           ║"
    log_info "╚═══════════════════════════════════════════════════════════════════════╝"
    log_info ""
    log_info "Configuration:"
    log_info "  Duration:      ${DURATION}s"
    log_info "  Workers:       $WORKERS"
    log_info "  Batch Size:    $BATCH_SIZE"
    log_info "  Max Inflight:  $MAX_INFLIGHT"
    log_info "  Create %:      $CREATE_PCT"
    log_info ""
    
    # Build benchmark binary
    build_benchmark || {
        log_error "Failed to build benchmark"
        exit 1
    }
    
    log_info "Caching sudo credentials..."
    sudo -v || { log_error "sudo access required"; exit 1; }
    
    trap 'log_warning "Caught interrupt"; cleanup_benchmark; exit 130' INT TERM
    
    local nfdp_waf="N/A" fdp_waf="N/A"
    
    if [ "$NONFDP" != "yes" ]; then
        run_fdp_disabled_benchmark
        nfdp_waf=$(grep "^final_waf=" "$RESULTS_DIR/nfdp/benchmark_info.txt" 2>/dev/null | cut -d= -f2)
        nfdp_waf=${nfdp_waf:-N/A}
        
        if [ "$NOFDP" != "yes" ]; then
            log_info "Sleeping 30s between benchmarks..."
            sleep 30
        fi
    fi
    
    if [ "$NOFDP" != "yes" ]; then
        run_fdp_enabled_benchmark
        fdp_waf=$(grep "^final_waf=" "$RESULTS_DIR/fdp/benchmark_info.txt" 2>/dev/null | cut -d= -f2)
        fdp_waf=${fdp_waf:-N/A}
    fi
    
    log_section "FINAL RESULTS"
    {
        echo "Non-FDP WAF: $nfdp_waf"
        echo "FDP WAF:     $fdp_waf"
        if [[ "$nfdp_waf" != "N/A" && "$fdp_waf" != "N/A" ]]; then
            local improvement=$(echo "scale=2; (($nfdp_waf - $fdp_waf) / $nfdp_waf) * 100" | bc 2>/dev/null || echo "N/A")
            echo "WAF Improvement: ${improvement}%"
        fi
    } | tee "$RESULTS_DIR/final_comparison.txt"
    
    log_info "Results: $RESULTS_DIR"
    log_success "SDK benchmark completed successfully"
    
    cleanup_benchmark
    exit 0
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"
