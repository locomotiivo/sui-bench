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
FDP_STATS="$SCRIPT_DIR/fdp_stats"
MOUNT_POINT="/home/femu/f2fs_fdp_mount"
NVME_DEVICE="nvme0n1"
NVME_DEV_PATH="/dev/$NVME_DEVICE"
MOVE_DIR="$BENCH_DIR/move/io_churn"

# Benchmark binary
BENCH_BINARY="$BENCH_DIR/target/release/fdp-sui-bench"

# sui-single-node-benchmark for rapid disk prefill
SUI_SINGLE_NODE_BENCH="/home/femu/sui/target/release/sui-single-node-benchmark"

# ═══════════════════════════════════════════════════════════════════════════════
# BENCHMARK PARAMETERS - NFT MINTING APPROACH
# ═══════════════════════════════════════════════════════════════════════════════

# NOTE: We use sui-single-node-benchmark with NFT minting instead of SDK benchmark
# The below parameters are NOT used anymore - keeping for reference only
# DURATION, WORKERS, BATCH_SIZE etc. are SDK benchmark parameters

# Control flags
NOFDP="${NOFDP:-no}"                          # Skip FDP benchmark
NONFDP="${NONFDP:-no}"                        # Skip non-FDP benchmark

# ═══════════════════════════════════════════════════════════════════════════════
# NFT MINTING BENCHMARK PARAMETERS - TURBO MODE
# 
# Strategy: sui-single-node-benchmark has INTERNAL RocksDB + execution engine.
# TIME-BASED mode: Fixed duration for each phase (guarantees all phases run)
#   - 50K TX × 5 NFTs × 6KB = ~1.5GB raw data per batch (manageable)
#   - Phase 3 counter updates for rapid invalidation burst
# ═══════════════════════════════════════════════════════════════════════════════
# UPDATE-HEAVY STRATEGY (realistic blockchain workload)
#
# Real blockchains: Create objects once, UPDATE them millions of times
# NFT minting = CREATE (append-only, no compaction pressure)
# Counter updates = UPDATE (version churn, massive compaction, WAF > 1)
#
# Phase 1: Short setup - create initial state + counters
# Phase 2: Long update phase - hammer counters with updates (realistic steady-state)
# Phase 3: Optional cleanup phase
# ═══════════════════════════════════════════════════════════════════════════════

# TIME-BASED PHASE DURATIONS (in minutes)
# GC expected within 15-30 minutes with UPDATE-heavy workload
SETUP_DURATION_MIN="${SETUP_DURATION_MIN:-5}"     # 5 min setup (create initial objects)
# ═══════════════════════════════════════════════════════════════════════════════
# UNIFIED WORKLOAD PARAMETERS
# Inspired by Solana's bench-tps: continuous workload with state updates
# ═══════════════════════════════════════════════════════════════════════════════
BENCH_DURATION_MIN="${BENCH_DURATION_MIN:-40}"    # 40 min total (like Solana's --duration)

# WORKLOAD MODE: "balanced" or "solana-style"
# - balanced: More NFT mints for faster disk fill + counter updates
# - solana-style: Fewer mints, more counter updates (closer to Solana transfers)
WORKLOAD_MODE="${WORKLOAD_MODE:-balanced}"

# Per-batch parameters (keep small for memory safety!)
TX_COUNT="${TX_COUNT:-5000}"                      # 5K TXs per batch (like Solana's --tx-count)

if [ "$WORKLOAD_MODE" = "solana-style" ]; then
    # SOLANA-STYLE: More like bench-tps - repeated state updates (transfers)
    # Solana transfers 1 lamport back and forth - we update counters repeatedly
    NUM_MINTS="${NUM_MINTS:-1}"                   # Minimal mints (just for some data)
    NFT_SIZE="${NFT_SIZE:-1000}"                  # 1KB per NFT (smaller)
    NUM_COUNTERS="${NUM_COUNTERS:-50}"            # 50 counter updates per TX (like 50 transfers)
    # Per batch: 5000 × 1 × 1KB = 5MB new + 250K state updates (high churn)
else
    # BALANCED: Disk fill + compaction pressure
    NUM_MINTS="${NUM_MINTS:-5}"                   # 5 NFTs per TX (disk fill)
    NFT_SIZE="${NFT_SIZE:-4000}"                  # 4KB per NFT
    NUM_COUNTERS="${NUM_COUNTERS:-20}"            # 20 counter updates per TX
    # Per batch: 5000 × 5 × 4KB = 100MB new + 100K state updates
fi

# ═══════════════════════════════════════════════════════════════════════════════
# COMPARISON TO SOLANA BENCH-TPS:
# ═══════════════════════════════════════════════════════════════════════════════
# Solana bench-tps:
#   - Transfers 1 lamport between pre-funded keypairs
#   - Alternates direction (A→B, then B→A) = same accounts updated repeatedly
#   - Focuses on TPS, not disk fill
#   - Transaction size: ~600 bytes
#
# SUI equivalent:
#   - Counter updates = state changes (like transfers updating balances)
#   - NFT mints = initial funding (but larger to stress disk I/O)
#   - --num-shared-objects updates = like repeated transfers on same accounts
#
# Key difference: Solana's tiny txs maximize TPS; we use larger objects to
# stress disk I/O since that's our WAF measurement goal.
# ═══════════════════════════════════════════════════════════════════════════════

# MEMORY LIMITS - more aggressive to prevent ballooning
MAX_MEMORY_PCT="${MAX_MEMORY_PCT:-60}"            # Pause if memory exceeds 60%
DB_PARALLELISM="${DB_PARALLELISM:-4}"             # Limit to 4 cores for memory safety

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

# Memory monitoring for 16GB RAM safety
check_memory() {
    local mem_info=$(free -m | awk 'NR==2{print $2,$3,$4,$7}')
    local total=$(echo "$mem_info" | cut -d' ' -f1)
    local used=$(echo "$mem_info" | cut -d' ' -f2)
    local free=$(echo "$mem_info" | cut -d' ' -f3)
    local available=$(echo "$mem_info" | cut -d' ' -f4)
    local pct=$((used * 100 / total))
    
    echo "$pct $used $total $available"
}

wait_for_memory() {
    local max_pct="${MAX_MEMORY_PCT:-80}"
    local mem_check=$(check_memory)
    local pct=$(echo "$mem_check" | cut -d' ' -f1)
    local used=$(echo "$mem_check" | cut -d' ' -f2)
    local total=$(echo "$mem_check" | cut -d' ' -f3)
    local available=$(echo "$mem_check" | cut -d' ' -f4)
    
    if [ "$pct" -ge "$max_pct" ]; then
        log_warning "Memory at ${pct}% (${used}MB/${total}MB), waiting for GC..."
        # Force sync and drop caches to free memory
        sync
        sudo sh -c "echo 3 > /proc/sys/vm/drop_caches" 2>/dev/null || true
        sleep 10
        
        # Check again
        mem_check=$(check_memory)
        pct=$(echo "$mem_check" | cut -d' ' -f1)
        if [ "$pct" -ge 90 ]; then
            log_error "CRITICAL: Memory at ${pct}%, aborting to prevent OOM!"
            return 1
        fi
        log_info "Memory now at ${pct}%, continuing..."
    fi
    return 0
}

log_memory_status() {
    local mem_check=$(check_memory)
    local pct=$(echo "$mem_check" | cut -d' ' -f1)
    local used=$(echo "$mem_check" | cut -d' ' -f2)
    local available=$(echo "$mem_check" | cut -d' ' -f4)
    log_info "Memory: ${pct}% used (${used}MB), ${available}MB available"
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
    local use_fdp="${1:-false}"
    local fdp_streams=1  # Default: single stream (all data mixed = high WAF baseline)
    
    if [ "$use_fdp" = "true" ]; then
        fdp_streams=8    # FDP mode: 8 streams (data separated = low WAF)
        log_info "Mounting F2FS with FDP (8 streams - data separation enabled)..."
    else
        log_info "Mounting F2FS WITHOUT FDP (1 stream - all data mixed)..."
    fi
    
    mkdir -p "$MOUNT_POINT"
    sudo "$FDP_STATS" "$NVME_DEV_PATH" --reset >/dev/null 2>&1 || true
    sudo "$FDP_TOOLS_DIR"/mkfs/mkfs.f2fs -f -O lost_found "$NVME_DEV_PATH"
    sudo "$FDP_TOOLS_DIR/fdp_f2fs_mount" "$fdp_streams"
    sudo chmod -R 777 "$MOUNT_POINT"
    
    # CRITICAL: Force-clean all PID directories to prevent WAL corruption on restart
    # Using sudo rm -rf to ensure all files (including root-owned) are removed
    for pid in 0 1 2 3 4 5 6 7; do
        sudo rm -rf "$MOUNT_POINT/p${pid}"/* 2>/dev/null || true
        sudo rm -rf "$MOUNT_POINT/p${pid}"/.[!.]* 2>/dev/null || true  # Hidden files too
    done
    sync
    sleep 1
    
    log_success "F2FS mounted at $MOUNT_POINT (streams=$fdp_streams)"
}

start_sui_node() {
    local use_fdp="${1:-false}"
    # Config goes in p7 (metadata PID) for proper FDP placement
    local config_dir="$MOUNT_POINT/p7/sui_config"
    
    log_info "Starting SUI node (FDP=$use_fdp)..."
    
    # ═══════════════════════════════════════════════════════════════════════════
    # CLEANUP: Remove any existing SUI DB directories to avoid WAL corruption
    # Prefill writes to isolated directories (prefill_sui/batch_N), but previous
    # failed runs may have left DB state that conflicts with new genesis.
    # ═══════════════════════════════════════════════════════════════════════════
    log_info "Cleaning up previous SUI data..."
    rm -rf "$config_dir" 2>/dev/null || true
    if [ "$use_fdp" = "true" ]; then
        # Clean FDP directories (but not prefill data)
        rm -rf "$MOUNT_POINT/p0"/*.log "$MOUNT_POINT/p0"/WAL* 2>/dev/null || true
        rm -rf "$MOUNT_POINT/p1/authority_db"* 2>/dev/null || true
        rm -rf "$MOUNT_POINT/p2/authority_db"* 2>/dev/null || true
        rm -rf "$MOUNT_POINT/p3/consensus_db"* 2>/dev/null || true
        rm -rf "$MOUNT_POINT/p4/consensus_db"* 2>/dev/null || true
        rm -rf "$MOUNT_POINT/p5/fullnode_db"* 2>/dev/null || true
        rm -rf "$MOUNT_POINT/p6/fullnode_db"* 2>/dev/null || true
    fi
    
    # Disable SUI_DISABLE_GAS for now - it causes execution issues
    # The faucet provides enough gas for benchmarking
    unset SUI_DISABLE_GAS
    
    # ═══════════════════════════════════════════════════════════════════════════
    # AGGRESSIVE ROCKSDB COMPACTION SETTINGS
    # These settings force more frequent flushes and compactions to stress the SSD
    # and trigger garbage collection in FEMU
    # ═══════════════════════════════════════════════════════════════════════════
    # ═══════════════════════════════════════════════════════════════════════════
    # CRITICAL: SST files SMALLER than F2FS segment (2MB) = fragmented invalidation
    # When multiple SST files share a segment, deleting one leaves others valid
    # GC must COPY valid blocks from surviving files → page copies → WAF > 1.0
    # ═══════════════════════════════════════════════════════════════════════════
    # MEMORY-SAFE ROCKSDB SETTINGS (for 16GB RAM)
    export MAX_WRITE_BUFFER_SIZE_MB=1           # 1MB write buffer → 1MB L0 files (< 2MB segment)
    export MAX_WRITE_BUFFER_NUMBER=2            # 2 buffers (was 4) - reduce memory
    export L0_NUM_FILES_COMPACTION_TRIGGER=4    # Trigger compaction at 4 L0 files
    export TARGET_FILE_SIZE_BASE_MB=1           # 1MB SST files (< 2MB segment)
    export ROCKSDB_BLOCK_CACHE_SIZE_MB=256      # Limit block cache to 256MB
    # Result: ~2 SST files per F2FS segment
    # When compaction deletes one SST, the other in same segment stays valid
    # F2FS GC must copy valid blocks to free the segment!
    export DB_PARALLELISM="${DB_PARALLELISM:-4}" # Use 4 cores (half) for memory safety
    
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
    
    # Trigger FDP stats read to update FEMU log
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
    local use_fdp="${3:-false}"
    
    log_section "NFT BENCHMARK: $mode"
    
    local start_time=$(date +%s)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # STORE PATH: Where sui-single-node-benchmark writes RocksDB data
    # - Non-FDP: All data in single directory (p7)
    # - FDP: Set env vars so it uses FDP-separated directories
    # ═══════════════════════════════════════════════════════════════════════════
    local store_path
    if [ "$use_fdp" = "true" ]; then
        # FDP mode: use p1 as base, env vars will redirect to appropriate PIDs
        store_path="$MOUNT_POINT/p1/nft_bench"
        export SUI_FDP_WAL_SEMANTIC=1
        export SUI_FDP_BASE_PATH="$MOUNT_POINT"
        export SUI_FDP_HOT_SIZE_MB=64
        log_info "FDP mode: WAL→p0, SST hot→p1-3-5, SST cold→p2-4-6"
    else
        # Non-FDP: all data to single directory
        store_path="$MOUNT_POINT/p7/nft_bench"
        unset SUI_FDP_ENABLED SUI_FDP_BASE_PATH SUI_FDP_MODE SUI_FDP_SEMANTIC SUI_FDP_WAL_SEMANTIC
        log_info "Non-FDP mode: all data to $store_path"
    fi
    mkdir -p "$store_path"
    
    # UNIFIED WORKLOAD: Every TX does BOTH minting AND counter updates
    local duration_min="${BENCH_DURATION_MIN:-30}"
    local duration_sec=$((duration_min * 60))
    
    local tx_count="${TX_COUNT:-5000}"         # TXs per batch
    local num_mints="${NUM_MINTS:-5}"          # NFTs per TX
    local nft_size="${NFT_SIZE:-4000}"         # Bytes per NFT
    local num_counters="${NUM_COUNTERS:-20}"   # Counter updates per TX
    
    # Estimates per batch
    local data_per_batch_mb=$((tx_count * num_mints * nft_size / 1024 / 1024))
    local updates_per_batch=$((tx_count * num_counters))
    
    log_info "UNIFIED WORKLOAD Configuration:"
    log_info "  Store path:    $store_path"
    log_info "  Duration:      ${duration_min} min"
    log_info "  Per batch:     ${tx_count} TXs"
    log_info "  Per TX:        ${num_mints} NFTs × ${nft_size}B + ${num_counters} counter updates"
    log_info "  Batch totals:  ~${data_per_batch_mb}MB new data + ${updates_per_batch} version updates"
    log_info ""
    log_info "  HOW THIS WORKS:"
    log_info "    - NFT mints fill disk rapidly (${data_per_batch_mb}MB/batch)"
    log_info "    - Counter updates create compaction pressure (${updates_per_batch}/batch)"
    log_info "    - Combined: disk fills + LBA reuse from compaction → GC trigger"
    log_memory_status
    
    # Record initial disk stats
    local initial_sectors=$(get_device_sectors)
    log_info "Initial device sectors: $initial_sectors"
    
    # Record benchmark info
    {
        echo "benchmark_mode=$mode"
        echo "strategy=unified_workload"
        echo "start_time=$(date -Iseconds)"
        echo "start_sectors=$initial_sectors"
        echo "store_path=$store_path"
        echo "duration_min=$duration_min"
        echo "tx_count=$tx_count"
        echo "num_mints=$num_mints"
        echo "nft_size=$nft_size"
        echo "num_counters=$num_counters"
        echo "use_fdp=$use_fdp"
    } > "$run_dir/benchmark_info.txt"
    
    # ═══════════════════════════════════════════════════════════════════════════
    # UNIFIED WORKLOAD: NFT mints + counter updates in every batch
    # ═══════════════════════════════════════════════════════════════════════════
    log_info ""
    log_info "═══════════════════════════════════════════════════════════════"
    log_info "  UNIFIED WORKLOAD: Running for ${duration_min} minutes"
    log_info "═══════════════════════════════════════════════════════════════"
    
    local bench_start=$(date +%s)
    local bench_deadline=$((bench_start + duration_sec))
    local gc_before=$(cat /sys/fs/f2fs/$NVME_DEVICE/gc_foreground_calls 2>/dev/null || echo 0)
    local batch_num=1
    local total_data_mb=0
    
    while [ $(date +%s) -lt $bench_deadline ]; do
        local current_pct=$(df "$MOUNT_POINT" | awk 'NR==2{print int($5)}')
        local gc_now=$(cat /sys/fs/f2fs/$NVME_DEVICE/gc_foreground_calls 2>/dev/null || echo 0)
        local gc_delta=$((gc_now - gc_before))
        local elapsed=$(($(date +%s) - bench_start))
        local remaining=$((bench_deadline - $(date +%s)))
        local mem_pct=$(check_memory | cut -d' ' -f1)
        
        log_info "Batch $batch_num: disk ${current_pct}%, mem ${mem_pct}%, GC +${gc_delta}, ${remaining}s remaining"
        
        # Memory safety check - CRITICAL
        if ! wait_for_memory; then
            log_warning "Memory pressure - forcing cleanup..."
            sync
            sudo sh -c "echo 3 > /proc/sys/vm/drop_caches" 2>/dev/null || true
            sleep 3
            if ! wait_for_memory; then
                log_error "Memory still critical after cleanup - aborting batch"
                sleep 5
                continue
            fi
        fi
        
        # Append flag for all batches after first
        local append_flag=""
        if [ $batch_num -gt 1 ]; then
            append_flag="--append"
        fi
        
        # UNIFIED: Both NFT mints AND counter updates in EVERY transaction
        "$SUI_SINGLE_NODE_BENCH" \
            --tx-count "$tx_count" \
            --component baseline \
            --store-path "$store_path" \
            $append_flag \
            ptb --num-mints "$num_mints" --nft-size "$nft_size" --num-shared-objects "$num_counters" \
            2>&1 | tee -a "$run_dir/workload.log" | grep -E "TPS=|Execution|error" | tail -2
        
        batch_num=$((batch_num + 1))
        total_data_mb=$((total_data_mb + data_per_batch_mb))
        
        # MEMORY CLEANUP after every batch - CRITICAL for stability
        sync
        sudo sh -c "echo 3 > /proc/sys/vm/drop_caches" 2>/dev/null || true
        sleep 1
    done
    
    local bench_elapsed=$(($(date +%s) - bench_start))
    local gc_after=$(cat /sys/fs/f2fs/$NVME_DEVICE/gc_foreground_calls 2>/dev/null || echo 0)
    local total_gc=$((gc_after - gc_before))
    local final_pct=$(df "$MOUNT_POINT" | awk 'NR==2{print int($5)}')
    
    log_success "Benchmark complete: ${bench_elapsed}s, $((batch_num-1)) batches, ~${total_data_mb}MB written"
    log_info "Final disk: ${final_pct}%, GC calls: +$total_gc"
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Collect final stats
    # ═══════════════════════════════════════════════════════════════════════════
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
    log_section "NON-FDP NFT BENCHMARK (single-stream = data mixing)"
    local run_dir="$RESULTS_DIR/nfdp"
    mkdir -p "$run_dir"
    
    stop_sui_node
    unmount_f2fs
    mount_f2fs "false"    # Single stream - all data mixed = HIGH WAF baseline
    
    # NO prefill needed - run_benchmark handles disk fill via NFT minting
    # NO SUI node needed - sui-single-node-benchmark runs self-contained
    
    run_benchmark "nofdp" "$run_dir" "false"
    
    unmount_f2fs
}

run_fdp_enabled_benchmark() {
    log_section "FDP-ENABLED NFT BENCHMARK (8 streams = data separation)"
    local run_dir="$RESULTS_DIR/fdp"
    mkdir -p "$run_dir"
    
    stop_sui_node
    unmount_f2fs
    mount_f2fs "true"     # 8 streams - data separated = LOW WAF with FDP
    
    # NO prefill needed - run_benchmark handles disk fill via NFT minting
    # NO SUI node needed - sui-single-node-benchmark runs self-contained
    
    run_benchmark "fdp" "$run_dir" "true"
    
    unmount_f2fs
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

main() {
    mkdir -p "$RESULTS_DIR" "$LOG_DIR"
    
    log_section "FDP NFT Benchmark for SUI Blockchain"
    log_info ""
    log_info "╔═══════════════════════════════════════════════════════════════════════╗"
    log_info "║  UPDATE-HEAVY BENCHMARK: Realistic steady-state workload              ║"
    log_info "╠═══════════════════════════════════════════════════════════════════════╣"
    log_info "║  Real blockchains: Create objects ONCE, UPDATE them MILLIONS of times ║"
    log_info "║  - SETUP:   Short phase to create initial state + counters            ║"
    log_info "║  - UPDATE:  Long phase hammering counter updates (realistic workload) ║"
    log_info "║  - CLEANUP: Let GC catch up with light updates                        ║"
    log_info "╚═══════════════════════════════════════════════════════════════════════╝"
    log_info ""
    
    # UNIFIED WORKLOAD parameters (use global vars set based on WORKLOAD_MODE)
    local duration_min="${BENCH_DURATION_MIN:-30}"
    local tx_count="${TX_COUNT:-5000}"
    local num_mints="${NUM_MINTS}"
    local nft_size="${NFT_SIZE}"
    local num_counters="${NUM_COUNTERS}"
    local data_per_batch_mb=$((tx_count * num_mints * nft_size / 1024 / 1024))
    local updates_per_batch=$((tx_count * num_counters))
    local workload_mode="${WORKLOAD_MODE:-balanced}"
    
    log_info "UNIFIED WORKLOAD Configuration (mode: $workload_mode):"
    log_info "  Duration:        ${duration_min} min per mode (like Solana --duration)"
    log_info "  Per batch:       ${tx_count} TXs (like Solana --tx-count)"
    log_info "  Per TX:          ${num_mints} NFTs × ${nft_size}B + ${num_counters} counter updates"
    log_info "  Batch totals:    ~${data_per_batch_mb}MB new data + ${updates_per_batch} version updates"
    log_info "  Memory limit:    ${MAX_MEMORY_PCT:-60}% (auto-pause + drop_caches between batches)"
    log_info ""
    log_info "╔═══════════════════════════════════════════════════════════════════════╗"
    log_info "║  COMPARISON TO SOLANA BENCH-TPS:                                      ║"
    log_info "║  ─────────────────────────────────────────────────────────────────────║"
    log_info "║  Solana bench-tps:        SUI equivalent:                             ║"
    log_info "║  • Transfer 1 lamport  →  • Counter update (state change)             ║"
    log_info "║  • Fund keypairs       →  • NFT mint (create objects)                 ║"
    log_info "║  • --tx-count          →  • TX_COUNT=$tx_count                          ║"
    log_info "║  • --duration          →  • BENCH_DURATION_MIN=$duration_min                     ║"
    log_info "║  • --sustained         →  • Continuous batch loop                     ║"
    log_info "║  ─────────────────────────────────────────────────────────────────────║"
    log_info "║  Counter updates = state churn (like Solana transfers)                ║"
    log_info "║  NFT mints = bulk data (Solana uses tiny txs, we need disk I/O)       ║"
    log_info "╚═══════════════════════════════════════════════════════════════════════╝"
    log_info ""
    log_info "╔═══════════════════════════════════════════════════════════════════════╗"
    log_info "║  FDP Comparison Strategy:                                             ║"
    log_info "║  • Non-FDP: F2FS with fdp_log_n=1 (single stream, all data mixed)    ║"
    log_info "║  • FDP:     F2FS with fdp_log_n=8 (8 streams, data separated)        ║"
    log_info "║  Expected: Non-FDP shows HIGH WAF, FDP shows LOW WAF (~1.0)          ║"
    log_info "╚═══════════════════════════════════════════════════════════════════════╝"
    log_info ""
    
    # Verify sui-single-node-benchmark exists
    if [ ! -x "$SUI_SINGLE_NODE_BENCH" ]; then
        log_error "sui-single-node-benchmark not found at $SUI_SINGLE_NODE_BENCH"
        log_error "Build it: cd /home/femu/sui && cargo build --release -p sui-single-node-benchmark"
        exit 1
    fi
    log_success "Using sui-single-node-benchmark: $SUI_SINGLE_NODE_BENCH"
    
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
