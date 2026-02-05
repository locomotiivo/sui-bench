#!/bin/bash
#
# High I/O Churn FDP Benchmark for SUI Blockchain
#
# Strategy: Maximize small I/O operations to stress RocksDB compaction
# and generate high Write Amplification Factor (WAF).
#
# Key differences from large-blob approach:
# - Many small objects (~100 bytes each) instead of few large blobs
# - High transaction throughput (many TPS)
# - Frequent object updates (key overwrites)
# - Data is unique per update (no compression)
#
# This compares FDP-enabled vs FDP-disabled benchmarks.
#   - PID 0: Account state (objects, locks) - high churn, mutable
#   - PID 1: Ledger data (transactions, effects, consensus) - append-only
#
# IMPORTANT: FEMU Stats Architecture
# ==================================
# FEMU logs ([FEMU] FTL-Log:) are printed by the QEMU/FEMU emulator on the HOST,
# NOT inside the guest VM. The guest's journalctl/dmesg cannot see these logs.
#
# WAF Formula (from FEMU):
#   WAF = 1 + (GC copied pages) / (Host written pages)
#

set -e

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR="/home/femu/fdp-scripts/sui-bench/scripts"
FDP_TOOLS_DIR="/home/femu/fdp-scripts/f2fs-tools-fdp"
FDP_STATS="$FDP_TOOLS_DIR/fdp_stats"
MOUNT_POINT="/home/femu/f2fs_fdp_mount"
NVME_DEVICE="nvme0n1"
NVME_DEV_PATH="/dev/$NVME_DEVICE"
MOVE_DIR="$SCRIPT_DIR/../move/io_churn"

# Benchmark parameters
# NOTE: Using PTB (Programmable Transaction Blocks) with FIRE-AND-FORGET submission
# Each PTB batches multiple increment_simple calls into a single transaction
#
# FIRE_AND_FORGET mode: Submit transactions without waiting for finality
# This is critical for high throughput - waiting for finality limits to ~1 TPS
#
# If PTB fails, fallback to batch_increment_8 Move function
#
# BLOB_MODE: Use LargeBlob (4KB) objects instead of MicroCounter (100 bytes)
# This is critical for achieving high disk throughput (500MB/s+)
# MicroCounter: 307,200 updates = ~3MB data (metadata overhead dominates)
# LargeBlob:    1,000 updates  = ~4MB data (actual 4KB payload per object)
#
# ═══════════════════════════════════════════════════════════════════════════════
# WORKLOAD MODES (Academic Justification for FDP WAF Comparison)
# ═══════════════════════════════════════════════════════════════════════════════
#
# GOAL: Maximize GC cycles to expose FDP's data segregation benefits.
# KEY INSIGHT: FDP reduces WAF by separating hot (frequently updated) from
#              cold (write-once) data at the flash block level.
#
# WAF Formula: WAF = 1 + (GC copied pages) / (Host written pages)
#
# Without FDP: Hot and cold data mixed in same blocks
#   → When GC reclaims a block with hot data, it must copy cold valid pages
#   → High copy cost → High WAF
#
# With FDP: Hot data in PID0 blocks, cold data in PID1 blocks
#   → Hot blocks have high invalidity (data already overwritten) → low copy cost
#   → Cold blocks rarely need GC (not being overwritten)
#   → Low WAF
#
# 1. UPDATE_ONLY: Update existing objects repeatedly
#    - Pure hot workload - all data has same lifetime
#    - Minimal FDP benefit (no lifetime variance to segregate)
#    - Use as BASELINE to verify system works
#
# 2. APPEND_ONLY: Continuously create new objects, never update
#    - Pure cold workload - all data is write-once
#    - Minimal FDP benefit (no hot data to separate)
#    - Useful for space amplification testing
#
# 3. MIXED: Create (cold) + Update (hot) - NO READS
#    - Academic basis: Rosenblum & Ousterhout, "LFS" (1992)
#      "Segment cleaning cost depends on fraction of live data"
#    - Hot updates → high invalidity in hot blocks
#    - Cold creates → stable data in cold blocks  
#    - FDP segregates these → dramatic WAF reduction
#    - THIS IS THE PRIMARY MODE FOR FDP COMPARISON
#
# 4. APPEND_DELETE: Create then delete (tombstone generation)
#    - Creates space amplification pressure
#    - Tombstones are "cold" until compaction removes them
#
WORKERS="${WORKERS:-4}"                           # Increased: more parallelism
DURATION="${DURATION:-7200}"
OBJECTS_PER_WORKER="${OBJECTS_PER_WORKER:-2000}"  # Large pool reduces contention
PTB_BATCH_SIZE="${PTB_BATCH_SIZE:-50}"            # Moderate batch for balance
PARALLEL_PTBS="${PARALLEL_PTBS:-20}"              # More parallel submissions
FIRE_AND_FORGET="${FIRE_AND_FORGET:-yes}"         # Don't wait for finality (critical!)
MAX_INFLIGHT="${MAX_INFLIGHT:-100}"               # Allow more background jobs
USE_PTB="${USE_PTB:-auto}"                        # "yes", "no", or "auto" (detect)
STATS_INTERVAL="${STATS_INTERVAL:-1800}"

# WORKLOAD MODE: Controls object lifecycle pattern
# Options: "update_only", "append_only", "mixed", "append_delete"
WORKLOAD_MODE="${WORKLOAD_MODE:-mixed}"

# Mixed workload ratios (must sum to 100) - NO READS, only writes matter for GC/WAF
# Academic basis: Hot/cold data ratio determines GC efficiency
# - Higher CREATE_PCT = more cold data = tests FDP's cold block isolation
# - Higher UPDATE_PCT = more hot data = tests FDP's hot block invalidity rate
MIXED_CREATE_PCT="${MIXED_CREATE_PCT:-30}"        # % of ops that create new objects (COLD)
MIXED_UPDATE_PCT="${MIXED_UPDATE_PCT:-70}"        # % of ops that update existing (HOT)

# BLOB MODE: For high disk throughput - uses 4KB LargeBlob objects
# Set BLOB_MODE=yes for disk I/O benchmarks, BLOB_MODE=no for TPS benchmarks
BLOB_MODE="${BLOB_MODE:-yes}"                      # "yes" = LargeBlob (4KB), "no" = MicroCounter
BLOB_BATCH_SIZE="${BLOB_BATCH_SIZE:-30}"           # Blobs per PTB (balance TPS vs I/O)
BLOBS_PER_WORKER="${BLOBS_PER_WORKER:-1500}"       # Larger pool = less contention

# Control flags
NOFDP="${NOFDP:-no}"
NONFDP="${NONFDP:-no}"

# HOST access for FEMU stats (FEMU logs go to HOST stdout, not guest journal)
HOST_IP="${HOST_IP:-10.0.2.2}"
HOST_USER="${HOST_USER:-hajin}"
HOST_FEMU_LOG="${HOST_FEMU_LOG:-/home/hajin/femu-scripts/run-fdp.log}"
USE_HOST_STATS="${USE_HOST_STATS:-yes}"

# Output
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/high_churn_results}"
BASE_RESULTS_DIR="$RESULTS_DIR"
LOG_DIR="$RESULTS_DIR/logs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

# Get disk stats: returns "read_sectors write_sectors" from /proc/diskstats
get_disk_stats() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $6, $10}' /proc/diskstats 2>/dev/null || echo "0 0"
}

get_device_sectors() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats 2>/dev/null || echo 0
}

# Format bytes to human readable
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

# Format rate (bytes/sec) to human readable
format_rate() {
    local bytes_per_sec=$1
    if [ "$bytes_per_sec" -ge 1048576 ]; then
        echo "$(echo "scale=2; $bytes_per_sec / 1048576" | bc) MB/s"
    elif [ "$bytes_per_sec" -ge 1024 ]; then
        echo "$(echo "scale=2; $bytes_per_sec / 1024" | bc) KB/s"
    else
        echo "$bytes_per_sec B/s"
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
    set +e
    log_info "═══════════════════════════════════════════════════════════════"
    log_info "  BENCHMARK CLEANUP"
    log_info "═══════════════════════════════════════════════════════════════"
    
    log_info "Step 1: Stopping benchmark workers..."
    # Kill other high-churn-benchmark processes, but NOT ourselves
    pgrep -f "high-churn-benchmark" | grep -v "^$$\$" | xargs -r kill -9 2>/dev/null || true
    pkill -9 -f "sui client call" 2>/dev/null || true
    pkill -9 -f "tee.*benchmark.log" 2>/dev/null || true
    sleep 1
    
    log_info "Step 2: Stopping SUI node..."
    stop_sui_node
    
    log_info "Step 3: Stopping stats collector..."
    stop_periodic_stats_collector
    
    log_info "Step 4: Releasing mount point..."
    if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
        local pids=$(timeout 10 sudo lsof +D "$MOUNT_POINT" 2>/dev/null | awk 'NR>1 {print $2}' | sort -u || true)
        if [ -n "$pids" ]; then
            log_info "  Killing processes: $pids"
            echo "$pids" | xargs -r sudo kill -9 2>/dev/null || true
            sleep 1
        fi
        timeout 5 sudo fuser -km "$MOUNT_POINT" 2>/dev/null || true
        sleep 1
    fi
    
    log_info "Step 5: Syncing filesystem..."
    sync
    sleep 2
    
    log_info "Step 6: Unmounting F2FS..."
    if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
        if ! timeout 30 sudo umount "$MOUNT_POINT" 2>/dev/null; then
            log_warning "  Normal unmount failed, trying lazy unmount..."
            sudo umount -l "$MOUNT_POINT" 2>/dev/null || true
        fi
        sleep 1
    fi
    
    log_info "Step 7: Releasing device..."
    sudo fuser -k "${NVME_DEV_PATH}" 2>/dev/null || true
    sync
    
    log_success "Cleanup complete"
    set -e
    return 0
}

unmount_f2fs() {
    log_info "Unmounting F2FS..."
    cleanup_benchmark
}

mount_f2fs() {
    local nlogs="${1:-8}"
    log_info "Mounting F2FS with FDP (nlogs=$nlogs)..."
    
    mkdir -p "$MOUNT_POINT"
    sudo "$FDP_STATS" "$NVME_DEV_PATH" --reset >/dev/null 2>&1 || true
    sudo "$FDP_TOOLS_DIR"/mkfs/mkfs.f2fs -f -O lost_found "$NVME_DEV_PATH"
    sudo "$FDP_TOOLS_DIR/fdp_f2fs_mount" "$nlogs"
    sudo chmod -R 777 "$MOUNT_POINT"
    
    log_info "Cleaning FDP directories for fresh start..."
    rm -rf "$MOUNT_POINT/account_state" 2>/dev/null || true
    rm -rf "$MOUNT_POINT/ledger" 2>/dev/null || true
    
    log_success "F2FS mounted at $MOUNT_POINT"
}

setup_fdp_directories() {
    local config_dir="${1:-$MOUNT_POINT/p0/sui_node}"
    log_info "Setting up FDP directories..."
    "$SCRIPT_DIR/fdp_semantic_mount.sh" "$MOUNT_POINT"
}

start_sui_node() {
    local config_dir="${1:-$MOUNT_POINT/p0/sui_node}"
    local use_fdp="${2:-false}"
    
    log_info "Starting SUI node (FDP=$use_fdp)..."
    
    export SUI_DISABLE_GAS=1
    export SUI_ROCKSDB_BENCHMARK=1
    
    if [ "$use_fdp" = "true" ]; then
        export SUI_FDP_SEMANTIC=1
        export SUI_FDP_BASE_PATH="$MOUNT_POINT"
        unset SUI_FDP_ENABLED SUI_FDP_MODE
        log_info "FDP env: SEMANTIC mode (account_state=PID0, ledger=PID1)"
    else
        unset SUI_FDP_ENABLED SUI_FDP_BASE_PATH SUI_FDP_MODE SUI_FDP_SEMANTIC
    fi
    
    log_info "RocksDB benchmark mode: ENABLED (aggressive compaction)"
    
    mkdir -p "$config_dir"
    sui genesis -f --working-dir "$config_dir"
    
    if [ "$use_fdp" = "true" ]; then
        setup_fdp_directories "$config_dir"
    fi
    
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
    
    set +e
    # Request gas from faucet
    local address=$(SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui client active-address)
    log_info "Active address: $address"
    
    local faucet_response=$(curl -s --location --request POST 'http://127.0.0.1:9123/gas' \
      --header 'Content-Type: application/json' \
      --data-raw "{
        \"FixedAmountRequest\": {
          \"recipient\": \"$address\"
        }
      }")
    
    if echo "$faucet_response" | jq -e '.' >/dev/null 2>&1; then
        local gas_amount=$(echo "$faucet_response" | jq -r '.transferred_gas_objects[0].amount // "unknown"')
        log_info "Faucet response: received $gas_amount gas"
    else
        log_warning "Faucet response not valid JSON: $faucet_response"
    fi
    sleep 1
    
    # Publish contract using test-publish (ephemeral publication)
    cd "$MOVE_DIR"
    
    # Clear any cached publication files
    rm -f Pub.localnet.toml Pub.testnet.toml 2>/dev/null || true
    
    log_info "Running: sui client test-publish --build-env localnet --json"
    local raw_output=$(SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui client test-publish --build-env localnet --json 2>&1)
    local exit_code=$?

    log_info "Publish exit code: $exit_code"
    
    # Extract JSON from output
    local result=$(echo "$raw_output" | sed -n '/^{/,$p')
    
    if ! echo "$result" | jq -e '.' >/dev/null 2>&1; then
        log_error "Could not extract valid JSON from publish output"
        log_error "Raw output (first 500 chars): ${raw_output:0:500}"
        set -e
        return 1
    fi
    
    log_info "JSON extracted successfully"
    
    if [ $exit_code -eq 0 ] && echo "$result" | jq -e '.effects.V2.status == "Success"' >/dev/null 2>&1; then
        local package_id=$(echo "$result" | jq -r '.changed_objects[] | select(.objectType == "package") | .objectId')
        set -e
        
        if [ -n "$package_id" ] && [ "$package_id" != "null" ]; then
            echo "$package_id" > "$config_dir/.package_id"
            log_info "Contract published: $package_id"
            echo "$package_id"
            return 0
        fi
    fi
    
    log_error "Failed to publish contract (exit code: $exit_code)"
    log_error "Status from JSON: $(echo "$result" | jq -r '.effects.V2.status // "unknown"')"
    set -e
    return 1
}

smash_gas_coins() {
    local config_dir="${1:-$MOUNT_POINT/p0/sui_node}"
    local target_coins="${2:-$WORKERS}"
    
    export SUI_CONFIG_DIR="$config_dir"
    
    log_info "Checking gas coin count for $target_coins workers..."
    
    local current_coins=$(sui client gas --json 2>/dev/null | jq 'length' 2>/dev/null || echo "0")
    log_info "Current gas coins: $current_coins"
    
    if [ "$current_coins" -ge "$target_coins" ]; then
        log_info "Sufficient gas coins available"
        return 0
    fi
    
    local needed=$((target_coins - current_coins + 10))
    log_info "Need to create $needed more gas coins..."
    
    local gas_info=$(sui client gas --json 2>/dev/null)
    local largest_coin=$(echo "$gas_info" | jq -r 'sort_by(.mistBalance | tonumber) | last | .gasCoinId' 2>/dev/null)
    local balance=$(echo "$gas_info" | jq -r 'sort_by(.mistBalance | tonumber) | last | .mistBalance' 2>/dev/null)
    
    if [ -z "$largest_coin" ] || [ "$largest_coin" = "null" ]; then
        log_warning "Could not find gas coin to split"
        return 1
    fi
    
    log_info "Splitting coin $largest_coin (balance: $balance MIST)..."
    
    local balance_numeric=$(echo "$balance" | awk '{printf "%.0f", $1}')
    local amount_per_coin=$(echo "$balance_numeric * 9 / 10 / $needed" | bc 2>/dev/null || echo "0")
    
    if [ "$amount_per_coin" -lt 100000000 ] 2>/dev/null; then
        log_warning "Insufficient balance, requesting from faucet..."
        
        local address=$(sui client active-address)
        for i in $(seq 1 5); do
            curl -s --location --request POST 'http://127.0.0.1:9123/gas' \
              --header 'Content-Type: application/json' \
              --data-raw "{\"FixedAmountRequest\": {\"recipient\": \"$address\"}}" >/dev/null 2>&1
            sleep 1
        done
        
        gas_info=$(sui client gas --json 2>/dev/null)
        largest_coin=$(echo "$gas_info" | jq -r 'sort_by(.mistBalance | tonumber) | last | .gasCoinId' 2>/dev/null)
        balance=$(echo "$gas_info" | jq -r 'sort_by(.mistBalance | tonumber) | last | .mistBalance' 2>/dev/null)
        balance_numeric=$(echo "$balance" | awk '{printf "%.0f", $1}')
        amount_per_coin=$(echo "$balance_numeric * 9 / 10 / $needed" | bc 2>/dev/null || echo "0")
    fi
    
    local sui_per_coin=$(echo "$amount_per_coin / 1000000000" | bc 2>/dev/null || echo "?")
    log_info "Creating $needed coins with ~$sui_per_coin SUI each..."
    
    local amounts=""
    for i in $(seq 1 $needed); do
        amounts="${amounts}${amount_per_coin} "
    done
    
    local split_result=$(sui client split-coin \
        --coin-id "$largest_coin" \
        --amounts $amounts \
        --gas-budget 500000000 \
        --json 2>/dev/null)
    
    if echo "$split_result" | grep -q '"status"'; then
        local final_count=$(sui client gas --json 2>/dev/null | jq 'length' 2>/dev/null || echo "0")
        log_success "Gas coins split successfully. Total coins: $final_count"
        return 0
    else
        log_warning "Split-coin failed, will proceed with fewer workers"
        return 1
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# FEMU STATS
# ═══════════════════════════════════════════════════════════════════════════════

read_femu_stats_from_host() {
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
        log_info "  FEMU stats from host: written=$sum_written, copied=$copied, waf=$waf"
    else
        log_warning "  Could not fetch FEMU log from host"
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

reset_femu_stats() {
    log_info "Resetting FEMU stats (fdp_stats --reset)..."
    if [ -x "$FDP_STATS" ]; then
        sudo "$FDP_STATS" "$NVME_DEV_PATH" --reset >/dev/null 2>&1 || true
    fi
    
    if [ "$USE_HOST_STATS" = "yes" ]; then
        read_femu_stats_from_host
    else
        echo "0 0 N/A"
    fi
}

read_femu_stats() {
    if [ "$USE_HOST_STATS" = "yes" ]; then
        read_femu_stats_from_host
    else
        echo "0 0 N/A"
    fi
}

STATS_COLLECTOR_PID=""
start_periodic_stats_collector() {
    local run_dir="$1"
    local snapshots_dir="$run_dir/stats_snapshots"
    local csv_file="$run_dir/waf_timeseries.csv"
    local interval="${STATS_INTERVAL}"
    
    mkdir -p "$snapshots_dir"
    
    log_info "Starting periodic stats collector (interval=${interval}s)"
    
    echo "timestamp,elapsed_sec,host_sectors,sum_written,copied,waf" > "$csv_file"
    
    local start_time=$(date +%s)
    local snapshot_num=0
    
    (
        # Clear inherited traps in subshell to prevent interference with main script
        trap - INT TERM EXIT
        set +e  # Disable errexit in background collector
        
        while true; do
            sleep "$interval"
            
            snapshot_num=$((snapshot_num + 1))
            local now=$(date +%s)
            local elapsed=$((now - start_time))
            local timestamp=$(date +"%Y%m%d_%H%M%S")
            local timestamp_iso=$(date -Iseconds)
            
            local host_sectors=$(get_device_sectors)
            local femu_stats=$(read_femu_stats)
            local sum_written=$(echo "$femu_stats" | awk '{print $1}')
            local copied=$(echo "$femu_stats" | awk '{print $2}')
            local waf=$(echo "$femu_stats" | awk '{print $3}')
            
            local snapshot_file="$snapshots_dir/snapshot_${snapshot_num}_${timestamp}.txt"
            {
                echo "Snapshot #$snapshot_num"
                echo "Timestamp: $timestamp_iso"
                echo "Elapsed: ${elapsed}s"
                echo ""
                echo "Host Statistics (diskstats):"
                echo "  Sectors written: $host_sectors"
                echo ""
                echo "FEMU GC Statistics:"
                echo "  sum(wpp->written): $sum_written pages"
                echo "  copied (GC): $copied pages"
                echo "  WAF: $waf"
            } > "$snapshot_file"
            
            echo "$timestamp_iso,$elapsed,$host_sectors,$sum_written,$copied,$waf" >> "$csv_file"
            
            log_info "Snapshot #$snapshot_num @${elapsed}s: WAF=$waf, written=$sum_written, copied=$copied"
        done
    ) &
    STATS_COLLECTOR_PID=$!
    
    log_info "Stats collector started (PID=$STATS_COLLECTOR_PID)"
}

stop_periodic_stats_collector() {
    if [ -n "$STATS_COLLECTOR_PID" ]; then
        log_info "Stopping stats collector (PID=$STATS_COLLECTOR_PID)..."
        kill "$STATS_COLLECTOR_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$STATS_COLLECTOR_PID" 2>/dev/null || true
        STATS_COLLECTOR_PID=""
    fi
    pkill -9 -f "stats_snapshots" 2>/dev/null || true
}

get_final_femu_stats() {
    log_info "Collecting final FEMU stats..."
    local stats=$(read_femu_stats)
    local sum_written=$(echo "$stats" | awk '{print $1}')
    local copied=$(echo "$stats" | awk '{print $2}')
    local waf=$(echo "$stats" | awk '{print $3}')
    
    log_info "  Final: sum_written=$sum_written, copied=$copied, WAF=$waf"
    echo "$sum_written $copied $waf"
}

# ═══════════════════════════════════════════════════════════════════════════════
# OBJECT CREATION AND WORKER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

create_objects() {
    local config_dir="$1"
    local package_id="$2"
    local objects_file="$3"
    local total_objects=$((WORKERS * OBJECTS_PER_WORKER))
    
    export SUI_CONFIG_DIR="$config_dir"
    
    # Disable errexit for this function - sui commands can have non-zero exits
    set +e
    
    log_info "Creating $total_objects objects ($OBJECTS_PER_WORKER per worker)..."
    
    local created=0
    local batch_create_size=1600  # Larger batches for faster creation
    local max_retries=5
    
    > "$objects_file"
    
    log_info "Starting object creation loop (batch size: $batch_create_size)..."
    log_info "  Package ID: $package_id"
    log_info "  Config dir: $config_dir"
    
    while [ $created -lt $total_objects ]; do
        local remaining=$((total_objects - created))
        local batch=$((remaining < batch_create_size ? remaining : batch_create_size))
        
        log_info "  Requesting batch of $batch objects..."
        
        # Run sui client call with timeout, capture both stdout and stderr
        local output exit_code
        output=$(timeout 120 sui client call \
            --package "$package_id" \
            --module io_churn \
            --function create_batch \
            --args "$batch" \
            --gas-budget 500000000 \
            --json 2>&1)
        exit_code=$?
        
        log_info "  sui client call exit code: $exit_code"
        
        if [ $exit_code -ne 0 ]; then
            log_warning "  Command failed (exit $exit_code): ${output:0:300}"
            sleep 2
            continue
        fi
        
        # Extract created MicroCounter objects from changed_objects
        # Filter by idOperation="CREATED" and objectType containing "MicroCounter"
        local new_objects
        new_objects=$(echo "$output" | jq -r '.changed_objects[] | select(.idOperation == "CREATED") | select(.objectType | contains("MicroCounter")) | .objectId' 2>/dev/null)
        
        if [ -n "$new_objects" ]; then
            echo "$new_objects" >> "$objects_file"
            local count=$(echo "$new_objects" | wc -l)
            created=$((created + count))
            log_info "  Created $created / $total_objects objects"
        else
            log_warning "  Failed to create batch (output: ${output:0:200}), retrying..."
            sleep 1
        fi
    done
    
    set -e  # Re-enable errexit
    log_success "Object creation complete: $created objects"
}

# Create LargeBlob objects for high-throughput I/O benchmarks (4KB each)
create_blobs() {
    local config_dir="$1"
    local package_id="$2"
    local objects_file="$3"
    local total_blobs=$((WORKERS * BLOBS_PER_WORKER))
    
    export SUI_CONFIG_DIR="$config_dir"
    set +e
    
    log_info "Creating $total_blobs LargeBlob objects ($BLOBS_PER_WORKER per worker)..."
    log_info "  Each blob is 4KB - total data: $((total_blobs * 4))KB"
    
    local created=0
    local batch_create_size=50  # Smaller batches due to larger object size
    
    > "$objects_file"
    
    while [ $created -lt $total_blobs ]; do
        local remaining=$((total_blobs - created))
        local batch=$((remaining < batch_create_size ? remaining : batch_create_size))
        
        log_info "  Requesting batch of $batch blobs..."
        
        local output exit_code
        output=$(timeout 180 sui client call \
            --package "$package_id" \
            --module io_churn \
            --function create_blob_batch \
            --args "$batch" \
            --gas-budget 2000000000 \
            --json 2>&1)
        exit_code=$?
        
        if [ $exit_code -ne 0 ]; then
            log_warning "  Command failed (exit $exit_code): ${output:0:300}"
            sleep 2
            continue
        fi
        
        # Extract created LargeBlob objects
        local new_blobs
        new_blobs=$(echo "$output" | jq -r '.changed_objects[] | select(.idOperation == "CREATED") | select(.objectType | contains("LargeBlob")) | .objectId' 2>/dev/null)
        
        if [ -n "$new_blobs" ]; then
            echo "$new_blobs" >> "$objects_file"
            local count=$(echo "$new_blobs" | wc -l)
            created=$((created + count))
            log_info "  Created $created / $total_blobs blobs"
        else
            log_warning "  Failed to create batch (output: ${output:0:200}), retrying..."
            sleep 1
        fi
    done
    
    set -e
    log_success "Blob creation complete: $created blobs (~$((created * 4))KB)"
}

# ═══════════════════════════════════════════════════════════════════════════════
# PTB (PROGRAMMABLE TRANSACTION BLOCK) FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

# Check if PTB syntax works with current sui version
check_ptb_support() {
    # PTB syntax: sui client ptb --move-call pkg::mod::func @object
    # Test with a dry-run style check
    local test_output
    test_output=$(sui client ptb --help 2>&1 || echo "ERROR")
    
    if echo "$test_output" | grep -qiE "move-call|MoveCall"; then
        return 0  # PTB with --move-call is supported
    else
        return 1  # PTB not supported or different syntax
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# MIXED WORKLOAD PTB BUILDER
# ═══════════════════════════════════════════════════════════════════════════════
#
# Academic Justification for Mixed Workloads:
#
# 1. CREATE operations:
#    - Generate new SST entries at L0
#    - Data starts "cold" immediately (new keys never seen before)
#    - Forces compaction as L0 fills up
#    - Reference: LSM-tree design (O'Neil et al.)
#
# 2. UPDATE operations:
#    - Overwrite existing keys → old versions become garbage
#    - Creates version chains that compaction must resolve
#    - Hot keys stay in memtable; cold keys force deeper compaction
#    - Reference: RocksDB compaction triggers
#
# 3. READ operations:
#    - Force block cache misses for cold data → disk I/O
#    - Amplify read path: bloom filter → index block → data block
#    - Mixed with writes can trigger write stalls
#    - Reference: RocksDB read amplification analysis
#
# 4. DELETE operations (append_delete mode):
#    - Create tombstones that persist until compaction
#    - Tombstones consume space and slow reads
#    - Reference: "Dostoevsky" paper on space amplification
#

# Build a PTB command based on workload mode
# Returns: PTB arguments string
#
# For FDP WAF comparison, only WRITE operations matter:
# - CREATE = cold data (write-once, stored in PID1/ledger blocks)
# - UPDATE = hot data (frequently rewritten, stored in PID0/account blocks)
#
# FDP Benefit Mechanism:
# - Hot blocks: High invalidity rate → low GC copy cost
# - Cold blocks: Rarely need GC → isolated from hot churn
# - Result: Lower WAF compared to mixed hot/cold blocks in non-FDP
build_ptb_command() {
    local package_id="$1"
    local workload_mode="$2"
    shift 2
    local objects=("$@")
    
    local ptb_args=""
    local num_objects=${#objects[@]}
    
    case "$workload_mode" in
        update_only)
            # Pure HOT workload: repeatedly update same objects
            # All data has same lifetime → minimal FDP benefit
            # Use as baseline to verify system works
            if [ "$BLOB_MODE" = "yes" ]; then
                for obj_id in "${objects[@]}"; do
                    ptb_args+=" --move-call ${package_id}::io_churn::update_blob @${obj_id}"
                done
            else
                for obj_id in "${objects[@]}"; do
                    ptb_args+=" --move-call ${package_id}::io_churn::increment_simple @${obj_id}"
                done
            fi
            ;;
        
        append_only)
            # Pure COLD workload: continuously create new objects
            # All data is write-once → tests space amplification
            # Useful for verifying append-only segregation (PID1)
            local create_count=$num_objects
            if [ "$BLOB_MODE" = "yes" ]; then
                ptb_args+=" --move-call ${package_id}::io_churn::create_blob_batch ${create_count}"
            else
                ptb_args+=" --move-call ${package_id}::io_churn::create_batch ${create_count}"
            fi
            ;;
        
        mixed)
            # HOT + COLD workload: THE PRIMARY MODE FOR FDP COMPARISON
            #
            # Academic basis: Rosenblum & Ousterhout, "LFS" (1992)
            # "The cost of cleaning a segment depends on the fraction of 
            #  live data in the segment"
            #
            # CREATE operations → COLD data (write-once, goes to PID1)
            # UPDATE operations → HOT data (frequently rewritten, goes to PID0)
            #
            # With FDP: Hot and cold segregated → clean hot blocks cheaply
            # Without FDP: Mixed → must copy cold data when cleaning hot blocks
            #
            local create_count=$(( num_objects * MIXED_CREATE_PCT / 100 ))
            local update_count=$(( num_objects * MIXED_UPDATE_PCT / 100 ))
            
            # Ensure at least 1 of each if percentage > 0
            [ $MIXED_CREATE_PCT -gt 0 ] && [ $create_count -eq 0 ] && create_count=1
            [ $MIXED_UPDATE_PCT -gt 0 ] && [ $update_count -eq 0 ] && update_count=1
            
            # Adjust to use all objects (in case rounding leaves some out)
            if [ $((create_count + update_count)) -lt $num_objects ]; then
                update_count=$((num_objects - create_count))
            fi
            
            # CREATE operations → COLD data (append new, goes to ledger/PID1)
            if [ $create_count -gt 0 ]; then
                if [ "$BLOB_MODE" = "yes" ]; then
                    ptb_args+=" --move-call ${package_id}::io_churn::create_blob_batch ${create_count}"
                else
                    ptb_args+=" --move-call ${package_id}::io_churn::create_batch ${create_count}"
                fi
            fi
            
            # UPDATE operations → HOT data (overwrite existing, goes to account/PID0)
            local idx=0
            for ((i = 0; i < update_count && idx < num_objects; i++, idx++)); do
                local obj_id="${objects[$idx]}"
                if [ "$BLOB_MODE" = "yes" ]; then
                    ptb_args+=" --move-call ${package_id}::io_churn::update_blob @${obj_id}"
                else
                    ptb_args+=" --move-call ${package_id}::io_churn::increment_simple @${obj_id}"
                fi
            done
            ;;
        
        append_delete)
            # Create then delete - generates tombstones
            # Tombstones are metadata that must be compacted
            # Creates space amplification pressure
            local half=$((num_objects / 2))
            
            # Create batch (COLD - new data)
            if [ "$BLOB_MODE" = "yes" ]; then
                ptb_args+=" --move-call ${package_id}::io_churn::create_blob_batch ${half}"
            else
                ptb_args+=" --move-call ${package_id}::io_churn::create_batch ${half}"
            fi
            
            # Delete existing objects (generates tombstones)
            for ((i = 0; i < half && i < num_objects; i++)); do
                local obj_id="${objects[$i]}"
                if [ "$BLOB_MODE" = "yes" ]; then
                    ptb_args+=" --move-call ${package_id}::io_churn::delete_blob @${obj_id}"
                fi
                # Note: MicroCounter doesn't have delete, would need to add
            done
            ;;
        
        *)
            # Default to update_only
            if [ "$BLOB_MODE" = "yes" ]; then
                for obj_id in "${objects[@]}"; do
                    ptb_args+=" --move-call ${package_id}::io_churn::update_blob @${obj_id}"
                done
            else
                for obj_id in "${objects[@]}"; do
                    ptb_args+=" --move-call ${package_id}::io_churn::increment_simple @${obj_id}"
                done
            fi
            ;;
    esac
    
    echo "$ptb_args"
}

# Legacy function for backward compatibility
build_ptb_command_update_only() {
    local package_id="$1"
    shift
    local objects=("$@")
    build_ptb_command "$package_id" "update_only" "${objects[@]}"
}

# Execute a single PTB with multiple object updates
execute_ptb() {
    local package_id="$1"
    local workload_mode="$2"
    shift 2
    local objects=("$@")
    
    local ptb_args=$(build_ptb_command "$package_id" "$workload_mode" "${objects[@]}")
    
    # sui client ptb executes all --move-call commands in a single transaction
    sui client ptb $ptb_args --gas-budget 500000000 --json 2>&1
}

# Execute using batch_increment_8 Move function (fallback when PTB not available)
execute_batch8() {
    local package_id="$1"
    shift
    local objects=("$@")
    
    # Need exactly 8 objects
    if [ ${#objects[@]} -lt 8 ]; then
        echo "FAIL: need 8 objects"
        return 1
    fi
    
    sui client call \
        --package "$package_id" \
        --module io_churn \
        --function batch_increment_8 \
        --args "${objects[0]}" "${objects[1]}" "${objects[2]}" "${objects[3]}" \
               "${objects[4]}" "${objects[5]}" "${objects[6]}" "${objects[7]}" \
        --gas-budget 500000000 \
        --json 2>&1
}

# Submit multiple PTBs in parallel and wait for all to complete
# This implements "parallel transaction building and submission"
#
# FIRE_AND_FORGET mode: 
# - Submit transactions without waiting for JSON response
# - Use background processes (&) with output to /dev/null
# - Track only submission count, not success/failure
# - This achieves 100-1000x higher throughput than waiting for finality
#
# WORKLOAD_MODE: Determines the mix of create/update/read operations
submit_parallel_ptbs() {
    local package_id="$1"
    local result_file="$2"
    local num_parallel="$3"
    local workload_mode="$4"
    shift 4
    local all_objects=("$@")
    
    local num_objects=${#all_objects[@]}
    # Use appropriate batch size based on blob mode
    local objects_per_ptb
    if [ "$BLOB_MODE" = "yes" ]; then
        objects_per_ptb=$BLOB_BATCH_SIZE
    else
        objects_per_ptb=$PTB_BATCH_SIZE
    fi
    
    if [ "$FIRE_AND_FORGET" = "yes" ]; then
        # ═══════════════════════════════════════════════════════════════
        # FIRE-AND-FORGET MODE: Maximum throughput, no finality wait
        # ═══════════════════════════════════════════════════════════════
        # CRITICAL: Use sequential non-overlapping object ranges to avoid contention
        # Each PTB gets a UNIQUE slice of objects - no overlap allowed!
        #
        # Strategy: Limit parallel PTBs so each uses unique objects
        # If num_parallel * objects_per_ptb > num_objects, reduce parallelism
        #
        local max_parallel=$((num_objects / objects_per_ptb))
        if [ $max_parallel -lt 1 ]; then
            max_parallel=1
        fi
        local effective_parallel=$((num_parallel < max_parallel ? num_parallel : max_parallel))
        
        local submitted=0
        local gas_budget=500000000
        if [ "$BLOB_MODE" = "yes" ]; then
            gas_budget=2000000000  # Higher gas for blob writes
        fi
        
        for ((p = 0; p < effective_parallel; p++)); do
            # Each PTB gets a unique contiguous slice - NO OVERLAP
            local start_idx=$((p * objects_per_ptb))
            local ptb_objects=()
            
            for ((i = 0; i < objects_per_ptb; i++)); do
                local idx=$((start_idx + i))
                # Should never wrap since we limited effective_parallel
                ptb_objects+=("${all_objects[$idx]}")
            done
            
            # Build PTB command with workload mode
            local ptb_args=$(build_ptb_command "$package_id" "$workload_mode" "${ptb_objects[@]}")
            
            # Fire-and-forget: submit in background, redirect output to /dev/null
            # The transaction is submitted to the node but we don't wait for confirmation
            (sui client ptb $ptb_args --gas-budget $gas_budget >/dev/null 2>&1) &
            submitted=$((submitted + 1))
        done
        
        # Report all as "submitted" (we can't know success without waiting)
        echo "$submitted 0 $((submitted * objects_per_ptb))" >> "$result_file"
        
    else
        # ═══════════════════════════════════════════════════════════════
        # WAIT MODE: Wait for finality (slower but accurate success count)
        # ═══════════════════════════════════════════════════════════════
        local pids=()
        local temp_results=()
        
        # Launch multiple PTB submissions in parallel
        for ((p = 0; p < num_parallel; p++)); do
            local start_idx=$(( (p * objects_per_ptb) % num_objects ))
            local ptb_objects=()
            
            # Collect objects for this PTB
            for ((i = 0; i < objects_per_ptb; i++)); do
                local idx=$(( (start_idx + i) % num_objects ))
                ptb_objects+=("${all_objects[$idx]}")
            done
            
            # Create temp file for this PTB's result
            local temp_file=$(mktemp)
            temp_results+=("$temp_file")
            
            # Submit PTB in background with workload mode
            (
                local output exit_code
                output=$(execute_ptb "$package_id" "$workload_mode" "${ptb_objects[@]}")
                exit_code=$?
                
                if [ $exit_code -eq 0 ] && echo "$output" | grep -q '"status"'; then
                    echo "SUCCESS ${#ptb_objects[@]}" > "$temp_file"
                else
                    echo "FAIL ${output:0:100}" > "$temp_file"
                fi
            ) &
            pids+=($!)
        done
        
        # Wait for all parallel PTBs to complete
        local success_count=0
        local fail_count=0
        local total_objects=0
        
        for ((i = 0; i < ${#pids[@]}; i++)); do
            wait ${pids[$i]} 2>/dev/null || true
            
            local result=$(cat "${temp_results[$i]}" 2>/dev/null || echo "FAIL unknown")
            rm -f "${temp_results[$i]}"
            
            if [[ "$result" == SUCCESS* ]]; then
                success_count=$((success_count + 1))
                local obj_count=$(echo "$result" | awk '{print $2}')
                total_objects=$((total_objects + obj_count))
            else
                fail_count=$((fail_count + 1))
            fi
        done
        
        # Return results: success_ptbs fail_ptbs total_object_updates
        echo "$success_count $fail_count $total_objects" >> "$result_file"
    fi
}

# Submit multiple batch8 calls in parallel (fallback mode)
submit_parallel_batch8() {
    local package_id="$1"
    local result_file="$2"
    local num_parallel="$3"
    shift 3
    local all_objects=("$@")
    
    local num_objects=${#all_objects[@]}
    local pids=()
    local temp_results=()
    
    # Launch multiple batch8 submissions in parallel
    for ((p = 0; p < num_parallel; p++)); do
        local start_idx=$(( (p * 8) % num_objects ))
        local batch_objects=()
        
        # Collect 8 objects for this batch
        for ((i = 0; i < 8; i++)); do
            local idx=$(( (start_idx + i) % num_objects ))
            batch_objects+=("${all_objects[$idx]}")
        done
        
        local temp_file=$(mktemp)
        temp_results+=("$temp_file")
        
        # Submit batch8 in background
        (
            local output exit_code
            output=$(execute_batch8 "$package_id" "${batch_objects[@]}")
            exit_code=$?
            
            if [ $exit_code -eq 0 ] && echo "$output" | grep -q '"status"'; then
                echo "SUCCESS 8" > "$temp_file"
            else
                echo "FAIL ${output:0:100}" > "$temp_file"
            fi
        ) &
        pids+=($!)
    done
    
    # Wait for all parallel calls to complete
    local success_count=0
    local fail_count=0
    local total_objects=0
    
    for ((i = 0; i < ${#pids[@]}; i++)); do
        wait ${pids[$i]} 2>/dev/null || true
        
        local result=$(cat "${temp_results[$i]}" 2>/dev/null || echo "FAIL unknown")
        rm -f "${temp_results[$i]}"
        
        if [[ "$result" == SUCCESS* ]]; then
            success_count=$((success_count + 1))
            total_objects=$((total_objects + 8))
        else
            fail_count=$((fail_count + 1))
        fi
    done
    
    echo "$success_count $fail_count $total_objects" >> "$result_file"
}

run_worker() {
    local worker_id="$1"
    local config_dir="$2"
    local package_id="$3"
    local objects_file="$4"
    local duration="$5"
    local counter_file="$6"
    local stats_file="$7"
    local use_ptb_mode="$8"  # "ptb" or "batch8"
    
    export SUI_CONFIG_DIR="$config_dir"
    
    # Use appropriate objects count based on blob mode
    local objects_per_worker batch_size
    if [ "$BLOB_MODE" = "yes" ]; then
        objects_per_worker=$BLOBS_PER_WORKER
        batch_size=$BLOB_BATCH_SIZE
    else
        objects_per_worker=$OBJECTS_PER_WORKER
        batch_size=$PTB_BATCH_SIZE
    fi
    
    local start_line=$((worker_id * objects_per_worker + 1))
    local end_line=$((start_line + objects_per_worker - 1))
    
    # Read objects for this worker
    local my_objects=()
    while IFS= read -r line; do
        my_objects+=("$line")
    done < <(sed -n "${start_line},${end_line}p" "$objects_file")
    
    if [ ${#my_objects[@]} -eq 0 ]; then
        echo "Worker $worker_id: No objects assigned!" >&2
        return 1
    fi
    
    local end_time=$((SECONDS + duration))
    local tx_count=0
    local fail_count=0
    local object_updates=0
    local obj_idx=0
    local num_objects=${#my_objects[@]}
    local batch_result_file=$(mktemp)
    local round=0
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN LOOP: Parallel transaction submission
    # ═══════════════════════════════════════════════════════════════════════════
    # 
    # Two modes:
    # 1. PTB Mode: Each PTB contains batch_size object updates
    #    - PARALLEL_PTBS × batch_size = object updates per round
    #    - Example: 8 × 64 = 512 objects/round (conservative for OOM safety)
    #
    # 2. Batch8 Mode (fallback): Each call updates 8 objects via batch_increment_8
    #    - PARALLEL_PTBS × 8 = object updates per round
    #
    # FIRE_AND_FORGET mode: Background processes accumulate, need throttling
    #
    # CRITICAL FOR OBJECT CONTENTION:
    # - Each round uses a rotating slice of objects to avoid conflicts
    # - Max non-overlapping PTBs = num_objects / batch_size
    # - Rounds rotate through objects to allow previous TXs to finalize
    #
    local max_non_overlap_ptbs=$((num_objects / batch_size))
    if [ $max_non_overlap_ptbs -lt 1 ]; then
        max_non_overlap_ptbs=1
    fi
    local round_offset=0  # Tracks where in the object array we are across rounds
    
    while [ $SECONDS -lt $end_time ]; do
        > "$batch_result_file"
        round=$((round + 1))
        
        # In fire-and-forget mode, throttle based on background job count
        if [ "$FIRE_AND_FORGET" = "yes" ]; then
            local job_count=$(jobs -p 2>/dev/null | wc -l)
            while [ $job_count -gt $MAX_INFLIGHT ]; do
                # Wait briefly for some jobs to complete
                sleep 0.01
                job_count=$(jobs -p 2>/dev/null | wc -l)
            done
        fi
        
        if [ "$use_ptb_mode" = "ptb" ]; then
            # PTB mode: batch many operations per transaction
            # Use effective_parallel to avoid object overlap within a round
            local effective_parallel=$((PARALLEL_PTBS < max_non_overlap_ptbs ? PARALLEL_PTBS : max_non_overlap_ptbs))
            local batch_objects=()
            local total_for_batch=$((effective_parallel * batch_size))
            
            # Rotate starting position each round to allow previous TXs to finalize
            # This spreads writes across objects temporally
            for ((i = 0; i < total_for_batch; i++)); do
                local actual_idx=$(( (round_offset + i) % num_objects ))
                batch_objects+=("${my_objects[$actual_idx]}")
            done
            
            # Advance the round offset for next round
            round_offset=$(( (round_offset + total_for_batch) % num_objects ))
            
            submit_parallel_ptbs "$package_id" "$batch_result_file" "$effective_parallel" "$WORKLOAD_MODE" "${batch_objects[@]}"
        else
            # Batch8 mode: use batch_increment_8 function
            local batch_objects=()
            local total_for_batch=$((PARALLEL_PTBS * 8))
            
            for ((i = 0; i < total_for_batch; i++)); do
                batch_objects+=("${my_objects[$obj_idx]}")
                obj_idx=$(( (obj_idx + 1) % num_objects ))
            done
            
            submit_parallel_batch8 "$package_id" "$batch_result_file" "$PARALLEL_PTBS" "${batch_objects[@]}"
        fi
        
        # Parse results
        local results=$(cat "$batch_result_file" 2>/dev/null || echo "0 0 0")
        local batch_success=$(echo "$results" | awk '{print $1}')
        local batch_fail=$(echo "$results" | awk '{print $2}')
        local batch_objects_updated=$(echo "$results" | awk '{print $3}')
        
        tx_count=$((tx_count + batch_success))
        fail_count=$((fail_count + batch_fail))
        object_updates=$((object_updates + batch_objects_updated))
        
        # Update shared stats: worker_id tx_count fail_count object_updates
        echo "$worker_id $tx_count $fail_count $object_updates" >> "$stats_file"
    done
    
    rm -f "$batch_result_file"
    
    # Final update
    echo "$worker_id $tx_count $fail_count $object_updates FINAL" >> "$stats_file"
    echo "$tx_count $object_updates" >> "$counter_file"
    
    # Log summary for this worker
    echo "Worker $worker_id: TXs=$tx_count, Failed=$fail_count, ObjectUpdates=$object_updates" >&2
}

# ═══════════════════════════════════════════════════════════════════════════════
# BENCHMARK RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

run_benchmark() {
    local mode="$1" run_dir="$2"
    local config_dir="$MOUNT_POINT/p0/sui_node"
    
    # Disable errexit for benchmark - many commands can fail non-fatally
    set +e
    
    local start_time=$(date +%s)
    
    log_info "Running high-churn benchmark: WORKERS=$WORKERS, DURATION=${DURATION}s"
    log_info "WORKLOAD MODE: $WORKLOAD_MODE"
    
    # Log workload mode details
    case "$WORKLOAD_MODE" in
        mixed)
            log_info "  Mixed workload: ${MIXED_CREATE_PCT}% create (COLD→PID1), ${MIXED_UPDATE_PCT}% update (HOT→PID0)"
            log_info "  FDP benefit: Hot/cold segregation reduces GC copy cost"
            ;;
        append_only)
            log_info "  Append-only: Continuously creating new objects (COLD data)"
            log_info "  All data goes to PID1 - tests cold block isolation"
            ;;
        append_delete)
            log_info "  Append-delete: Create + delete (tombstone generation)"
            ;;
        update_only)
            log_info "  Update-only: Repeatedly updating same objects (HOT data)"
            log_info "  All data goes to PID0 - baseline for comparison"
            ;;
        *)
            log_info "  Unknown mode: defaulting to update-only"
            ;;
    esac
    
    # Adjust parameters based on BLOB_MODE
    local actual_batch_size actual_objects_per_worker
    if [ "$BLOB_MODE" = "yes" ]; then
        actual_batch_size=$BLOB_BATCH_SIZE
        actual_objects_per_worker=$BLOBS_PER_WORKER
        log_info "BLOB MODE: 4KB objects for high disk I/O"
        log_info "  Blobs per worker: $actual_objects_per_worker"
        log_info "  PTB batch: $actual_batch_size blobs/PTB"
        log_info "  Expected data per round: $((WORKERS * PARALLEL_PTBS * actual_batch_size * 4))KB"
    else
        actual_batch_size=$PTB_BATCH_SIZE
        actual_objects_per_worker=$OBJECTS_PER_WORKER
        log_info "COUNTER MODE: MicroCounter objects for high TPS"
        log_info "Objects per worker: $actual_objects_per_worker, Total objects: $((WORKERS * actual_objects_per_worker))"
    fi
    
    log_info "PTB config: ${actual_batch_size} objects/PTB, ${PARALLEL_PTBS} parallel PTBs"
    log_info "FIRE_AND_FORGET mode: $FIRE_AND_FORGET (max inflight: $MAX_INFLIGHT)"
    
    # Calculate effective parallelism based on object contention
    local max_non_overlap_ptbs=$((actual_objects_per_worker / actual_batch_size))
    local effective_parallel=$((PARALLEL_PTBS < max_non_overlap_ptbs ? PARALLEL_PTBS : max_non_overlap_ptbs))
    
    if [ $effective_parallel -lt $PARALLEL_PTBS ]; then
        log_warning "Object contention limit: Only $effective_parallel PTBs can run without overlap (requested: $PARALLEL_PTBS)"
        log_warning "  To increase parallelism, use: OBJECTS_PER_WORKER=$((PARALLEL_PTBS * actual_batch_size))"
        log_warning "  Current: $actual_objects_per_worker objects / $actual_batch_size = $max_non_overlap_ptbs max parallel PTBs"
    fi
    
    local actual_per_round=$((WORKERS * effective_parallel * actual_batch_size))
    log_info "Actual throughput: ${WORKERS} workers × ${effective_parallel} parallel × ${actual_batch_size} objects = ${actual_per_round} objects/round"
    
    local pkg_id=$(cat "$config_dir/.package_id" 2>/dev/null || echo "")
    if [ -z "$pkg_id" ]; then
        log_error "No package ID found at $config_dir/.package_id"
        return 1
    fi
    
    export SUI_CONFIG_DIR="$config_dir"
    
    local start_sectors=$(get_device_sectors)
    local objects_file="$run_dir/objects.txt"
    local counter_file="$run_dir/tx_counts.txt"
    
    # Determine PTB mode early so we can record it
    local use_ptb_mode
    if [ "$USE_PTB" = "yes" ]; then
        use_ptb_mode="ptb"
        log_info "PTB mode: FORCED (USE_PTB=yes)"
    elif [ "$USE_PTB" = "no" ]; then
        use_ptb_mode="batch8"
        log_info "PTB mode: DISABLED (USE_PTB=no), using batch_increment_8 fallback"
    else
        # Auto-detect
        if check_ptb_support; then
            use_ptb_mode="ptb"
            log_info "PTB mode: AUTO-DETECTED as supported"
        else
            use_ptb_mode="batch8"
            log_info "PTB mode: AUTO-DETECTED as unsupported, using batch_increment_8 fallback"
        fi
    fi
    
    # Calculate expected throughput based on mode
    local objects_per_round
    if [ "$use_ptb_mode" = "ptb" ]; then
        objects_per_round=$((WORKERS * PARALLEL_PTBS * actual_batch_size))
    else
        objects_per_round=$((WORKERS * PARALLEL_PTBS * 8))
    fi
    
    {
        echo "benchmark_mode=$mode"
        echo "transaction_mode=$use_ptb_mode"
        echo "blob_mode=$BLOB_MODE"
        echo "workload_mode=$WORKLOAD_MODE"
        echo "mixed_create_pct=$MIXED_CREATE_PCT"
        echo "mixed_update_pct=$MIXED_UPDATE_PCT"
        echo "# Note: No read operations - only writes affect WAF/GC"
        echo "start_time=$(date -Iseconds)"
        echo "start_sectors_written=$start_sectors"
        echo "workers=$WORKERS"
        echo "duration=$DURATION"
        echo "objects_per_worker=$actual_objects_per_worker"
        echo "total_objects=$((WORKERS * actual_objects_per_worker))"
        echo "ptb_batch_size=$actual_batch_size"
        echo "parallel_ptbs=$PARALLEL_PTBS"
        echo "objects_per_round=$objects_per_round"
        echo "max_inflight=$MAX_INFLIGHT"
        echo "fire_and_forget=$FIRE_AND_FORGET"
    } > "$run_dir/benchmark_info.txt"
    
    # Create objects for all workers
    if [ "$BLOB_MODE" = "yes" ]; then
        create_blobs "$config_dir" "$pkg_id" "$objects_file"
    else
        create_objects "$config_dir" "$pkg_id" "$objects_file"
    fi
    log_info "Object creation finished, starting stats collector..."
    
    # Start periodic stats collection
    start_periodic_stats_collector "$run_dir"
    log_info "Stats collector initialized, preparing workers..."
    
    > "$counter_file"
    
    local stats_file="$run_dir/worker_stats.log"
    > "$stats_file"
    > "${stats_file}.errors"
    
    # Record initial disk stats (read_sectors write_sectors)
    local initial_disk_stats=$(get_disk_stats)
    local initial_read_sectors=$(echo "$initial_disk_stats" | awk '{print $1}')
    local initial_write_sectors=$(echo "$initial_disk_stats" | awk '{print $2}')
    
    # Log expected throughput based on mode
    if [ "$use_ptb_mode" = "ptb" ]; then
        log_info "Expected throughput (PTB): ${WORKERS} workers × ${PARALLEL_PTBS} parallel × ${PTB_BATCH_SIZE} objects = $objects_per_round objects/round"
    else
        log_info "Expected throughput (batch8): ${WORKERS} workers × ${PARALLEL_PTBS} parallel × 8 objects = $objects_per_round objects/round"
    fi
    
    log_info "Starting $WORKERS workers for $DURATION seconds..."
    log_info "Initial disk: read=${initial_read_sectors} sectors, write=${initial_write_sectors} sectors"
    
    local pids=()
    for ((i=0; i<WORKERS; i++)); do
        run_worker "$i" "$config_dir" "$pkg_id" "$objects_file" "$DURATION" "$counter_file" "$stats_file" "$use_ptb_mode" &
        pids+=($!)
    done
    
    # Progress monitoring with comprehensive stats
    local elapsed=0
    local report_interval=30
    local last_txs=0
    local last_objs=0
    local last_time=$start_time
    local last_read_sectors=$initial_read_sectors
    local last_write_sectors=$initial_write_sectors
    
    log_info ""
    log_info "╔══════════════════════════════════════════════════════════════════════════════════════╗"
    log_info "║  BENCHMARK PROGRESS - High I/O Churn                                                 ║"
    log_info "╠══════════════════════════════════════════════════════════════════════════════════════╣"
    
    while [ $elapsed -lt $DURATION ]; do
        sleep "$report_interval"
        local now=$(date +%s)
        elapsed=$((now - start_time))
        
        # Aggregate stats from all workers
        # Stats format: worker_id tx_count fail_count object_updates [FINAL]
        local total_success=0
        local total_fail=0
        local total_obj_updates=0
        if [ -f "$stats_file" ]; then
            # Get latest stats per worker and sum them
            local worker_stats=$(awk '{latest[$1]=$2" "$3" "$4} END {for(w in latest) {split(latest[w],a); success+=a[1]; fail+=a[2]; objs+=a[3]} print success, fail, objs}' "$stats_file" 2>/dev/null)
            total_success=$(echo "$worker_stats" | awk '{print $1}')
            total_fail=$(echo "$worker_stats" | awk '{print $2}')
            total_obj_updates=$(echo "$worker_stats" | awk '{print $3}')
        fi
        total_success=${total_success:-0}
        total_fail=${total_fail:-0}
        total_obj_updates=${total_obj_updates:-0}
        
        # Calculate TPS (instant and average)
        local interval_txs=$((total_success - last_txs))
        local interval_time=$((now - last_time))
        local instant_tps=0
        local avg_tps=0
        if [ $interval_time -gt 0 ]; then
            instant_tps=$((interval_txs / interval_time))
        fi
        if [ $elapsed -gt 0 ]; then
            avg_tps=$((total_success / elapsed))
        fi
        last_txs=$total_success
        last_time=$now
        
        # Get current disk stats
        local current_disk_stats=$(get_disk_stats)
        local current_read_sectors=$(echo "$current_disk_stats" | awk '{print $1}')
        local current_write_sectors=$(echo "$current_disk_stats" | awk '{print $2}')
        
        # Calculate deltas from initial
        local delta_read_sectors=$((current_read_sectors - initial_read_sectors))
        local delta_write_sectors=$((current_write_sectors - initial_write_sectors))
        local delta_read_bytes=$((delta_read_sectors * 512))
        local delta_write_bytes=$((delta_write_sectors * 512))
        
        # Calculate I/O rates (from last interval)
        local interval_read_sectors=$((current_read_sectors - last_read_sectors))
        local interval_write_sectors=$((current_write_sectors - last_write_sectors))
        local read_rate=0
        local write_rate=0
        if [ $interval_time -gt 0 ]; then
            read_rate=$((interval_read_sectors * 512 / interval_time))
            write_rate=$((interval_write_sectors * 512 / interval_time))
        fi
        last_read_sectors=$current_read_sectors
        last_write_sectors=$current_write_sectors
        
        # Calculate success rate
        local total_attempts=$((total_success + total_fail))
        local success_rate=100
        if [ $total_attempts -gt 0 ]; then
            success_rate=$(echo "scale=1; $total_success * 100 / $total_attempts" | bc)
        fi
        
        # Object updates are tracked directly from workers now (PTB-based)
        local object_updates=$total_obj_updates
        local interval_objs=$((object_updates - last_objs))
        local obj_update_rate=0
        if [ $interval_time -gt 0 ]; then
            obj_update_rate=$((interval_objs / interval_time))
        fi
        last_objs=$object_updates
        
        # Estimate application writes: ~400 bytes per object update
        local estimated_app_writes=$((object_updates * 400))
        local write_amp_ratio="N/A"
        if [ $estimated_app_writes -gt 0 ] && [ $delta_write_bytes -gt 0 ]; then
            write_amp_ratio=$(echo "scale=2; $delta_write_bytes / $estimated_app_writes" | bc)
        fi
        
        # Format elapsed time
        local elapsed_min=$((elapsed / 60))
        local elapsed_sec=$((elapsed % 60))
        local duration_min=$((DURATION / 60))
        
        # Print comprehensive progress
        log_info "║ Time: ${elapsed_min}m${elapsed_sec}s / ${duration_min}m | Progress: $((elapsed * 100 / DURATION))%"
        log_info "║ PTB TXs: success=$total_success, failed=$total_fail (${success_rate}% success rate)"
        log_info "║ Object Updates: $object_updates total | Rate: ${obj_update_rate}/sec"
        log_info "║ TPS: instant=$instant_tps, avg=$avg_tps ptb_tx/sec (each PTB = ${PTB_BATCH_SIZE} objects)"
        log_info "║ Disk Read:  $(format_bytes $delta_read_bytes) total | $(format_rate $read_rate)"
        log_info "║ Disk Write: $(format_bytes $delta_write_bytes) total | $(format_rate $write_rate)"
        log_info "║ Est. App Writes: $(format_bytes $estimated_app_writes) | Device/App ratio: ${write_amp_ratio}x"
        
        # Show last error if any
        if [ -f "${stats_file}.errors" ] && [ -s "${stats_file}.errors" ]; then
            local last_err=$(tail -1 "${stats_file}.errors" | head -c 70)
            log_info "║ Last Error: $last_err..."
        fi
        
        log_info "╠══════════════════════════════════════════════════════════════════════════════════════╣"
    done
    
    log_info "╚══════════════════════════════════════════════════════════════════════════════════════╝"
    log_info ""
    
    log_info "Waiting for workers to complete..."
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    
    local end_time=$(date +%s)
    
    stop_periodic_stats_collector
    
    # Final FEMU stats
    log_info "Collecting final FEMU stats from host..."
    local final_stats=$(get_final_femu_stats)
    local sum_written=$(echo "$final_stats" | awk '{print $1}')
    local copied=$(echo "$final_stats" | awk '{print $2}')
    local waf=$(echo "$final_stats" | awk '{print $3}')
    
    # Final disk stats
    local final_disk_stats=$(get_disk_stats)
    local final_read_sectors=$(echo "$final_disk_stats" | awk '{print $1}')
    local final_write_sectors=$(echo "$final_disk_stats" | awk '{print $2}')
    local delta_read_sectors=$((final_read_sectors - initial_read_sectors))
    local delta_write_sectors=$((final_write_sectors - initial_write_sectors))
    local delta_read_bytes=$((delta_read_sectors * 512))
    local delta_write_bytes=$((delta_write_sectors * 512))
    
    local total_duration=$((end_time - start_time))
    # Counter file format: tx_count object_updates
    local total_txs=$(awk '{sum+=$1} END {print sum+0}' "$counter_file")
    local total_obj_updates=$(awk '{sum+=$2} END {print sum+0}' "$counter_file")
    local total_fail=$(awk '{latest[$1]=$3} END {for(w in latest) fail+=latest[w]; print fail+0}' "$stats_file" 2>/dev/null || echo 0)
    local tps=0
    local obj_rate=0
    if [ $total_duration -gt 0 ]; then
        tps=$((total_txs / total_duration))
        obj_rate=$((total_obj_updates / total_duration))
    fi
    
    # Calculate final write amplification from disk stats
    # Object updates tracked directly from PTB execution
    local object_updates=$total_obj_updates
    local estimated_app_writes=$((object_updates * 400))
    local disk_write_amp="N/A"
    if [ $estimated_app_writes -gt 0 ] && [ $delta_write_bytes -gt 0 ]; then
        disk_write_amp=$(echo "scale=2; $delta_write_bytes / $estimated_app_writes" | bc)
    fi
    
    {
        echo ""
        echo "end_time=$(date -Iseconds)"
        echo "initial_read_sectors=$initial_read_sectors"
        echo "initial_write_sectors=$initial_write_sectors"
        echo "final_read_sectors=$final_read_sectors"
        echo "final_write_sectors=$final_write_sectors"
        echo "delta_read_sectors=$delta_read_sectors"
        echo "delta_write_sectors=$delta_write_sectors"
        echo "delta_read_bytes=$delta_read_bytes"
        echo "delta_write_bytes=$delta_write_bytes"
        echo "sum_wpp_written=$sum_written"
        echo "copied=$copied"
        echo "final_waf=$waf"
        echo "disk_write_amp=$disk_write_amp"
        echo "total_txs=$total_txs"
        echo "total_failed=$total_fail"
        echo "tps=$tps"
    } >> "$run_dir/benchmark_info.txt"
    
    {
        echo "═══════════════════════════════════════════════════════════════════════════════"
        echo "  High-Churn Benchmark Summary: $mode"
        echo "═══════════════════════════════════════════════════════════════════════════════"
        echo ""
        echo "Configuration:"
        echo "  Workers: $WORKERS"
        echo "  Duration: ${total_duration}s (target: ${DURATION}s)"
        echo "  Objects per worker: $OBJECTS_PER_WORKER"
        echo "  Total objects: $((WORKERS * OBJECTS_PER_WORKER))"
        echo "  PTB Batch Size: $PTB_BATCH_SIZE objects/transaction"
        echo "  Parallel PTBs: $PARALLEL_PTBS per worker round"
        echo ""
        echo "Transaction Performance:"
        echo "  Total PTB TXs:   $total_txs"
        echo "  Object Updates:  $object_updates"
        echo "  Failed TXs:      $total_fail"
        echo "  Success Rate:    $(echo "scale=1; $total_txs * 100 / ($total_txs + $total_fail + 1)" | bc)%"
        echo "  PTB TPS:         $tps tx/sec"
        echo "  Obj Updates/s:   $obj_rate obj/sec"
        echo ""
        echo "Disk I/O (from /proc/diskstats):"
        echo "  Read:  $(format_bytes $delta_read_bytes) ($delta_read_sectors sectors)"
        echo "  Write: $(format_bytes $delta_write_bytes) ($delta_write_sectors sectors)"
        echo "  Est. App Writes: $(format_bytes $estimated_app_writes)"
        echo "  Device/App Ratio: ${disk_write_amp}x"
        echo ""
        echo "FEMU GC Statistics:"
        echo "  Host written (sum wpp->written): $sum_written pages"
        echo "  GC copied: $copied pages"
        echo "  WAF = 1 + (copied / written) = $waf"
        echo ""
        echo "Output Files:"
        echo "  WAF timeseries: $run_dir/waf_timeseries.csv"
        echo "  Worker stats:   $run_dir/worker_stats.log"
        echo "═══════════════════════════════════════════════════════════════════════════════"
    } > "$run_dir/summary.txt"
    
    cat "$run_dir/summary.txt" >&2
    
    log_success "Benchmark $mode complete: WAF=$waf, TPS=$tps, Disk Write Amp=${disk_write_amp}x"
}

run_fdp_disabled_benchmark() {
    log_section "NON-FDP BENCHMARK (Control)"
    local run_dir="$RESULTS_DIR/nfdp"
    mkdir -p "$run_dir"
    
    stop_sui_node
    unmount_f2fs
    mount_f2fs 8
    start_sui_node "$MOUNT_POINT/p0/sui_node" "false"
    publish_contract
    smash_gas_coins "$MOUNT_POINT/p0/sui_node" "$WORKERS"
    
    run_benchmark "nofdp" "$run_dir"
    
    stop_sui_node
    unmount_f2fs
}

run_fdp_enabled_benchmark() {
    log_section "FDP-ENABLED BENCHMARK"
    local run_dir="$RESULTS_DIR/fdp"
    mkdir -p "$run_dir"
    
    stop_sui_node
    unmount_f2fs
    mount_f2fs 8
    start_sui_node "$MOUNT_POINT/p0/sui_node" "true"
    publish_contract
    smash_gas_coins "$MOUNT_POINT/p0/sui_node" "$WORKERS"
    
    run_benchmark "fdp" "$run_dir"
    
    stop_sui_node
    unmount_f2fs
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

main() {
    mkdir -p "$RESULTS_DIR" "$LOG_DIR"
    
    log_section "High I/O Churn FDP Benchmark for SUI Blockchain"
    log_info "Strategy: Many small objects with frequent updates"
    log_info "  - Objects: $((WORKERS * OBJECTS_PER_WORKER)) total ($OBJECTS_PER_WORKER per worker)"
    log_info "  - Workers: $WORKERS"
    log_info "  - Duration: ${DURATION}s"
    log_info "  - PID 0: Account state (objects) - high churn"
    log_info "  - PID 1: Ledger (transactions, effects) - append-only"
    
    log_info "Caching sudo credentials..."
    sudo -v || { log_error "sudo access required"; exit 1; }
    
    MAIN_PID=$$
    trap 'if [ $$ -eq $MAIN_PID ]; then log_warning "Caught interrupt, cleaning up..."; cleanup_benchmark; fi; exit 130' INT TERM
    
    local nfdp_waf="N/A" fdp_waf="N/A"
    set +e
    
    if [ "$NONFDP" != "yes" ]; then
        run_fdp_disabled_benchmark
        nfdp_waf=$(grep "^final_waf=" "$BASE_RESULTS_DIR/nfdp/benchmark_info.txt" 2>/dev/null | cut -d= -f2)
        nfdp_waf=${nfdp_waf:-N/A}
        log_info "Non-FDP benchmark WAF: '$nfdp_waf'"
        if [ "$NOFDP" != "yes" ]; then
            log_info "Sleeping 30s between benchmarks..."
            sleep 30
        fi
    fi
    
    if [ "$NOFDP" != "yes" ]; then
        run_fdp_enabled_benchmark
        fdp_waf=$(grep "^final_waf=" "$BASE_RESULTS_DIR/fdp/benchmark_info.txt" 2>/dev/null | cut -d= -f2)
        fdp_waf=${fdp_waf:-N/A}
        log_info "FDP benchmark WAF: '$fdp_waf'"
    fi
    
    set -e
    
    log_section "FINAL RESULTS"
    if [ "$NONFDP" != "yes" ]; then
        echo "Non-FDP WAF: $nfdp_waf" | tee "$BASE_RESULTS_DIR/final_comparison.txt"
        echo "  Timeseries: $BASE_RESULTS_DIR/nfdp/waf_timeseries.csv" | tee -a "$BASE_RESULTS_DIR/final_comparison.txt"
    fi
    if [ "$NOFDP" != "yes" ]; then
        echo "FDP WAF:     $fdp_waf" | tee -a "$BASE_RESULTS_DIR/final_comparison.txt"
        echo "  Timeseries: $BASE_RESULTS_DIR/fdp/waf_timeseries.csv" | tee -a "$BASE_RESULTS_DIR/final_comparison.txt"
    fi
    
    if [[ "$nfdp_waf" != "N/A" && "$fdp_waf" != "N/A" ]]; then
        local improvement=$(echo "scale=2; (($nfdp_waf - $fdp_waf) / $nfdp_waf) * 100" | bc 2>/dev/null || echo "N/A")
        echo "WAF Improvement: ${improvement}%" | tee -a "$BASE_RESULTS_DIR/final_comparison.txt"
    fi
    
    log_info "Results: $BASE_RESULTS_DIR"
    log_success "High-churn benchmark completed successfully"
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"
