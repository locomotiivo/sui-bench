#!/bin/bash
#
# FDP Benchmark Script for SUI Blockchain
#
# This script compares FDP-enabled vs FDP-disabled benchmarks.
#   - PID 0: Account state (objects, locks) - high churn, mutable
#   - PID 1: Ledger data (transactions, effects, consensus) - append-only
#

set -e

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR="/home/femu/fdp-scripts/sui-bench/sui2/scripts"
FDP_TOOLS_DIR="/home/femu/fdp-scripts/f2fs-tools-fdp"
FDP_STATS="$FDP_TOOLS_DIR/fdp_stats"
MOUNT_POINT="/home/femu/f2fs_fdp_mount"
NVME_DEVICE="nvme0n1"
NVME_DEV_PATH="/dev/$NVME_DEVICE"

# Benchmark parameters
WORKERS="${WORKERS:-256}"
DURATION="${DURATION:-7200}"
BLOB_SIZE_KB="${BLOB_SIZE_KB:-200}"
BATCH_COUNT="${BATCH_COUNT:-5}"

# Benchmark strategy: 'blobs' (create only) or 'update_heavy' (update churn)
BENCH_STRATEGY="${BENCH_STRATEGY:-blobs}"

# Update-heavy specific parameters (only used if BENCH_STRATEGY=update_heavy)
UPDATE_POOL_SIZE="${UPDATE_POOL_SIZE:-100}"
UPDATE_RATIO="${UPDATE_RATIO:-80}"

# Periodic stats collection interval (seconds) - default 15 minutes
# Now uses fdp_stats --read-only which doesn't reset counters
STATS_INTERVAL="${STATS_INTERVAL:-900}"

# Control flags
NOFDP="${NOFDP:-no}"
NONFDP="${NONFDP:-no}"

# Output
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/fdp_lsm_results}"
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

get_device_sectors() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats 2>/dev/null || echo 0
}

format_bytes() {
    local bytes=$1
    if [ "$bytes" -ge 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1073741824" | bc) GB"
    elif [ "$bytes" -ge 1048576 ]; then
        echo "$(echo "scale=2; $bytes / 1048576" | bc) MB"
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
    pkill -9 -x "sui" 2>/dev/null || true
    pkill -9 -f "sui-node" 2>/dev/null || true
    pkill -9 -f "sui start" 2>/dev/null || true
    sleep 2
}

unmount_f2fs() {
    log_info "Unmounting F2FS..."
    if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
        sudo umount -l ${MOUNT_POINT} 2>/dev/null || true
    fi
    sudo fuser -k ${NVME_DEV_PATH} 2>/dev/null || true
    sync
    sleep 2
}

mount_f2fs() {
    local nlogs="${1:-8}"
    log_info "Mounting F2FS with FDP (nlogs=$nlogs)..."
    
    mkdir -p "$MOUNT_POINT"
    # Reset FEMU stats before formatting (clean slate)
    sudo "$FDP_STATS" "$NVME_DEV_PATH" --reset >/dev/null 2>&1 || true
    sudo "$FDP_TOOLS_DIR"/mkfs/mkfs.f2fs -f -O lost_found "$NVME_DEV_PATH"
    sudo "$FDP_TOOLS_DIR/fdp_f2fs_mount" "$nlogs"
    sudo chmod -R 777 "$MOUNT_POINT"
    
    # Clean up any leftover FDP database directories to prevent corruption
    log_info "Cleaning FDP directories for fresh start..."
    
    # Semantic mode uses account_state/ and ledger/ directories
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
    
    if [ "$use_fdp" = "true" ]; then
        # Semantic FDP: separates by data characteristics (account state vs ledger)
        export SUI_FDP_SEMANTIC=1
        export SUI_FDP_BASE_PATH="$MOUNT_POINT"
        unset SUI_FDP_ENABLED SUI_FDP_MODE
        log_info "FDP env: SEMANTIC mode (account_state=PID0, ledger=PID1)"
    else
        unset SUI_FDP_ENABLED SUI_FDP_BASE_PATH SUI_FDP_MODE SUI_FDP_SEMANTIC
    fi
    
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
    log_info "Publishing bloat contract..."
    
    export SUI_CONFIG_DIR="$config_dir"
    
    set +e  # Temporarily disable exit on error
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
    
    # Validate faucet response is JSON before parsing
    if echo "$faucet_response" | jq -e '.' >/dev/null 2>&1; then
        local gas_amount=$(echo "$faucet_response" | jq -r '.transferred_gas_objects[0].amount // "unknown"')
        log_info "Faucet response: received $gas_amount gas"
    else
        log_warning "Faucet response not valid JSON: $faucet_response"
    fi
    sleep 1
    
    # Publish contract
    cd "$SCRIPT_DIR/../move/bloat_storage"
    
    # Clear any cached publication files to avoid chain ID conflicts
    rm -f Pub.localnet.toml Pub.testnet.toml 2>/dev/null || true
    
    log_info "Running: sui client test-publish --build-env localnet --json"
    local raw_output=$(SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui client test-publish --build-env localnet --json 2>&1)
    local exit_code=$?

    log_info "Publish exit code: $exit_code"
    
    # Extract JSON from output (skip build logs/warnings that precede the JSON)
    # The JSON starts with '{' at the beginning of a line
    local result=$(echo "$raw_output" | sed -n '/^{/,$p')
    
    # Check if we extracted valid JSON
    if ! echo "$result" | jq -e '.' >/dev/null 2>&1; then
        log_error "Could not extract valid JSON from publish output"
        log_error "Raw output (first 500 chars): ${raw_output:0:500}"
        set -e
        return 1
    fi
    
    log_info "JSON extracted successfully"
    
    # Check for successful publish
    if [ $exit_code -eq 0 ] && echo "$result" | jq -e '.effects.V2.status == "Success"' >/dev/null 2>&1; then
        local package_id=$(echo "$result" | jq -r '.changed_objects[] | select(.objectType == "package") | .objectId')
        set -e  # Re-enable exit on error
        
        if [ -n "$package_id" ] && [ "$package_id" != "null" ]; then
            echo "$package_id" > "$config_dir/.package_id"
            log_info "Contract published: $package_id"
            echo "$package_id"
            return 0
        fi
    fi
    
    # If we get here, publish failed
    log_error "Failed to publish contract (exit code: $exit_code)"
    log_error "Status from JSON: $(echo "$result" | jq -r '.effects.V2.status // "unknown"')"
    set -e
    return 1
}

# ═══════════════════════════════════════════════════════════════════════════════
# FEMU STATS
# ═══════════════════════════════════════════════════════════════════════════════

# Get host writes from /proc/diskstats (non-destructive, cumulative)
# Returns: sectors_written (can convert to pages via /8)
get_diskstats_writes() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats 2>/dev/null || echo 0
}

# Parse FEMU stats from journalctl output
# The fdp_stats binary triggers FEMU to print stats to its console
# We capture this from journalctl
# Returns: "sum_written copied waf" (space-separated)
parse_femu_stats_from_journal() {
    local journal_cursor="$1"
    local sum_written=0
    local copied=0
    local waf="N/A"
    
    # Small delay for journal to capture FEMU output
    sleep 1
    
    # Capture FEMU output from journalctl
    local femu_output=""
    if [ -n "$journal_cursor" ]; then
        femu_output=$(journalctl --after-cursor="$journal_cursor" 2>/dev/null | \
                      grep -E "wpp->written|copied|WAF|Host written|GC copied" || true)
    else
        # Fallback: get last 100 lines from journal
        femu_output=$(journalctl -n 100 2>/dev/null | \
                      grep -E "wpp->written|copied|WAF|Host written|GC copied" || true)
    fi
    
    if [ -n "$femu_output" ]; then
        # Parse Host written (from ftl_log format): "Host written: VALUE pages"
        local direct_sum=$(echo "$femu_output" | grep -oE 'Host written: [0-9]+' | grep -oE '[0-9]+' | tail -1)
        if [ -n "$direct_sum" ] && [ "$direct_sum" -gt 0 ] 2>/dev/null; then
            sum_written=$direct_sum
        else
            # Fallback: sum wpp->written values from print_sungjin format
            local written_vals=$(echo "$femu_output" | grep -oE 'wpp->written.*\{[0-9]+\}' | grep -oE '\{[0-9]+\}' | tr -d '{}')
            if [ -n "$written_vals" ]; then
                sum_written=$(echo "$written_vals" | awk '{sum+=$1} END {print sum+0}')
            fi
        fi
        
        # Parse GC copied: "GC copied: VALUE pages" or print_sungjin format
        copied=$(echo "$femu_output" | grep -oE 'GC copied: [0-9]+' | grep -oE '[0-9]+' | tail -1)
        if [ -z "$copied" ] || [ "$copied" = "0" ]; then
            copied=$(echo "$femu_output" | grep -oE 'copied.*\{[0-9]+\}' | grep -oE '\{[0-9]+\}' | tr -d '{}' | tail -1)
        fi
        
        # Parse WAF if directly available: "WAF: X.XX"
        waf=$(echo "$femu_output" | grep -oE 'WAF: [0-9]+\.[0-9]+' | grep -oE '[0-9]+\.[0-9]+' | tail -1)
    fi
    
    sum_written=${sum_written:-0}
    copied=${copied:-0}
    
    # Calculate WAF if not directly available
    if [ -z "$waf" ] || [ "$waf" = "N/A" ]; then
        if [ "$sum_written" -gt 0 ] 2>/dev/null; then
            waf=$(echo "scale=4; 1 + $copied / $sum_written" | bc)
        else
            waf="N/A"
        fi
    fi
    
    echo "$sum_written $copied $waf"
}

# Reset FEMU stats using fdp_stats --reset
# CAUTION: This RESETS the device counters!
# Use at benchmark START for clean baseline
# Returns: "sum_written copied waf" (space-separated, values before reset)
reset_femu_stats() {
    log_info "Resetting FEMU stats (fdp_stats --reset)..."
    
    if [ ! -x "$FDP_STATS" ]; then
        log_warning "fdp_stats not found, cannot reset stats"
        echo "0 0 N/A"
        return
    fi
    
    # Mark journal position before the call
    local journal_cursor=$(journalctl --show-cursor -n 0 2>/dev/null | grep "cursor" | cut -d' ' -f3)
    
    # Call fdp_stats --reset
    sudo "$FDP_STATS" "$NVME_DEV_PATH" --reset >/dev/null 2>&1 || true
    
    # Parse and return stats
    parse_femu_stats_from_journal "$journal_cursor"
}

# Read FEMU stats WITHOUT resetting (fdp_stats --read-only)
# Safe to call periodically during benchmarks
# Returns: "sum_written copied waf" (space-separated)
read_femu_stats() {
    if [ ! -x "$FDP_STATS" ]; then
        echo "0 0 N/A"
        return
    fi
    
    # Mark journal position before the call
    local journal_cursor=$(journalctl --show-cursor -n 0 2>/dev/null | grep "cursor" | cut -d' ' -f3)
    
    # Call fdp_stats --read-only
    sudo "$FDP_STATS" "$NVME_DEV_PATH" --read-only >/dev/null 2>&1 || true
    
    # Parse and return stats
    parse_femu_stats_from_journal "$journal_cursor"
}

# Background periodic stats collector
# Uses fdp_stats --read-only for cumulative GC stats (no reset!)
# Writes snapshots to individual files and summary CSV
STATS_COLLECTOR_PID=""
start_periodic_stats_collector() {
    local run_dir="$1"
    local snapshots_dir="$run_dir/stats_snapshots"
    local csv_file="$run_dir/waf_timeseries.csv"
    local interval="${STATS_INTERVAL}"
    
    mkdir -p "$snapshots_dir"
    
    log_info "Starting periodic stats collector (interval=${interval}s = $(echo "scale=1; $interval / 60" | bc)min)"
    log_info "Snapshots dir: $snapshots_dir"
    
    # Initialize CSV with header
    echo "timestamp,elapsed_sec,host_sectors,sum_written,copied,waf" > "$csv_file"
    
    local start_time=$(date +%s)
    local snapshot_num=0
    
    (
        while true; do
            sleep "$interval"
            
            snapshot_num=$((snapshot_num + 1))
            local now=$(date +%s)
            local elapsed=$((now - start_time))
            local timestamp=$(date +"%Y%m%d_%H%M%S")
            local timestamp_iso=$(date -Iseconds)
            
            # Get host writes from diskstats
            local host_sectors=$(get_diskstats_writes)
            
            # Get FEMU stats (read-only, no reset!)
            local femu_stats=$(read_femu_stats)
            local sum_written=$(echo "$femu_stats" | awk '{print $1}')
            local copied=$(echo "$femu_stats" | awk '{print $2}')
            local waf=$(echo "$femu_stats" | awk '{print $3}')
            
            # Save individual snapshot file
            local snapshot_file="$snapshots_dir/snapshot_${snapshot_num}_${timestamp}.txt"
            {
                echo "Snapshot #$snapshot_num"
                echo "Timestamp: $timestamp_iso"
                echo "Elapsed: ${elapsed}s ($(echo "scale=1; $elapsed / 60" | bc)min)"
                echo ""
                echo "Host Statistics (diskstats):"
                echo "  Sectors written: $host_sectors"
                echo "  MB written: $(echo "scale=2; $host_sectors * 512 / 1048576" | bc)"
                echo ""
                echo "FEMU GC Statistics (cumulative):"
                echo "  sum(wpp->written): $sum_written pages"
                echo "  copied (GC): $copied pages"
                echo "  WAF: $waf"
                echo ""
                echo "WAF = 1 + (copied / sum_written)"
                echo "    = 1 + ($copied / $sum_written)"
            } > "$snapshot_file"
            
            # Append to CSV
            echo "$timestamp_iso,$elapsed,$host_sectors,$sum_written,$copied,$waf" >> "$csv_file"
            
            # Log progress
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
        wait "$STATS_COLLECTOR_PID" 2>/dev/null || true
        STATS_COLLECTOR_PID=""
    fi
}

# Collect final stats (read-only, no reset)
get_final_femu_stats() {
    log_info "Collecting final FEMU stats (read-only)..."
    local stats=$(read_femu_stats)
    local sum_written=$(echo "$stats" | awk '{print $1}')
    local copied=$(echo "$stats" | awk '{print $2}')
    local waf=$(echo "$stats" | awk '{print $3}')
    
    log_info "  Final: sum_written=$sum_written, copied=$copied, WAF=$waf"
    echo "$sum_written $copied $waf"
}

# ═══════════════════════════════════════════════════════════════════════════════
# BENCHMARK RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

run_benchmark() {
    local mode="$1" run_dir="$2"
    local config_dir="$MOUNT_POINT/p0/sui_node"
    
    local start_time=$(date +%s)
    
    log_info "Running benchmark: WORKERS=$WORKERS, DURATION=${DURATION}s"
    log_info "Stats interval: ${STATS_INTERVAL}s ($(echo "scale=1; $STATS_INTERVAL / 60" | bc)min)"
    
    # Read package ID from file
    local pkg_id=$(cat "$config_dir/.package_id" 2>/dev/null || echo "")
    if [ -z "$pkg_id" ]; then
        log_error "No package ID found at $config_dir/.package_id"
        echo "N/A"
        return 1
    fi
    
    export SUI_CONFIG_DIR="$config_dir"
    export PACKAGE_ID="$pkg_id"
    export WORKERS DURATION BLOB_SIZE_KB BATCH_COUNT
    export UPDATE_POOL_SIZE UPDATE_RATIO
    export RESULTS_DIR="$run_dir"
    
    # Record starting diskstats
    local start_sectors=$(get_diskstats_writes)
    {
        echo "benchmark_mode=$mode"
        echo "benchmark_strategy=$BENCH_STRATEGY"
        echo "start_time=$(date -Iseconds)"
        echo "start_sectors_written=$start_sectors"
        echo "stats_interval=${STATS_INTERVAL}s"
    } > "$run_dir/benchmark_info.txt"
    
    # Start periodic stats collection (now uses fdp_stats --read-only)
    start_periodic_stats_collector "$run_dir"
    
    local bench_output="$run_dir/benchmark.log"
    cd "$SCRIPT_DIR"
    
    # Run selected benchmark strategy
    if [ "$BENCH_STRATEGY" = "update_heavy" ]; then
        log_info "Running update-heavy benchmark (maximize LSM churn)..."
        ./update-heavy-bench.sh 2>&1 | tee "$bench_output"
    else
        log_info "Running create-only benchmark..."
        ./max-device-write-bench.sh 2>&1 | tee "$bench_output"
    fi
    
    local end_time=$(date +%s)
    
    # Stop the periodic collector
    stop_periodic_stats_collector
    
    # Collect final stats (read-only, no reset)
    log_info "Collecting final FEMU stats..."
    local final_stats=$(get_final_femu_stats)
    local sum_written=$(echo "$final_stats" | awk '{print $1}')
    local copied=$(echo "$final_stats" | awk '{print $2}')
    local waf=$(echo "$final_stats" | awk '{print $3}')
    
    # Record final diskstats
    local final_sectors=$(get_diskstats_writes)
    local delta_sectors=$((final_sectors - start_sectors))
    
    # Append to benchmark info
    {
        echo ""
        echo "end_time=$(date -Iseconds)"
        echo "final_sectors_written=$final_sectors"
        echo "delta_sectors=$delta_sectors"
        echo "sum_wpp_written=$sum_written"
        echo "copied=$copied"
        echo "final_waf=$waf"
    } >> "$run_dir/benchmark_info.txt"
    
    local elapsed=$((end_time - start_time))
    # TPS line format: "  TPS:              $tps tx/sec"
    local tps=$(grep "TPS:" "$bench_output" | tail -1 | awk '{print $2}' | tr -d ' ')
    tps=${tps:-N/A}
    
    # Count snapshots collected
    local snapshot_count=$(ls -1 "$run_dir/stats_snapshots/"*.txt 2>/dev/null | wc -l || echo 0)
    
    # Generate summary
    {
        echo "═══════════════════════════════════════════════════════════════"
        echo "  Benchmark Summary: $mode"
        echo "═══════════════════════════════════════════════════════════════"
        echo ""
        echo "Configuration:"
        echo "  Workers: $WORKERS"
        echo "  Duration: ${elapsed}s (target: ${DURATION}s)"
        echo "  Stats interval: ${STATS_INTERVAL}s"
        echo "  Snapshots collected: $snapshot_count"
        echo ""
        echo "Performance:"
        echo "  TPS: $tps tx/sec"
        echo ""
        echo "Write Amplification:"
        echo "  sum(wpp->written): $sum_written pages"
        echo "  GC copied: $copied pages"
        echo "  Final WAF: $waf"
        echo ""
        echo "  WAF = 1 + (copied / sum_written)"
        echo "      = 1 + ($copied / $sum_written)"
        echo ""
        echo "Host Writes (diskstats):"
        echo "  Start sectors: $start_sectors"
        echo "  Final sectors: $final_sectors"
        echo "  Delta: $delta_sectors sectors ($(echo "scale=2; $delta_sectors * 512 / 1073741824" | bc) GB)"
        echo ""
        echo "Output Files:"
        echo "  WAF timeseries: $run_dir/waf_timeseries.csv"
        echo "  Snapshots: $run_dir/stats_snapshots/"
        echo "  Benchmark log: $run_dir/benchmark.log"
        echo "═══════════════════════════════════════════════════════════════"
    } > "$run_dir/summary.txt"
    
    cat "$run_dir/summary.txt" >&2
    
    log_success "Benchmark $mode complete: WAF=$waf, TPS=$tps, Snapshots=$snapshot_count"
    
    # Only this goes to stdout (for capture)
    echo "$waf"
}

run_fdp_disabled_benchmark() {
    log_section "NON-FDP BENCHMARK (Control)"
    local run_dir="$RESULTS_DIR/nfdp"
    mkdir -p "$run_dir"
    
    stop_sui_node; unmount_f2fs; mount_f2fs 8
    start_sui_node "$MOUNT_POINT/p0/sui_node" "false"
    publish_contract
    run_benchmark "nofdp" "$run_dir"
    stop_sui_node; unmount_f2fs
}

run_fdp_enabled_benchmark() {
    log_section "FDP-ENABLED BENCHMARK"
    local run_dir="$RESULTS_DIR/fdp"
    mkdir -p "$run_dir"
    
    stop_sui_node; unmount_f2fs; mount_f2fs 8
    start_sui_node "$MOUNT_POINT/p0/sui_node" "true"
    publish_contract
    run_benchmark "fdp" "$run_dir"
    stop_sui_node; unmount_f2fs
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

main() {
    mkdir -p "$RESULTS_DIR" "$LOG_DIR"
    
    log_section "FDP Benchmark for SUI Blockchain"
    log_info "  PID 0: Account state (objects, locks) - high churn"
    log_info "  PID 1: Ledger (transactions, effects, consensus) - append-only"
    log_info "Duration: ${DURATION}s, Workers: $WORKERS"
    log_info "Stats collection interval: ${STATS_INTERVAL}s ($(echo "scale=1; $STATS_INTERVAL / 60" | bc) min)"
    
    # Cache sudo credentials upfront to avoid prompts during benchmark
    log_info "Caching sudo credentials..."
    sudo -v || { log_error "sudo access required"; exit 1; }
    
    # Cleanup on exit
    cleanup() {
        stop_periodic_stats_collector
        stop_sui_node
    }
    trap cleanup EXIT
    
    local nfdp_waf="N/A" fdp_waf="N/A"
    set +e
    
    if [ "$NONFDP" != "yes" ]; then
        nfdp_waf=$(run_fdp_disabled_benchmark)
        if [ "$NOFDP" != "yes" ]; then
            sleep 30
        fi
    fi
    
    if [ "$NOFDP" != "yes" ]; then
        fdp_waf=$(run_fdp_enabled_benchmark)
    fi
    
    set -e
    
    log_section "FINAL RESULTS"
    if [ "$NONFDP" != "yes" ]; then
        echo "Non-FDP WAF: $nfdp_waf" | tee "$RESULTS_DIR/final_comparison.txt"
        echo "  Timeseries: $RESULTS_DIR/nfdp/waf_timeseries.csv" | tee -a "$RESULTS_DIR/final_comparison.txt"
    fi
    if [ "$NOFDP" != "yes" ]; then
        echo "FDP WAF:     $fdp_waf" | tee -a "$RESULTS_DIR/final_comparison.txt"
        echo "  Timeseries: $RESULTS_DIR/fdp/waf_timeseries.csv" | tee -a "$RESULTS_DIR/final_comparison.txt"
    fi
    
    if [[ "$nfdp_waf" != "N/A" && "$fdp_waf" != "N/A" ]]; then
        local improvement=$(echo "scale=2; (($nfdp_waf - $fdp_waf) / $nfdp_waf) * 100" | bc 2>/dev/null || echo "N/A")
        echo "WAF Improvement: ${improvement}%" | tee -a "$RESULTS_DIR/final_comparison.txt"
    fi
    
    log_info "Results: $RESULTS_DIR"
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"
