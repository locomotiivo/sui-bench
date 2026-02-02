#!/bin/bash
#
# FDP LSM-Level Benchmark Script
#
# This script compares FDP-enabled vs FDP-disabled benchmarks,
# using the SAME approach as fdp-benchmark-compare.sh but with
# LSM-level FDP placement configuration for RocksDB.

set -e

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR="/home/femu/fdp-scripts/sui-bench/sui2/scripts"
FDP_TOOLS_DIR="/home/femu/fdp-scripts/f2fs-tools-fdp"
FDP_SEND_SUNGJIN="$FDP_TOOLS_DIR/fdp_send_sungjin"
MOUNT_POINT="/home/femu/f2fs_fdp_mount"
NVME_DEVICE="nvme0n1"
NVME_DEV_PATH="/dev/$NVME_DEVICE"

# Benchmark parameters
WORKERS="${WORKERS:-256}"
DURATION="${DURATION:-7200}"
BLOB_SIZE_KB="${BLOB_SIZE_KB:-200}"
BATCH_COUNT="${BATCH_COUNT:-5}"

# FDP mode
FDP_MODE="${FDP_MODE:-combined}"

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
    sudo "$FDP_SEND_SUNGJIN" "$NVME_DEV_PATH"
    sudo "$FDP_TOOLS_DIR"/mkfs/mkfs.f2fs -f -O lost_found "$NVME_DEV_PATH"
    sudo "$FDP_TOOLS_DIR/fdp_f2fs_mount" "$nlogs"
    sudo chmod -R 777 "$MOUNT_POINT"
    
    # Clean up any leftover FDP database directories to prevent corruption
    log_info "Cleaning FDP directories for fresh start..."
    for i in {0..7}; do
        rm -rf "$MOUNT_POINT/p$i/authorities_db" 2>/dev/null || true
        rm -rf "$MOUNT_POINT/p$i/consensus_db" 2>/dev/null || true
        rm -rf "$MOUNT_POINT/p$i/epoch_db" 2>/dev/null || true
        rm -rf "$MOUNT_POINT/p$i/full_node_db" 2>/dev/null || true
    done
    
    log_success "F2FS mounted at $MOUNT_POINT"
}

setup_fdp_symlinks() {
    local config_dir="${1:-$MOUNT_POINT/p0/sui_node}"
    log_info "Setting up FDP directories..."
    
    # Use fdp_mount_setup.sh for proper FDP directory structure
    if [ -x "$SCRIPT_DIR/fdp_mount_setup.sh" ]; then
        "$SCRIPT_DIR/fdp_mount_setup.sh" "$MOUNT_POINT" "$FDP_MODE"
    elif [ -x "$SCRIPT_DIR/setup-fdp-symlinks.sh" ]; then
        "$SCRIPT_DIR/setup-fdp-symlinks.sh" "$config_dir"
    else
        log_warning "FDP setup scripts not found, using basic setup"
        for i in {0..7}; do
            mkdir -p "$MOUNT_POINT/p$i"
        done
    fi
}

start_sui_node() {
    local config_dir="${1:-$MOUNT_POINT/p0/sui_node}"
    local use_fdp="${2:-false}"
    
    log_info "Starting SUI node (FDP=$use_fdp)..."
    
    export SUI_DISABLE_GAS=1
    
    if [ "$use_fdp" = "true" ]; then
        export SUI_FDP_ENABLED=1
        export SUI_FDP_BASE_PATH="$MOUNT_POINT"
        export SUI_FDP_MODE="$FDP_MODE"
        log_info "FDP env: MODE=$FDP_MODE"
    else
        unset SUI_FDP_ENABLED SUI_FDP_BASE_PATH SUI_FDP_MODE
    fi
    
    mkdir -p "$config_dir"
    sui genesis -f --working-dir "$config_dir"
    
    if [ "$use_fdp" = "true" ]; then
        setup_fdp_symlinks "$config_dir"
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

get_femu_stats_final() {
    if [ -x "$FDP_SEND_SUNGJIN" ]; then
        log_info "Collecting FEMU stats via fdp_send_sungjin (this resets stats)..."
        local output=$(sudo "$FDP_SEND_SUNGJIN" "$NVME_DEV_PATH" 2>&1)
        
        # Parse the print_sungjin output format: print_sungjin(VAR_NAME) : {VALUE}
        local write_io=$(echo "$output" | grep "sungjin_stat.write_io_n" | sed 's/.*{\([0-9]*\)}.*/\1/')
        local copied=$(echo "$output" | grep "sungjin_stat.copied" | sed 's/.*{\([0-9]*\)}.*/\1/')
        
        write_io=${write_io:-0}
        copied=${copied:-0}
        
        log_info "  Host writes: $write_io, GC copies: $copied"
        echo "$write_io $copied"
        return 0
    fi
    log_warning "fdp_send_sungjin not available"
    echo "0 0"
}

calculate_waf() {
    local host_writes=$1 gc_copies=$2
    if [ "$host_writes" -gt 0 ] 2>/dev/null; then
        echo "scale=4; 1 + $gc_copies / $host_writes" | bc
    else
        echo "N/A"
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# BENCHMARK RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

run_benchmark() {
    local mode="$1" run_dir="$2"
    local config_dir="$MOUNT_POINT/p0/sui_node"
    
    local start_time=$(date +%s)
    
    log_info "Running benchmark: WORKERS=$WORKERS, DURATION=${DURATION}s"
    
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
    
    local bench_output="$run_dir/benchmark.log"
    cd "$SCRIPT_DIR"
    ./max-device-write-bench.sh 2>&1 | tee "$bench_output"
    
    local end_time=$(date +%s)
    
    log_info "Collecting FEMU stats..."
    local femu_stats=$(get_femu_stats_final)
    local host_writes=$(echo "$femu_stats" | awk '{print $1}')
    local gc_copies=$(echo "$femu_stats" | awk '{print $2}')
    echo "$femu_stats" > "$run_dir/femu_stats.txt"
    
    local elapsed=$((end_time - start_time))
    # TPS line format: "  TPS:              $tps tx/sec"
    local tps=$(grep "TPS:" "$bench_output" | tail -1 | awk '{print $2}' | tr -d ' ')
    tps=${tps:-N/A}
    local waf=$(calculate_waf "$host_writes" "$gc_copies")
    
    echo "Mode: $mode, Duration: ${elapsed}s, TPS: $tps, WAF: $waf" > "$run_dir/summary.txt"
    log_success "Benchmark $mode: WAF=$waf, TPS=$tps"
    
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
    log_section "FDP-ENABLED BENCHMARK ($FDP_MODE)"
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
    
    log_section "FDP LSM-Level Benchmark"
    log_info "Duration: ${DURATION}s, Workers: $WORKERS, FDP Mode: $FDP_MODE"
    
    # Cache sudo credentials upfront to avoid prompts during benchmark
    log_info "Caching sudo credentials..."
    sudo -v || { log_error "sudo access required"; exit 1; }
    
    trap stop_sui_node EXIT
    
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
    fi
    if [ "$NOFDP" != "yes" ]; then
        echo "FDP WAF:     $fdp_waf" | tee -a "$RESULTS_DIR/final_comparison.txt"
    fi
    
    if [[ "$nfdp_waf" != "N/A" && "$fdp_waf" != "N/A" ]]; then
        local improvement=$(echo "scale=2; (($nfdp_waf - $fdp_waf) / $nfdp_waf) * 100" | bc 2>/dev/null || echo "N/A")
        echo "WAF Improvement: ${improvement}%" | tee -a "$RESULTS_DIR/final_comparison.txt"
    fi
    
    log_info "Results: $RESULTS_DIR"
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"
