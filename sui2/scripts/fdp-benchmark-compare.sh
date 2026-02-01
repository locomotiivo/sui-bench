#!/bin/bash
#
# fdp-benchmark-compare.sh - Compare FDP-enabled vs FDP-disabled benchmark results
#
# This script runs identical benchmarks in both modes and outputs a comparison
# report with WAF (Write Amplification Factor) and TPS metrics.
#
# Usage:
#   ./fdp-benchmark-compare.sh [OPTIONS]
#
# Options:
#   --fdp-only      Run only FDP-enabled benchmark
#   --nofdp-only    Run only FDP-disabled benchmark
#   --duration SEC  Benchmark duration in seconds (default: 1800)
#   --workers N     Number of concurrent workers (default: 256)
#   --report FILE   Output report file (default: fdp_comparison_report.txt)
#

set -e

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# Optimal parameters (determined from extensive testing)
OPTIMAL_WORKERS=256
OPTIMAL_BLOB_SIZE_KB=200
OPTIMAL_BATCH_COUNT=5
OPTIMAL_DURATION=1800  # 30 minutes for meaningful comparison

# Paths
SCRIPT_DIR="/home/femu/fdp-scripts/sui-bench/sui2/scripts"
FDP_TOOLS_DIR="/home/femu/fdp-scripts/f2fs-tools-fdp"
FDP_STATS_TOOL="$FDP_TOOLS_DIR/fdp_get_stats"
FDP_SEND_SUNGJIN="$FDP_TOOLS_DIR/fdp_send_sungjin"
MOUNT_POINT="/home/femu/f2fs_fdp_mount"
NVME_DEVICE="nvme0n1"
NVME_DEV_PATH="/dev/$NVME_DEVICE"

# Output
REPORT_DIR="${SCRIPT_DIR}/reports"
REPORT_FILE="${REPORT_DIR}/fdp_comparison.txt"
CSV_FILE="${REPORT_DIR}/fdp_comparison.csv"

# Parse arguments
RUN_FDP=true
RUN_NOFDP=true
WORKERS=$OPTIMAL_WORKERS
DURATION=$OPTIMAL_DURATION

while [[ $# -gt 0 ]]; do
    case $1 in
        --fdp-only)
            RUN_NOFDP=false
            shift
            ;;
        --nofdp-only)
            RUN_FDP=false
            shift
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --report)
            REPORT_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg"
    echo "$msg" >> "$REPORT_FILE"
}

log_section() {
    log ""
    log "═══════════════════════════════════════════════════════════════════════════════"
    log "  $1"
    log "═══════════════════════════════════════════════════════════════════════════════"
}

get_device_sectors() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats 2>/dev/null || echo 0
}

get_device_bytes() {
    local sectors=$(get_device_sectors)
    echo $((sectors * 512))
}

format_bytes() {
    local bytes=$1
    if [ $bytes -ge 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1073741824" | bc) GB"
    elif [ $bytes -ge 1048576 ]; then
        echo "$(echo "scale=2; $bytes / 1048576" | bc) MB"
    else
        echo "$bytes B"
    fi
}

check_sui_node() {
    curl -s http://127.0.0.1:9000 \
        -d '{"jsonrpc":"2.0","id":1,"method":"sui_getLatestCheckpointSequenceNumber"}' \
        -H 'Content-Type: application/json' 2>/dev/null | grep -q result
}

stop_sui_node() {
    log "Stopping SUI node..."
    pkill -9 -x "sui" 2>/dev/null || true
    pkill -9 -f "sui-node" 2>/dev/null || true
    pkill -9 -f "sui start" 2>/dev/null || true
    sleep 2
}
trap stop_sui_node EXIT

unmount_f2fs() {
    log "Unmounting F2FS..."
    if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
        echo "  Unmounting ${MOUNT_POINT}..."
        sudo umount -l ${MOUNT_POINT} 2>/dev/null || true
    fi
    sudo fuser -k ${DEVICE} 2>/dev/null || true
   
    sync
    sleep 2

    if mount | grep -q "${DEVICE}"; then
        echo "ERROR: Device still mounted!"
        mount | grep "${DEVICE}"
        exit 1
    fi
}

format_nvme() {
    log "Formatting NVMe device..."
    # Note: This requires the device to be unmounted
    sudo nvme format "$NVME_DEV_PATH" -n 1 2>/dev/null || true
}

mount_f2fs() {
    local nlogs="${1:-8}"  # Default 8 placement IDs for FDP
    log "Mounting F2FS with FDP enabled (nlogs=$nlogs)..."
    
    mkdir -p "$MOUNT_POINT"
    sudo "$FDP_SEND_SUNGJIN" "$NVME_DEV_PATH"
    sudo "$FDP_TOOLS_DIR"/mkfs/mkfs.f2fs -f -O lost_found "$NVME_DEV_PATH"
    sudo "$FDP_TOOLS_DIR/fdp_f2fs_mount" "$nlogs"
    sudo chmod -R 777 "$MOUNT_POINT"
}

setup_fdp_symlinks() {
    local config_dir="${1:-$MOUNT_POINT/p0/sui_node}"
    log "Setting up FDP symlinks for data placement..."
    
    # Use the symlink setup script if available
    if [ -x "$SCRIPT_DIR/setup-fdp-symlinks.sh" ]; then
        "$SCRIPT_DIR/setup-fdp-symlinks.sh" "$config_dir"
    else
        log "WARNING: setup-fdp-symlinks.sh not found, using basic setup"
        # Basic fallback: just ensure directories exist
        for i in {0..7}; do
            mkdir -p "$MOUNT_POINT/p$i"
        done
    fi
}

start_sui_node() {
    local config_dir="${1:-$MOUNT_POINT/p0/sui_node}"
    local use_fdp_symlinks="${2:-false}"
    
    log "Starting SUI node with config: $config_dir"
    
    export SUI_DISABLE_GAS=1
    
    # Initialize if needed
    mkdir -p "$config_dir"
    sudo chmod -R 777 "$MOUNT_POINT"
    sui genesis -f --working-dir "$config_dir"
    
    # Set up FDP symlinks AFTER genesis but BEFORE starting node
    if [ "$use_fdp_symlinks" = true ]; then
        setup_fdp_symlinks "$config_dir"
    fi
    
    # Start node in background
    SUI_CONFIG_DIR="$config_dir" sui start --network.config "$config_dir" \
        --fullnode-rpc-port 9000 --with-faucet > /tmp/sui_node.log 2>&1 &
    
    # Wait for node to be ready
    log "Waiting for SUI node to start..."
    local max_wait=120
    local waited=0
    while ! check_sui_node && [ $waited -lt $max_wait ]; do
        sleep 2
        waited=$((waited + 2))
    done
    
    if check_sui_node; then
        log "SUI node started successfully"
        return 0
    else
        log "ERROR: SUI node failed to start within ${max_wait}s"
        return 1
    fi
}

publish_contract() {
    local config_dir="${1:-$MOUNT_POINT/p0/sui_node}"
    log "Publishing bloat contract..."
    
    export SUI_CONFIG_DIR="$config_dir"
    
    set +e  # Temporarily disable exit on error
    # Request gas from faucet
    local address=$(SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui client active-address)
    log "Active address: $address"
    
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
        log "Faucet response: received $gas_amount gas"
    else
        log "WARNING: Faucet response not valid JSON: $faucet_response"
    fi
    sleep 1
    
    # Publish contract
    cd "$SCRIPT_DIR/../move/bloat_storage"
    
    # Clear any cached publication files to avoid chain ID conflicts
    rm -f Pub.localnet.toml Pub.testnet.toml 2>/dev/null || true
    
    log "Running: sui client test-publish --build-env localnet --json"
    local raw_output=$(SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui client test-publish --build-env localnet --json 2>&1)
    local exit_code=$?

    log "Publish exit code: $exit_code"
    
    # Extract JSON from output (skip build logs/warnings that precede the JSON)
    # The JSON starts with '{' at the beginning of a line
    local result=$(echo "$raw_output" | sed -n '/^{/,$p')
    
    # Check if we extracted valid JSON
    if ! echo "$result" | jq -e '.' >/dev/null 2>&1; then
        log "ERROR: Could not extract valid JSON from publish output"
        log "Raw output (first 500 chars): ${raw_output:0:500}"
        set -e
        return 1
    fi
    
    log "JSON extracted successfully"
    
    # Check for successful publish
    if [ $exit_code -eq 0 ] && echo "$result" | jq -e '.effects.V2.status == "Success"' >/dev/null 2>&1; then
        local package_id=$(echo "$result" | jq -r '.changed_objects[] | select(.objectType == "package") | .objectId')
        set -e  # Re-enable exit on error
        
        if [ -n "$package_id" ] && [ "$package_id" != "null" ]; then
            echo "$package_id" > "$config_dir/.package_id"
            log "Contract published: $package_id"
            echo "$package_id"
            return 0
        fi
    fi
    
    # If we get here, publish failed
    log "ERROR: Failed to publish contract (exit code: $exit_code)"
    log "Status from JSON: $(echo "$result" | jq -r '.effects.V2.status // "unknown"')"
    set -e
    return 1
}

get_fdp_stats() {
    # Try to get FDP-specific stats using custom tool
    if [ -x "$FDP_STATS_TOOL" ]; then
        log "Collecting FDP statistics..."
        "$FDP_STATS_TOOL" "$NVME_DEV_PATH" 2>&1 || echo "FDP stats unavailable"
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# IN-DEVICE WAF CALCULATION (using FEMU internal stats)
# ═══════════════════════════════════════════════════════════════════════════════

# Parse FEMU stats from fdp_send_sungjin output
# IMPORTANT: fdp_send_sungjin RESETS device stats after printing!
#            Only call this ONCE at the end of benchmark.
# Returns: host_writes gc_copies block_erased (space-separated)
# Output format from fdp_send_sungjin:
#   print_sungjin(ssd->sungjin_stat.write_io_n) : {VALUE}
#   print_sungjin(ssd->sungjin_stat.copied) : {VALUE}
#   print_sungjin(ssd->sungjin_stat.block_erased) : {VALUE}
get_femu_stats_final() {
    local stats_output
    
    if [ -x "$FDP_SEND_SUNGJIN" ]; then
        log "Collecting FEMU stats via fdp_send_sungjin (WARNING: this resets stats)..."
        stats_output=$(sudo "$FDP_SEND_SUNGJIN" "$NVME_DEV_PATH" 2>&1)
        
        # Parse the print_sungjin output format: print_sungjin(VAR_NAME) : {VALUE}
        local host_writes=$(echo "$stats_output" | grep "sungjin_stat.write_io_n" | sed 's/.*{\([0-9]*\)}.*/\1/')
        local gc_copies=$(echo "$stats_output" | grep "sungjin_stat.copied" | sed 's/.*{\([0-9]*\)}.*/\1/')
        local block_erased=$(echo "$stats_output" | grep "sungjin_stat.block_erased" | sed 's/.*{\([0-9]*\)}.*/\1/')
        local discards=$(echo "$stats_output" | grep "sungjin_stat.discard)" | sed 's/.*{\([0-9]*\)}.*/\1/')
        local read_io=$(echo "$stats_output" | grep "sungjin_stat.read_io_n" | sed 's/.*{\([0-9]*\)}.*/\1/')
        
        host_writes=${host_writes:-0}
        gc_copies=${gc_copies:-0}
        block_erased=${block_erased:-0}
        discards=${discards:-0}
        read_io=${read_io:-0}
        
        log "  FEMU stats captured:"
        log "    Host writes (write_io_n): $host_writes"
        log "    GC copies (copied):       $gc_copies"
        log "    Blocks erased:            $block_erased"
        log "    Discards:                 $discards"
        log "    Read I/Os:                $read_io"
        
        echo "$host_writes $gc_copies $block_erased $discards $read_io"
        return 0
    fi
    
    log "WARNING: fdp_send_sungjin not available"
    echo "0 0 0 0 0"
    return 1
}

# Placeholder for "before" stats - these are captured fresh after device reset
# Since fdp_send_sungjin resets stats, we assume start values are 0
get_femu_stats() {
    # For non-destructive reads, return zeros as baseline
    # The real stats will be collected at the end with get_femu_stats_final
    echo "0 0 0 0 0"
}

# Calculate in-device WAF from cumulative stats (since start is 0 after reset)
# WAF = 1 + (gc_copies / host_writes)
calculate_in_device_waf() {
    local host_writes=$1
    local gc_copies=$2
    local block_erased=${3:-0}
    local discards=${4:-0}
    local read_io=${5:-0}
    
    log "In-device WAF calculation:"
    log "  Host writes (write_io_n): $host_writes"
    log "  GC copies (copied):       $gc_copies"
    log "  Blocks erased:            $block_erased"
    log "  Discards:                 $discards"
    log "  Read I/Os:                $read_io"
    
    if [ "$host_writes" -gt 0 ]; then
        # WAF = 1 + gc_copies / host_writes
        local waf=$(echo "scale=3; 1 + $gc_copies / $host_writes" | bc 2>/dev/null)
        log "  In-device WAF: $waf (formula: 1 + $gc_copies / $host_writes)"
        echo "$waf"
    else
        log "  In-device WAF: N/A (no host writes recorded)"
        echo "N/A"
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# BENCHMARK RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

run_benchmark() {
    local mode="$1"  # "fdp" or "nofdp"
    local result_var="$2"
    
    log_section "RUNNING BENCHMARK: $mode mode"
    
    local config_dir="$MOUNT_POINT/p0/sui_node"
    
    # Record initial state (host-side)
    local start_sectors=$(get_device_sectors)
    local start_time=$(date +%s)
    
    # NOTE: FEMU stats start at 0 after mount (fdp_send_sungjin resets stats)
    # We'll collect final cumulative stats at the end
    log "Note: FEMU stats start at 0 (fresh mount/reset)"
    
    # Run benchmark
    log "Starting benchmark with: WORKERS=$WORKERS, DURATION=${DURATION}s"
    log "  Blob size: ${OPTIMAL_BLOB_SIZE_KB}KB x ${OPTIMAL_BATCH_COUNT} = $((OPTIMAL_BLOB_SIZE_KB * OPTIMAL_BATCH_COUNT))KB per TX"
    
    export SUI_CONFIG_DIR="$config_dir"
    export WORKERS="$WORKERS"
    export DURATION="$DURATION"
    export BLOB_SIZE_KB="$OPTIMAL_BLOB_SIZE_KB"
    export BATCH_COUNT="$OPTIMAL_BATCH_COUNT"
    
    # Run benchmark and capture output
    local bench_output=$(mktemp)
    cd "$SCRIPT_DIR"
    ./max-device-write-bench.sh 2>&1 | tee "$bench_output"
    
    # Record final state (host-side)
    local end_sectors=$(get_device_sectors)
    local end_time=$(date +%s)
    
    # CRITICAL: Get FEMU stats AFTER benchmark completes
    # NOTE: fdp_send_sungjin RESETS stats after printing, so only call ONCE at end!
    log "Capturing final FEMU stats (this will reset device counters)..."
    local femu_stats=$(get_femu_stats_final)
    local host_writes=$(echo "$femu_stats" | awk '{print $1}')
    local gc_copies=$(echo "$femu_stats" | awk '{print $2}')
    local block_erased=$(echo "$femu_stats" | awk '{print $3}')
    local discards=$(echo "$femu_stats" | awk '{print $4}')
    local read_io=$(echo "$femu_stats" | awk '{print $5}')
    
    # Calculate host-side metrics
    local total_sectors=$((end_sectors - start_sectors))
    local total_bytes=$((total_sectors * 512))
    local elapsed=$((end_time - start_time))
    
    # Extract TPS from benchmark output
    local tps=$(grep "TPS:" "$bench_output" | tail -1 | awk '{print $2}')
    
    # Extract success count and calculate app writes
    local success=$(grep "Success:" "$bench_output" | tail -1 | awk -F'[: ,]+' '{for(i=1;i<=NF;i++) if($i=="Success") print $(i+1)}')
    local app_bytes=$((success * OPTIMAL_BLOB_SIZE_KB * OPTIMAL_BATCH_COUNT * 1024))
    
    # Calculate host-side WAF (App vs Device)
    local host_waf="N/A"
    if [ "$app_bytes" -gt 0 ] 2>/dev/null; then
        host_waf=$(echo "scale=3; $total_bytes / $app_bytes" | bc 2>/dev/null || echo "N/A")
    fi
    
    # Calculate IN-DEVICE WAF (using FEMU stats)
    # WAF = 1 + gc_copies / host_writes
    local in_device_waf=$(calculate_in_device_waf "$host_writes" "$gc_copies" "$block_erased" "$discards" "$read_io")
    
    # Calculate rates
    local rate_mb_min=0
    if [ "$elapsed" -gt 0 ]; then
        rate_mb_min=$((total_bytes * 60 / elapsed / 1024 / 1024))
    fi
    
    # Get FDP-specific stats for logging
    local fdp_stats=""
    if [ "$mode" = "fdp" ]; then
        fdp_stats=$(get_fdp_stats)
    fi
    
    # Store results (use in-device WAF as primary WAF metric)
    eval "${result_var}_MODE='$mode'"
    eval "${result_var}_ELAPSED='$elapsed'"
    eval "${result_var}_TPS='$tps'"
    eval "${result_var}_DEVICE_BYTES='$total_bytes'"
    eval "${result_var}_APP_BYTES='$app_bytes'"
    eval "${result_var}_HOST_WAF='$host_waf'"
    eval "${result_var}_WAF='$in_device_waf'"
    eval "${result_var}_RATE_MB_MIN='$rate_mb_min'"
    eval "${result_var}_SUCCESS='$success'"
    eval "${result_var}_HOST_WRITES='$host_writes'"
    eval "${result_var}_GC_COPIES='$gc_copies'"
    eval "${result_var}_BLOCK_ERASED='$block_erased'"
    eval "${result_var}_DISCARDS='$discards'"
    eval "${result_var}_READ_IO='$read_io'"
    
    rm -f "$bench_output"
    
    log ""
    log "Benchmark $mode completed:"
    log "  Duration:       ${elapsed}s"
    log "  TPS:            $tps tx/sec"
    log "  Device bytes:   $(format_bytes $total_bytes)"
    log "  App bytes:      $(format_bytes $app_bytes)"
    log "  Host WAF:       ${host_waf}x (app vs device)"
    log "  In-Device WAF:  ${in_device_waf}x (1 + gc_copies/host_writes)"
    log "  Rate:           ${rate_mb_min} MB/min"
    log "  Host writes:    $host_writes"
    log "  GC copies:      $gc_copies"
    log "  Blocks erased:  $block_erased"
    log "  Discards:       $discards"
}

# ═══════════════════════════════════════════════════════════════════════════════
# REPORT GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

generate_report() {
    log_section "COMPARISON REPORT"
    
    log ""
    log "Test Configuration:"
    log "  Workers:        $WORKERS"
    log "  Duration:       ${DURATION}s per test"
    log "  Blob Size:      ${OPTIMAL_BLOB_SIZE_KB}KB"
    log "  Batch Count:    $OPTIMAL_BATCH_COUNT"
    log "  Bytes per TX:   $((OPTIMAL_BLOB_SIZE_KB * OPTIMAL_BATCH_COUNT))KB"
    log ""
    log "WAF Formula: In-Device WAF = 1 + (GC copies / Host writes)"
    log ""
    
    # Table header
    log "┌──────────────────┬─────────────────┬─────────────────┬─────────────┐"
    log "│ Metric           │ FDP Disabled    │ FDP Enabled     │ Improvement │"
    log "├──────────────────┼─────────────────┼─────────────────┼─────────────┤"
    
    if [ -n "$NOFDP_TPS" ] && [ -n "$FDP_TPS" ]; then
        local tps_imp=$(echo "scale=1; ($FDP_TPS - $NOFDP_TPS) / $NOFDP_TPS * 100" | bc 2>/dev/null || echo "N/A")
        printf "│ %-16s │ %15s │ %15s │ %+10s%% │\n" \
            "TPS (tx/sec)" "$NOFDP_TPS" "$FDP_TPS" "$tps_imp" | tee -a "$REPORT_FILE"
    fi
    
    # In-Device WAF (primary metric)
    if [ -n "$NOFDP_WAF" ] && [ -n "$FDP_WAF" ] && [ "$NOFDP_WAF" != "N/A" ] && [ "$FDP_WAF" != "N/A" ]; then
        # For WAF, lower is better, so improvement = (old - new) / old * 100
        local waf_imp=$(echo "scale=1; ($NOFDP_WAF - $FDP_WAF) / $NOFDP_WAF * 100" | bc 2>/dev/null || echo "N/A")
        printf "│ %-16s │ %14sx │ %14sx │ %+10s%% │\n" \
            "In-Device WAF" "$NOFDP_WAF" "$FDP_WAF" "$waf_imp" | tee -a "$REPORT_FILE"
    elif [ -n "$NOFDP_WAF" ] || [ -n "$FDP_WAF" ]; then
        printf "│ %-16s │ %14sx │ %14sx │             │\n" \
            "In-Device WAF" "${NOFDP_WAF:-N/A}" "${FDP_WAF:-N/A}" | tee -a "$REPORT_FILE"
    fi
    
    # Host writes (internal stat)
    if [ -n "$NOFDP_HOST_WRITES" ] && [ -n "$FDP_HOST_WRITES" ]; then
        printf "│ %-16s │ %15s │ %15s │             │\n" \
            "Host Writes" "$NOFDP_HOST_WRITES" "$FDP_HOST_WRITES" | tee -a "$REPORT_FILE"
    fi
    
    # GC copies (internal stat)
    if [ -n "$NOFDP_GC_COPIES" ] && [ -n "$FDP_GC_COPIES" ]; then
        local gc_imp=""
        if [ "$NOFDP_GC_COPIES" -gt 0 ] 2>/dev/null; then
            gc_imp=$(echo "scale=1; ($NOFDP_GC_COPIES - $FDP_GC_COPIES) / $NOFDP_GC_COPIES * 100" | bc 2>/dev/null || echo "N/A")
        fi
        printf "│ %-16s │ %15s │ %15s │ %+10s%% │\n" \
            "GC Copies" "$NOFDP_GC_COPIES" "$FDP_GC_COPIES" "$gc_imp" | tee -a "$REPORT_FILE"
    fi
    
    if [ -n "$NOFDP_RATE_MB_MIN" ] && [ -n "$FDP_RATE_MB_MIN" ]; then
        local rate_imp=$(echo "scale=1; ($FDP_RATE_MB_MIN - $NOFDP_RATE_MB_MIN) / $NOFDP_RATE_MB_MIN * 100" | bc 2>/dev/null || echo "N/A")
        printf "│ %-16s │ %12s MB │ %12s MB │ %+10s%% │\n" \
            "Rate (MB/min)" "$NOFDP_RATE_MB_MIN" "$FDP_RATE_MB_MIN" "$rate_imp" | tee -a "$REPORT_FILE"
    fi
    
    if [ -n "$NOFDP_DEVICE_BYTES" ] && [ -n "$FDP_DEVICE_BYTES" ]; then
        local nofdp_dev=$(echo "scale=2; $NOFDP_DEVICE_BYTES / 1073741824" | bc)
        local fdp_dev=$(echo "scale=2; $FDP_DEVICE_BYTES / 1073741824" | bc)
        printf "│ %-16s │ %12s GB │ %12s GB │             │\n" \
            "Device Writes" "$nofdp_dev" "$fdp_dev" | tee -a "$REPORT_FILE"
    fi
    
    if [ -n "$NOFDP_APP_BYTES" ] && [ -n "$FDP_APP_BYTES" ]; then
        local nofdp_app=$(echo "scale=2; $NOFDP_APP_BYTES / 1073741824" | bc)
        local fdp_app=$(echo "scale=2; $FDP_APP_BYTES / 1073741824" | bc)
        printf "│ %-16s │ %12s GB │ %12s GB │             │\n" \
            "App Writes" "$nofdp_app" "$fdp_app" | tee -a "$REPORT_FILE"
    fi
    
    log "└──────────────────┴─────────────────┴─────────────────┴─────────────┘"
    log ""
    
    # CSV output with extended stats
    echo "mode,duration,tps,in_device_waf,host_writes,gc_copies,device_bytes,app_bytes,rate_mb_min,success" > "$CSV_FILE"
    if [ -n "$NOFDP_TPS" ]; then
        echo "nofdp,$NOFDP_ELAPSED,$NOFDP_TPS,$NOFDP_WAF,$NOFDP_HOST_WRITES,$NOFDP_GC_COPIES,$NOFDP_DEVICE_BYTES,$NOFDP_APP_BYTES,$NOFDP_RATE_MB_MIN,$NOFDP_SUCCESS" >> "$CSV_FILE"
    fi
    if [ -n "$FDP_TPS" ]; then
        echo "fdp,$FDP_ELAPSED,$FDP_TPS,$FDP_WAF,$FDP_HOST_WRITES,$FDP_GC_COPIES,$FDP_DEVICE_BYTES,$FDP_APP_BYTES,$FDP_RATE_MB_MIN,$FDP_SUCCESS" >> "$CSV_FILE"
    fi
    
    log "Results saved to:"
    log "  Report: $REPORT_FILE"
    log "  CSV:    $CSV_FILE"
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

main() {
    mkdir -p "$REPORT_DIR"
    
    log_section "FDP vs Non-FDP BENCHMARK COMPARISON"
    log ""
    log "Started: $(date)"
    log "Report:  $REPORT_FILE"
    log ""
    
    # Declare result variables
    declare NOFDP_MODE NOFDP_ELAPSED NOFDP_TPS NOFDP_DEVICE_BYTES NOFDP_APP_BYTES NOFDP_WAF NOFDP_RATE_MB_MIN NOFDP_SUCCESS
    declare FDP_MODE FDP_ELAPSED FDP_TPS FDP_DEVICE_BYTES FDP_APP_BYTES FDP_WAF FDP_RATE_MB_MIN FDP_SUCCESS
    
    # Run FDP-disabled benchmark (baseline - no symlinks, all data to p0)
    if [ "$RUN_NOFDP" = true ]; then
        log_section "PHASE 1: FDP-DISABLED BENCHMARK (baseline)"
        log "Data placement: All data goes to p0 (single PID)"
        
        # Setup
        stop_sui_node
        unmount_f2fs
        mount_f2fs 8
        start_sui_node "$MOUNT_POINT/p0/sui_node" false  # No FDP symlinks
        publish_contract
        
        # Run
        run_benchmark "nofdp" "NOFDP"
        
        # Cleanup
        stop_sui_node
        unmount_f2fs
        
        log "Cooling down for 30s before next test..."
        sleep 30
    fi
    
    # Run FDP-enabled benchmark (with symlink-based data placement)
    if [ "$RUN_FDP" = true ]; then
        log_section "PHASE 2: FDP-ENABLED BENCHMARK (symlink-based placement)"
        log "Data placement: Using symlinks to distribute data across PIDs 0-7"
        
        # Setup
        stop_sui_node
        unmount_f2fs
        mount_f2fs 8  # 8 placement IDs
        start_sui_node "$MOUNT_POINT/p0/sui_node" true  # USE FDP symlinks
        publish_contract
        
        # Run
        run_benchmark "fdp" "FDP"
        
        # Collect FDP stats
        get_fdp_stats >> "$REPORT_FILE"
        
        # Cleanup
        stop_sui_node
        unmount_f2fs
    fi
    
    # Generate comparison report
    generate_report
    
    log_section "BENCHMARK COMPLETE"
    log "Total time: $(($(date +%s) - START_TIME))s"
}

START_TIME=$(date +%s)
main "$@"
