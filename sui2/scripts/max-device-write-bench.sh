#!/bin/bash
#
# max-device-write-bench.sh - Maximum Device Write Rate Benchmark
#
# This benchmark focuses purely on maximizing device write rate (not app writes).
# It runs fewer workers (avoiding gas contention) but with larger batches.
#
# Key insight: The bottleneck is CLI/gas contention, not the SUI node.
# Running 8-16 workers with 10MB/tx batches is more efficient than
# running 64 workers with smaller batches.
#

# set -e

SUI_CONFIG_DIR="${SUI_CONFIG_DIR:-$HOME/f2fs_fdp_mount/p0/sui_node}"
PACKAGE_ID="${PACKAGE_ID:-$(cat $SUI_CONFIG_DIR/.package_id 2>/dev/null || echo "")}"
NVME_DEVICE="${NVME_DEVICE:-nvme0n1}"

# Conservative settings to avoid gas contention
WORKERS="${WORKERS:-16}"
DURATION="${DURATION:-120}"

# Batch size (can be overridden via env vars)
BLOB_SIZE_KB="${BLOB_SIZE_KB:-150}"
BATCH_COUNT="${BATCH_COUNT:-40}"
BYTES_PER_TX=$((BLOB_SIZE_KB * BATCH_COUNT * 1024))

export SUI_CONFIG_DIR

log() { echo "[$(date '+%H:%M:%S')] $1"; }

get_device_bytes() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10 * 512}' /proc/diskstats 2>/dev/null || echo 0
}

format_bytes() {
    local bytes=$1
    if [ $bytes -ge 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1024 / 1024 / 1024" | bc) GB"
    else
        echo "$(echo "scale=1; $bytes / 1024 / 1024" | bc) MB"
    fi
}

# Validate
if [ -z "$PACKAGE_ID" ]; then
    log "ERROR: No package ID found. Run sui-benchmark.sh first to publish."
    exit 1
fi

# Check node is responding
if ! curl -s http://127.0.0.1:9000/health >/dev/null 2>&1; then
    log "WARNING: Health endpoint not responding, checking RPC..."
fi
if ! curl -s http://127.0.0.1:9000 -d '{"jsonrpc":"2.0","id":1,"method":"sui_getLatestCheckpointSequenceNumber"}' -H 'Content-Type: application/json' | grep -q result; then
    log "ERROR: SUI node not responding on port 9000"
    exit 1
fi

log "═══════════════════════════════════════════════════════════════"
log "  MAXIMUM DEVICE WRITE BENCHMARK"
log "═══════════════════════════════════════════════════════════════"
log ""
log "  Package:    $PACKAGE_ID"
log "  Workers:    $WORKERS (conservative to avoid gas contention)"
log "  Duration:   ${DURATION}s"
log "  Per TX:     ${BLOB_SIZE_KB}KB × ${BATCH_COUNT} = $((BYTES_PER_TX / 1024 / 1024))MB"
log "  Device:     /dev/$NVME_DEVICE"
log ""

# Record baseline
device_start=$(get_device_bytes)
time_start=$(date +%s)
end_time=$((time_start + DURATION))

log "Initial device writes: $(format_bytes $device_start)"
log ""

# Create result tracking file
RESULT_FILE=$(mktemp)
trap "rm -f $RESULT_FILE" EXIT

# Worker function - uses timeout and fire-and-forget
worker() {
    local id=$1
    while [ $(date +%s) -lt $end_time ]; do
        # Fire transaction with 60s timeout (increased for large batches)
        if timeout 60 sui client call \
            --package "$PACKAGE_ID" \
            --module bloat \
            --function create_blobs_batch \
            --args "$BLOB_SIZE_KB" "$BATCH_COUNT" \
            --gas-budget 5000000000 \
            --json 2>/dev/null | grep -q '"status"'; then
            echo "S" >> "$RESULT_FILE"
        else
            echo "F" >> "$RESULT_FILE"
        fi
    done
}

log "Starting $WORKERS workers..."
log ""

# Start workers with slight stagger to reduce initial contention
PIDS=""
for i in $(seq 1 $WORKERS); do
    worker $i &
    PIDS="$PIDS $!"
    sleep 0.2  # Stagger starts
done

log "All workers started. Monitoring device writes..."
log ""

# Monitor loop
prev_bytes=$device_start
prev_time=$time_start

while [ $(date +%s) -lt $end_time ]; do
    sleep 10
    
    now=$(date +%s)
    elapsed=$((now - time_start))
    
    # Current stats
    device_now=$(get_device_bytes)
    total_written=$((device_now - device_start))
    interval_written=$((device_now - prev_bytes))
    interval_time=$((now - prev_time))
    
    # Success/fail counts - use grep with default 0 for no matches
    success=$(grep -c "^S$" "$RESULT_FILE" 2>/dev/null) || success=0
    fail=$(grep -c "^F$" "$RESULT_FILE" 2>/dev/null) || fail=0
    [ -z "$success" ] && success=0
    [ -z "$fail" ] && fail=0
    app_bytes=$((success * BYTES_PER_TX))
    
    # Rates
    if [ $elapsed -gt 0 ] && [ $total_written -gt 0 ]; then
        avg_rate=$((total_written * 60 / elapsed / 1024 / 1024))
        avg_rate_gb=$(echo "scale=2; $avg_rate / 1024" | bc 2>/dev/null || echo "0")
    else
        avg_rate=0
        avg_rate_gb="0"
    fi
    
    if [ $interval_time -gt 0 ] && [ $interval_written -gt 0 ]; then
        instant_rate=$((interval_written * 60 / interval_time / 1024 / 1024))
    else
        instant_rate=0
    fi
    
    # Write amplification
    if [ "$app_bytes" -gt 0 ] 2>/dev/null; then
        wa=$(echo "scale=2; $total_written / $app_bytes" | bc 2>/dev/null || echo "N/A")
    else
        wa="N/A"
    fi
    
    log "After ${elapsed}s: Txs=$((success+fail)) (ok=$success, fail=$fail)"
    log "  Device: $(format_bytes $total_written), Rate=${avg_rate} MB/min (${avg_rate_gb} GB/min), Instant=${instant_rate} MB/min"
    log "  App: $(format_bytes $app_bytes), WA=${wa}x"
    log ""
    
    prev_bytes=$device_now
    prev_time=$now
done

# Wait for workers
log "Waiting for workers to complete..."
for pid in $PIDS; do
    wait $pid 2>/dev/null || true
done

# Final stats
time_end=$(date +%s)
device_end=$(get_device_bytes)
total_elapsed=$((time_end - time_start))
total_device_bytes=$((device_end - device_start))

success=$(grep -c "^S$" "$RESULT_FILE" 2>/dev/null) || success=0
fail=$(grep -c "^F$" "$RESULT_FILE" 2>/dev/null) || fail=0
[ -z "$success" ] && success=0
[ -z "$fail" ] && fail=0
total_app_bytes=$((success * BYTES_PER_TX))

rate_mb_min=$((total_device_bytes * 60 / total_elapsed / 1024 / 1024))
rate_gb_min=$(echo "scale=2; $rate_mb_min / 1024" | bc 2>/dev/null || echo "0")

# Calculate TPS (transactions per second)
if [ "$total_elapsed" -gt 0 ]; then
    tps=$(echo "scale=2; $success / $total_elapsed" | bc 2>/dev/null || echo "0")
else
    tps=0
fi

if [ "$total_app_bytes" -gt 0 ] 2>/dev/null; then
    final_wa=$(echo "scale=2; $total_device_bytes / $total_app_bytes" | bc 2>/dev/null || echo "N/A")
else
    final_wa="N/A"
fi

# Export for fdp-stats.sh
export APP_BYTES=$total_app_bytes

log ""
log "═══════════════════════════════════════════════════════════════"
log "  BENCHMARK COMPLETE"
log "═══════════════════════════════════════════════════════════════"
log ""
log "  Duration:         ${total_elapsed}s"
log "  Workers:          $WORKERS"
log "  Transactions:     $((success + fail)) (Success: $success, Failed: $fail)"
log "  TPS:              $tps tx/sec"
log ""
log "  ─── APPLICATION WRITES ───"
log "  Total:            $(format_bytes $total_app_bytes)"
log ""
log "  ─── DEVICE WRITES (actual SSD I/O) ───"
log "  Total:            $(format_bytes $total_device_bytes)"
log "  Rate:             ${rate_mb_min} MB/min (${rate_gb_min} GB/min)"
log "  Write Amp (WAF):  ${final_wa}x"
log ""
log "  Target for FDP GC: 5-10 GB/min (5120-10240 MB/min)"
if [ $rate_mb_min -ge 5120 ]; then
    log "  Status:           ✓ TARGET ACHIEVED!"
elif [ $rate_mb_min -ge 2048 ]; then
    log "  Status:           ~ Close to target (try more workers)"
else
    log "  Status:           ✗ Need more throughput"
fi
log ""
# ═══════════════════════════════════════════════════════════════
# RESEARCH OUTPUT - Machine-readable format for FDP comparison
# ═══════════════════════════════════════════════════════════════

# Determine FDP mode from mount options
FDP_MODE="nofdp"
if mount | grep -q "f2fs.*fdp_log_n"; then
    FDP_MODE="fdp"
fi

# Get total device writes (all time)
total_device_sectors=$(awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats)
total_device_all_time=$((total_device_sectors * 512))
total_device_all_time_gb=$(echo "scale=2; $total_device_all_time / 1073741824" | bc 2>/dev/null || echo "0")

# Get F2FS GC stats
gc_bg=$(cat /sys/fs/f2fs/$NVME_DEVICE/gc_background_calls 2>/dev/null || echo "0")
gc_fg=$(cat /sys/fs/f2fs/$NVME_DEVICE/gc_foreground_calls 2>/dev/null || echo "0")

# Calculate device capacity overwrite ratio
device_capacity_gb=64
overwrite_ratio=$(echo "scale=2; $total_device_all_time_gb / $device_capacity_gb" | bc 2>/dev/null || echo "0")

log "═══════════════════════════════════════════════════════════════"
log "  RESEARCH METRICS (for FDP comparison)"
log "═══════════════════════════════════════════════════════════════"
log ""
log "  # Copy these for your research data:"
log "  MODE=$FDP_MODE"
log "  DURATION_SEC=$total_elapsed"
log "  WORKERS=$WORKERS"
log "  TPS=$tps"
log "  SUCCESS_TX=$success"
log "  FAILED_TX=$fail"
log "  APP_WRITES_BYTES=$total_app_bytes"
log "  DEVICE_WRITES_BYTES=$total_device_bytes"
log "  WAF=$final_wa"
log "  DEVICE_RATE_MB_MIN=$rate_mb_min"
log "  TOTAL_DEVICE_WRITES_GB=$total_device_all_time_gb"
log "  OVERWRITE_RATIO=$overwrite_ratio"
log "  F2FS_GC_BACKGROUND=$gc_bg"
log "  F2FS_GC_FOREGROUND=$gc_fg"
log ""

# Save to CSV file for easy comparison
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"
RESULTS_CSV="$RESULTS_DIR/benchmark_results.csv"

# Create header if file doesn't exist
if [ ! -f "$RESULTS_CSV" ]; then
    echo "timestamp,mode,duration_sec,workers,tps,success_tx,failed_tx,app_writes_bytes,device_writes_bytes,waf,rate_mb_min,total_device_gb,overwrite_ratio,gc_bg,gc_fg" > "$RESULTS_CSV"
fi

# Append results
echo "$(date -Iseconds),$FDP_MODE,$total_elapsed,$WORKERS,$tps,$success,$fail,$total_app_bytes,$total_device_bytes,$final_wa,$rate_mb_min,$total_device_all_time_gb,$overwrite_ratio,$gc_bg,$gc_fg" >> "$RESULTS_CSV"

log "  Results appended to: $RESULTS_CSV"
log ""

# Cleanup
rm -f "$RESULT_FILE"