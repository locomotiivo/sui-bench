#!/bin/bash
#
# update-heavy-bench.sh - Update-Heavy Benchmark for Maximum LSM Churn
#
# This benchmark maximizes LSM-tree version churn by repeatedly updating
# the same objects, which triggers compaction and garbage collection.
#
# Strategy:
# 1. Warmup: Create a pool of blob objects
# 2. Churn:  80% updates to existing blobs, 20% new creates
#
# Why this works:
# - Updates create new object versions in RocksDB
# - Each version accumulates until compaction
# - Compaction merges versions (write amplification)
# - Old versions become tombstones for GC
#

set -e

SUI_CONFIG_DIR="${SUI_CONFIG_DIR:-$HOME/f2fs_fdp_mount/p0/sui_node}"
PACKAGE_ID="${PACKAGE_ID:-$(cat $SUI_CONFIG_DIR/.package_id 2>/dev/null || echo "")}"
NVME_DEVICE="${NVME_DEVICE:-nvme0n1}"

# Conservative settings
WORKERS="${WORKERS:-8}"
DURATION="${DURATION:-7200}"

# Blob settings
BLOB_SIZE_KB="${BLOB_SIZE_KB:-150}"
BATCH_COUNT="${BATCH_COUNT:-10}"
BYTES_PER_TX=$((BLOB_SIZE_KB * BATCH_COUNT * 1024))

# Update-heavy parameters
UPDATE_POOL_SIZE="${UPDATE_POOL_SIZE:-100}"   # Blobs per worker to maintain
UPDATE_RATIO="${UPDATE_RATIO:-80}"             # % updates vs creates (after warmup)
WARMUP_BATCHES=$((UPDATE_POOL_SIZE / BATCH_COUNT + 1))

export SUI_CONFIG_DIR

log() { echo "[$(date '+%H:%M:%S')] $1"; }

get_device_bytes() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10 * 512}' /proc/diskstats 2>/dev/null || echo 0
}

format_bytes() {
    local bytes=$1
    if [ "$bytes" -ge 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1024 / 1024 / 1024" | bc) GB"
    else
        echo "$(echo "scale=1; $bytes / 1024 / 1024" | bc) MB"
    fi
}

# Validate
if [ -z "$PACKAGE_ID" ]; then
    log "ERROR: No package ID found. Set PACKAGE_ID or run publish first."
    exit 1
fi

# Check node
if ! curl -s http://127.0.0.1:9000 -d '{"jsonrpc":"2.0","id":1,"method":"sui_getLatestCheckpointSequenceNumber"}' -H 'Content-Type: application/json' | grep -q result; then
    log "ERROR: SUI node not responding on port 9000"
    exit 1
fi

log "═══════════════════════════════════════════════════════════════"
log "  UPDATE-HEAVY BENCHMARK (Maximum LSM Churn)"
log "═══════════════════════════════════════════════════════════════"
log ""
log "  Package:       $PACKAGE_ID"
log "  Workers:       $WORKERS"
log "  Duration:      ${DURATION}s"
log "  Per TX:        ${BLOB_SIZE_KB}KB × ${BATCH_COUNT} = $((BYTES_PER_TX / 1024))KB"
log "  Device:        /dev/$NVME_DEVICE"
log ""
log "  Update Pool:   ${UPDATE_POOL_SIZE} blobs/worker"
log "  Update Ratio:  ${UPDATE_RATIO}% updates after warmup"
log "  Warmup:        ${WARMUP_BATCHES} batches to build pool"
log ""

# Record baseline
device_start=$(get_device_bytes)
time_start=$(date +%s)
end_time=$((time_start + DURATION))

log "Initial device writes: $(format_bytes $device_start)"
log ""

# Working directories
WORK_DIR=$(mktemp -d)
RESULT_FILE="$WORK_DIR/results"
touch "$RESULT_FILE"

trap "rm -rf $WORK_DIR" EXIT

# Worker function with update-heavy logic
update_worker() {
    local id=$1
    local blob_file="$WORK_DIR/blobs_${id}"
    touch "$blob_file"
    
    local iteration=0
    local warmup_done=false
    
    while [ $(date +%s) -lt $end_time ]; do
        iteration=$((iteration + 1))
        local blob_count=$(wc -l < "$blob_file" 2>/dev/null || echo "0")
        
        # Decide: warmup (create) or churn (update/create mix)
        local do_update=false
        if [ "$blob_count" -ge "$UPDATE_POOL_SIZE" ]; then
            # We have enough blobs - use update ratio
            if [ "$warmup_done" != "true" ]; then
                log "Worker $id: Warmup complete ($blob_count blobs). Entering churn mode."
                warmup_done=true
            fi
            # Check if we should update
            local rand=$((RANDOM % 100))
            if [ $rand -lt $UPDATE_RATIO ]; then
                do_update=true
            fi
        fi
        
        if [ "$do_update" = "true" ] && [ "$blob_count" -gt 0 ]; then
            # UPDATE: Pick a random blob to update
            local line_num=$((RANDOM % blob_count + 1))
            local blob_id=$(sed -n "${line_num}p" "$blob_file")
            
            if [ -n "$blob_id" ]; then
                # Vary the size slightly on each update
                local new_size=$((BLOB_SIZE_KB + (iteration % 10) * 10))
                
                # Update the blob
                if timeout 60 sui client call \
                    --package "$PACKAGE_ID" \
                    --module bloat \
                    --function update_blob \
                    --args "$blob_id" "$new_size" \
                    --gas-budget 2000000000 \
                    --json 2>/dev/null | grep -q '"status"'; then
                    echo "U" >> "$RESULT_FILE"
                else
                    echo "F" >> "$RESULT_FILE"
                fi
            fi
        else
            # CREATE: Create new blobs and track their IDs
            local output=$(timeout 60 sui client call \
                --package "$PACKAGE_ID" \
                --module bloat \
                --function create_blobs_batch \
                --args "$BLOB_SIZE_KB" "$BATCH_COUNT" \
                --gas-budget 5000000000 \
                --json 2>/dev/null)
            
            if echo "$output" | grep -q '"status"'; then
                echo "C" >> "$RESULT_FILE"
                
                # Extract created object IDs and add to pool
                # Look for objects with objectType containing "Blob"
                local new_ids=$(echo "$output" | jq -r '
                    .objectChanges[]? 
                    | select(.type == "created") 
                    | select(.objectType | contains("Blob")) 
                    | .objectId' 2>/dev/null)
                
                if [ -n "$new_ids" ]; then
                    echo "$new_ids" >> "$blob_file"
                fi
                
                # Trim pool if too large (FIFO rotation)
                local current_count=$(wc -l < "$blob_file")
                if [ "$current_count" -gt $((UPDATE_POOL_SIZE * 2)) ]; then
                    tail -n $UPDATE_POOL_SIZE "$blob_file" > "$blob_file.tmp"
                    mv "$blob_file.tmp" "$blob_file"
                fi
            else
                echo "F" >> "$RESULT_FILE"
            fi
        fi
    done
}

log "Starting $WORKERS workers..."
log ""

# Start workers
PIDS=""
for i in $(seq 1 $WORKERS); do
    update_worker $i &
    PIDS="$PIDS $!"
    sleep 0.3  # Stagger starts
done

log "All workers started. Monitoring..."
log ""

# Monitor loop
prev_bytes=$device_start
prev_time=$time_start

while [ $(date +%s) -lt $end_time ]; do
    sleep 15
    
    now=$(date +%s)
    elapsed=$((now - time_start))
    
    # Current stats
    device_now=$(get_device_bytes)
    total_written=$((device_now - device_start))
    interval_written=$((device_now - prev_bytes))
    interval_time=$((now - prev_time))
    
    # Transaction counts
    creates=$(grep -c "^C$" "$RESULT_FILE" 2>/dev/null) || creates=0
    updates=$(grep -c "^U$" "$RESULT_FILE" 2>/dev/null) || updates=0
    fails=$(grep -c "^F$" "$RESULT_FILE" 2>/dev/null) || fails=0
    total_tx=$((creates + updates + fails))
    success_tx=$((creates + updates))
    
    # Approximate app bytes (creates = batch, updates = single blob)
    app_bytes=$((creates * BYTES_PER_TX + updates * BLOB_SIZE_KB * 1024))
    
    # Rates
    if [ $elapsed -gt 0 ] && [ $total_written -gt 0 ]; then
        avg_rate=$((total_written * 60 / elapsed / 1024 / 1024))
    else
        avg_rate=0
    fi
    
    if [ $interval_time -gt 0 ] && [ $interval_written -gt 0 ]; then
        instant_rate=$((interval_written * 60 / interval_time / 1024 / 1024))
    else
        instant_rate=0
    fi
    
    # Update ratio achieved
    if [ "$success_tx" -gt 0 ]; then
        update_pct=$((updates * 100 / success_tx))
    else
        update_pct=0
    fi
    
    # Write amplification
    if [ "$app_bytes" -gt 0 ] 2>/dev/null; then
        wa=$(echo "scale=2; $total_written / $app_bytes" | bc 2>/dev/null || echo "N/A")
    else
        wa="N/A"
    fi
    
    log "After ${elapsed}s: Creates=$creates, Updates=$updates, Fails=$fails (${update_pct}% updates)"
    log "  Device: $(format_bytes $total_written), Rate=${avg_rate} MB/min, Instant=${instant_rate} MB/min"
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

creates=$(grep -c "^C$" "$RESULT_FILE" 2>/dev/null) || creates=0
updates=$(grep -c "^U$" "$RESULT_FILE" 2>/dev/null) || updates=0
fails=$(grep -c "^F$" "$RESULT_FILE" 2>/dev/null) || fails=0
success=$((creates + updates))

total_app_bytes=$((creates * BYTES_PER_TX + updates * BLOB_SIZE_KB * 1024))

rate_mb_min=$((total_device_bytes * 60 / total_elapsed / 1024 / 1024))
rate_gb_min=$(echo "scale=2; $rate_mb_min / 1024" | bc 2>/dev/null || echo "0")

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

if [ "$success" -gt 0 ]; then
    final_update_pct=$((updates * 100 / success))
else
    final_update_pct=0
fi

log ""
log "═══════════════════════════════════════════════════════════════"
log "  UPDATE-HEAVY BENCHMARK COMPLETE"
log "═══════════════════════════════════════════════════════════════"
log ""
log "  Duration:         ${total_elapsed}s"
log "  Workers:          $WORKERS"
log ""
log "  ─── TRANSACTION BREAKDOWN ───"
log "  Creates:          $creates"
log "  Updates:          $updates (${final_update_pct}% of success)"
log "  Failed:           $fails"
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
log "  Key insight: Updates should increase WAF compared to create-only,"
log "  because each update creates a new version requiring compaction."
log ""

# Research output
FDP_MODE="nofdp"
if mount | grep -q "f2fs.*fdp_log_n"; then
    FDP_MODE="fdp"
fi

total_device_sectors=$(awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats)
total_device_all_time=$((total_device_sectors * 512))
total_device_all_time_gb=$(echo "scale=2; $total_device_all_time / 1073741824" | bc 2>/dev/null || echo "0")

gc_bg=$(cat /sys/fs/f2fs/$NVME_DEVICE/gc_background_calls 2>/dev/null || echo "0")
gc_fg=$(cat /sys/fs/f2fs/$NVME_DEVICE/gc_foreground_calls 2>/dev/null || echo "0")

device_capacity_gb=64
overwrite_ratio=$(echo "scale=2; $total_device_all_time_gb / $device_capacity_gb" | bc 2>/dev/null || echo "0")

log "═══════════════════════════════════════════════════════════════"
log "  RESEARCH METRICS"
log "═══════════════════════════════════════════════════════════════"
log ""
log "  MODE=$FDP_MODE"
log "  STRATEGY=update_heavy"
log "  UPDATE_PCT=${final_update_pct}"
log "  DURATION_SEC=$total_elapsed"
log "  TPS=$tps"
log "  CREATES=$creates"
log "  UPDATES=$updates"
log "  FAILED=$fails"
log "  APP_WRITES_BYTES=$total_app_bytes"
log "  DEVICE_WRITES_BYTES=$total_device_bytes"
log "  WAF=$final_wa"
log "  RATE_MB_MIN=$rate_mb_min"
log "  TOTAL_DEVICE_GB=$total_device_all_time_gb"
log "  OVERWRITE_RATIO=$overwrite_ratio"
log "  GC_BG=$gc_bg"
log "  GC_FG=$gc_fg"
log ""

# Save to CSV
RESULTS_DIR="${RESULTS_DIR:-/home/femu/fdp-scripts/sui-bench/fdp_lsm_results}"
mkdir -p "$RESULTS_DIR"
RESULTS_CSV="$RESULTS_DIR/update_heavy_results.csv"

if [ ! -f "$RESULTS_CSV" ]; then
    echo "timestamp,mode,strategy,duration_sec,workers,tps,creates,updates,update_pct,failed,app_writes_bytes,device_writes_bytes,waf,rate_mb_min,total_device_gb,overwrite_ratio,gc_bg,gc_fg" > "$RESULTS_CSV"
fi

echo "$(date -Iseconds),$FDP_MODE,update_heavy,$total_elapsed,$WORKERS,$tps,$creates,$updates,$final_update_pct,$fails,$total_app_bytes,$total_device_bytes,$final_wa,$rate_mb_min,$total_device_all_time_gb,$overwrite_ratio,$gc_bg,$gc_fg" >> "$RESULTS_CSV"

log "Results saved to: $RESULTS_CSV"
log ""

# Cleanup
rm -rf "$WORK_DIR"
