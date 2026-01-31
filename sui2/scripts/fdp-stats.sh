#!/bin/bash
#
# fdp-stats.sh - Collect FDP and GC statistics from FEMU
#
# Usage:
#   ./fdp-stats.sh start    # Record initial stats
#   ./fdp-stats.sh stop     # Record final stats and calculate WAF
#   ./fdp-stats.sh reset    # Reset device stats (calls fdp_send_sungjin)
#   ./fdp-stats.sh show     # Show current stats
#
# GC Stats Source:
#   GC stats come from FEMU's host-side output. To capture them:
#   
#   1. On the HOST (outside FEMU VM), run:
#      tail -f /path/to/femu_output.log | ./femu-gc-parser.sh - /shared/gc_stats.txt
#   
#   2. Set FEMU_GC_STATS to point to the shared file:
#      FEMU_GC_STATS=/shared/gc_stats.txt ./fdp-stats.sh show
#
# Key metrics:
#   - Host writes (in pages) - actual application writes
#   - GC copies (pages moved during garbage collection)
#   - WAF = (host_writes + gc_copies) / host_writes
#

STATS_DIR="${STATS_DIR:-/tmp/fdp_stats}"
NVME_DEVICE="${NVME_DEVICE:-nvme0n1}"
FDP_SEND_TOOL="${FDP_SEND_TOOL:-/home/femu/fdp-scripts/fdp_send_sungjin}"
FDP_GET_STATS="${FDP_GET_STATS:-/home/femu/fdp-scripts/fdp_get_stats}"
FEMU_GC_LOG="${FEMU_GC_LOG:-}"  # FEMU output log (if accessible)
FEMU_GC_STATS="${FEMU_GC_STATS:-}"  # Shared file with parsed GC stats from host

mkdir -p "$STATS_DIR"

# Get device write sectors from /proc/diskstats
get_device_sectors() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats 2>/dev/null || echo 0
}

# Parse GC log line to extract stats
# Format: GC-ing line:X,ipc=Y,victim=Z,full=A,free=B,stream_id=C,rg_id=D, discard E read/write F/G block_erased H copied I
parse_gc_log() {
    local log_line="$1"
    
    # Extract read/write (format: read/write 259/5102029)
    local rw=$(echo "$log_line" | grep -oP 'read/write \K[0-9]+/[0-9]+')
    local reads=$(echo "$rw" | cut -d'/' -f1)
    local writes=$(echo "$rw" | cut -d'/' -f2)
    
    # Extract copied pages
    local copied=$(echo "$log_line" | grep -oP 'copied \K[0-9]+')
    
    # Extract block_erased
    local erased=$(echo "$log_line" | grep -oP 'block_erased \K[0-9]+')
    
    # Extract discard count
    local discards=$(echo "$log_line" | grep -oP 'discard \K[0-9]+')
    
    echo "reads=$reads"
    echo "writes=$writes"
    echo "copied=$copied"
    echo "erased=$erased"
    echo "discards=$discards"
}

# Get the latest GC stats from fdp_get_stats tool, shared file, dmesg, or log file
get_gc_stats() {
    local gc_line=""
    
    # Priority 1: Try fdp_get_stats tool (uses xNVMe mgmt_recv)
    # Note: Requires root for NVMe commands - try without sudo first, then with
    if [ -x "$FDP_GET_STATS" ]; then
        local stats_output=$("$FDP_GET_STATS" /dev/$NVME_DEVICE 2>/dev/null)
        if [ -z "$stats_output" ]; then
            # Try without /dev/ prefix
            stats_output=$("$FDP_GET_STATS" "$NVME_DEVICE" 2>/dev/null)
        fi
        if [ -n "$stats_output" ] && ! echo "$stats_output" | grep -q "error=true"; then
            echo "$stats_output" | grep -E "^(reads|writes|host_writes|host_reads|gc_copies|blocks_erased|discards|waf)="
            return
        fi
    fi
    
    # Priority 2: Read from shared stats file (written by femu-gc-parser.sh on host)
    if [ -n "$FEMU_GC_STATS" ] && [ -f "$FEMU_GC_STATS" ]; then
        # The shared file is already in key=value format
        grep -E "^(reads|writes|copied|blocks_erased|discards|waf)=" "$FEMU_GC_STATS" 2>/dev/null
        return
    fi
    
    # Priority 3: Try to get from FEMU GC log file if specified
    if [ -n "$FEMU_GC_LOG" ] && [ -f "$FEMU_GC_LOG" ]; then
        gc_line=$(tail -1 "$FEMU_GC_LOG" 2>/dev/null | grep "GC-ing")
    fi
    
    # Priority 4: Try dmesg as fallback
    if [ -z "$gc_line" ]; then
        gc_line=$(dmesg 2>/dev/null | grep "GC-ing" | tail -1)
    fi
    
    # Priority 5: Try journalctl as another fallback
    if [ -z "$gc_line" ]; then
        gc_line=$(journalctl -k --no-pager 2>/dev/null | grep "GC-ing" | tail -1)
    fi
    
    if [ -n "$gc_line" ]; then
        parse_gc_log "$gc_line"
    else
        echo "reads=0"
        echo "writes=0"
        echo "copied=0"
        echo "erased=0"
        echo "discards=0"
    fi
}

case "$1" in
    start)
        echo "Recording initial FDP stats..."
        
        # Record device sectors
        get_device_sectors > "$STATS_DIR/start_sectors"
        
        # Record timestamp
        date +%s > "$STATS_DIR/start_time"
        
        # Try to get GC stats (may not be available at start)
        get_gc_stats > "$STATS_DIR/start_gc"
        
        echo "Initial stats saved to $STATS_DIR/"
        cat "$STATS_DIR/start_gc"
        ;;
        
    stop)
        echo "Recording final FDP stats..."
        
        # Accept optional app_bytes parameter (from benchmark)
        APP_BYTES="${APP_BYTES:-0}"
        
        # Record device sectors
        get_device_sectors > "$STATS_DIR/end_sectors"
        
        # Record timestamp
        date +%s > "$STATS_DIR/end_time"
        
        # Get final GC stats
        get_gc_stats > "$STATS_DIR/end_gc"
        
        # Trigger FEMU to output GC stats (visible on host console)
        if [ -x "$FDP_SEND_TOOL" ]; then
            echo "Triggering FEMU GC stats output (check host console)..."
            "$FDP_SEND_TOOL" /dev/$NVME_DEVICE >/dev/null 2>&1 || \
                "$FDP_SEND_TOOL" "$NVME_DEVICE" >/dev/null 2>&1 || true
        fi
        
        # Calculate differences
        start_sectors=$(cat "$STATS_DIR/start_sectors" 2>/dev/null || echo 0)
        end_sectors=$(cat "$STATS_DIR/end_sectors")
        start_time=$(cat "$STATS_DIR/start_time" 2>/dev/null || echo 0)
        end_time=$(cat "$STATS_DIR/end_time")
        
        # Load GC stats (may be 0 if not available)
        source "$STATS_DIR/start_gc" 2>/dev/null
        start_writes=${writes:-${host_writes:-0}}
        start_copied=${copied:-${gc_copies:-0}}
        
        source "$STATS_DIR/end_gc"
        end_writes=${writes:-${host_writes:-0}}
        end_copied=${copied:-${gc_copies:-0}}
        
        # Calculate metrics
        elapsed=$((end_time - start_time))
        device_sectors=$((end_sectors - start_sectors))
        device_bytes=$((device_sectors * 512))
        device_mb=$((device_bytes / 1024 / 1024))
        device_gb=$(echo "scale=2; $device_mb / 1024" | bc)
        
        gc_writes=$((end_writes - start_writes))
        gc_copies=$((end_copied - start_copied))
        
        # Calculate WAF using GC stats if available
        if [ $gc_writes -gt 0 ]; then
            gc_waf=$(echo "scale=2; ($gc_writes + $gc_copies) / $gc_writes" | bc)
        else
            gc_waf="N/A"
        fi
        
        # Calculate WAF using application bytes (more reliable)
        if [ "$APP_BYTES" -gt 0 ] 2>/dev/null; then
            app_mb=$((APP_BYTES / 1024 / 1024))
            app_waf=$(echo "scale=2; $device_bytes / $APP_BYTES" | bc 2>/dev/null || echo "N/A")
        else
            app_mb=0
            app_waf="N/A"
        fi
        
        # Calculate throughput
        if [ $elapsed -gt 0 ]; then
            rate_mb_min=$((device_mb * 60 / elapsed))
            rate_gb_min=$(echo "scale=2; $rate_mb_min / 1024" | bc)
        else
            rate_mb_min=0
            rate_gb_min=0
        fi
        
        echo ""
        echo "═══════════════════════════════════════════════════════════════"
        echo "  FDP BENCHMARK RESULTS"
        echo "═══════════════════════════════════════════════════════════════"
        echo ""
        echo "  Duration:           ${elapsed}s"
        echo ""
        echo "  ─── APPLICATION WRITES ───"
        echo "  App Written:        ${app_mb} MB"
        echo ""
        echo "  ─── DEVICE I/O (from /proc/diskstats) ───"
        echo "  Sectors Written:    $device_sectors"
        echo "  Total Written:      ${device_gb} GB (${device_mb} MB)"
        echo "  Write Rate:         ${rate_mb_min} MB/min (${rate_gb_min} GB/min)"
        echo ""
        echo "  ─── WRITE AMPLIFICATION ───"
        echo "  App WAF:            ${app_waf}x (device_writes / app_writes)"
        if [ "$gc_waf" != "N/A" ]; then
        echo "  GC WAF:             ${gc_waf}x (from FEMU GC stats)"
        echo "  GC Copies:          $gc_copies pages"
        fi
        echo ""
        echo "  NOTE: Check FEMU host console for detailed GC stats."
        echo "        Look for: GC-ing line:X,...,copied Y"
        echo ""
        
        # Save results to file
        cat > "$STATS_DIR/results.txt" << EOF
duration_seconds=$elapsed
app_bytes=$APP_BYTES
app_mb=$app_mb
device_sectors=$device_sectors
device_bytes=$device_bytes
device_mb=$device_mb
device_gb=$device_gb
rate_mb_min=$rate_mb_min
rate_gb_min=$rate_gb_min
app_waf=$app_waf
gc_waf=$gc_waf
gc_copies=$gc_copies
EOF
        echo "Results saved to $STATS_DIR/results.txt"
        ;;
        
    reset)
        echo "Resetting/triggering FDP device stats..."
        # Call fdp_send_sungjin to trigger FEMU to output GC stats (visible on host console)
        if [ -x "$FDP_SEND_TOOL" ]; then
            echo "Triggering FEMU GC stats output..."
            "$FDP_SEND_TOOL" /dev/$NVME_DEVICE 2>&1 || \
                "$FDP_SEND_TOOL" "$NVME_DEVICE" 2>&1 || echo "Warning: fdp_send failed"
            echo ""
            echo "NOTE: GC stats should now be visible on the FEMU host console."
            echo "Look for lines like: GC-ing line:X,ipc=Y,... copied Z"
        else
            echo "Warning: $FDP_SEND_TOOL not found or not executable"
        fi
        ;;
        
    show)
        echo "Current FDP Stats:"
        echo ""
        echo "Device sectors written: $(get_device_sectors)"
        echo ""
        echo "GC Stats (from logs):"
        get_gc_stats
        ;;
        
    *)
        echo "Usage: $0 {start|stop|reset|show}"
        echo ""
        echo "  start  - Record initial stats before benchmark"
        echo "  stop   - Record final stats and calculate WAF"
        echo "  reset  - Reset device counters (requires sudo)"
        echo "  show   - Show current stats"
        exit 1
        ;;
esac
