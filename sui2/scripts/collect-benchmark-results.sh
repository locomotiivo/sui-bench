#!/bin/bash
#
# collect-benchmark-results.sh - Collect and format benchmark results
#
# Run this after a benchmark completes to get formatted WAF, TPS, and other metrics.
# Can also collect FDP-specific stats if available.
#
# Usage:
#   ./collect-benchmark-results.sh [--fdp|--nofdp] [--output FILE]
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NVME_DEVICE="${NVME_DEVICE:-nvme0n1}"
FDP_STATS_TOOL="/home/femu/fdp-scripts/fdp_send_sungjin"
MODE="${1:-unknown}"
OUTPUT_FILE="${2:-}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
}

print_metric() {
    local name="$1"
    local value="$2"
    local unit="$3"
    printf "  %-25s ${GREEN}%s${NC} %s\n" "$name:" "$value" "$unit"
}

# Get device statistics
get_device_stats() {
    local sectors=$(awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats)
    local bytes=$((sectors * 512))
    echo "$bytes"
}

# Get filesystem usage
get_fs_usage() {
    df -B1 /home/femu/f2fs_fdp_mount 2>/dev/null | tail -1 | awk '{print $3}'
}

# Get F2FS GC stats
get_f2fs_gc_stats() {
    local gc_bg=$(cat /sys/fs/f2fs/$NVME_DEVICE/gc_background_calls 2>/dev/null || echo "0")
    local gc_fg=$(cat /sys/fs/f2fs/$NVME_DEVICE/gc_foreground_calls 2>/dev/null || echo "0")
    echo "$gc_bg $gc_fg"
}

# Main collection
collect_results() {
    print_header "BENCHMARK RESULTS COLLECTION"
    echo ""
    echo "  Timestamp:  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  Mode:       $MODE"
    echo "  Device:     /dev/$NVME_DEVICE"
    echo ""
    
    # Device writes
    local device_bytes=$(get_device_stats)
    local device_gb=$(echo "scale=2; $device_bytes / 1073741824" | bc)
    
    print_header "DEVICE STATISTICS"
    print_metric "Total Device Writes" "$device_gb" "GB"
    
    # Get device capacity
    local capacity=$(lsblk -b -d -n -o SIZE /dev/$NVME_DEVICE 2>/dev/null || echo "68719476736")
    local capacity_gb=$(echo "scale=0; $capacity / 1073741824" | bc)
    local overwrite_cycles=$(echo "scale=2; $device_bytes / $capacity" | bc)
    print_metric "Device Capacity" "$capacity_gb" "GB"
    print_metric "Overwrite Cycles" "$overwrite_cycles" "x"
    
    # Filesystem stats
    print_header "FILESYSTEM STATISTICS"
    local fs_used=$(get_fs_usage)
    local fs_used_gb=$(echo "scale=2; $fs_used / 1073741824" | bc 2>/dev/null || echo "N/A")
    print_metric "F2FS Used" "$fs_used_gb" "GB"
    
    # F2FS GC stats
    read gc_bg gc_fg <<< $(get_f2fs_gc_stats)
    print_metric "F2FS BG GC Calls" "$gc_bg" ""
    print_metric "F2FS FG GC Calls" "$gc_fg" ""
    
    # Calculate WAF if we have app writes info
    print_header "WRITE AMPLIFICATION"
    if [ -n "$APP_BYTES" ] && [ "$APP_BYTES" -gt 0 ]; then
        local app_gb=$(echo "scale=2; $APP_BYTES / 1073741824" | bc)
        local waf=$(echo "scale=3; $device_bytes / $APP_BYTES" | bc)
        print_metric "Application Writes" "$app_gb" "GB"
        print_metric "Device Writes" "$device_gb" "GB"
        print_metric "WAF (Device/App)" "$waf" "x"
    else
        echo "  (Set APP_BYTES environment variable to calculate WAF)"
        echo "  Example: APP_BYTES=50000000000 ./collect-benchmark-results.sh"
    fi
    
    # Try to get FDP stats
    print_header "FDP STATISTICS"
    if [ -x "$FDP_STATS_TOOL" ]; then
        echo "  Attempting to collect FDP RUH status..."
        local fdp_output=$("$FDP_STATS_TOOL" /dev/$NVME_DEVICE 2>&1)
        if echo "$fdp_output" | grep -q "error\|Error"; then
            echo -e "  ${YELLOW}FDP stats collection failed (device may need special access)${NC}"
        else
            echo "$fdp_output" | while read line; do
                echo "  $line"
            done
        fi
    else
        echo "  FDP stats tool not found at: $FDP_STATS_TOOL"
    fi
    
    # Summary
    print_header "SUMMARY FOR RESEARCH"
    echo ""
    echo "  Copy these values for your research:"
    echo ""
    echo "  MODE=$MODE"
    echo "  DEVICE_WRITES_GB=$device_gb"
    echo "  OVERWRITE_CYCLES=$overwrite_cycles"
    echo "  FS_USED_GB=$fs_used_gb"
    echo "  F2FS_GC_BG=$gc_bg"
    echo "  F2FS_GC_FG=$gc_fg"
    [ -n "$APP_BYTES" ] && echo "  APP_WRITES_GB=$app_gb"
    [ -n "$APP_BYTES" ] && echo "  WAF=$waf"
    echo ""
    
    # Output to file if specified
    if [ -n "$OUTPUT_FILE" ]; then
        {
            echo "timestamp,mode,device_writes_gb,overwrite_cycles,fs_used_gb,gc_bg,gc_fg,app_writes_gb,waf"
            echo "$(date -Iseconds),$MODE,$device_gb,$overwrite_cycles,$fs_used_gb,$gc_bg,$gc_fg,${app_gb:-N/A},${waf:-N/A}"
        } >> "$OUTPUT_FILE"
        echo "  Results appended to: $OUTPUT_FILE"
    fi
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --fdp)
            MODE="fdp"
            shift
            ;;
        --nofdp)
            MODE="nofdp"
            shift
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

collect_results
