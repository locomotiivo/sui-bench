#!/bin/bash

# Monitor storage bloat and disk growth with FDP awareness
# Shows per-PID storage distribution when FDP_MODE=1

FDP_MODE="${FDP_MODE:-0}"
MOUNT_POINT="${MOUNT_POINT:-$HOME/f2fs_fdp_mount}"
SUI_DIR="${1:-$MOUNT_POINT/p0/sui_node}"
LOG_FILE="${2:-bloat_monitor.csv}"

# PID descriptions for FDP mode
declare -A PID_DESC=(
    [0]="config"
    [1]="hot-epochs"
    [2]="perpetual"
    [3]="consensus"
    [4]="checkpoints"
    [5]="warm-epoch"
    [6]="cold-epochs"
    [7]="indexes"
)

if [ ! -d "$MOUNT_POINT" ]; then
    echo "❌ Error: Mount point $MOUNT_POINT does not exist"
    exit 1
fi

echo "📊 Monitoring storage bloat"
echo "   Mount: $MOUNT_POINT"
echo "   FDP Mode: $FDP_MODE"
echo "   Log: $LOG_FILE"
echo "Press Ctrl+C to stop"
echo ""

# Create log file with header
if [ "$FDP_MODE" -eq 1 ]; then
    echo "Timestamp,TotalMB,DiskUsedPct,GrowthRateMB/min,p0,p1,p2,p3,p4,p5,p6,p7" > "$LOG_FILE"
else
    echo "Timestamp,DiskUsedMB,DiskUsedPct,GrowthRateMB/min,FilesCount,IOps" > "$LOG_FILE"
fi

LAST_SIZE=0
LAST_TIME=$(date +%s)
LAST_IO=0

# Array to track per-PID sizes
declare -a LAST_PID_SIZES=(0 0 0 0 0 0 0 0)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Get per-PID sizes
get_pid_sizes() {
    local sizes=""
    for pid in 0 1 2 3 4 5 6 7; do
        local size=$(du -sm "$MOUNT_POINT/p$pid" 2>/dev/null | cut -f1 || echo "0")
        sizes="$sizes${size:-0},"
    done
    echo "${sizes%,}"
}

while true; do
    CURRENT_TIME=$(date +%s)
    
    # Get per-PID sizes if FDP mode
    if [ "$FDP_MODE" -eq 1 ]; then
        declare -a CURRENT_PID_SIZES=()
        CURRENT_SIZE=0
        for pid in 0 1 2 3 4 5 6 7; do
            local size=$(du -sm "$MOUNT_POINT/p$pid" 2>/dev/null | cut -f1 || echo "0")
            CURRENT_PID_SIZES[$pid]=${size:-0}
            CURRENT_SIZE=$((CURRENT_SIZE + ${size:-0}))
        done
    else
        CURRENT_SIZE=$(du -sm "$SUI_DIR" 2>/dev/null | cut -f1 || echo "0")
    fi
    
    # Calculate growth
    ELAPSED=$(( CURRENT_TIME - LAST_TIME ))
    GROWTH=$(( CURRENT_SIZE - LAST_SIZE ))
    
    if [ $ELAPSED -gt 0 ]; then
        RATE=$(echo "scale=2; $GROWTH * 60 / $ELAPSED" | bc 2>/dev/null || echo "0")
    else
        RATE=0
    fi
    
    # Get disk usage percentage
    DISK_INFO=$(df -h "$MOUNT_POINT" | tail -1)
    DISK_PCT=$(echo "$DISK_INFO" | awk '{print $5}' | tr -d '%')
    DISK_USED=$(echo "$DISK_INFO" | awk '{print $3}')
    DISK_TOTAL=$(echo "$DISK_INFO" | awk '{print $2}')
    
    # Count files
    FILE_COUNT=$(find "$MOUNT_POINT" -type f 2>/dev/null | wc -l)
    
    # Get I/O stats (if iostat available)
    if command -v iostat &> /dev/null; then
        IOPS=$(iostat -x 1 2 2>/dev/null | grep -A1 "Device" | tail -1 | awk '{print int($4+$5)}' || echo "N/A")
    else
        IOPS="N/A"
    fi
    
    # Log to file
    if [ "$FDP_MODE" -eq 1 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S'),$CURRENT_SIZE,$DISK_PCT,$RATE,${CURRENT_PID_SIZES[0]},${CURRENT_PID_SIZES[1]},${CURRENT_PID_SIZES[2]},${CURRENT_PID_SIZES[3]},${CURRENT_PID_SIZES[4]},${CURRENT_PID_SIZES[5]},${CURRENT_PID_SIZES[6]},${CURRENT_PID_SIZES[7]}" >> "$LOG_FILE"
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S'),$CURRENT_SIZE,$DISK_PCT,$RATE,$FILE_COUNT,$IOPS" >> "$LOG_FILE"
    fi
    
    # Display with colors
    clear
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
    if [ "$FDP_MODE" -eq 1 ]; then
        echo -e "${BLUE}║${NC}       ${YELLOW}SUI STORAGE BLOAT MONITOR${NC} ${CYAN}[FDP MODE]${NC}            ${BLUE}║${NC}"
    else
        echo -e "${BLUE}║${NC}          ${YELLOW}SUI STORAGE BLOAT MONITOR${NC}                       ${BLUE}║${NC}"
    fi
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${GREEN}📁 Mount Point:${NC} $MOUNT_POINT"
    echo -e "${GREEN}🕐 Time:${NC} $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    
    # FDP-specific display
    if [ "$FDP_MODE" -eq 1 ]; then
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${CYAN}FDP Placement ID Usage${NC}"
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        
        for pid in 0 1 2 3 4 5 6 7; do
            local size=${CURRENT_PID_SIZES[$pid]:-0}
            local last_size=${LAST_PID_SIZES[$pid]:-0}
            local delta=$((size - last_size))
            local delta_str=""
            
            if [ $delta -gt 0 ]; then
                delta_str="${GREEN}+${delta}MB${NC}"
            elif [ $delta -lt 0 ]; then
                delta_str="${RED}${delta}MB${NC}"
            fi
            
            # Color by temperature
            case $pid in
                1|3) color=$RED ;;      # Hot
                5) color=$YELLOW ;;      # Warm
                2|4|7) color=$CYAN ;;    # Medium
                0|6) color=$GREEN ;;     # Cold
                *) color=$NC ;;
            esac
            
            printf "  ${color}p%d${NC} %-12s: %6s MB  %s\n" $pid "(${PID_DESC[$pid]})" "$size" "$delta_str"
        done
        echo ""
    fi
    
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Disk Usage${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "  Total Size:     ${GREEN}$DISK_TOTAL${NC}"
    echo -e "  Used:           ${YELLOW}$DISK_USED${NC} (${YELLOW}$DISK_PCT%${NC})"
    echo -e "  DB Size:        ${YELLOW}${CURRENT_SIZE} MB${NC}"
    echo -e "  Files:          ${YELLOW}$FILE_COUNT${NC}"
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}Performance${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Color code the growth rate
    if (( $(echo "$RATE > 5000" | bc -l 2>/dev/null || echo 0) )); then
        RATE_COLOR=$GREEN
        RATE_LABEL="🔥 EXCELLENT"
    elif (( $(echo "$RATE > 2000" | bc -l 2>/dev/null || echo 0) )); then
        RATE_COLOR=$YELLOW
        RATE_LABEL="✅ GOOD"
    elif (( $(echo "$RATE > 500" | bc -l 2>/dev/null || echo 0) )); then
        RATE_COLOR=$YELLOW
        RATE_LABEL="⚠️  MODERATE"
    else
        RATE_COLOR=$RED
        RATE_LABEL="❌ LOW"
    fi
    
    echo -e "  Growth Rate:    ${RATE_COLOR}${RATE} MB/min${NC} ${RATE_LABEL}"
    echo -e "  I/O Ops:        ${YELLOW}${IOPS}${NC}"
    echo ""
    
    # Calculate estimated time to fill disk
    REMAINING_MB=$(( $(echo "$DISK_TOTAL" | sed 's/G$//' | awk '{print $1*1024}' 2>/dev/null || echo 64000) - CURRENT_SIZE ))
    if (( $(echo "$RATE > 0" | bc -l 2>/dev/null || echo 0) )); then
        MINS_TO_FILL=$(echo "scale=1; $REMAINING_MB / $RATE" | bc 2>/dev/null || echo "N/A")
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${YELLOW}Estimates${NC}"
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "  Time to fill:   ${YELLOW}${MINS_TO_FILL} minutes${NC}"
        echo -e "  Time to 80%:    ${YELLOW}$(echo "scale=1; (64000 * 0.8 - $CURRENT_SIZE) / $RATE" | bc 2>/dev/null || echo "N/A") minutes${NC}"
    fi
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}💡 Tips:${NC}"
    echo -e "   F2FS GC status:  ${YELLOW}cat /sys/fs/f2fs/*/gc_*${NC}"
    if [ "$FDP_MODE" -eq 1 ]; then
        echo -e "   FDP epoch watch: ${YELLOW}./fdp-epoch-watcher.sh status${NC}"
    fi
    echo ""
    
    LAST_SIZE=$CURRENT_SIZE
    LAST_TIME=$CURRENT_TIME
    if [ "$FDP_MODE" -eq 1 ]; then
        for pid in 0 1 2 3 4 5 6 7; do
            LAST_PID_SIZES[$pid]=${CURRENT_PID_SIZES[$pid]:-0}
        done
    fi
    
    sleep 10
done
