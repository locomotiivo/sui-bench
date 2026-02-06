#!/bin/bash
#
# Memory-Safe Swarm Benchmark for FDP Testing
#
# Strategy: Maximize small I/O operations with aggressive OOM prevention.
# Creates a "swarm" of small objects (100 bytes each) that cannot be compressed
# or efficiently compacted, forcing high write amplification.
#
# Memory Safety Features:
# - Real-time memory monitoring with configurable threshold
# - Automatic throttling when memory exceeds threshold
# - Graceful degradation instead of OOM crash
# - Progressive backoff when under memory pressure
#
# Key Metrics Tracked:
# - Host disk writes (sectors from /proc/diskstats)
# - SUI data growth (du -sk)
# - Write amplification (host writes / logical data)
# - Memory pressure events
#

set -e

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NVME_DEVICE="nvme0n1"
SUI_BINARY="${SUI_BINARY:-sui}"

# SUI Config
export SUI_CONFIG_DIR="${SUI_CONFIG_DIR:-/home/femu/f2fs_fdp_mount/p0/sui_node}"
MOVE_DIR="$SCRIPT_DIR/../move/io_churn"

# ═══════════════════════════════════════════════════════════════════════════════
# MEMORY THRESHOLDS (Critical for 16GB VM)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Memory usage levels:
#   0-70%:   GREEN  - Full speed
#   70-85%:  YELLOW - Reduced parallelism
#   85-92%:  ORANGE - Single-threaded, with delays
#   92-95%:  RED    - Pause and wait for GC
#   95%+:    ABORT  - Stop benchmark gracefully
#
MEM_GREEN_THRESHOLD="${MEM_GREEN_THRESHOLD:-70}"
MEM_YELLOW_THRESHOLD="${MEM_YELLOW_THRESHOLD:-85}"
MEM_ORANGE_THRESHOLD="${MEM_ORANGE_THRESHOLD:-92}"
MEM_RED_THRESHOLD="${MEM_RED_THRESHOLD:-95}"

# Workload parameters (conservative defaults for 16GB)
WORKERS="${WORKERS:-4}"                    # Start with fewer workers
DURATION="${DURATION:-300}"                # 5 minutes default
BATCH_SIZE="${BATCH_SIZE:-100}"            # Objects per transaction
INTER_TX_DELAY="${INTER_TX_DELAY:-0.1}"    # Delay between transactions (seconds)
MAX_OBJECTS="${MAX_OBJECTS:-100000}"       # Safety cap on total objects

# Output
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/results/swarm_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$RESULTS_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
ORANGE='\033[0;33m'
NC='\033[0m'

# Global state
THROTTLE_EVENTS=0
ABORT_REQUESTED=0

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }
log_mem()     { echo -e "${CYAN}[MEM]${NC} $1"; }

# ═══════════════════════════════════════════════════════════════════════════════
# MEMORY MONITORING
# ═══════════════════════════════════════════════════════════════════════════════

get_memory_stats() {
    # Returns: used_mb total_mb available_mb usage_pct
    local stats=$(free -m | awk '/Mem:/ {printf "%d %d %d %.0f", $3, $2, $7, ($3/$2)*100}')
    echo "$stats"
}

get_memory_pct() {
    free | awk '/Mem:/ {printf "%.0f", ($3/$2)*100}'
}

get_memory_available_mb() {
    free -m | awk '/Mem:/ {print $7}'
}

# Determine throttle level based on memory usage
# Returns: 0=green, 1=yellow, 2=orange, 3=red, 4=abort
get_throttle_level() {
    local mem_pct=$(get_memory_pct)
    
    if [ "$mem_pct" -ge "$MEM_RED_THRESHOLD" ]; then
        echo "4"  # ABORT
    elif [ "$mem_pct" -ge "$MEM_ORANGE_THRESHOLD" ]; then
        echo "3"  # RED - pause
    elif [ "$mem_pct" -ge "$MEM_YELLOW_THRESHOLD" ]; then
        echo "2"  # ORANGE - slow
    elif [ "$mem_pct" -ge "$MEM_GREEN_THRESHOLD" ]; then
        echo "1"  # YELLOW - reduced
    else
        echo "0"  # GREEN - full speed
    fi
}

# Apply throttling based on memory level
apply_throttle() {
    local level=$(get_throttle_level)
    local mem_pct=$(get_memory_pct)
    
    case $level in
        0)  # GREEN - no throttle
            return 0
            ;;
        1)  # YELLOW - slight delay
            sleep 0.05
            return 0
            ;;
        2)  # ORANGE - longer delay
            sleep 0.2
            THROTTLE_EVENTS=$((THROTTLE_EVENTS + 1))
            return 0
            ;;
        3)  # RED - pause and sync
            log_mem "Memory critical (${mem_pct}%) - pausing..."
            sync
            sleep 2
            THROTTLE_EVENTS=$((THROTTLE_EVENTS + 1))
            
            # Wait for memory to recover
            while [ "$(get_memory_pct)" -ge "$MEM_ORANGE_THRESHOLD" ]; do
                sleep 1
                sync
            done
            log_mem "Memory recovered to $(get_memory_pct)%"
            return 0
            ;;
        4)  # ABORT
            log_error "Memory critically high (${mem_pct}%) - aborting!"
            ABORT_REQUESTED=1
            return 1
            ;;
    esac
}

# ═══════════════════════════════════════════════════════════════════════════════
# DISK STATS
# ═══════════════════════════════════════════════════════════════════════════════

get_disk_write_sectors() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats 2>/dev/null || echo 0
}

get_sui_data_size_kb() {
    du -sk "$SUI_CONFIG_DIR" 2>/dev/null | cut -f1 || echo 0
}

# ═══════════════════════════════════════════════════════════════════════════════
# SUI HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

check_sui_node() {
    curl -s http://localhost:9000/health >/dev/null 2>&1 || \
    curl -s http://127.0.0.1:9000 -H 'Content-Type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"sui_getChainIdentifier","params":[]}' 2>/dev/null | grep -q "result"
}

get_package_id() {
    local pkg_file="$SUI_CONFIG_DIR/.package_id"
    if [ -f "$pkg_file" ]; then
        cat "$pkg_file"
    else
        echo ""
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# WORKER FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

run_worker() {
    local worker_id=$1
    local package_id=$2
    local batch_size=$3
    local end_time=$4
    local result_file="$RESULTS_DIR/worker_${worker_id}.result"
    local log_file="$RESULTS_DIR/worker_${worker_id}.log"
    
    local tx_count=0
    local err_count=0
    local throttle_count=0
    local objects_created=0
    
    echo "Worker $worker_id started at $(date)" > "$log_file"
    
    while [ "$(date +%s)" -lt "$end_time" ]; do
        # Check for abort signal
        if [ "$ABORT_REQUESTED" -eq 1 ]; then
            echo "Worker $worker_id: abort requested" >> "$log_file"
            break
        fi
        
        # Apply memory throttling
        if ! apply_throttle; then
            echo "Worker $worker_id: throttle returned abort" >> "$log_file"
            break
        fi
        
        # Check total objects cap
        if [ "$objects_created" -ge "$MAX_OBJECTS" ]; then
            echo "Worker $worker_id: reached max objects ($MAX_OBJECTS)" >> "$log_file"
            break
        fi
        
        # Submit transaction to create objects
        local output
        output=$($SUI_BINARY client call \
            --package "$package_id" \
            --module io_churn \
            --function create_batch \
            --args "$batch_size" \
            --gas-budget 500000000 \
            --json 2>&1)
        
        if echo "$output" | grep -q '"status"'; then
            tx_count=$((tx_count + 1))
            objects_created=$((objects_created + batch_size))
            
            # Log progress every 10 transactions
            if [ $((tx_count % 10)) -eq 0 ]; then
                local mem_pct=$(get_memory_pct)
                echo "Worker $worker_id: tx=$tx_count objs=$objects_created mem=${mem_pct}%" >> "$log_file"
            fi
        else
            err_count=$((err_count + 1))
            echo "Worker $worker_id: error - ${output:0:200}" >> "$log_file"
            sleep 0.5
        fi
        
        # Inter-transaction delay
        if [ -n "$INTER_TX_DELAY" ] && [ "$INTER_TX_DELAY" != "0" ]; then
            sleep "$INTER_TX_DELAY"
        fi
    done
    
    echo "Worker $worker_id finished: tx=$tx_count err=$err_count objs=$objects_created" >> "$log_file"
    echo "$tx_count $err_count $objects_created" > "$result_file"
}

# ═══════════════════════════════════════════════════════════════════════════════
# MONITORING THREAD
# ═══════════════════════════════════════════════════════════════════════════════

run_monitor() {
    local end_time=$1
    local monitor_file="$RESULTS_DIR/monitor.log"
    local init_sectors=$(get_disk_write_sectors)
    local init_sui_kb=$(get_sui_data_size_kb)
    local start_time=$(date +%s)
    
    echo "timestamp,elapsed,mem_pct,mem_avail_mb,disk_write_mb,sui_data_kb,workers_alive,throttle_level" > "$monitor_file"
    
    while [ "$(date +%s)" -lt "$end_time" ] && [ "$ABORT_REQUESTED" -eq 0 ]; do
        local now=$(date +%s)
        local elapsed=$((now - start_time))
        local mem_pct=$(get_memory_pct)
        local mem_avail=$(get_memory_available_mb)
        local curr_sectors=$(get_disk_write_sectors)
        local curr_sui_kb=$(get_sui_data_size_kb)
        local disk_write_mb=$(( (curr_sectors - init_sectors) * 512 / 1048576 ))
        local workers=$(pgrep -c -f "sui client call.*io_churn" 2>/dev/null || echo 0)
        local level=$(get_throttle_level)
        
        echo "$now,$elapsed,$mem_pct,$mem_avail,$disk_write_mb,$curr_sui_kb,$workers,$level" >> "$monitor_file"
        
        # Display status
        local level_color
        case $level in
            0) level_color="${GREEN}GREEN${NC}" ;;
            1) level_color="${YELLOW}YELLOW${NC}" ;;
            2) level_color="${ORANGE}ORANGE${NC}" ;;
            3) level_color="${RED}RED${NC}" ;;
            4) level_color="${RED}ABORT${NC}" ;;
        esac
        
        printf "\r[%3ds] Mem:%2d%% (%4dMB free) | Disk:+%4dMB | SUI:+%4dKB | Workers:%d | %b    " \
            "$elapsed" "$mem_pct" "$mem_avail" "$disk_write_mb" "$((curr_sui_kb - init_sui_kb))" "$workers" "$level_color"
        
        sleep 2
    done
    echo ""
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

main() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Memory-Safe Swarm Benchmark for FDP Testing${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    # Check prerequisites
    if ! check_sui_node; then
        log_error "SUI node not running on localhost:9000"
        log_info "Start with: SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui start --network.config \$SUI_CONFIG_DIR"
        exit 1
    fi
    log_success "SUI node is running"
    
    # Get package ID
    local PACKAGE_ID="${PACKAGE_ID:-$(get_package_id)}"
    if [ -z "$PACKAGE_ID" ]; then
        log_error "No package ID found"
        log_info "Set PACKAGE_ID env var or ensure $SUI_CONFIG_DIR/.package_id exists"
        exit 1
    fi
    log_info "Package ID: $PACKAGE_ID"
    
    # System info
    local mem_stats=$(get_memory_stats)
    local mem_used=$(echo $mem_stats | cut -d' ' -f1)
    local mem_total=$(echo $mem_stats | cut -d' ' -f2)
    local mem_avail=$(echo $mem_stats | cut -d' ' -f3)
    local mem_pct=$(echo $mem_stats | cut -d' ' -f4)
    
    echo ""
    log_info "System Status:"
    log_info "  Memory: ${mem_used}MB / ${mem_total}MB (${mem_pct}% used, ${mem_avail}MB available)"
    log_info "  Thresholds: GREEN<${MEM_GREEN_THRESHOLD}% YELLOW<${MEM_YELLOW_THRESHOLD}% ORANGE<${MEM_ORANGE_THRESHOLD}% RED<${MEM_RED_THRESHOLD}%"
    echo ""
    log_info "Configuration:"
    log_info "  Workers: $WORKERS"
    log_info "  Duration: ${DURATION}s"
    log_info "  Batch size: $BATCH_SIZE objects/tx"
    log_info "  Max objects: $MAX_OBJECTS"
    log_info "  Inter-TX delay: ${INTER_TX_DELAY}s"
    echo ""
    
    # Initial stats
    local init_sectors=$(get_disk_write_sectors)
    local init_sui_kb=$(get_sui_data_size_kb)
    local start_time=$(date +%s)
    local end_time=$((start_time + DURATION))
    
    log_info "Initial: sectors=$init_sectors, SUI=${init_sui_kb}KB"
    echo ""
    
    # Start monitor
    run_monitor "$end_time" &
    local monitor_pid=$!
    
    # Start workers
    log_info "Launching $WORKERS workers..."
    local pids=()
    for ((i=1; i<=WORKERS; i++)); do
        run_worker "$i" "$PACKAGE_ID" "$BATCH_SIZE" "$end_time" &
        pids+=($!)
        sleep 0.2  # Stagger worker starts
    done
    
    # Wait for completion
    log_info "Workers launched, monitoring..."
    echo ""
    
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    
    # Stop monitor
    kill "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
    
    # Final sync
    log_info "Syncing filesystem..."
    sync
    sleep 2
    
    # Collect results
    local final_sectors=$(get_disk_write_sectors)
    local final_sui_kb=$(get_sui_data_size_kb)
    local actual_duration=$(($(date +%s) - start_time))
    
    local total_tx=0
    local total_err=0
    local total_objects=0
    
    for ((i=1; i<=WORKERS; i++)); do
        local result_file="$RESULTS_DIR/worker_${i}.result"
        if [ -f "$result_file" ]; then
            read tx err objs < "$result_file"
            total_tx=$((total_tx + tx))
            total_err=$((total_err + err))
            total_objects=$((total_objects + objs))
        fi
    done
    
    # Calculate metrics
    local total_write_sectors=$((final_sectors - init_sectors))
    local total_write_mb=$((total_write_sectors * 512 / 1048576))
    local write_rate_mb_min=$((total_write_mb * 60 / actual_duration))
    local sui_growth_kb=$((final_sui_kb - init_sui_kb))
    local logical_mb=$((total_objects * 100 / 1048576))  # ~100 bytes per object
    local write_amp="N/A"
    [ "$logical_mb" -gt 0 ] && write_amp=$(echo "scale=1; $total_write_mb / $logical_mb" | bc 2>/dev/null || echo "N/A")
    
    # Report
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  RESULTS${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    log_info "Duration: ${actual_duration}s"
    log_info "Transactions: $total_tx (errors: $total_err)"
    log_info "Objects created: $total_objects (~100 bytes each = ${logical_mb}MB logical)"
    log_info "Host disk writes: $total_write_mb MB (${write_rate_mb_min} MB/min)"
    log_info "SUI data growth: ${sui_growth_kb}KB"
    log_info "Write amplification: ${write_amp}x"
    log_info "Memory throttle events: $THROTTLE_EVENTS"
    
    if [ "$actual_duration" -gt 0 ]; then
        log_info "TPS: $((total_tx / actual_duration))"
        log_info "Objects/sec: $((total_objects / actual_duration))"
    fi
    
    # Save summary
    cat > "$RESULTS_DIR/summary.json" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "duration_sec": $actual_duration,
    "workers": $WORKERS,
    "batch_size": $BATCH_SIZE,
    "memory_thresholds": {
        "green": $MEM_GREEN_THRESHOLD,
        "yellow": $MEM_YELLOW_THRESHOLD,
        "orange": $MEM_ORANGE_THRESHOLD,
        "red": $MEM_RED_THRESHOLD
    },
    "total_transactions": $total_tx,
    "total_errors": $total_err,
    "total_objects": $total_objects,
    "logical_data_mb": $logical_mb,
    "host_write_mb": $total_write_mb,
    "write_rate_mb_per_min": $write_rate_mb_min,
    "write_amplification": "$write_amp",
    "sui_data_growth_kb": $sui_growth_kb,
    "throttle_events": $THROTTLE_EVENTS,
    "abort_requested": $ABORT_REQUESTED,
    "package_id": "$PACKAGE_ID"
}
EOF
    
    log_success "Results saved to $RESULTS_DIR/"
    
    # Return status
    if [ "$ABORT_REQUESTED" -eq 1 ]; then
        log_warning "Benchmark aborted due to memory pressure"
        return 1
    fi
    
    return 0
}

# Trap for cleanup
trap 'ABORT_REQUESTED=1; echo ""; log_warning "Interrupt received, cleaning up..."' INT TERM

main "$@"
