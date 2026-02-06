#!/bin/bash
#
# Swarm Benchmark - Sustained High I/O for FDP Testing
#
# Strategy: Create massive numbers of small objects to maximize RocksDB
# compaction and generate sustained 1+ GB/min disk writes.
#
# Key Insights from Testing:
# - Small objects (100 bytes) generate 30-35x write amplification
# - Multi-address parallelism avoids gas coin contention
# - Memory throttling prevents OOM crashes
#

set -e

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUI_BINARY="/home/femu/sui/target/release/sui"
NVME_DEVICE="nvme0n1"

# SUI Config - Use existing node
export SUI_CONFIG_DIR="${SUI_CONFIG_DIR:-/home/femu/f2fs_fdp_mount/p0/sui_node}"

# Package ID (must be published already)
PACKAGE_ID="${PACKAGE_ID:-}"

# Benchmark parameters
DURATION="${DURATION:-300}"                   # 5 minutes default
WORKERS_PER_ADDR="${WORKERS_PER_ADDR:-4}"     # Workers per address
BATCH_SIZE="${BATCH_SIZE:-150}"               # Objects per transaction
MEM_THRESHOLD="${MEM_THRESHOLD:-88}"          # Memory throttle %

# Output
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/results/swarm_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$RESULTS_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

get_memory_pct() {
    free | awk '/Mem:/ {printf "%.0f", ($3/$2)*100}'
}

get_disk_write_sectors() {
    awk -v dev="$NVME_DEVICE" '$3 == dev {print $10}' /proc/diskstats 2>/dev/null || echo 0
}

get_sui_data_size() {
    du -sk "$SUI_CONFIG_DIR" 2>/dev/null | cut -f1 || echo 0
}

check_sui_node() {
    curl -s http://localhost:9000/health >/dev/null 2>&1
}

# ═══════════════════════════════════════════════════════════════════════════════
# WORKER FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

run_worker() {
    local worker_id=$1
    local addr=$2
    local package=$3
    local batch=$4
    local end_time=$5
    local out_file="$RESULTS_DIR/worker_${worker_id}.out"
    
    local tx_count=0
    local err_count=0
    
    # Switch to our address at start
    $SUI_BINARY client switch --address "$addr" >/dev/null 2>&1 || true
    
    while [ $(date +%s) -lt $end_time ]; do
        # Memory throttle
        local mem_pct=$(get_memory_pct)
        while [ "$mem_pct" -gt "$MEM_THRESHOLD" ]; do
            sleep 0.3
            mem_pct=$(get_memory_pct)
        done
        
        # Submit transaction
        if $SUI_BINARY client call --package "$package" --module io_churn \
           --function create_batch --args "$batch" --gas-budget 500000000 \
           2>/dev/null | grep -q "Successfully"; then
            tx_count=$((tx_count + 1))
        else
            err_count=$((err_count + 1))
        fi
    done
    
    echo "$tx_count $err_count" > "$out_file"
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

main() {
    log_info "╔═══════════════════════════════════════════════════════════════╗"
    log_info "║  Swarm Benchmark - Sustained High I/O for FDP Testing        ║"
    log_info "╚═══════════════════════════════════════════════════════════════╝"
    echo ""
    
    # Check SUI node
    if ! check_sui_node; then
        log_error "SUI node not running on localhost:9000"
        exit 1
    fi
    
    # Get addresses
    log_info "Getting available addresses..."
    local addrs=($($SUI_BINARY client addresses --json 2>/dev/null | jq -r '.[].alias' | head -5))
    local num_addrs=${#addrs[@]}
    
    if [ $num_addrs -eq 0 ]; then
        log_error "No addresses found"
        exit 1
    fi
    log_info "Found $num_addrs addresses: ${addrs[*]}"
    
    # Get or find package ID
    if [ -z "$PACKAGE_ID" ]; then
        PACKAGE_ID=$(cat "$SUI_CONFIG_DIR/.package_id" 2>/dev/null || echo "")
    fi
    
    if [ -z "$PACKAGE_ID" ]; then
        log_error "No package ID. Set PACKAGE_ID env var or ensure .package_id exists"
        exit 1
    fi
    log_info "Package ID: $PACKAGE_ID"
    
    # Calculate totals
    local total_workers=$((num_addrs * WORKERS_PER_ADDR))
    
    log_info ""
    log_info "Configuration:"
    log_info "  Addresses: $num_addrs"
    log_info "  Workers per address: $WORKERS_PER_ADDR"
    log_info "  Total workers: $total_workers"
    log_info "  Duration: ${DURATION}s"
    log_info "  Batch size: $BATCH_SIZE objects/tx"
    log_info "  Memory threshold: ${MEM_THRESHOLD}%"
    log_info ""
    
    # Initial stats
    local init_sectors=$(get_disk_write_sectors)
    local init_sui_size=$(get_sui_data_size)
    local start_time=$(date +%s)
    local end_time=$((start_time + DURATION))
    
    log_info "Initial: sectors=$init_sectors, SUI=${init_sui_size}KB"
    log_info ""
    
    # Clean old outputs
    rm -f "$RESULTS_DIR"/worker_*.out
    
    # Start workers
    log_info "Launching $total_workers workers..."
    local worker_id=0
    local pids=()
    
    for addr in "${addrs[@]}"; do
        for ((w=0; w<WORKERS_PER_ADDR; w++)); do
            run_worker "$worker_id" "$addr" "$PACKAGE_ID" "$BATCH_SIZE" "$end_time" &
            pids+=($!)
            worker_id=$((worker_id + 1))
        done
    done
    
    log_info "Launched $worker_id workers"
    log_info ""
    
    # Monitor loop
    local peak_rate=0
    local last_sectors=$init_sectors
    local last_time=$start_time
    
    while [ -n "$(pgrep -f 'sui client call.*io_churn')" ]; do
        sleep 5
        
        local now=$(date +%s)
        local elapsed=$((now - start_time))
        local current_sectors=$(get_disk_write_sectors)
        local current_sui_size=$(get_sui_data_size)
        
        # Calculate rates
        local delta_sectors=$((current_sectors - last_sectors))
        local delta_time=$((now - last_time))
        local instant_rate=0
        if [ $delta_time -gt 0 ]; then
            instant_rate=$((delta_sectors * 512 / delta_time / 1048576))
        fi
        
        local total_sectors=$((current_sectors - init_sectors))
        local total_mb=$((total_sectors * 512 / 1048576))
        local avg_rate=$((total_mb * 60 / elapsed))
        
        [ $avg_rate -gt $peak_rate ] && peak_rate=$avg_rate
        
        local mem_pct=$(get_memory_pct)
        local workers=$(pgrep -c -f 'sui client call.*io_churn' 2>/dev/null || echo 0)
        local sui_growth=$((current_sui_size - init_sui_size))
        
        printf "\r[%3ds] Mem:%2d%% | Rate:%2dMB/s Avg:%dMB/min | Total:%dMB | SUI:+%dKB | Workers:%d    " \
            "$elapsed" "$mem_pct" "$instant_rate" "$avg_rate" "$total_mb" "$sui_growth" "$workers"
        
        last_sectors=$current_sectors
        last_time=$now
    done
    
    echo ""
    log_info ""
    log_info "Waiting for workers to finish..."
    
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    
    # Sync and measure
    sync
    sleep 2
    
    local final_sectors=$(get_disk_write_sectors)
    local final_sui_size=$(get_sui_data_size)
    local actual_duration=$(($(date +%s) - start_time))
    
    # Collect worker results
    local total_tx=0
    local total_err=0
    for f in "$RESULTS_DIR"/worker_*.out; do
        if [ -f "$f" ]; then
            read tx err < "$f" 2>/dev/null || { tx=0; err=0; }
            total_tx=$((total_tx + tx))
            total_err=$((total_err + err))
        fi
    done
    
    # Calculate final stats
    local total_write_sectors=$((final_sectors - init_sectors))
    local total_write_mb=$((total_write_sectors * 512 / 1048576))
    local write_rate_mb=$((total_write_mb * 60 / actual_duration))
    local sui_growth=$((final_sui_size - init_sui_size))
    local objects_created=$((total_tx * BATCH_SIZE))
    local logical_mb=$((objects_created * 100 / 1048576))
    local write_amp="N/A"
    [ $logical_mb -gt 0 ] && write_amp=$((total_write_mb / logical_mb))
    
    log_info ""
    log_info "╔═══════════════════════════════════════════════════════════════╗"
    log_info "║  RESULTS                                                      ║"
    log_info "╚═══════════════════════════════════════════════════════════════╝"
    log_info "Duration: ${actual_duration}s"
    log_info "Transactions: $total_tx (errors: $total_err)"
    log_info "Objects created: $objects_created (~100 bytes each = ${logical_mb}MB logical)"
    log_info "Host writes: $total_write_mb MB ($write_rate_mb MB/min)"
    log_info "Peak rate: $peak_rate MB/min"
    log_info "SUI data growth: ${sui_growth}KB"
    log_info "Write amplification: ${write_amp}x"
    
    if [ $actual_duration -gt 0 ]; then
        log_info "TPS: $((total_tx / actual_duration))"
        log_info "Objects/sec: $((objects_created / actual_duration))"
    fi
    
    # Save results
    cat > "$RESULTS_DIR/summary.json" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "duration_sec": $actual_duration,
    "workers": $total_workers,
    "batch_size": $BATCH_SIZE,
    "memory_threshold_pct": $MEM_THRESHOLD,
    "total_transactions": $total_tx,
    "total_errors": $total_err,
    "objects_created": $objects_created,
    "logical_data_mb": $logical_mb,
    "host_write_mb": $total_write_mb,
    "write_rate_mb_per_min": $write_rate_mb,
    "peak_rate_mb_per_min": $peak_rate,
    "write_amplification": "$write_amp",
    "sui_data_growth_kb": $sui_growth,
    "package_id": "$PACKAGE_ID"
}
EOF
    
    log_success "Results saved to $RESULTS_DIR/summary.json"
    
    # Return success if we hit 1GB/min target
    if [ $write_rate_mb -ge 1000 ]; then
        log_success "TARGET ACHIEVED: $write_rate_mb MB/min >= 1000 MB/min (1 GB/min)"
        return 0
    else
        log_warning "Below target: $write_rate_mb MB/min < 1000 MB/min (1 GB/min)"
        return 1
    fi
}

main "$@"
