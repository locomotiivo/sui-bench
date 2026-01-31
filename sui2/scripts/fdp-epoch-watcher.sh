#!/bin/bash

# fdp-epoch-watcher.sh - Watches for new epoch directories and redirects them to appropriate PIDs
#
# Problem: Sui creates new epoch_N directories dynamically as epochs change.
# These are created in authorities_db/<hash>/live/store/epoch_N
# We want to redirect old epochs to p6 (cold storage) while current stays in p1 (hot)
#
# Strategy:
#   - epoch_0 to epoch_(current-2): p6 (cold, historical)
#   - epoch_(current-1): p5 (warm, previous epoch)
#   - epoch_current: p1 (hot, active)

set -e

MOUNT_POINT=${MOUNT_POINT:-$HOME/f2fs_fdp_mount}
CONFIG_DIR=${CONFIG_DIR:-$MOUNT_POINT/p0/sui_node}
POLL_INTERVAL=${POLL_INTERVAL:-10}  # Check every 10 seconds

log() { echo "[$(date '+%H:%M:%S')] [epoch-watcher] $1"; }

# Find the authorities_db hash directory
find_auth_hash() {
    find "$CONFIG_DIR/authorities_db" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -1
}

# Get current epoch from store directory
get_current_epoch() {
    local store_dir=$1
    local max_epoch=-1
    
    for epoch_dir in "$store_dir"/epoch_*; do
        if [ -d "$epoch_dir" ] || [ -L "$epoch_dir" ]; then
            local epoch_num=$(basename "$epoch_dir" | sed 's/epoch_//')
            if [ "$epoch_num" -gt "$max_epoch" ] 2>/dev/null; then
                max_epoch=$epoch_num
            fi
        fi
    done
    echo $max_epoch
}

# Redirect an epoch directory to a PID
redirect_epoch() {
    local epoch_dir=$1
    local target_pid=$2
    local hash=$3
    local epoch_num=$4
    
    local target="$MOUNT_POINT/p${target_pid}/auth_${hash}_epoch_${epoch_num}"
    
    # Skip if already a symlink to correct target
    if [ -L "$epoch_dir" ]; then
        local current=$(readlink "$epoch_dir")
        if [ "$current" = "$target" ]; then
            return 0
        fi
    fi
    
    # Skip if symlink (pointing elsewhere)
    if [ -L "$epoch_dir" ]; then
        log "  epoch_$epoch_num already symlinked to $(readlink "$epoch_dir")"
        return 0
    fi
    
    # Move data and create symlink
    if [ -d "$epoch_dir" ]; then
        log "  Moving epoch_$epoch_num to p$target_pid..."
        mkdir -p "$target"
        if [ "$(ls -A "$epoch_dir" 2>/dev/null)" ]; then
            cp -a "$epoch_dir"/* "$target"/ 2>/dev/null || true
        fi
        rm -rf "$epoch_dir"
        ln -sf "$target" "$epoch_dir"
        log "  ✓ epoch_$epoch_num -> p$target_pid"
    fi
}

# Main watch loop
watch_epochs() {
    log "Starting epoch watcher..."
    log "  Config dir: $CONFIG_DIR"
    log "  Poll interval: ${POLL_INTERVAL}s"
    
    local auth_hash_dir=""
    local hash=""
    
    while true; do
        # Find auth hash dir if not found yet
        if [ -z "$auth_hash_dir" ] || [ ! -d "$auth_hash_dir" ]; then
            auth_hash_dir=$(find_auth_hash)
            if [ -n "$auth_hash_dir" ]; then
                hash=$(basename "$auth_hash_dir")
                log "Found validator: $hash"
            fi
        fi
        
        if [ -n "$auth_hash_dir" ]; then
            local store_dir="$auth_hash_dir/live/store"
            
            if [ -d "$store_dir" ]; then
                local current_epoch=$(get_current_epoch "$store_dir")
                
                if [ "$current_epoch" -ge 0 ]; then
                    # Process all epoch directories
                    for epoch_dir in "$store_dir"/epoch_*; do
                        if [ -d "$epoch_dir" ] || [ -L "$epoch_dir" ]; then
                            local epoch_num=$(basename "$epoch_dir" | sed 's/epoch_//')
                            
                            if [ "$epoch_num" = "$current_epoch" ]; then
                                # Current epoch -> p1 (hot)
                                redirect_epoch "$epoch_dir" 1 "$hash" "$epoch_num"
                            elif [ "$epoch_num" = "$((current_epoch - 1))" ]; then
                                # Previous epoch -> p5 (warm)
                                redirect_epoch "$epoch_dir" 5 "$hash" "$epoch_num"
                            else
                                # Older epochs -> p6 (cold)
                                redirect_epoch "$epoch_dir" 6 "$hash" "$epoch_num"
                            fi
                        fi
                    done
                fi
            fi
        fi
        
        sleep $POLL_INTERVAL
    done
}

# Run once mode (for initial setup)
run_once() {
    log "Running one-time epoch placement..."
    
    local auth_hash_dir=$(find_auth_hash)
    if [ -z "$auth_hash_dir" ]; then
        log "No authorities_db found yet"
        return 1
    fi
    
    local hash=$(basename "$auth_hash_dir")
    local store_dir="$auth_hash_dir/live/store"
    
    if [ ! -d "$store_dir" ]; then
        log "Store directory not found: $store_dir"
        return 1
    fi
    
    local current_epoch=$(get_current_epoch "$store_dir")
    log "Current epoch: $current_epoch"
    
    local count=0
    for epoch_dir in "$store_dir"/epoch_*; do
        if [ -d "$epoch_dir" ] || [ -L "$epoch_dir" ]; then
            local epoch_num=$(basename "$epoch_dir" | sed 's/epoch_//')
            
            if [ "$epoch_num" = "$current_epoch" ]; then
                redirect_epoch "$epoch_dir" 1 "$hash" "$epoch_num"
            elif [ "$epoch_num" = "$((current_epoch - 1))" ]; then
                redirect_epoch "$epoch_dir" 5 "$hash" "$epoch_num"
            else
                redirect_epoch "$epoch_dir" 6 "$hash" "$epoch_num"
            fi
            ((count++))
        fi
    done
    
    log "Processed $count epoch directories"
}

# Print status
print_status() {
    log "FDP Epoch Placement Status:"
    log "=================================================="
    
    local auth_hash_dir=$(find_auth_hash)
    if [ -z "$auth_hash_dir" ]; then
        log "No authorities_db found"
        return
    fi
    
    local hash=$(basename "$auth_hash_dir")
    local store_dir="$auth_hash_dir/live/store"
    
    echo ""
    echo "Epoch Directory Mapping:"
    echo "------------------------"
    for epoch_dir in "$store_dir"/epoch_* "$store_dir/perpetual"; do
        if [ -L "$epoch_dir" ]; then
            echo "  $(basename $epoch_dir) -> $(readlink $epoch_dir)"
        elif [ -d "$epoch_dir" ]; then
            local size=$(du -sh "$epoch_dir" 2>/dev/null | cut -f1)
            echo "  $(basename $epoch_dir): $size (local)"
        fi
    done
    
    echo ""
    echo "PID Usage:"
    echo "----------"
    for pid in 0 1 2 3 4 5 6 7; do
        local size=$(du -sh "$MOUNT_POINT/p$pid" 2>/dev/null | cut -f1 || echo "0")
        local desc=""
        case $pid in
            0) desc="config/genesis" ;;
            1) desc="hot epochs" ;;
            2) desc="perpetual" ;;
            3) desc="consensus" ;;
            4) desc="checkpoints" ;;
            5) desc="warm epoch" ;;
            6) desc="cold epochs" ;;
            7) desc="indexes" ;;
        esac
        printf "  p%d: %6s  (%s)\n" $pid "$size" "$desc"
    done
}

# Main
case "${1:-watch}" in
    watch)
        watch_epochs
        ;;
    once)
        run_once
        ;;
    status)
        print_status
        ;;
    *)
        echo "Usage: $0 {watch|once|status}"
        echo "  watch  - Continuously monitor and redirect new epochs"
        echo "  once   - Run once and exit"
        echo "  status - Show current FDP placement status"
        exit 1
        ;;
esac
