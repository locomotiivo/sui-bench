#!/bin/bash
#
# setup-fdp-symlinks.sh - Set up FDP-aware directory structure using symlinks
#
# This script creates symlinks to guide SUI data placement to specific FDP
# Placement IDs (PIDs) WITHOUT needing to periodically move files.
#
# Strategy:
#   1. Each partition (p0-p7) maps to a different FDP PID
#   2. Create actual directories in appropriate partitions
#   3. Create symlinks in SUI config directory pointing to those directories
#   4. SUI writes to symlinks → data goes directly to correct PID
#
# SUI Directory Structure and Recommended PID Mapping:
#   - authorities_db/    → p0 (PID 0) - Hot: consensus, frequently updated
#   - consensus_db/      → p1 (PID 1) - Hot: consensus data
#   - full_node_db/      → p2 (PID 2) - Warm: node state
#   - genesis.blob       → p3 (PID 3) - Cold: rarely changes
#   - client.yaml        → p3 (PID 3) - Cold: config files
#   - *.yaml configs     → p3 (PID 3) - Cold: config files
#   - sui.aliases        → p3 (PID 3) - Cold: config files
#   - logs/              → p4 (PID 4) - Hot: append-only logs
#   - checkpoints/       → p5 (PID 5) - Warm: checkpoint data
#   - objects/           → p6 (PID 6) - Warm: object store
#   - transactions/      → p7 (PID 7) - Hot: transaction data
#

set -e

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

MOUNT_POINT="${MOUNT_POINT:-/home/femu/f2fs_fdp_mount}"
SUI_CONFIG_DIR="${SUI_CONFIG_DIR:-$MOUNT_POINT/p0/sui_node}"

# PID mapping (partition number = PID)
# Adjust based on your workload characteristics
declare -A PID_MAP=(
    # Hot data - frequently written/updated
    ["authorities_db"]="p0"      # PID 0: Authority/validator data
    ["consensus_db"]="p1"        # PID 1: Consensus data
    
    # Warm data - moderate write frequency  
    ["full_node_db"]="p2"        # PID 2: Full node database
    ["checkpoints"]="p5"         # PID 5: Checkpoint data
    ["objects"]="p6"             # PID 6: Object store
    
    # Cold data - rarely updated
    ["config"]="p3"              # PID 3: Config files (yaml, genesis)
    
    # Append-only / sequential
    ["logs"]="p4"                # PID 4: Log files
    ["transactions"]="p7"        # PID 7: Transaction logs
)

# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

create_partition_dirs() {
    log "Creating partition directories..."
    for i in {0..7}; do
        local pdir="$MOUNT_POINT/p$i"
        if [ ! -d "$pdir" ]; then
            sudo mkdir -p "$pdir"
            log "  Created $pdir"
        fi
    done
    sudo chmod -R 777 "$MOUNT_POINT"
}

setup_sui_symlinks() {
    local config_dir="$1"
    
    log "Setting up FDP symlinks for: $config_dir"
    
    # Create the base config directory if it doesn't exist
    mkdir -p "$config_dir"
    
    # Create actual directories in target partitions and symlinks in config dir
    for dir_name in "${!PID_MAP[@]}"; do
        local pid="${PID_MAP[$dir_name]}"
        local actual_dir="$MOUNT_POINT/$pid/$dir_name"
        local symlink_path="$config_dir/$dir_name"
        
        # Create actual directory in the target partition
        if [ ! -d "$actual_dir" ]; then
            mkdir -p "$actual_dir"
            log "  Created actual dir: $actual_dir (PID ${pid#p})"
        fi
        
        # Create or update symlink
        if [ -L "$symlink_path" ]; then
            # Already a symlink - check if pointing to right place
            local current_target=$(readlink -f "$symlink_path")
            if [ "$current_target" != "$actual_dir" ]; then
                rm "$symlink_path"
                ln -s "$actual_dir" "$symlink_path"
                log "  Updated symlink: $dir_name → $pid (PID ${pid#p})"
            else
                log "  Symlink OK: $dir_name → $pid"
            fi
        elif [ -d "$symlink_path" ]; then
            # Existing directory - migrate data then replace with symlink
            log "  Migrating existing data: $dir_name → $pid"
            if [ "$(ls -A "$symlink_path" 2>/dev/null)" ]; then
                # Directory has content - move it
                cp -a "$symlink_path"/* "$actual_dir"/ 2>/dev/null || true
                rm -rf "$symlink_path"
            else
                # Empty directory - just remove
                rmdir "$symlink_path" 2>/dev/null || rm -rf "$symlink_path"
            fi
            ln -s "$actual_dir" "$symlink_path"
            log "  Created symlink: $dir_name → $pid (PID ${pid#p})"
        else
            # Nothing exists - create symlink
            ln -s "$actual_dir" "$symlink_path"
            log "  Created symlink: $dir_name → $pid (PID ${pid#p})"
        fi
    done
}

setup_config_files() {
    local config_dir="$1"
    local config_pid="${PID_MAP[config]}"
    local config_actual="$MOUNT_POINT/$config_pid"
    
    log "Setting up config file symlinks..."
    
    # List of config files that should go to cold storage
    local config_files=("genesis.blob" "client.yaml" "fullnode.yaml" "network.yaml" "sui.aliases")
    
    for file in "${config_files[@]}"; do
        local src="$config_dir/$file"
        local dst="$config_actual/$file"
        
        if [ -f "$src" ] && [ ! -L "$src" ]; then
            # Regular file exists - move to config partition and symlink
            mv "$src" "$dst"
            ln -s "$dst" "$src"
            log "  Moved config: $file → $config_pid"
        elif [ ! -e "$src" ] && [ -f "$dst" ]; then
            # File exists in target, create symlink
            ln -s "$dst" "$src"
            log "  Linked config: $file → $config_pid"
        fi
    done
}

verify_symlinks() {
    local config_dir="$1"
    
    log ""
    log "Verifying FDP symlink setup:"
    log "┌────────────────────┬────────┬─────────────────────────────────────────┐"
    log "│ Directory          │ PID    │ Target                                  │"
    log "├────────────────────┼────────┼─────────────────────────────────────────┤"
    
    for dir_name in "${!PID_MAP[@]}"; do
        local pid="${PID_MAP[$dir_name]}"
        local symlink_path="$config_dir/$dir_name"
        
        if [ -L "$symlink_path" ]; then
            local target=$(readlink "$symlink_path")
            printf "│ %-18s │ %-6s │ %-39s │\n" "$dir_name" "${pid#p}" "$target"
        else
            printf "│ %-18s │ %-6s │ %-39s │\n" "$dir_name" "${pid#p}" "(NOT A SYMLINK)"
        fi
    done
    
    log "└────────────────────┴────────┴─────────────────────────────────────────┘"
}

print_usage() {
    cat << EOF
Usage: $0 [OPTIONS] [SUI_CONFIG_DIR]

Set up FDP-aware symlinks for SUI data placement.

Options:
    -h, --help      Show this help message
    -v, --verify    Only verify existing symlinks (don't create)
    -m, --mount     Mount point (default: $MOUNT_POINT)

Arguments:
    SUI_CONFIG_DIR  SUI configuration directory (default: $SUI_CONFIG_DIR)

Examples:
    # Set up symlinks for default location
    $0
    
    # Set up for custom config directory
    $0 /path/to/sui/config
    
    # Verify existing setup
    $0 --verify

PID Mapping:
    PID 0 (p0): authorities_db  - Hot: validator data
    PID 1 (p1): consensus_db    - Hot: consensus data
    PID 2 (p2): full_node_db    - Warm: node database
    PID 3 (p3): config          - Cold: config files
    PID 4 (p4): logs            - Sequential: log files
    PID 5 (p5): checkpoints     - Warm: checkpoints
    PID 6 (p6): objects         - Warm: object store
    PID 7 (p7): transactions    - Hot: transaction data

EOF
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

VERIFY_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            print_usage
            exit 0
            ;;
        -v|--verify)
            VERIFY_ONLY=true
            shift
            ;;
        -m|--mount)
            MOUNT_POINT="$2"
            shift 2
            ;;
        *)
            SUI_CONFIG_DIR="$1"
            shift
            ;;
    esac
done

# Verify mount point exists
if [ ! -d "$MOUNT_POINT" ]; then
    log "ERROR: Mount point does not exist: $MOUNT_POINT"
    exit 1
fi

if [ "$VERIFY_ONLY" = true ]; then
    verify_symlinks "$SUI_CONFIG_DIR"
    exit 0
fi

log "═══════════════════════════════════════════════════════════════════════════════"
log "  FDP SYMLINK SETUP"
log "═══════════════════════════════════════════════════════════════════════════════"
log ""
log "Mount point:    $MOUNT_POINT"
log "SUI config:     $SUI_CONFIG_DIR"
log ""

# Create partition directories
create_partition_dirs

# Set up symlinks
setup_sui_symlinks "$SUI_CONFIG_DIR"

# Set up config file symlinks
setup_config_files "$SUI_CONFIG_DIR"

# Verify
verify_symlinks "$SUI_CONFIG_DIR"

log ""
log "FDP symlink setup complete!"
log ""
log "Now when SUI writes to these directories, data will be placed directly"
log "on the appropriate FDP partition (PID) without needing to move files later."
