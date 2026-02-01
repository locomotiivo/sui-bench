#!/bin/bash
#
# FDP Directory Setup Script for SUI RocksDB
#
# This script creates the directory structure needed for FDP-aware RocksDB placement.
# Each p{N} directory will have a different i_generation value that becomes the FDP PID.
#
# COMBINED MODE (default, 8 PIDs):
#   authorities_db (account state): p0 (hot), p1 (warm), p2 (cold)
#   consensus_db (ledger/DAG):      p3 (hot), p4 (warm), p5 (cold)
#   full_node_db (historical):      p6 (hot/warm), p7 (cold)
#
# LSM_ONLY MODE (3 PIDs):
#   All DBs share: p0 (hot), p1 (warm), p2 (cold)
#
# Usage: ./fdp_mount_setup.sh [base_path] [mode]
#   base_path: Base path for FDP mount (default: /home/femu/f2fs_fdp_mount)
#   mode: "combined" (default, 8 PIDs) or "lsm_only" (3 PIDs)
#
# Prerequisites:
# - F2FS filesystem must be mounted with fdp_log_n option
# - Example mount: mount -t f2fs -o fdp_log_n=8 /dev/nvme0n1 /home/femu/f2fs_fdp_mount

set -e

# Configuration
BASE_PATH="${1:-/home/femu/f2fs_fdp_mount}"
MODE="${2:-combined}"

# Database configurations
# Format: "db_name:pid_start:num_streams"
if [ "$MODE" = "combined" ]; then
    NUM_PIDS=8
    declare -A DB_CONFIG=(
        ["authorities_db"]="0:3"   # PIDs 0,1,2 - Account state (hot/warm/cold)
        ["consensus_db"]="3:3"     # PIDs 3,4,5 - Ledger/DAG (hot/warm/cold)
        ["full_node_db"]="6:2"     # PIDs 6,7   - Historical (warm/cold)
        ["epoch_db"]="6:2"         # PIDs 6,7   - Shares with full_node_db
    )
else
    NUM_PIDS=3
    declare -A DB_CONFIG=(
        ["authorities_db"]="0:3"
        ["consensus_db"]="0:3"
        ["full_node_db"]="0:3"
        ["epoch_db"]="0:3"
    )
fi

echo "=========================================="
echo "FDP Directory Setup for SUI RocksDB"
echo "=========================================="
echo "Base path: $BASE_PATH"
echo "Mode: $MODE"
echo "Number of PIDs: $NUM_PIDS"
echo ""

# Check if base path exists
if [ ! -d "$BASE_PATH" ]; then
    echo "ERROR: Base path does not exist: $BASE_PATH"
    echo "Please mount F2FS filesystem first with fdp_log_n option"
    echo "Example: mount -t f2fs -o fdp_log_n=8 /dev/nvme0n1 $BASE_PATH"
    exit 1
fi

# Check if it's an F2FS mount
FSTYPE=$(stat -f -c '%T' "$BASE_PATH" 2>/dev/null || echo "unknown")
if [ "$FSTYPE" != "f2fs" ]; then
    echo "WARNING: $BASE_PATH is not an F2FS filesystem (detected: $FSTYPE)"
    echo "FDP placement may not work without F2FS with fdp_log_n option"
    # Skip interactive prompt in automated mode (when stdin is not a terminal)
    if [ -t 0 ]; then
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        echo "Running in automated mode, continuing..."
    fi
fi

# Create PID directories
echo "Creating FDP stream directories (p0-p$((NUM_PIDS-1)))..."
for i in $(seq 0 $((NUM_PIDS - 1))); do
    STREAM_DIR="$BASE_PATH/p$i"
    
    if [ ! -d "$STREAM_DIR" ]; then
        echo "  Creating $STREAM_DIR (PID $i)"
        mkdir -p "$STREAM_DIR"
    else
        echo "  Directory exists: $STREAM_DIR"
    fi
done

# Create database subdirectories in appropriate PID directories
echo ""
echo "Creating database directories..."
for DB_NAME in "${!DB_CONFIG[@]}"; do
    IFS=':' read -r PID_START NUM_STREAMS <<< "${DB_CONFIG[$DB_NAME]}"
    
    echo "  $DB_NAME: PIDs $PID_START-$((PID_START + NUM_STREAMS - 1))"
    for i in $(seq 0 $((NUM_STREAMS - 1))); do
        PID=$((PID_START + i))
        DB_DIR="$BASE_PATH/p$PID/$DB_NAME"
        if [ ! -d "$DB_DIR" ]; then
            mkdir -p "$DB_DIR"
            echo "    Created: $DB_DIR"
        fi
    done
done

echo ""
echo "=========================================="
echo "Directory Structure Created:"
echo "=========================================="
tree -L 2 "$BASE_PATH" 2>/dev/null || find "$BASE_PATH" -maxdepth 2 -type d | sort

echo ""
echo "=========================================="
echo "FDP PID Assignment ($MODE mode):"
echo "=========================================="
if [ "$MODE" = "combined" ]; then
    echo ""
    echo "  authorities_db (Account State - high churn, random access):"
    echo "    PID 0: HOT  - L0/L1 SST files (<256MB) - frequently accessed"
    echo "    PID 1: WARM - L2/L3 SST files (<1GB)   - moderate access"
    echo "    PID 2: COLD - L4+   SST files (>1GB)   - rarely accessed"
    echo ""
    echo "  consensus_db (Ledger/DAG - sequential append):"
    echo "    PID 3: HOT  - Recent blocks, active DAG"
    echo "    PID 4: WARM - Older blocks"
    echo "    PID 5: COLD - Historical blocks"
    echo ""
    echo "  full_node_db (Historical - archival, read-heavy):"
    echo "    PID 6: WARM - Semi-recent data"
    echo "    PID 7: COLD - Archival data"
    echo ""
    echo "  WHY THIS HELPS:"
    echo "  - Account state GC won't interfere with ledger data"
    echo "  - Hot ledger blocks separate from hot account objects"
    echo "  - Each workload type has isolated GC behavior"
else
    echo ""
    echo "  All databases share the same PIDs:"
    echo "    PID 0: HOT  - L0/L1 SST files from all DBs"
    echo "    PID 1: WARM - L2/L3 SST files from all DBs"
    echo "    PID 2: COLD - L4+   SST files from all DBs"
    echo ""
    echo "  NOTE: Simpler but less workload isolation"
fi

echo ""
echo "=========================================="
echo "Usage Instructions:"
echo "=========================================="
echo "1. Set environment variables before starting SUI:"
echo "   export SUI_FDP_ENABLED=1"
echo "   export SUI_FDP_BASE_PATH=$BASE_PATH"
echo "   export SUI_FDP_MODE=$MODE"
echo ""
echo "2. Optional: Adjust size thresholds (in MB):"
echo "   export SUI_FDP_HOT_SIZE_MB=256    # Files < 256MB → hot stream"
echo "   export SUI_FDP_WARM_SIZE_MB=1024  # Files < 1GB → warm stream"
echo ""
echo "=========================================="
echo "Setup complete!"

