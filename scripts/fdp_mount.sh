#!/bin/bash
#
# Semantic FDP Directory Setup Script for SUI Blockchain
#
# This script creates a directory structure based on DATA SEMANTICS rather than
# LSM file sizes. The goal is to physically separate:
#
#   1. ACCOUNT STATE (PID 0): High-churn, mutable data
#      - Objects, transaction locks, per-epoch markers
#      - Frequently updated, old versions pruned
#
#   2. LEDGER DATA (PID 1): Append-only, immutable data
#      - Transactions, effects, events, consensus blocks
#      - Written once, never modified, bulk-pruned only
#
# This semantic separation reduces GC interference because:
# - Account state invalidates frequently → high GC activity
# - Ledger data rarely invalidates → low GC activity
# - Mixing them causes unnecessary copying of immutable ledger data
#
# Usage: ./fdp_semantic_mount.sh [base_path]
#   base_path: Base path for FDP mount (default: /home/femu/f2fs_fdp_mount)
#
# Prerequisites:
# - F2FS filesystem must be mounted with fdp_log_n=2 (only 2 PIDs needed!)
# - Example mount: mount -t f2fs -o fdp_log_n=2 /dev/nvme0n1 /home/femu/f2fs_fdp_mount

set -e

# Configuration
BASE_PATH="${1:-/home/femu/f2fs_fdp_mount}"
FDP_TOOLS_DIR="/home/femu/fdp-scripts/f2fs-tools-fdp"

echo "════════════════════════════════════════════════════════════════════════"
echo "  Semantic FDP Directory Setup for SUI Blockchain"
echo "════════════════════════════════════════════════════════════════════════"
echo ""
echo "Base path: $BASE_PATH"
echo ""

# Check if base path exists
if [ ! -d "$BASE_PATH" ]; then
    echo "ERROR: Base path does not exist: $BASE_PATH"
    echo "Please mount F2FS filesystem first with fdp_log_n option"
    echo "Example: mount -t f2fs -o fdp_log_n=2 /dev/nvme0n1 $BASE_PATH"
    exit 1
fi

# Check if it's an F2FS mount
FSTYPE=$(stat -f -c '%T' "$BASE_PATH" 2>/dev/null || echo "unknown")
if [ "$FSTYPE" != "f2fs" ]; then
    echo "WARNING: $BASE_PATH is not an F2FS filesystem (detected: $FSTYPE)"
    echo "FDP placement will not work without F2FS with fdp_log_n option"
    if [ -t 0 ]; then
        read -p "Continue anyway for testing? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        echo "Running in automated mode, continuing..."
    fi
fi

echo "Creating semantic FDP directories..."
echo ""

# ════════════════════════════════════════════════════════════════════════════
# PID 0: ACCOUNT STATE (High Churn, Mutable)
# ════════════════════════════════════════════════════════════════════════════
ACCOUNT_STATE_DIR="$BASE_PATH/account_state"
echo "Creating ACCOUNT_STATE directory (PID 0): $ACCOUNT_STATE_DIR"
if [ ! -d "$ACCOUNT_STATE_DIR" ]; then
    mkdir -p "$ACCOUNT_STATE_DIR"
    echo "  ✓ Created: $ACCOUNT_STATE_DIR"
else
    echo "  ✓ Already exists: $ACCOUNT_STATE_DIR"
fi

# Create subdirectories for each database
mkdir -p "$ACCOUNT_STATE_DIR/authorities_db"
mkdir -p "$ACCOUNT_STATE_DIR/epoch_db"
echo "  ✓ Created authorities_db and epoch_db subdirectories"

# ════════════════════════════════════════════════════════════════════════════
# PID 1: LEDGER DATA (Append-Only, Immutable)
# ════════════════════════════════════════════════════════════════════════════
LEDGER_DIR="$BASE_PATH/ledger"
echo ""
echo "Creating LEDGER directory (PID 1): $LEDGER_DIR"
if [ ! -d "$LEDGER_DIR" ]; then
    mkdir -p "$LEDGER_DIR"
    echo "  ✓ Created: $LEDGER_DIR"
else
    echo "  ✓ Already exists: $LEDGER_DIR"
fi

# Create subdirectories for each database
mkdir -p "$LEDGER_DIR/authorities_db"
mkdir -p "$LEDGER_DIR/consensus_db"
mkdir -p "$LEDGER_DIR/full_node_db"
mkdir -p "$LEDGER_DIR/checkpoints"
echo "  ✓ Created authorities_db, consensus_db, full_node_db, checkpoints subdirectories"

# Set permissions
sudo chmod -R 777 "$BASE_PATH" 2>/dev/null || chmod -R 777 "$BASE_PATH"

echo ""
echo "════════════════════════════════════════════════════════════════════════"
echo "  Directory Structure Created"
echo "════════════════════════════════════════════════════════════════════════"
echo ""
tree -L 3 "$BASE_PATH" 2>/dev/null || find "$BASE_PATH" -maxdepth 3 -type d | sort

echo ""
echo "════════════════════════════════════════════════════════════════════════"
echo "  Semantic FDP Data Placement Strategy"
echo "════════════════════════════════════════════════════════════════════════"
echo ""
echo "  PID 0: ACCOUNT_STATE ($ACCOUNT_STATE_DIR)"
echo "  ├── High churn, mutable data"
echo "  ├── authorities_db/"
echo "  │   ├── objects (object state - frequently updated)"
echo "  │   ├── owned_object_transaction_locks (very high churn)"
echo "  │   └── object_per_epoch_marker_table (pruned per epoch)"
echo "  └── epoch_db/ (rotates each epoch)"
echo ""
echo "  PID 1: LEDGER ($LEDGER_DIR)"
echo "  ├── Append-only, immutable data"
echo "  ├── authorities_db/"
echo "  │   ├── transactions (write-once, never modified)"
echo "  │   ├── effects (write-once, never modified)"
echo "  │   └── events (append-only)"
echo "  ├── consensus_db/"
echo "  │   ├── blocks (append-only DAG)"
echo "  │   ├── commits (append-only)"
echo "  │   └── votes (append-only)"
echo "  └── full_node_db/"
echo "      ├── checkpoints (append-only)"
echo "      └── indexes (read-heavy)"
echo ""
echo "════════════════════════════════════════════════════════════════════════"
echo "  Why This Works Better Than LSM-Level Separation"
echo "════════════════════════════════════════════════════════════════════════"
echo ""
echo "  LSM-Level (Old Approach):"
echo "    - Separates by file SIZE (L0/L1 vs L4+)"
echo "    - RocksDB db_paths uses capacity overflow, NOT level assignment"
echo "    - Hot account data and hot ledger data STILL mix together"
echo "    - GC improvement is indirect and unreliable"
echo ""
echo "  Semantic-Level (New Approach):"
echo "    - Separates by DATA CHARACTERISTICS"
echo "    - Account state: high update/delete rate → high GC"
echo "    - Ledger data: append-only → minimal GC"
echo "    - True lifetime isolation → no interference"
echo ""
echo "════════════════════════════════════════════════════════════════════════"
echo "  Usage Instructions"
echo "════════════════════════════════════════════════════════════════════════"
echo ""
echo "1. Set environment variables before starting SUI:"
echo ""
echo "   export SUI_FDP_SEMANTIC=1"
echo "   export SUI_FDP_BASE_PATH=$BASE_PATH"
echo ""
echo "2. Mount F2FS with only 2 FDP streams (simpler!):"
echo ""
echo "   sudo $FDP_TOOLS_DIR/fdp_send_sungjin /dev/nvme0n1"
echo "   sudo $FDP_TOOLS_DIR/mkfs/mkfs.f2fs -f -O lost_found /dev/nvme0n1"
echo "   sudo mount -t f2fs -o fdp_log_n=2 /dev/nvme0n1 $BASE_PATH"
echo ""
echo "3. Start SUI node normally - it will use semantic FDP automatically"
echo ""
echo "════════════════════════════════════════════════════════════════════════"
echo "  Setup Complete!"
echo "════════════════════════════════════════════════════════════════════════"
