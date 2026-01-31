#!/bin/bash
# 01-setup.sh - Optimized FDP setup for SUI benchmark
# Uses SYMLINKS for FDP stream placement (lighter than mount --bind)
#
# Why symlinks instead of mount --bind for FEMU:
# - No kernel mount table operations
# - No I/O burst that overwhelms FEMU's emulated NVMe
# - Avoids NVMe timeout/reset/kernel panic
# - Simple cleanup (just rm, no unmount ordering)
#
# FDP Placement Strategy (4 PIDs):
# ================================
#   PID 0 (HOT):  live/, consensus_db/ - heavy random I/O, short lifetime
#   PID 1 (WARM): indexes/ - frequent updates, medium lifetime
#   PID 2 (COOL): perpetual/ - historical data, medium-long lifetime
#   PID 3 (COLD): checkpoints/, logs/ - append-only, very long lifetime

set -e

FDP_MODE=${FDP_MODE:-0}
DEVICE=${DEVICE:-/dev/nvme0n1}
MOUNT_POINT=${MOUNT_POINT:-$HOME/f2fs_fdp_mount}
DATA_DIR=${DATA_DIR:-$HOME/sui_config}

echo "============================================================"
echo " FDP Setup for SUI Blockchain Benchmark"
echo " Mode: $([ $FDP_MODE -eq 1 ] && echo 'OPTIMIZED (multi-PID)' || echo 'BASELINE (single PID)')"
echo " Method: SYMLINKS (FEMU-friendly)"
echo "============================================================"

# ============================================================
# Step 1: Cleanup (symlinks are easy - just remove them)
# ============================================================
echo "[1/6] Cleaning up existing setup..."

# Kill any SUI processes
pkill -9 -x "sui" 2>/dev/null || true
pkill -9 -f "sui-node" 2>/dev/null || true
pkill -9 -f "sui start" 2>/dev/null || true
sleep 2

# Remove old symlinks and directories (no unmount ordering needed!)
rm -rf ${DATA_DIR} 2>/dev/null || true
rm -rf $HOME/.sui/sui_config 2>/dev/null || true

# Unmount if mounted
if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
    echo "  Unmounting ${MOUNT_POINT}..."
    sudo umount -l ${MOUNT_POINT} 2>/dev/null || true
fi
sudo fuser -k ${DEVICE} 2>/dev/null || true

sync
sleep 2

if mount | grep -q "${DEVICE}"; then
    echo "ERROR: Device still mounted!"
    mount | grep "${DEVICE}"
    exit 1
fi

echo "  Cleanup complete"

# ============================================================
# Step 2: Configure FDP
# ============================================================
echo "[2/6] Configuring FDP device..."
sudo /home/femu/fdp-scripts/fdp_send_sungjin ${DEVICE}

# ============================================================
# Step 3: Format with F2FS
# ============================================================
echo "[3/6] Formatting with F2FS (FDP-aware)..."
sudo /home/femu/fdp-scripts/f2fs-tools-fdp/mkfs/mkfs.f2fs -f -O lost_found ${DEVICE}

# ============================================================
# Step 4: Mount F2FS with FDP
# ============================================================
echo "[4/6] Mounting F2FS with 8 FDP streams..."
sudo /home/femu/fdp-scripts/f2fs-tools-fdp/fdp_f2fs_mount 8

sync
sleep 3  # Give FEMU time to stabilize

if ! mountpoint -q "${MOUNT_POINT}"; then
    echo "ERROR: Mount failed!"
    exit 1
fi