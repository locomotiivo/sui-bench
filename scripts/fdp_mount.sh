#!/bin/bash
# ============================================================================
# FDP WAL-Semantic Mount Script
# ============================================================================
# Creates the directory structure for WAL-first semantic FDP placement.
# 
# This script implements an 8-PID strategy that consolidates ALL WAL files
# into a single PID (0), enabling highly efficient garbage collection.
#
# 8-PID ALLOCATION:
# ┌─────────────────────────────────────────────────────────────────────────┐
# │ PID 0: ALL WAL FILES (consolidated across all databases)                │
# │        Lifetime: Very short (seconds to minutes)                        │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ PID 1: authority_db SST L0-L1 (HOT)    Lifetime: Short (minutes-hours)  │
# │ PID 2: authority_db SST L2+ (COLD)     Lifetime: Long (hours-days)      │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ PID 3: consensus_db SST L0-L1 (HOT)    Lifetime: Short                  │
# │ PID 4: consensus_db SST L2+ (COLD)     Lifetime: Long                   │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ PID 5: fullnode_db SST L0-L1 (HOT)     Lifetime: Short                  │
# │ PID 6: fullnode_db SST L2+ (COLD)      Lifetime: Long                   │
# ├─────────────────────────────────────────────────────────────────────────┤
# │ PID 7: MANIFEST, OPTIONS, CURRENT      Lifetime: Very long (days-weeks) │
# └─────────────────────────────────────────────────────────────────────────┘
#
# USAGE:
#   ./fdp_mount.sh [mount_point] [device]
#
# EXAMPLE:
#   ./fdp_mount.sh /home/femu/f2fs_fdp_mount /dev/nvme0n1
#
# After running this script, set the following environment variables:
#   export SUI_FDP_WAL_SEMANTIC=1
#   export SUI_FDP_BASE_PATH=/home/femu/f2fs_fdp_mount
#   export SUI_FDP_HOT_SIZE_MB=256  # optional, default 256MB
# ============================================================================

set -e

# Default values
MOUNT_POINT="${1:-/home/femu/f2fs_fdp_mount}"
DEVICE="${2:-/dev/nvme0n1}"
FDP_LOG_N="${FDP_LOG_N:-8}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE} $1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# ============================================================================
# Pre-flight checks
# ============================================================================
log_section "Pre-flight Checks"

if [ "$EUID" -ne 0 ]; then
    log_error "This script must be run as root (for mounting)"
    exit 1
fi

# Check if device exists
if [ ! -b "$DEVICE" ]; then
    log_error "Device $DEVICE does not exist"
    exit 1
fi

# Check for FDP support
if command -v nvme &> /dev/null; then
    FDP_STATUS=$(nvme fdp status "$DEVICE" 2>/dev/null | grep -i "fdp enabled" || echo "")
    if [[ -z "$FDP_STATUS" ]]; then
        log_warn "Could not verify FDP status. Continuing anyway..."
    else
        log_info "FDP Status: $FDP_STATUS"
    fi
fi

log_info "Mount point: $MOUNT_POINT"
log_info "Device: $DEVICE"
log_info "FDP PIDs: $FDP_LOG_N"

# ============================================================================
# Unmount if already mounted
# ============================================================================
log_section "Preparing Mount Point"

if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    log_warn "Mount point already mounted. Unmounting..."
    umount "$MOUNT_POINT" || {
        log_error "Failed to unmount. Please unmount manually."
        exit 1
    }
fi

# Create mount point if needed
mkdir -p "$MOUNT_POINT"

# ============================================================================
# Format and Mount F2FS with FDP
# ============================================================================
log_section "Formatting F2FS with FDP"

log_info "Formatting $DEVICE with F2FS (fdp_log_n=$FDP_LOG_N)..."
mkfs.f2fs -f -m "fdp_log_n=$FDP_LOG_N" "$DEVICE"

log_info "Mounting F2FS with nodiscard option..."
mount -t f2fs -o nodiscard "$DEVICE" "$MOUNT_POINT"

log_info "F2FS mounted successfully"

# ============================================================================
# Create FDP Directory Structure
# ============================================================================
log_section "Creating WAL-Semantic FDP Directory Structure"

# PID 0: Consolidated WAL (all databases)
log_info "Creating PID 0: ALL WAL (consolidated)"
mkdir -p "$MOUNT_POINT/p0/wal"
# Set i_generation = 0 via chattr or use the F2FS FDP extension
# Note: The actual FDP PID assignment happens via i_generation in the F2FS kernel

# PID 1: authority_db HOT SST (L0-L1)
log_info "Creating PID 1: authority_db HOT SST (L0-L1)"
mkdir -p "$MOUNT_POINT/p1/authority_db_hot"

# PID 2: authority_db COLD SST (L2+)
log_info "Creating PID 2: authority_db COLD SST (L2+)"
mkdir -p "$MOUNT_POINT/p2/authority_db_cold"

# PID 3: consensus_db HOT SST (L0-L1)
log_info "Creating PID 3: consensus_db HOT SST (L0-L1)"
mkdir -p "$MOUNT_POINT/p3/consensus_db_hot"

# PID 4: consensus_db COLD SST (L2+)
log_info "Creating PID 4: consensus_db COLD SST (L2+)"
mkdir -p "$MOUNT_POINT/p4/consensus_db_cold"

# PID 5: fullnode_db HOT SST (L0-L1)
log_info "Creating PID 5: fullnode_db HOT SST (L0-L1)"
mkdir -p "$MOUNT_POINT/p5/fullnode_db_hot"

# PID 6: fullnode_db COLD SST (L2+)
log_info "Creating PID 6: fullnode_db COLD SST (L2+)"
mkdir -p "$MOUNT_POINT/p6/fullnode_db_cold"

# PID 7: Metadata (MANIFEST, OPTIONS, CURRENT, etc.)
log_info "Creating PID 7: Metadata (MANIFEST, etc.)"
mkdir -p "$MOUNT_POINT/p7/metadata"

# ============================================================================
# Set i_generation for FDP PID assignment
# ============================================================================
log_section "Setting FDP PIDs via i_generation"

# The F2FS FDP kernel uses i_generation to determine which PID to use
# We use a custom tool or ioctl to set i_generation

# Check if we have the FDP generation setter tool
FDP_SET_GEN="${FDP_SET_GEN:-/home/femu/fdp-scripts/f2fs-tools-fdp/set_fdp_gen}"

if [ -x "$FDP_SET_GEN" ]; then
    log_info "Using FDP generation setter: $FDP_SET_GEN"
    
    # Set i_generation for each PID directory
    "$FDP_SET_GEN" "$MOUNT_POINT/p0" 0 || log_warn "Failed to set i_gen for p0"
    "$FDP_SET_GEN" "$MOUNT_POINT/p1" 1 || log_warn "Failed to set i_gen for p1"
    "$FDP_SET_GEN" "$MOUNT_POINT/p2" 2 || log_warn "Failed to set i_gen for p2"
    "$FDP_SET_GEN" "$MOUNT_POINT/p3" 3 || log_warn "Failed to set i_gen for p3"
    "$FDP_SET_GEN" "$MOUNT_POINT/p4" 4 || log_warn "Failed to set i_gen for p4"
    "$FDP_SET_GEN" "$MOUNT_POINT/p5" 5 || log_warn "Failed to set i_gen for p5"
    "$FDP_SET_GEN" "$MOUNT_POINT/p6" 6 || log_warn "Failed to set i_gen for p6"
    "$FDP_SET_GEN" "$MOUNT_POINT/p7" 7 || log_warn "Failed to set i_gen for p7"
    
    log_info "FDP PIDs assigned via i_generation"
else
    log_warn "FDP generation setter not found at $FDP_SET_GEN"
    log_warn "PIDs will be assigned based on directory structure only"
    log_warn "For proper FDP, ensure the F2FS kernel supports directory-based PID inheritance"
fi

# ============================================================================
# Set Permissions
# ============================================================================
log_section "Setting Permissions"

chown -R femu:femu "$MOUNT_POINT" 2>/dev/null || chown -R $SUDO_USER:$SUDO_USER "$MOUNT_POINT" 2>/dev/null || true
chmod -R 755 "$MOUNT_POINT"

# ============================================================================
# Verification
# ============================================================================
log_section "Verification"

log_info "Directory structure created:"
tree -L 3 "$MOUNT_POINT" 2>/dev/null || find "$MOUNT_POINT" -type d | head -30

log_info ""
log_info "Mount info:"
df -h "$MOUNT_POINT"

# ============================================================================
# Generate Environment Configuration
# ============================================================================
log_section "Environment Configuration"

ENV_FILE="$MOUNT_POINT/fdp_env.sh"
cat > "$ENV_FILE" << EOF
#!/bin/bash
# FDP WAL-Semantic Environment Configuration
# Source this file before running SUI: source $ENV_FILE

# Enable WAL-Semantic FDP
export SUI_FDP_WAL_SEMANTIC=1
export SUI_FDP_BASE_PATH="$MOUNT_POINT"

# Optional: Adjust hot threshold (default 256MB)
# Files smaller than this go to HOT PIDs (L0-L1)
# export SUI_FDP_HOT_SIZE_MB=256

# Disable other FDP modes (they are overridden by WAL-semantic anyway)
unset SUI_FDP_ENABLED
unset SUI_FDP_SEMANTIC

# Verify settings
echo "FDP WAL-Semantic Configuration:"
echo "  SUI_FDP_WAL_SEMANTIC=\$SUI_FDP_WAL_SEMANTIC"
echo "  SUI_FDP_BASE_PATH=\$SUI_FDP_BASE_PATH"
echo ""
echo "PID Allocation:"
echo "  PID 0: ALL WAL (consolidated)"
echo "  PID 1-2: authority_db SST (hot/cold)"
echo "  PID 3-4: consensus_db SST (hot/cold)"
echo "  PID 5-6: fullnode_db SST (hot/cold)"
echo "  PID 7: metadata"
EOF
chmod +x "$ENV_FILE"

log_info "Environment file created: $ENV_FILE"
log_info "To enable FDP, run: source $ENV_FILE"

# ============================================================================
# Summary
# ============================================================================
log_section "Setup Complete"

echo ""
echo "WAL-Semantic FDP Directory Structure:"
echo ""
echo "┌─────────────────────────────────────────────────────────────────────────┐"
echo "│ PID 0: $MOUNT_POINT/p0/wal/              │ ALL WAL │"
echo "│        Lifetime: Very short (seconds to minutes)                        │"
echo "├─────────────────────────────────────────────────────────────────────────┤"
echo "│ PID 1: $MOUNT_POINT/p1/authority_db_hot/ │ auth L0-L1 │"
echo "│ PID 2: $MOUNT_POINT/p2/authority_db_cold/│ auth L2+   │"
echo "├─────────────────────────────────────────────────────────────────────────┤"
echo "│ PID 3: $MOUNT_POINT/p3/consensus_db_hot/ │ cons L0-L1 │"
echo "│ PID 4: $MOUNT_POINT/p4/consensus_db_cold/│ cons L2+   │"
echo "├─────────────────────────────────────────────────────────────────────────┤"
echo "│ PID 5: $MOUNT_POINT/p5/fullnode_db_hot/  │ fn L0-L1   │"
echo "│ PID 6: $MOUNT_POINT/p6/fullnode_db_cold/ │ fn L2+     │"
echo "├─────────────────────────────────────────────────────────────────────────┤"
echo "│ PID 7: $MOUNT_POINT/p7/metadata/         │ MANIFEST   │"
echo "└─────────────────────────────────────────────────────────────────────────┘"
echo ""
echo "Next steps:"
echo "  1. Source the environment: source $ENV_FILE"
echo "  2. Start SUI localnet: sui start"
echo "  3. Run benchmarks to verify FDP placement"
echo ""
