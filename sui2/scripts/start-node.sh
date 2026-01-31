#!/bin/bash

# Setup script for local Sui node with FDP-aware storage placement
# 
# FDP Data Placement Strategy (8 PIDs):
# =====================================
# When FDP_MODE=1, data is segregated by write pattern and lifetime:
#
#   PID 0 (p0): Config & genesis - static, write-once, never changes
#   PID 1 (p1): Live object store - HOTTEST, highest random I/O, object mutations
#   PID 2 (p2): Perpetual store - historical objects, append-heavy
#   PID 3 (p3): Consensus DB - very hot, short-lived consensus data
#   PID 4 (p4): Checkpoints - sequential writes, medium lifetime
#   PID 5 (p5): Epoch data (current) - periodic bursts per epoch
#   PID 6 (p6): Epoch data (historical) - cold after epoch ends
#   PID 7 (p7): Indexes & RPC - query-heavy, medium write frequency
#
# This segregation minimizes GC overhead because:
#   - Hot data (p1,p3) won't cause GC to move cold data (p0,p6)
#   - Data with similar lifetimes share PIDs → better block utilization
#   - WAL-like consensus data isolated from long-lived objects
#
# Environment Variables:
#   FDP_MODE=1         Enable FDP data segregation (default: 0)
#   SUI_DISABLE_GAS=1  Disable gas fees (requires custom SUI build)
#   EPOCH_DURATION_MS  Epoch duration in milliseconds (default: 60000)

set -e

FDP_MODE="${FDP_MODE:-0}"
SUI_DISABLE_GAS="${SUI_DISABLE_GAS:-0}"
EPOCH_DURATION_MS="${EPOCH_DURATION_MS:-10000}"  # Default: 10 seconds

# Auto-detect project root from script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR=${RESULTS_DIR:-$SCRIPT_DIR/log}
MOUNT_POINT=${MOUNT_POINT:-$HOME/f2fs_fdp_mount}

# ============================================================
# Helper functions
# ============================================================
log() { echo "[$(date '+%H:%M:%S')] $1"; }
die() { log "ERROR: $1"; exit 1; }

kill_all_sui() {
    log "  Stopping SUI processes..."
    pkill -9 -f "sui start" 2>/dev/null || true
    pkill -9 -f "sui-node" 2>/dev/null || true
    pkill -9 -f "sui-faucet" 2>/dev/null || true
    sleep 3
    # Check if ports are still in use - warn but continue
    if ss -tlnp 2>/dev/null | grep -qE ":(9000|9123) "; then
        log "  Warning: ports 9000/9123 may still be in use"
        # Wait a bit more for ports to be released
        sleep 5
    fi
}

# ============================================================
# FDP Symlink Setup Functions
# ============================================================

# Create directory with proper permissions
create_pid_dir() {
    local pid_dir=$1
    mkdir -p "$pid_dir"
    chmod 777 "$pid_dir" 2>/dev/null || true
}

# Create symlink from source -> target (idempotent)
# If source exists as directory, move contents first
create_fdp_symlink() {
    local target_dir=$1   # Where data actually lives (on PID directory)
    local link_path=$2    # Where symlink will be created
    local desc=$3
    
    # If already a symlink pointing to correct target, we're done
    if [ -L "$link_path" ]; then
        local current_target=$(readlink -f "$link_path" 2>/dev/null)
        if [ "$current_target" = "$(readlink -f $target_dir 2>/dev/null)" ]; then
            log "    ✓ $desc: already linked"
            return 0
        fi
        rm -f "$link_path"
    fi
    
    # If directory exists with data, move it
    if [ -d "$link_path" ] && [ ! -L "$link_path" ]; then
        log "    Moving existing data from $link_path to $target_dir..."
        create_pid_dir "$target_dir"
        if [ "$(ls -A "$link_path" 2>/dev/null)" ]; then
            cp -a "$link_path"/* "$target_dir"/ 2>/dev/null || true
        fi
        rm -rf "$link_path"
    fi
    
    # Create parent directory if needed
    mkdir -p "$(dirname "$link_path")"
    
    # Create target directory and symlink
    create_pid_dir "$target_dir"
    ln -sf "$target_dir" "$link_path"
    log "    ✓ $desc: $link_path -> $target_dir"
}

# Post-genesis FDP linking - called after genesis creates the hash directories
setup_fdp_post_genesis() {
    local config_dir=$1
    
    log "  Configuring FDP symlinks post-genesis..."
    
    # ================================================================
    # AUTHORITIES_DB: Validator's main database (most critical)
    # ================================================================
    local auth_base="$config_dir/authorities_db"
    
    if [ -d "$auth_base" ]; then
        # Find the hash directory (created by genesis)
        local auth_hash_dir=$(find "$auth_base" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -1)
        
        if [ -n "$auth_hash_dir" ] && [ -d "$auth_hash_dir" ]; then
            local hash=$(basename "$auth_hash_dir")
            log "    Validator hash: $hash"
            
            local live_dir="$auth_hash_dir/live"
            mkdir -p "$live_dir/store"
            
            # p1: Live store root (HOT - object mutations)
            # This contains the actively-written RocksDB for current state
            # NOTE: We can't symlink the entire store because epoch_* dirs get created dynamically
            # Instead, we prepare the target and let RocksDB write there
            
            # p2: Perpetual store (historical objects, append-heavy)
            create_fdp_symlink \
                "$MOUNT_POINT/p2/auth_${hash}_perpetual" \
                "$live_dir/store/perpetual" \
                "perpetual store (p2)"
            
            # p4: Checkpoints (sequential writes, accumulating)
            create_fdp_symlink \
                "$MOUNT_POINT/p4/auth_${hash}_checkpoints" \
                "$live_dir/checkpoints" \
                "checkpoints (p4)"
            
            # p5: Epochs metadata (periodic bursts)
            create_fdp_symlink \
                "$MOUNT_POINT/p5/auth_${hash}_epochs" \
                "$live_dir/epochs" \
                "epoch metadata (p5)"
            
            # p6: We'll handle historical epoch_N directories as they're created
            # For now, create the target area
            create_pid_dir "$MOUNT_POINT/p6/auth_${hash}_old_epochs"
            
            log "    ✓ authorities_db configured"
        fi
    fi
    
    # ================================================================
    # CONSENSUS_DB: Consensus protocol data (very hot, short-lived)
    # ================================================================
    local cons_base="$config_dir/consensus_db"
    
    if [ -d "$cons_base" ]; then
        local cons_hash_dir=$(find "$cons_base" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -1)
        
        if [ -n "$cons_hash_dir" ] && [ -d "$cons_hash_dir" ]; then
            local hash=$(basename "$cons_hash_dir")
            log "    Consensus hash: $hash"
            
            # p3: All consensus data (HOT, high churn)
            # Redirect the entire hash directory
            local cons_target="$MOUNT_POINT/p3/cons_$hash"
            
            if [ -d "$cons_hash_dir" ] && [ ! -L "$cons_hash_dir" ]; then
                create_pid_dir "$cons_target"
                if [ "$(ls -A "$cons_hash_dir" 2>/dev/null)" ]; then
                    cp -a "$cons_hash_dir"/* "$cons_target"/ 2>/dev/null || true
                fi
                rm -rf "$cons_hash_dir"
                ln -sf "$cons_target" "$cons_hash_dir"
                log "    ✓ consensus_db (p3): $cons_hash_dir -> $cons_target"
            fi
        fi
    fi
    
    # ================================================================
    # FULL_NODE_DB: Indexes and RPC data (if exists)
    # ================================================================
    local fn_base="$config_dir/full_node_db"
    
    if [ -d "$fn_base" ]; then
        local fn_hash_dir=$(find "$fn_base" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -1)
        
        if [ -n "$fn_hash_dir" ] && [ -d "$fn_hash_dir" ]; then
            local hash=$(basename "$fn_hash_dir")
            log "    Fullnode hash: $hash"
            
            local fn_live="$fn_hash_dir/live"
            if [ -d "$fn_live" ]; then
                # p7: Indexes and RPC data (query-heavy)
                if [ -d "$fn_live/indexes" ]; then
                    create_fdp_symlink \
                        "$MOUNT_POINT/p7/fn_${hash}_indexes" \
                        "$fn_live/indexes" \
                        "indexes (p7)"
                fi
                
                if [ -d "$fn_live/rpc-index" ]; then
                    create_fdp_symlink \
                        "$MOUNT_POINT/p7/fn_${hash}_rpc" \
                        "$fn_live/rpc-index" \
                        "rpc-index (p7)"
                fi
            fi
            log "    ✓ full_node_db configured"
        fi
    fi
    
    log "  ✓ FDP post-genesis setup complete"
}

# ============================================================
# Step 1: Setup FDP
# ============================================================
log ""
log "[1 1/7] Setting up FDP storage..."
log "------------------------------------------------------------"

kill_all_sui
mkdir -p "$RESULTS_DIR"

# Check if already mounted - skip 01-setup.sh if so (avoids sudo for mkfs)
SKIP_RESET="${SKIP_RESET:-0}"
if mountpoint -q "$MOUNT_POINT" && [ "$SKIP_RESET" -eq 0 ]; then
    log "  Mount point already exists - skipping reformat"
    log "  (Set SKIP_RESET=0 and run with sudo to reformat)"
    SKIP_RESET=1
fi

if [ "$SKIP_RESET" -eq 0 ]; then
    if [ -f "$SCRIPT_DIR/01-setup.sh" ]; then
        FDP_MODE=$FDP_MODE RESET="yes" "$SCRIPT_DIR/01-setup.sh" 2>&1 | tee "$RESULTS_DIR/00_setup.log" >/dev/null
    else
        die "01-setup.sh not found!"
    fi
fi

mountpoint -q "$MOUNT_POINT" || die "Mount failed!"
# Try chmod without sudo - should work if user owns the mount
chmod -R 777 "$MOUNT_POINT" 2>/dev/null || true
log "✓ FDP storage ready"

# ============================================================
# Step 2: Setup local Sui node with FDP placement
# ============================================================
log ""
log "[1 2/7] Setting up local Sui node with FDP-aware storage"
log "------------------------------------------------------------"

if [ "$FDP_MODE" -eq 1 ]; then
    log "FDP_MODE=1: Using multi-PID data segregation"
    log ""
    log "PID Allocation:"
    log "  p0: Config, genesis, keystore (static)"
    log "  p1: Live object store (HOT) - reserved for dynamic epoch stores"
    log "  p2: Perpetual store (historical)"
    log "  p3: Consensus DB (HOT, short-lived)"
    log "  p4: Checkpoints (sequential)"
    log "  p5: Epoch metadata (periodic)"
    log "  p6: Historical epochs (cold)"
    log "  p7: Indexes, RPC data (query-heavy)"
    log ""
else
    log "FDP_MODE=0: Using single PID (baseline)"
fi

# p0 is always the "anchor" directory - config files live here
LOCAL_DIR="$MOUNT_POINT/p0/sui_node"

mkdir -p "$LOCAL_DIR"
cd "$LOCAL_DIR"

# ============================================================
# Step 3: Generate genesis
# ============================================================
log ""
log "[1 3/7] Generating genesis configuration..."
log "------------------------------------------------------------"

if [ ! -f "genesis.blob" ]; then
    log "📝 Running sui genesis..."
    log "   Epoch duration: ${EPOCH_DURATION_MS}ms"
    SUI_CONFIG_DIR=$LOCAL_DIR sui genesis -f \
        --with-faucet \
        --epoch-duration-ms "$EPOCH_DURATION_MS" \
        2>&1 | tee "$RESULTS_DIR/02_genesis.log" >/dev/null
    log "✅ Genesis created"
else
    log "✅ Genesis already exists"
fi

# ============================================================
# Step 4: Setup FDP symlinks post-genesis
# ============================================================
log ""
log "[1 4/7] Setting up FDP data placement..."
log "------------------------------------------------------------"

if [ "$FDP_MODE" -eq 1 ]; then
    setup_fdp_post_genesis "$LOCAL_DIR"
    
    # Verify symlinks
    log ""
    log "  FDP Symlink Map:"
    find "$LOCAL_DIR" -type l 2>/dev/null | while read link; do
        target=$(readlink "$link")
        log "    $(basename "$link") -> $target"
    done
    log ""
else
    log "  FDP_MODE=0: Skipping symlink setup (all data in p0)"
fi

# ============================================================
# Step 5: Create optimized fullnode.yaml (for reference)
# ============================================================
log ""
log "[1 5/7] Creating storage-optimized configuration..."
log "------------------------------------------------------------"

cat > "$LOCAL_DIR/fullnode.yaml" << EOF
# Sui Fullnode Configuration - Storage Bloat Optimized
# Generated by start-node.sh for FDP benchmarking
# FDP_MODE: $FDP_MODE

db-path: $LOCAL_DIR/full_node_db

# DISABLE pruning to maximize storage bloat
authority-store-pruning-config:
  num-latest-epoch-dbs-to-retain: 10000
  epoch-db-pruning-period-secs: 31536000
  num-epochs-to-retain: 10000000
  max-checkpoints-in-batch: 10000
  max-transactions-in-batch: 10000

# Fast checkpoint execution
checkpoint-executor-config:
  checkpoint-execution-max-concurrency: 200
  local-execution-timeout-sec: 30

# Large caches
authority-store-config:
  object-cache-size: 100000

# Enable all indexing
enable-index-processing: true
enable-event-processing: true

# Network
p2p-config:
  listen-address: "0.0.0.0:8080"
  external-address: /ip4/127.0.0.1/tcp/8080

metrics-address: "0.0.0.0:9184"
json-rpc-address: "0.0.0.0:9000"

genesis:
  genesis-file-location: "$LOCAL_DIR/genesis.blob"
EOF

log "✅ Created fullnode.yaml"

# ============================================================
# Step 6: Display configuration summary
# ============================================================
log ""
log "[1 6/7] Configuration Summary"
log "------------------------------------------------------------"
log "  Config directory: $LOCAL_DIR"
log "  Genesis blob:     $LOCAL_DIR/genesis.blob"
log "  FDP Mode:         $FDP_MODE"
log ""

if [ "$FDP_MODE" -eq 1 ]; then
    log "  FDP Data Distribution:"
    for pid in 0 1 2 3 4 5 6 7; do
        size=$(du -sh "$MOUNT_POINT/p$pid" 2>/dev/null | cut -f1 || echo "0")
        log "    p$pid: $size"
    done
fi

log ""
log "  Disk usage:"
df -h "$MOUNT_POINT" | tail -1 | awk '{print "    Total: "$2", Used: "$3", Avail: "$4", Use%: "$5}'

# ============================================================
# Step 7: Start Sui local network (validator + fullnode RPC)
# ============================================================
log ""
log "[1 7/7] Starting Sui local network..."
log "------------------------------------------------------------"

if [ "$SUI_DISABLE_GAS" -eq 1 ]; then
    log "🔧 Gas fees DISABLED (SUI_DISABLE_GAS=1)"
fi

log "🚀 Starting sui start with fullnode RPC..."
log "   Config: $LOCAL_DIR"
log "   RPC Port: 9000"
log "   Faucet Port: 9123"
log "   Logs: $RESULTS_DIR/01_sui_node.log"
log ""

# Start sui with fullnode RPC and faucet (use sui start, not sui-node)
# SUI_DISABLE_GAS=1 disables gas fees (requires custom SUI build)
log "Starting sui process..."
SUI_DISABLE_GAS=$SUI_DISABLE_GAS SUI_CONFIG_DIR=$LOCAL_DIR nohup sui start \
    --network.config "$LOCAL_DIR" \
    --fullnode-rpc-port 9000 \
    --with-faucet \
    > "$RESULTS_DIR/01_sui_node.log" 2>&1 &
NODE_PID=$!

log "Node PID: $NODE_PID"

# Wait for node to initialize and expose RPC
log "Waiting for node to initialize (checking RPC)..."
MAX_WAIT=60
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    # Check if RPC is responding
    if curl -s http://127.0.0.1:9000 -X POST -H "Content-Type: application/json" \
       -d '{"jsonrpc":"2.0","id":1,"method":"sui_getTotalTransactionBlocks","params":[]}' \
       2>/dev/null | grep -q "result"; then
        log "✓ RPC is responding"
        break
    fi
    if ! kill -0 $NODE_PID 2>/dev/null; then
        log "❌ Node process died!"
        tail -30 "$RESULTS_DIR/01_sui_node.log"
        exit 1
    fi
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
    [ $((WAIT_COUNT % 10)) -eq 0 ] && log "  Still waiting... ($WAIT_COUNT sec)"
done

if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
    log "❌ Timeout waiting for node to start"
    tail -50 "$RESULTS_DIR/01_sui_node.log"
    exit 1
fi

# Check if node is running
if kill -0 $NODE_PID 2>/dev/null; then
    log "✅ Sui local network is running!"
    log ""
    log "============================================================"
    log " READY FOR BENCHMARKING"
    log "============================================================"
    log ""
    log "  Node PID:      $NODE_PID"
    log "  RPC Endpoint:  http://127.0.0.1:9000"
    log "  Faucet:        http://127.0.0.1:9123"
    log "  Gas Disabled:  $( [ $SUI_DISABLE_GAS -eq 1 ] && echo 'YES' || echo 'NO' )"
    log ""
    log "  Monitor storage growth by PID:"
    log "    watch -n 1 'du -sh $MOUNT_POINT/p*'"
    log ""
    log "  Check node status:"
    log "    curl -s http://127.0.0.1:9000 -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sui_getLatestCheckpointSequenceNumber\"}' -H 'Content-Type: application/json'"
    log ""
else
    log "❌ Node failed to start! Check: $RESULTS_DIR/01_sui_node.log"
    tail -50 "$RESULTS_DIR/01_sui_node.log"
    exit 1
fi
