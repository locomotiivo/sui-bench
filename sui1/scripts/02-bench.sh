#!/bin/bash

set -e
source ~/.bashrc

# ============================================================
# 02-bench.sh - High-Throughput SUI Benchmark for GC Triggering
# 
# TARGET: 64GB SSD, need 30-40GB writes to trigger F2FS GC
# APPROACH: Owned object operations (coin splits) for max parallelization
# EXPECTED: 5000+ TPS, 500+ MB/min disk growth
# ============================================================

# === MAIN CONFIGURATION ===
FDP_MODE=${FDP_MODE:-1}
DURATION=${DURATION:-1800}             # 30 minutes for serious GC testing
EPOCH=${EPOCH:-60000}
NUM_VALIDATORS=${NUM_VALIDATORS:-1}

# === HIGH-THROUGHPUT PARAMETERS ===
# Key insight: Owned objects bypass consensus → true parallelization
NUM_ACCOUNTS=${NUM_ACCOUNTS:-5000}     # 5000 accounts like Aptos/Solana
COMMANDS_PER_PTB=${COMMANDS_PER_PTB:-256}  # Max splits per TX
ITER_INTERVAL=${ITER_INTERVAL:-5}      # 5ms interval (aggressive)
CONCURRENT=${CONCURRENT:-500}          # 500 concurrent accounts

# Counter creation (if using counter mode)
NUM_COUNTERS=${NUM_COUNTERS:-250000}   # 250k counters
PARALLEL_CREATORS=${PARALLEL_CREATORS:-50}  # 50 parallel creators

# === GAS CONFIGURATION ===
SUI_PER_ACCOUNT=${SUI_PER_ACCOUNT:-50} # 50 SUI each

# === BENCHMARK MODE ===
# "owned" = Coin split/merge (fastest, max disk I/O)
# "counter" = Counter mutations (tests shared object perf)
BENCHMARK_MODE=${BENCHMARK_MODE:-owned}

# === MEMORY SAFETY ===
MEMORY_THRESHOLD=${MEMORY_THRESHOLD:-85}

# === PATHS ===
ROOT_DIR=${ROOT_DIR:-$HOME/fdp-scripts/sui-tps-benchmark}
SCRIPTS_DIR=${SCRIPTS_DIR:-$ROOT_DIR/scripts}
RESULTS_DIR=${RESULTS_DIR:-$SCRIPTS_DIR/log}
MOUNT_POINT=${MOUNT_POINT:-$HOME/f2fs_fdp_mount}
DATA_DIR=${DATA_DIR:-$HOME/sui_config}

MONITOR_INTERVAL=10  # More frequent monitoring

mkdir -p $RESULTS_DIR $DATA_DIR
rm -rf $RESULTS_DIR/* 2>/dev/null || true

# ============================================================
# Helper Functions
# ============================================================
log() { echo "[$(date '+%H:%M:%S')] $1"; }
die() { log "ERROR: $1"; exit 1; }

get_memory_pct() {
    free | awk '/Mem:/ {printf "%.0f", ($2-$7)/$2*100}'
}

get_disk_usage_mb() {
    df -m "$MOUNT_POINT" 2>/dev/null | tail -1 | awk '{print $3}'
}

kill_all_sui() {
    log "  Stopping SUI processes..."
    pkill -9 -f "sui start" 2>/dev/null || true
    pkill -9 -f "sui-node" 2>/dev/null || true
    pkill -9 -f "sui-faucet" 2>/dev/null || true
    sleep 3
    if ss -tlnp 2>/dev/null | grep -qE ":(9000|9123) "; then
        sudo fuser -k 9000/tcp 9123/tcp 2>/dev/null || true
        sleep 2
    fi
}

cleanup() {
    log "Cleanup..."
    pkill -f "disk_monitor" 2>/dev/null || true
    kill_all_sui
}
trap cleanup EXIT

move_and_link() {
    local src=$1 dest=$2
    if [ -d "$src" ] && [ ! -L "$src" ]; then
        mkdir -p "$(dirname "$dest")"
        mv "$src" "$dest"
        ln -s "$dest" "$src"
    fi
}

start_disk_monitor() {
    (
        echo "ts,used_mb,mem_pct" > $RESULTS_DIR/disk_usage.csv
        while true; do
            echo "$(date +%s),$(get_disk_usage_mb),$(get_memory_pct)" >> $RESULTS_DIR/disk_usage.csv
            sleep $MONITOR_INTERVAL
        done
    ) &
    echo $! > $RESULTS_DIR/monitor.pid
}

update_benchmark_config() {
    local config_file="$SCRIPTS_DIR/config.json"
    jq --arg network "localnet" \
       --argjson targetCount "$NUM_ACCOUNTS" \
       --argjson iterInterval "$ITER_INTERVAL" \
       --argjson commandsPerPtb "$COMMANDS_PER_PTB" \
       --argjson duration "$DURATION" \
       --argjson suiPerAccount "$SUI_PER_ACCOUNT" \
       --argjson rpcIndex 0 \
       '
       .network = $network |
       .targetCount = $targetCount |
       .iterInterval = $iterInterval |
       .commandsPerPtb = $commandsPerPtb |
       .duration = $duration |
       .suiPerAccount = $suiPerAccount |
       .rpcIndex = $rpcIndex |
       .rpcs.localnet = ["http://127.0.0.1:9000"]
       ' "$config_file" > "${config_file}.tmp" && mv "${config_file}.tmp" "$config_file"
}

get_balance_sui() {
    local b=$(SUI_CONFIG_DIR="$DATA_DIR" sui client gas --json 2>/dev/null | \
              jq -r '[.[].mistBalance | tonumber] | add // 0' 2>/dev/null)
    [ -z "$b" ] || [ "$b" = "null" ] && echo "0" || awk "BEGIN {printf \"%.0f\", $b / 1000000000}"
}

# ============================================================
# BANNER
# ============================================================
INIT_DISK=$(get_disk_usage_mb 2>/dev/null || echo "0")
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║        SUI HIGH-THROUGHPUT BENCHMARK (GC Triggering Mode)                 ║"
echo "╠══════════════════════════════════════════════════════════════════════════╣"
printf "║  %-20s %-54s ║\n" "Mode:" "$BENCHMARK_MODE ($([ $BENCHMARK_MODE = owned ] && echo 'coin split/merge' || echo 'counter mutation'))"
printf "║  %-20s %-54s ║\n" "Duration:" "${DURATION}s ($(awk "BEGIN {print $DURATION/60}") min)"
printf "║  %-20s %-54s ║\n" "Accounts:" "$NUM_ACCOUNTS"
printf "║  %-20s %-54s ║\n" "Concurrent:" "$CONCURRENT"
printf "║  %-20s %-54s ║\n" "Ops per TX:" "$COMMANDS_PER_PTB"
printf "║  %-20s %-54s ║\n" "TX interval:" "${ITER_INTERVAL}ms"
printf "║  %-20s %-54s ║\n" "Target disk I/O:" "30-40 GB (to trigger GC)"
printf "║  %-20s %-54s ║\n" "FDP Mode:" "$([ $FDP_MODE -eq 1 ] && echo 'OPTIMIZED' || echo 'BASELINE')"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""

# ============================================================
# Step 1: Setup FDP
# ============================================================
log ""
log "[1/7] Setting up FDP storage..."
echo "------------------------------------------------------------"

kill_all_sui

if [ -f "$SCRIPTS_DIR/01-setup.sh" ]; then
    FDP_MODE=$FDP_MODE RESET="yes" $SCRIPTS_DIR/01-setup.sh 2>&1 | tee $RESULTS_DIR/00_setup.log
else
    die "01-setup.sh not found!"
fi

mountpoint -q "$MOUNT_POINT" || die "Mount failed!"
sudo chmod -R 777 "$MOUNT_POINT"
log "✓ FDP ready"

# ============================================================
# Step 2: Clean state
# ============================================================
log ""
log "[2/7] Cleaning state..."
rm -rf "$DATA_DIR"/* 2>/dev/null || true
for pid in p0 p1 p2 p3 p4 p5 p6 p7; do
    rm -rf "$MOUNT_POINT/$pid"/* 2>/dev/null || true
    mkdir -p "$MOUNT_POINT/$pid"
done
log "✓ Clean"

# Check memory
INIT_MEM=$(get_memory_pct)
log "Initial: Memory=${INIT_MEM}% Disk=${INIT_DISK:-0}MB"

# ============================================================
# Step 3: Genesis
# ============================================================
log ""
log "[3/7] Genesis..."
mkdir -p "$DATA_DIR"
SUI_CONFIG_DIR="$DATA_DIR" sui genesis \
    --working-dir "$DATA_DIR" \
    --with-faucet \
    --epoch-duration-ms $EPOCH \
    --committee-size $NUM_VALIDATORS \
    --force 2>&1 | tee $RESULTS_DIR/01_genesis.log
log "✓ Genesis"

# ============================================================
# Step 4: Init directories
# ============================================================
log ""
log "[4/7] Init node..."
SUI_CONFIG_DIR="$DATA_DIR" sui start \
    --network.config "$DATA_DIR/network.yaml" \
    --with-faucet > $RESULTS_DIR/02_init.log 2>&1 &

for i in {1..60}; do
    curl -s http://127.0.0.1:9000 -X POST -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"sui_getTotalTransactionBlocks","params":[]}' \
        2>/dev/null | grep -q "result" && break
    sleep 1
done

kill_all_sui
log "✓ Directories created"

# ============================================================
# Step 5: FDP placement
# ============================================================
log ""
log "[5/7] FDP placement..."
sudo chmod -R 777 "$DATA_DIR"

AUTH_HASH_DIR=$(ls -d $DATA_DIR/authorities_db/*/ 2>/dev/null | head -n 1)
FULLNODE_HASH_DIR=$(ls -d $DATA_DIR/full_node_db/*/ 2>/dev/null | head -n 1)

if [ $FDP_MODE -eq 1 ]; then
    move_and_link "${DATA_DIR}/consensus_db" "${MOUNT_POINT}/p0/consensus_db"
    [ -n "$AUTH_HASH_DIR" ] && move_and_link "${AUTH_HASH_DIR}live" "${MOUNT_POINT}/p1/auth_live"
    [ -n "$FULLNODE_HASH_DIR" ] && move_and_link "${FULLNODE_HASH_DIR}live/indexes" "${MOUNT_POINT}/p2/fn_indexes"
    [ -n "$AUTH_HASH_DIR" ] && move_and_link "${AUTH_HASH_DIR}live/store/perpetual" "${MOUNT_POINT}/p3/perpetual"
    [ -n "$AUTH_HASH_DIR" ] && move_and_link "${AUTH_HASH_DIR}live/checkpoints" "${MOUNT_POINT}/p4/checkpoints"
fi

mkdir -p "${MOUNT_POINT}/p7/sui_base"
for item in "${DATA_DIR}"/*; do
    [ -L "$item" ] || [ ! -e "$item" ] && continue
    mv "$item" "${MOUNT_POINT}/p7/sui_base/"
    ln -s "${MOUNT_POINT}/p7/sui_base/$(basename $item)" "$item"
done

log "✓ FDP placement complete"

# ============================================================
# Step 6: Start node
# ============================================================
log ""
log "[6/7] Starting localnet..."
cd "$DATA_DIR"
SUI_CONFIG_DIR="$DATA_DIR" sui start \
    --network.config "$DATA_DIR/network.yaml" \
    --with-faucet > $RESULTS_DIR/02_sui_node.log 2>&1 &
SUI_PID=$!
echo $SUI_PID > $RESULTS_DIR/sui.pid

for i in {1..60}; do
    curl -s http://127.0.0.1:9000 -X POST -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"sui_getTotalTransactionBlocks","params":[]}' \
        2>/dev/null | grep -q "result" && break
    sleep 1
done

kill -0 $SUI_PID 2>/dev/null || die "Node crashed!"

SUI_CONFIG_DIR="$DATA_DIR" sui client new-env --alias localnet --rpc http://127.0.0.1:9000 2>/dev/null || true
yes | SUI_CONFIG_DIR="$DATA_DIR" sui client switch --env localnet 2>/dev/null

SUI_ADDR=$(SUI_CONFIG_DIR="$DATA_DIR" sui client active-address)
log "  Address: $SUI_ADDR"
log "✓ Node running (PID: $SUI_PID)"

# ============================================================
# Step 7: Fund & Run
# ============================================================
log ""
log "[7/7] Fund accounts and run benchmark..."

FAUCET_URL="http://127.0.0.1:9123/gas"
REQUIRED_SUI=$((NUM_ACCOUNTS * SUI_PER_ACCOUNT + 1000))
log "  Need ~$REQUIRED_SUI SUI for $NUM_ACCOUNTS accounts"

# Faucet loop
for i in $(seq 1 200); do
    BALANCE_SUI=$(get_balance_sui)
    [[ ! "$BALANCE_SUI" =~ ^[0-9]+$ ]] && BALANCE_SUI=0
    [ "$BALANCE_SUI" -ge "$REQUIRED_SUI" ] && break
    [ $((i % 20)) -eq 0 ] && log "  Funding... ($BALANCE_SUI / $REQUIRED_SUI SUI)"
    curl -s -X POST "$FAUCET_URL" -H "Content-Type: application/json" \
        -d "{\"FixedAmountRequest\":{\"recipient\":\"$SUI_ADDR\"}}" > /dev/null 2>&1 || true
    sleep 0.2
done

log "  Balance: $(get_balance_sui) SUI"

# Export key
PRIV_KEY=$(SUI_CONFIG_DIR="$DATA_DIR" sui keytool export --key-identity "$SUI_ADDR" --json 2>/dev/null | jq -r '.exportedPrivateKey')
echo "SUI_PRIVATE_KEY=$PRIV_KEY" > "$SCRIPTS_DIR/.env"

cd "$SCRIPTS_DIR"
update_benchmark_config

# Generate accounts
log "  Generating $NUM_ACCOUNTS accounts..."
SUI_CONFIG_DIR="$DATA_DIR" npx tsx generate_accounts.ts 2>&1 | tee $RESULTS_DIR/03_accounts.log

if [ "$BENCHMARK_MODE" = "counter" ]; then
    # Deploy contract and create counters
    log "  Deploying contract..."
    SUI_CONFIG_DIR="$DATA_DIR" npx tsx deploy_tps_test.ts 2>&1 | tee $RESULTS_DIR/04_deploy.log
    
    log "  Creating $NUM_COUNTERS counters (parallel)..."
    NUM_COUNTERS=$NUM_COUNTERS SUI_CONFIG_DIR="$DATA_DIR" npx tsx create_counters_parallel.ts 2>&1 | tee $RESULTS_DIR/05_counters.log
    
    log "  Fetching counter IDs..."
    SUI_CONFIG_DIR="$DATA_DIR" npx tsx fetch_onchain_counters.ts 2>&1 | tee $RESULTS_DIR/06_fetch.log
fi

# Start monitoring
start_disk_monitor

# Record start
START_DISK=$(get_disk_usage_mb)
START_TIME=$(date +%s)
START_MEM=$(get_memory_pct)

cat > $RESULTS_DIR/benchmark_meta.txt << EOF
start_time=$START_TIME
start_disk_mb=$START_DISK
start_mem_pct=$START_MEM
num_accounts=$NUM_ACCOUNTS
commands_per_ptb=$COMMANDS_PER_PTB
iter_interval=$ITER_INTERVAL
duration=$DURATION
benchmark_mode=$BENCHMARK_MODE
fdp_mode=$FDP_MODE
EOF

log ""
log "════════════════════════════════════════════════════════════════════════════"
log "  BENCHMARK STARTING"
log "  Mode: $BENCHMARK_MODE | Duration: ${DURATION}s | Accounts: $NUM_ACCOUNTS"
log "  Ops/TX: $COMMANDS_PER_PTB | Interval: ${ITER_INTERVAL}ms | Concurrent: $CONCURRENT"
log "════════════════════════════════════════════════════════════════════════════"
log ""

# Run
npx tsx tps_run.ts 2>&1 | tee $RESULTS_DIR/07_benchmark.log
BENCH_EXIT=$?

# ============================================================
# Results
# ============================================================
[ -f $RESULTS_DIR/monitor.pid ] && kill $(cat $RESULTS_DIR/monitor.pid) 2>/dev/null || true

END_TIME=$(date +%s)
END_DISK=$(get_disk_usage_mb)
END_MEM=$(get_memory_pct)
ELAPSED=$((END_TIME - START_TIME))
DISK_GROWTH=$((END_DISK - START_DISK))
GROWTH_RATE=$(awk "BEGIN {printf \"%.2f\", $DISK_GROWTH / ($ELAPSED / 60)}")

cat >> $RESULTS_DIR/benchmark_meta.txt << EOF
end_time=$END_TIME
end_disk_mb=$END_DISK
end_mem_pct=$END_MEM
elapsed_sec=$ELAPSED
disk_growth_mb=$DISK_GROWTH
growth_rate_mb_min=$GROWTH_RATE
exit_code=$BENCH_EXIT
EOF

log ""
log "╔══════════════════════════════════════════════════════════════════════════╗"
log "║                        BENCHMARK COMPLETE                                 ║"
log "╠══════════════════════════════════════════════════════════════════════════╣"
printf "║  %-25s %-48s ║\n" "Duration:" "${ELAPSED}s"
printf "║  %-25s %-48s ║\n" "Start Disk:" "${START_DISK} MB"
printf "║  %-25s %-48s ║\n" "End Disk:" "${END_DISK} MB"
printf "║  %-25s %-48s ║\n" "Disk Growth:" "${DISK_GROWTH} MB"
printf "║  %-25s %-48s ║\n" "Growth Rate:" "${GROWTH_RATE} MB/min"
printf "║  %-25s %-48s ║\n" "Peak Memory:" "${END_MEM}%"
log "╠══════════════════════════════════════════════════════════════════════════╣"
log "║  Results: $RESULTS_DIR"
log "╚══════════════════════════════════════════════════════════════════════════╝"

# GC analysis
if [ "$DISK_GROWTH" -gt 30000 ]; then
    log ""
    log "✅ Disk growth ${DISK_GROWTH}MB should trigger F2FS GC on 64GB SSD"
elif [ "$DISK_GROWTH" -gt 10000 ]; then
    log ""
    log "⚠️  Disk growth ${DISK_GROWTH}MB may trigger some GC activity"
else
    log ""
    log "❌ Disk growth ${DISK_GROWTH}MB likely insufficient for GC. Try longer duration."
fi

log ""
log "FDP Usage:"
for pid in p0 p1 p2 p3 p4 p5 p6 p7; do
    SIZE=$(du -sh "$MOUNT_POINT/$pid" 2>/dev/null | cut -f1 || echo "0")
    printf "  %-6s %s\n" "$pid:" "$SIZE"
done