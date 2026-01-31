# Sui FDP Data Placement Strategy

## Overview

This document describes how Sui blockchain data is segregated across 8 FDP (Flexible Data Placement) Placement IDs to minimize SSD garbage collection overhead.

## The Problem

Without FDP, all data goes to a single placement stream. When F2FS runs garbage collection:
- It must move valid data from blocks containing invalidated data
- Hot data (frequently updated) mixed with cold data causes excessive movement
- This results in **write amplification** and **GC overhead**

## FDP Solution: Temperature-Based Segregation

By separating data based on update frequency and lifetime, we ensure:
- Hot data is grouped together (high churn, quick invalidation)
- Cold data is grouped together (stable, long-lived)
- GC only needs to process blocks with similar data patterns

## PID Allocation

```
┌─────┬──────────────────┬────────────┬───────────────────────────────────────┐
│ PID │ Data Type        │ Temp       │ Description                           │
├─────┼──────────────────┼────────────┼───────────────────────────────────────┤
│ p0  │ Config/Genesis   │ 🧊 COLD    │ Static config, genesis blob, keystores│
│     │                  │            │ Written once, never modified          │
├─────┼──────────────────┼────────────┼───────────────────────────────────────┤
│ p1  │ Current Epoch    │ 🔥 HOT     │ Active epoch store (epoch_N)          │
│     │ Store            │            │ Highest write churn - object mutations│
├─────┼──────────────────┼────────────┼───────────────────────────────────────┤
│ p2  │ Perpetual Store  │ 🌡️ WARM    │ Historical object data                │
│     │                  │            │ Append-mostly, occasional updates     │
├─────┼──────────────────┼────────────┼───────────────────────────────────────┤
│ p3  │ Consensus DB     │ 🔥 HOT     │ Consensus protocol data               │
│     │                  │            │ Very high churn, short-lived entries  │
├─────┼──────────────────┼────────────┼───────────────────────────────────────┤
│ p4  │ Checkpoints      │ 🌡️ WARM    │ Checkpoint data                       │
│     │                  │            │ Sequential writes, accumulating       │
├─────┼──────────────────┼────────────┼───────────────────────────────────────┤
│ p5  │ Previous Epoch   │ 🌡️ WARM    │ epoch_(N-1) - recently completed      │
│     │                  │            │ No longer written, occasional reads   │
├─────┼──────────────────┼────────────┼───────────────────────────────────────┤
│ p6  │ Historical       │ 🧊 COLD    │ epoch_(N-2) and older                 │
│     │ Epochs           │            │ Archive data, rarely accessed         │
├─────┼──────────────────┼────────────┼───────────────────────────────────────┤
│ p7  │ Indexes/RPC      │ 🌡️ WARM    │ Query indexes, RPC cache              │
│     │                  │            │ Read-heavy with periodic updates      │
└─────┴──────────────────┴────────────┴───────────────────────────────────────┘
```

## Implementation

### Symlink-Based Placement

We use symlinks to redirect Sui's internal directories to appropriate PID directories:

```bash
# Example: After setup with FDP_MODE=1

/home/femu/f2fs_fdp_mount/p0/sui_node/
├── genesis.blob                    # Static (stays in p0)
├── 127.0.0.1-*.yaml               # Config (stays in p0)
├── authorities_db/
│   └── <hash>/
│       └── live/
│           ├── store/
│           │   ├── perpetual -> /home/femu/f2fs_fdp_mount/p2/auth_<hash>_perpetual
│           │   ├── epoch_0 -> /home/femu/f2fs_fdp_mount/p6/auth_<hash>_epoch_0
│           │   ├── epoch_1 -> /home/femu/f2fs_fdp_mount/p6/auth_<hash>_epoch_1
│           │   └── epoch_N -> /home/femu/f2fs_fdp_mount/p1/auth_<hash>_epoch_N  # Current
│           ├── checkpoints -> /home/femu/f2fs_fdp_mount/p4/auth_<hash>_checkpoints
│           └── epochs -> /home/femu/f2fs_fdp_mount/p5/auth_<hash>_epochs
└── consensus_db/
    └── <hash> -> /home/femu/f2fs_fdp_mount/p3/cons_<hash>
```

### Dynamic Epoch Migration

The `fdp-epoch-watcher.sh` script monitors for new epoch directories and automatically:
1. Moves the current epoch to p1 (hot)
2. Moves the previous epoch to p5 (warm)
3. Moves older epochs to p6 (cold)

## Scripts

### start-node.sh
- Sets up FDP storage
- Creates initial symlinks after genesis
- Starts the validator node

### fdp-epoch-watcher.sh
- Watches for new epoch directories
- Automatically relocates them to appropriate PIDs
- Run modes: `watch` (daemon), `once` (single pass), `status` (show mapping)

### monitor-bloat.sh
- Real-time storage monitoring
- Shows per-PID usage in FDP mode
- Logs to CSV for analysis

### run-fdp-benchmark.sh
- Complete orchestration script
- Starts all components in correct order
- Saves results with timestamps

## Usage

```bash
# Baseline run (no FDP segregation)
FDP_MODE=0 ./scripts/run-fdp-benchmark.sh

# FDP-optimized run
FDP_MODE=1 ./scripts/run-fdp-benchmark.sh

# Monitor FDP placement
./scripts/fdp-epoch-watcher.sh status

# View real-time storage by PID
watch -n 1 'du -sh ~/f2fs_fdp_mount/p*'
```

## Expected Benefits

| Metric | Baseline | FDP Mode | Improvement |
|--------|----------|----------|-------------|
| GC Overhead | High | Low | ~50-70% reduction |
| Write Amplification | ~3-5x | ~1.5-2x | ~40-60% reduction |
| Tail Latency | High variance | More stable | Reduced P99 |

## Verifying FDP Placement

```bash
# Check F2FS GC statistics
cat /sys/fs/f2fs/*/gc_*

# Verify symlinks are correct
find ~/f2fs_fdp_mount/p0/sui_node -type l -exec ls -la {} \;

# Check per-PID usage
for p in 0 1 2 3 4 5 6 7; do
  echo "p$p: $(du -sh ~/f2fs_fdp_mount/p$p | cut -f1)"
done
```

## Troubleshooting

### Symlinks not created
- Ensure `FDP_MODE=1` is set
- Run `./fdp-epoch-watcher.sh once` manually after genesis

### Epoch directories not migrating
- Check if watcher is running: `pgrep -f fdp-epoch-watcher`
- Run status: `./fdp-epoch-watcher.sh status`

### Permission denied
- Ensure mount point has 777 permissions: `sudo chmod -R 777 $MOUNT_POINT`
