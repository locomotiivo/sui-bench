#!/bin/bash
#
# OPTIMAL BENCHMARK PARAMETERS FOR FDP RESEARCH
#
# These parameters have been empirically determined through extensive testing
# to maximize device write throughput while maintaining stable TPS.
#
# Key findings from testing:
# 1. 256 workers achieves ~1.7 GB/min device writes (optimal)
# 2. Larger batches (200KB * 5) reduce CLI overhead
# 3. WAF starts ~0.67x due to RocksDB compression, approaches 1.0x over time
# 4. 10-minute minimum duration needed for meaningful WAF comparison
#

# ═══════════════════════════════════════════════════════════════════════════════
# RECOMMENDED PARAMETERS FOR FDP vs NON-FDP COMPARISON
# ═══════════════════════════════════════════════════════════════════════════════

# Worker count
# - 256 workers: ~1.7 GB/min, ~24 TPS (RECOMMENDED)
# - 128 workers: ~1.2 GB/min, ~18 TPS
# - 64 workers:  ~0.8 GB/min, ~12 TPS
export OPTIMAL_WORKERS=256

# Blob size per transaction
# - 200KB: Good balance of throughput and transaction success rate
# - 150KB: More conservative, slightly lower throughput
# - 100KB: Very conservative, lower throughput but higher success rate
export OPTIMAL_BLOB_SIZE_KB=200

# Batch count (number of blobs per transaction)
# - 5: Optimal for avoiding gas exhaustion with 200KB blobs
# - 10: Higher throughput but may hit gas limits
# - 3: Very conservative
export OPTIMAL_BATCH_COUNT=5

# Duration in seconds
# - 600 (10 min): Minimum for meaningful WAF measurement
# - 1800 (30 min): Better for GC observation
# - 3600 (60 min): Comprehensive GC analysis
export OPTIMAL_DURATION=600

# Calculated values
export BYTES_PER_TX=$((OPTIMAL_BLOB_SIZE_KB * OPTIMAL_BATCH_COUNT * 1024))
# = 200KB * 5 = 1000KB = 1MB per transaction

# ═══════════════════════════════════════════════════════════════════════════════
# FDP-SPECIFIC PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════════

# Number of FDP placement IDs (Reclaim Unit Handles)
# SUI blockchain data types that map to different RUHs:
#   0: config         - Configuration data (cold)
#   1: hot-epochs     - Recent epoch data (hot)
#   2: perpetual      - Permanent storage (cold)
#   3: consensus      - Consensus data (hot)
#   4: checkpoints    - Checkpoint data (warm)
#   5: warm-epoch     - Warm epoch data (warm)
#   6: cold-epochs    - Old epoch data (cold)
#   7: indexes        - Index data (mixed)
export FDP_NUM_PLACEMENT_IDS=8

# ═══════════════════════════════════════════════════════════════════════════════
# EXPECTED RESULTS (based on testing)
# ═══════════════════════════════════════════════════════════════════════════════

# NON-FDP MODE (standard F2FS):
# - TPS: 20-25 tx/sec
# - Device Write Rate: 1.5-1.8 GB/min
# - WAF after 10 min: ~0.9-1.0x (RocksDB compression)
# - WAF after 60 min: ~1.2-1.5x (with FTL GC)

# FDP MODE (with F2FS FDP support):
# - TPS: 20-25 tx/sec (similar)
# - Device Write Rate: 1.5-1.8 GB/min (similar)
# - WAF after 10 min: ~0.9-1.0x (RocksDB compression)
# - WAF after 60 min: ~1.0-1.1x (REDUCED due to FDP)
#
# Expected WAF improvement with FDP: 20-30% reduction

# ═══════════════════════════════════════════════════════════════════════════════
# USAGE
# ═══════════════════════════════════════════════════════════════════════════════

usage() {
    echo "Optimal parameters for SUI FDP benchmark:"
    echo ""
    echo "  WORKERS=$OPTIMAL_WORKERS"
    echo "  BLOB_SIZE_KB=$OPTIMAL_BLOB_SIZE_KB"
    echo "  BATCH_COUNT=$OPTIMAL_BATCH_COUNT"
    echo "  DURATION=$OPTIMAL_DURATION"
    echo "  BYTES_PER_TX=$BYTES_PER_TX ($(echo "scale=0; $BYTES_PER_TX / 1024" | bc)KB)"
    echo ""
    echo "Quick start:"
    echo "  # FDP-disabled test:"
    echo "  WORKERS=$OPTIMAL_WORKERS DURATION=$OPTIMAL_DURATION ./max-device-write-bench.sh"
    echo ""
    echo "  # FDP-enabled test (after mounting with FDP):"
    echo "  FDP_MODE=1 WORKERS=$OPTIMAL_WORKERS DURATION=$OPTIMAL_DURATION ./max-device-write-bench.sh"
    echo ""
    echo "  # Full comparison:"
    echo "  ./fdp-benchmark-compare.sh --duration $OPTIMAL_DURATION"
}

# If sourced, just export variables. If run directly, show usage.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    usage
fi
