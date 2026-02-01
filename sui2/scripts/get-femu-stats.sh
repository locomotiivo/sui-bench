#!/bin/bash
#
# get-femu-stats.sh - Capture FEMU in-device stats
#
# IMPORTANT: fdp_send_sungjin RESETS stats after printing!
# Only run this when you want the final cumulative stats.
#
# Usage:
#   ./get-femu-stats.sh [device]
#
# Example:
#   ./get-femu-stats.sh /dev/nvme0n1
#
# Output:
#   Parses fdp_send_sungjin output and displays:
#   - host_writes (write_io_n)
#   - gc_copies (copied)
#   - block_erased
#   - discards
#   - read_io_n
#   - Calculated WAF
#

set -e

DEVICE="${1:-/dev/nvme0n1}"
FDP_SEND_SUNGJIN="/home/femu/fdp-scripts/f2fs-tools-fdp/fdp_send_sungjin"

if [ ! -x "$FDP_SEND_SUNGJIN" ]; then
    echo "ERROR: fdp_send_sungjin not found at $FDP_SEND_SUNGJIN"
    exit 1
fi

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║  FEMU In-Device Statistics - $(date '+%Y-%m-%d %H:%M:%S')           ║"
echo "║  Device: $DEVICE                                                     ║"
echo "║  WARNING: This resets device statistics after capture!               ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""

# Capture the output
stats_output=$(sudo "$FDP_SEND_SUNGJIN" "$DEVICE" 2>&1)

# Parse the stats from print_sungjin output format: print_sungjin(VAR) : {VALUE}
host_writes=$(echo "$stats_output" | grep "sungjin_stat.write_io_n" | sed 's/.*{\([0-9]*\)}.*/\1/')
gc_copies=$(echo "$stats_output" | grep "sungjin_stat.copied" | sed 's/.*{\([0-9]*\)}.*/\1/')
block_erased=$(echo "$stats_output" | grep "sungjin_stat.block_erased" | sed 's/.*{\([0-9]*\)}.*/\1/')
discards=$(echo "$stats_output" | grep "sungjin_stat.discard)" | sed 's/.*{\([0-9]*\)}.*/\1/')
discard_ignored=$(echo "$stats_output" | grep "sungjin_stat.discard_ignored" | sed 's/.*{\([0-9]*\)}.*/\1/')
invalidated=$(echo "$stats_output" | grep "sungjin_stat.invalidated" | sed 's/.*{\([0-9]*\)}.*/\1/')
read_io=$(echo "$stats_output" | grep "sungjin_stat.read_io_n" | sed 's/.*{\([0-9]*\)}.*/\1/')

# Default to 0 if not found
host_writes=${host_writes:-0}
gc_copies=${gc_copies:-0}
block_erased=${block_erased:-0}
discards=${discards:-0}
discard_ignored=${discard_ignored:-0}
invalidated=${invalidated:-0}
read_io=${read_io:-0}

# Calculate WAF
if [ "$host_writes" -gt 0 ]; then
    waf=$(echo "scale=3; 1 + $gc_copies / $host_writes" | bc)
else
    waf="N/A"
fi

# Display results
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│                         FEMU Statistics                            │"
echo "├─────────────────────────────────────────────────────────────────────┤"
printf "│  Host Writes (write_io_n):  %'15d pages                     │\n" "$host_writes"
printf "│  GC Copies (copied):        %'15d pages                     │\n" "$gc_copies"
printf "│  Blocks Erased:             %'15d blocks                    │\n" "$block_erased"
printf "│  Discards:                  %'15d                           │\n" "$discards"
printf "│  Discards Ignored:          %'15d                           │\n" "$discard_ignored"
printf "│  Invalidated:               %'15d                           │\n" "$invalidated"
printf "│  Read I/Os:                 %'15d                           │\n" "$read_io"
echo "├─────────────────────────────────────────────────────────────────────┤"
echo "│                     WAF CALCULATION                                │"
echo "├─────────────────────────────────────────────────────────────────────┤"
echo "│  Formula: WAF = 1 + (gc_copies / host_writes)                     │"
echo "│           WAF = 1 + ($gc_copies / $host_writes)"
echo "│                                                                    │"
printf "│  ★ IN-DEVICE WAF: %s                                            │\n" "$waf"
echo "└─────────────────────────────────────────────────────────────────────┘"

# Also output machine-readable format
echo ""
echo "# Machine-readable output (for scripts):"
echo "HOST_WRITES=$host_writes"
echo "GC_COPIES=$gc_copies"
echo "BLOCK_ERASED=$block_erased"
echo "DISCARDS=$discards"
echo "READ_IO=$read_io"
echo "IN_DEVICE_WAF=$waf"

# Parse per-stream write counts
echo ""
echo "# Per-stream write counts (wpp->written):"
stream_writes=$(echo "$stats_output" | grep "wpp->written" | sed 's/.*{\([0-9]*\)}.*/\1/')
stream_num=0
for writes in $stream_writes; do
    if [ "$writes" != "0" ] || [ $stream_num -lt 8 ]; then
        printf "  Stream %d: %'d pages\n" $stream_num "$writes"
    fi
    stream_num=$((stream_num + 1))
done
