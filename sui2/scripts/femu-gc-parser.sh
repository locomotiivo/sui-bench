#!/bin/bash
#
# femu-gc-parser.sh - Parse FEMU GC output and save stats
#
# This script runs on the HOST machine (outside FEMU VM) to capture
# and parse GC statistics from FEMU's output.
#
# Usage (on HOST):
#   ./femu-gc-parser.sh /path/to/femu_output.log /shared/gc_stats.txt
#
# The FEMU GC output format:
#   GC-ing line:40,ipc=216022,victim=55,full=0,free=42,stream_id=0,rg_id=0, discard 9865847 read/write 259/5102029 block_erased 960 copied 635866
#
# Output format (saved to gc_stats.txt):
#   timestamp=<unix_timestamp>
#   gc_line=<line_number>
#   ipc=<invalidated_page_count>
#   victim=<victim_count>
#   discards=<discard_count>
#   reads=<read_count>
#   writes=<write_count>
#   blocks_erased=<erased_count>
#   copied=<gc_copy_count>
#   waf=<calculated_waf>
#

if [ $# -lt 1 ]; then
    echo "Usage: $0 <femu_output_log> [output_stats_file]"
    echo ""
    echo "Parses FEMU GC output and extracts statistics."
    echo ""
    echo "Examples:"
    echo "  # Parse log file"
    echo "  $0 /tmp/femu_output.log"
    echo ""
    echo "  # Parse and save to file"
    echo "  $0 /tmp/femu_output.log /shared/gc_stats.txt"
    echo ""
    echo "  # Pipe FEMU output directly"
    echo "  tail -f /tmp/femu.log | $0 -"
    exit 1
fi

INPUT="$1"
OUTPUT="${2:-/dev/stdout}"

parse_gc_line() {
    local line="$1"
    local timestamp=$(date +%s)
    
    # Extract all fields using grep -oP
    local gc_line=$(echo "$line" | grep -oP 'line:\K[0-9]+')
    local ipc=$(echo "$line" | grep -oP 'ipc=\K[0-9]+')
    local victim=$(echo "$line" | grep -oP 'victim=\K[0-9]+')
    local stream_id=$(echo "$line" | grep -oP 'stream_id=\K[0-9]+')
    local rg_id=$(echo "$line" | grep -oP 'rg_id=\K[0-9]+')
    local discards=$(echo "$line" | grep -oP 'discard \K[0-9]+')
    local rw=$(echo "$line" | grep -oP 'read/write \K[0-9]+/[0-9]+')
    local reads=$(echo "$rw" | cut -d'/' -f1)
    local writes=$(echo "$rw" | cut -d'/' -f2)
    local erased=$(echo "$line" | grep -oP 'block_erased \K[0-9]+')
    local copied=$(echo "$line" | grep -oP 'copied \K[0-9]+')
    
    # Calculate WAF = (writes + copied) / writes
    local waf="N/A"
    if [ -n "$writes" ] && [ "$writes" -gt 0 ]; then
        waf=$(echo "scale=3; ($writes + $copied) / $writes" | bc 2>/dev/null || echo "N/A")
    fi
    
    # Output in parseable format
    cat << EOF
# FEMU GC Stats - $(date -Iseconds)
timestamp=$timestamp
gc_line=$gc_line
ipc=$ipc
victim=$victim
stream_id=$stream_id
rg_id=$rg_id
discards=$discards
reads=$reads
writes=$writes
blocks_erased=$erased
copied=$copied
waf=$waf
EOF
}

if [ "$INPUT" = "-" ]; then
    # Read from stdin (pipe mode)
    echo "Reading from stdin... (Ctrl+C to stop)"
    while read line; do
        if echo "$line" | grep -q "GC-ing"; then
            parse_gc_line "$line" > "$OUTPUT"
            echo "Updated: $(grep 'writes=' "$OUTPUT" 2>/dev/null)"
        fi
    done
else
    # Read from file
    if [ ! -f "$INPUT" ]; then
        echo "Error: File not found: $INPUT"
        exit 1
    fi
    
    # Get the last GC line
    gc_line=$(grep "GC-ing" "$INPUT" | tail -1)
    
    if [ -z "$gc_line" ]; then
        echo "No GC entries found in $INPUT"
        exit 0
    fi
    
    parse_gc_line "$gc_line" > "$OUTPUT"
    
    if [ "$OUTPUT" != "/dev/stdout" ]; then
        echo "Stats saved to $OUTPUT"
        cat "$OUTPUT"
    fi
fi
