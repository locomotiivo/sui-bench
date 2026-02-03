#!/bin/bash
#
# view-results.sh - View and compare FDP benchmark results
#
# Usage:
#   ./view-results.sh                    # View all results
#   ./view-results.sh --compare          # Compare FDP vs non-FDP
#   ./view-results.sh --latest           # Show latest result only
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_CSV="$SCRIPT_DIR/results/benchmark_results.csv"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

if [ ! -f "$RESULTS_CSV" ]; then
    echo -e "${RED}No results found at: $RESULTS_CSV${NC}"
    echo "Run a benchmark first with: ./max-device-write-bench.sh"
    exit 1
fi

show_all() {
    echo -e "${BLUE}══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  ALL BENCHMARK RESULTS${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    # Pretty print CSV
    column -t -s',' "$RESULTS_CSV" | head -1
    echo "────────────────────────────────────────────────────────────────────────────────"
    column -t -s',' "$RESULTS_CSV" | tail -n +2
    echo ""
}

show_latest() {
    echo -e "${BLUE}══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  LATEST BENCHMARK RESULT${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    local latest=$(tail -1 "$RESULTS_CSV")
    IFS=',' read -r timestamp mode duration workers tps success fail app_bytes device_bytes waf rate total_gb overwrite gc_bg gc_fg <<< "$latest"
    
    echo -e "  Timestamp:        ${CYAN}$timestamp${NC}"
    echo -e "  Mode:             ${CYAN}$mode${NC}"
    echo ""
    echo -e "  ${GREEN}── Performance ──${NC}"
    echo -e "  Duration:         $duration sec"
    echo -e "  Workers:          $workers"
    echo -e "  TPS:              ${GREEN}$tps${NC} tx/sec"
    echo -e "  Transactions:     $success success, $fail failed"
    echo ""
    echo -e "  ${GREEN}── Write Statistics ──${NC}"
    echo -e "  App Writes:       $(echo "scale=2; $app_bytes / 1073741824" | bc) GB"
    echo -e "  Device Writes:    $(echo "scale=2; $device_bytes / 1073741824" | bc) GB"
    echo -e "  WAF:              ${YELLOW}${waf}x${NC}"
    echo -e "  Rate:             $rate MB/min"
    echo ""
    echo -e "  ${GREEN}── Device Statistics ──${NC}"
    echo -e "  Total Device:     $total_gb GB"
    echo -e "  Overwrite Ratio:  ${overwrite}x"
    echo -e "  F2FS GC (BG/FG):  $gc_bg / $gc_fg"
    echo ""
}

compare_results() {
    echo -e "${BLUE}══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  FDP vs NON-FDP COMPARISON${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    # Get latest FDP and non-FDP results
    local fdp_line=$(grep ",fdp," "$RESULTS_CSV" | tail -1)
    local nofdp_line=$(grep ",nofdp," "$RESULTS_CSV" | tail -1)
    
    if [ -z "$fdp_line" ]; then
        echo -e "${YELLOW}No FDP results found. Run benchmark with FDP enabled.${NC}"
        fdp_tps="N/A"; fdp_waf="N/A"; fdp_rate="N/A"
    else
        IFS=',' read -r _ _ _ _ fdp_tps _ _ _ _ fdp_waf fdp_rate _ _ _ _ <<< "$fdp_line"
    fi
    
    if [ -z "$nofdp_line" ]; then
        echo -e "${YELLOW}No non-FDP results found. Run benchmark without FDP.${NC}"
        nofdp_tps="N/A"; nofdp_waf="N/A"; nofdp_rate="N/A"
    else
        IFS=',' read -r _ _ _ _ nofdp_tps _ _ _ _ nofdp_waf nofdp_rate _ _ _ _ <<< "$nofdp_line"
    fi
    
    echo ""
    echo "┌─────────────────────┬─────────────────┬─────────────────┬──────────────┐"
    echo "│ Metric              │ Non-FDP         │ FDP             │ Improvement  │"
    echo "├─────────────────────┼─────────────────┼─────────────────┼──────────────┤"
    
    # TPS comparison
    if [ "$nofdp_tps" != "N/A" ] && [ "$fdp_tps" != "N/A" ]; then
        tps_imp=$(echo "scale=1; ($fdp_tps - $nofdp_tps) / $nofdp_tps * 100" | bc 2>/dev/null || echo "N/A")
        printf "│ %-19s │ %15s │ %15s │ %+11s%% │\n" "TPS (tx/sec)" "$nofdp_tps" "$fdp_tps" "$tps_imp"
    else
        printf "│ %-19s │ %15s │ %15s │ %12s │\n" "TPS (tx/sec)" "$nofdp_tps" "$fdp_tps" "N/A"
    fi
    
    # WAF comparison (lower is better for FDP)
    if [ "$nofdp_waf" != "N/A" ] && [ "$fdp_waf" != "N/A" ]; then
        waf_imp=$(echo "scale=1; ($nofdp_waf - $fdp_waf) / $nofdp_waf * 100" | bc 2>/dev/null || echo "N/A")
        printf "│ %-19s │ %14sx │ %14sx │ %+11s%% │\n" "WAF (lower=better)" "$nofdp_waf" "$fdp_waf" "$waf_imp"
    else
        printf "│ %-19s │ %15s │ %15s │ %12s │\n" "WAF (lower=better)" "${nofdp_waf}x" "${fdp_waf}x" "N/A"
    fi
    
    # Rate comparison
    if [ "$nofdp_rate" != "N/A" ] && [ "$fdp_rate" != "N/A" ]; then
        rate_imp=$(echo "scale=1; ($fdp_rate - $nofdp_rate) / $nofdp_rate * 100" | bc 2>/dev/null || echo "N/A")
        printf "│ %-19s │ %12s MB │ %12s MB │ %+11s%% │\n" "Rate (MB/min)" "$nofdp_rate" "$fdp_rate" "$rate_imp"
    else
        printf "│ %-19s │ %15s │ %15s │ %12s │\n" "Rate (MB/min)" "$nofdp_rate" "$fdp_rate" "N/A"
    fi
    
    echo "└─────────────────────┴─────────────────┴─────────────────┴──────────────┘"
    echo ""
    
    echo -e "${GREEN}Key Insight:${NC}"
    echo "  - Lower WAF = Better SSD endurance (FDP reduces unnecessary writes)"
    echo "  - Higher TPS = Better blockchain throughput"
    echo "  - FDP should show ~20-30% WAF reduction under heavy GC pressure"
    echo ""
}

# Parse arguments
case "${1:-}" in
    --compare|-c)
        compare_results
        ;;
    --latest|-l)
        show_latest
        ;;
    --help|-h)
        echo "Usage: $0 [--compare|--latest|--help]"
        echo ""
        echo "Options:"
        echo "  --compare, -c   Compare FDP vs non-FDP results"
        echo "  --latest, -l    Show latest result only"
        echo "  --help, -h      Show this help"
        echo ""
        echo "Default: Show all results"
        ;;
    *)
        show_all
        compare_results
        ;;
esac
