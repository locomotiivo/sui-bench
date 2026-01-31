#!/bin/bash

# Pre-flight check script - validates environment before running heavy load benchmark

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

ERRORS=0
WARNINGS=0

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║${NC}    ${YELLOW}Sui Storage Bloat Benchmark - Pre-flight Check${NC}    ${BLUE}║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to check requirement
check_required() {
    local name=$1
    local command=$2
    local help=$3
    
    if eval "$command" &>/dev/null; then
        echo -e "${GREEN}✓${NC} $name"
        return 0
    else
        echo -e "${RED}✗${NC} $name"
        echo -e "  ${YELLOW}→${NC} $help"
        ERRORS=$((ERRORS + 1))
        return 1
    fi
}

# Function to check optional
check_optional() {
    local name=$1
    local command=$2
    local help=$3
    
    if eval "$command" &>/dev/null; then
        echo -e "${GREEN}✓${NC} $name"
        return 0
    else
        echo -e "${YELLOW}⚠${NC} $name (optional)"
        echo -e "  ${YELLOW}→${NC} $help"
        WARNINGS=$((WARNINGS + 1))
        return 1
    fi
}

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Required Components${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Check Node.js
check_required "Node.js" "node --version" "Install Node.js: https://nodejs.org/"

# Check pnpm
check_required "pnpm" "pnpm --version" "Install pnpm: npm install -g pnpm"

# Check Sui binary
check_required "Sui CLI" "test -f /home/femu/sui/target/release/sui" \
    "Build Sui: cd /home/femu/sui && cargo build --release --bin sui"

# Check Sui Node binary
check_required "Sui Node" "test -f /home/femu/sui/target/release/sui-node" \
    "Build Sui: cd /home/femu/sui && cargo build --release --bin sui-node"

# Check FEMU mount
check_required "FEMU SSD Mount" "test -d /home/femu/f2fs_fdp_mount" \
    "Mount FEMU SSD at /home/femu/f2fs_fdp_mount"

# Check write permission
check_required "SSD Write Permission" "test -w /home/femu/f2fs_fdp_mount" \
    "Ensure you have write permission: sudo chown -R $USER /home/femu/f2fs_fdp_mount"

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Project Setup${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Check if sui-local exists
check_required "Local Node Config" "test -d sui-local" \
    "Run: ./scripts/setup-local-node.sh"

# Check if genesis exists
check_required "Genesis Config" "test -f sui-local/genesis.blob" \
    "Run: cd sui-local && sui genesis -f --with-faucet"

# Check if fullnode.yaml exists
check_required "Fullnode Config" "test -f sui-local/fullnode.yaml" \
    "Run: ./scripts/setup-local-node.sh"

# Check if bloat contract exists
check_required "Bloat Move Contract" "test -f move/bloat_storage/sources/bloat.move" \
    "Files should exist in repository"

# Check if dependencies installed
check_required "Node Dependencies" "test -d node_modules" \
    "Run: pnpm install"

# Check if .env exists
if [ -f .env ]; then
    echo -e "${GREEN}✓${NC} .env file exists"
    
    # Check .env contents
    if grep -q "BLOAT_PACKAGE_ID=" .env && ! grep -q "BLOAT_PACKAGE_ID=$" .env; then
        echo -e "${GREEN}✓${NC} BLOAT_PACKAGE_ID configured"
    else
        echo -e "${YELLOW}⚠${NC} BLOAT_PACKAGE_ID not set in .env"
        echo -e "  ${YELLOW}→${NC} Publish contract: cd move/bloat_storage && sui client publish"
        WARNINGS=$((WARNINGS + 1))
    fi
    
    if grep -q "SUI_PRIVATE_KEY=" .env && ! grep -q "SUI_PRIVATE_KEY=$" .env; then
        echo -e "${GREEN}✓${NC} SUI_PRIVATE_KEY configured"
    else
        echo -e "${RED}✗${NC} SUI_PRIVATE_KEY not set in .env"
        echo -e "  ${YELLOW}→${NC} Export key: sui keytool export --key-identity <alias>"
        ERRORS=$((ERRORS + 1))
    fi
    
    if grep -q "SUI_JSON_RPC_URL=http://127.0.0.1:9000" .env; then
        echo -e "${GREEN}✓${NC} Local RPC URL configured"
    else
        echo -e "${YELLOW}⚠${NC} RPC URL not set to local (http://127.0.0.1:9000)"
        WARNINGS=$((WARNINGS + 1))
    fi
else
    echo -e "${RED}✗${NC} .env file missing"
    echo -e "  ${YELLOW}→${NC} Copy: cp .env.example .env and configure"
    ERRORS=$((ERRORS + 1))
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Optional Tools${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

check_optional "iostat (monitoring)" "command -v iostat" \
    "Install: sudo apt-get install sysstat"

check_optional "iotop (I/O monitoring)" "command -v iotop" \
    "Install: sudo apt-get install iotop"

check_optional "bc (calculations)" "command -v bc" \
    "Install: sudo apt-get install bc"

check_optional "jq (JSON parsing)" "command -v jq" \
    "Install: sudo apt-get install jq"

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Runtime Checks${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Check if sui node is running
if curl -s http://127.0.0.1:9000 -o /dev/null; then
    echo -e "${GREEN}✓${NC} Sui node is running"
    
    # Check if faucet is running
    if curl -s http://127.0.0.1:9123/gas -X POST -H "Content-Type: application/json" \
        -d '{"FixedAmountRequest":{"recipient":"0x0"}}' -o /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Faucet is running"
    else
        echo -e "${YELLOW}⚠${NC} Faucet not responding (may need to wait for node startup)"
        WARNINGS=$((WARNINGS + 1))
    fi
else
    echo -e "${YELLOW}⚠${NC} Sui node not running"
    echo -e "  ${YELLOW}→${NC} Start: cd sui-local && ./start-node.sh"
    WARNINGS=$((WARNINGS + 1))
fi

# Check disk space
DISK_AVAIL=$(df -BG /home/femu/f2fs_fdp_mount | tail -1 | awk '{print $4}' | sed 's/G//')
if [ "$DISK_AVAIL" -lt 10 ]; then
    echo -e "${RED}✗${NC} Low disk space: ${DISK_AVAIL}GB available"
    echo -e "  ${YELLOW}→${NC} Need at least 10GB free for testing"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}✓${NC} Sufficient disk space: ${DISK_AVAIL}GB available"
fi

# Check if F2FS
if mount | grep f2fs_fdp_mount | grep -q f2fs; then
    echo -e "${GREEN}✓${NC} F2FS filesystem detected"
else
    echo -e "${YELLOW}⚠${NC} Not F2FS filesystem (won't trigger F2FS GC)"
    WARNINGS=$((WARNINGS + 1))
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Summary${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed!${NC} Ready to run benchmark."
    echo ""
    echo "Start with:"
    echo -e "${YELLOW}HEAVY_LOAD=true BLOAT_STRATEGY=blobs pnpm run preview${NC}"
elif [ $ERRORS -eq 0 ]; then
    echo -e "${YELLOW}⚠ $WARNINGS warnings${NC} - Can proceed but may have issues"
    echo ""
    echo "You can start, but some features may not work optimally:"
    echo -e "${YELLOW}HEAVY_LOAD=true BLOAT_STRATEGY=blobs pnpm run preview${NC}"
else
    echo -e "${RED}✗ $ERRORS errors, $WARNINGS warnings${NC}"
    echo ""
    echo "Please fix the errors above before running the benchmark."
    exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo "For detailed instructions, see:"
echo -e "  ${YELLOW}QUICKSTART.md${NC} - Quick setup guide"
echo -e "  ${YELLOW}STORAGE_BLOAT_GUIDE.md${NC} - Comprehensive documentation"
echo ""
