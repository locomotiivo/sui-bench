#!/bin/bash

SCRIPTS_DIR="$HOME/fdp-scripts/sui-bench/sui2/scripts"
# Use the same config dir as the benchmark
SUI_CONFIG_DIR="${SUI_CONFIG_DIR:-$HOME/f2fs_fdp_mount/p0/sui_node}"

ADDRESS=$(SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui client active-address)
COUNT=${COUNT:-20}

echo "💰 Funding address $ADDRESS with $COUNT gas requests..."

for i in $(seq 1 $COUNT); do
    echo "Request $i/$COUNT..."
    curl -s --location --request POST 'http://127.0.0.1:9123/gas' \
      --header 'Content-Type: application/json' \
      --data-raw "{
        \"FixedAmountRequest\": {
          \"recipient\": \"$ADDRESS\"
        }
      }" | jq -r '.transferred_gas_objects[0].amount' 2>/dev/null || echo "Failed"
    sleep 0.5
done

echo "$(SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui client balance)"


# cd $SCRIPT_DIR
# get package id from published contract output
# PACKAGE_ID=$(SUI_CONFIG_DIR=$SUI_CONFIG_DIR sui client publish --gas-budget 500000000 --json | jq -r '.CreatedObjects[0].PackageID')
