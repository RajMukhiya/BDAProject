#!/bin/bash
# ============================================================================
# HBase Setup Script — Creates tables for low-latency ticker lookups
# Run on master: bash /app/src/batch/hbase_setup.sh
# ============================================================================

echo "============================================"
echo " HBase Table Setup for BDATL Engine"
echo "============================================"

# Check if HBase shell is available
if ! command -v hbase &> /dev/null; then
    echo "❌ HBase is not installed or not in PATH."
    echo "   Install HBase or run this inside the Docker container."
    exit 1
fi

# Create HBase tables via shell commands
hbase shell <<EOF

-- Ticker metadata: sector, market cap, exchange info
create_if_not_exists 'ticker_metadata', \
    {NAME => 'info', VERSIONS => 1, COMPRESSION => 'SNAPPY'}, \
    {NAME => 'sector', VERSIONS => 1}

-- Latest sentiment snapshot per ticker (fast dashboard lookups)
create_if_not_exists 'sentiment_snapshot', \
    {NAME => 'latest', VERSIONS => 3, TTL => 604800}, \
    {NAME => 'history', VERSIONS => 10}

-- Anomaly alerts (recent anomalies for real-time dashboard)
create_if_not_exists 'anomaly_alerts', \
    {NAME => 'alert', VERSIONS => 5, TTL => 2592000}, \
    {NAME => 'context', VERSIONS => 1}

-- Sector aggregates (for heatmap lookups)
create_if_not_exists 'sector_aggregates', \
    {NAME => 'momentum', VERSIONS => 3}, \
    {NAME => 'stats', VERSIONS => 1}

list

exit
EOF

echo ""
echo "============================================"
echo " HBase tables created successfully."
echo "============================================"

# Populate ticker metadata with Indian sector mappings
echo "Populating ticker_metadata with Indian stock sectors dynamically..."

python3 /app/src/batch/generate_hbase_commands.py | hbase shell

echo "✅ Ticker metadata populated."
