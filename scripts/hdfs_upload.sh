#!/bin/bash
# ============================================
# Big Data CW2 - HDFS Upload Script
# Air Quality and Mortality Analysis
# ============================================

echo "=========================================="
echo "Starting Data Lake Setup..."
echo "=========================================="

# Step 1: Create project directory
echo "[1/6] Creating local project directory..."
mkdir -p /home/airpollution
cd /home/airpollution

# Step 2: Download datasets from GitHub
echo "[2/6] Downloading datasets from GitHub..."
wget -q https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/air_quality_cleaned.csv
wget -q https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/mortality_by_country.csv

# Verify downloads
if [ -f "air_quality_cleaned.csv" ] && [ -f "mortality_by_country.csv" ]; then
    echo "    ✓ Files downloaded successfully"
else
    echo "    ✗ Download failed!"
    exit 1
fi

# Step 3: Create HDFS directory structure
echo "[3/6] Creating HDFS directory structure..."
hdfs dfs -mkdir -p /user/airpollution/data
hdfs dfs -mkdir -p /user/airpollution/output

# Step 4: Upload CSV files to HDFS
echo "[4/6] Uploading CSV files to HDFS..."
hdfs dfs -put -f air_quality_cleaned.csv /user/airpollution/data/
hdfs dfs -put -f mortality_by_country.csv /user/airpollution/data/

# Step 5: Verify files in HDFS
echo "[5/6] Verifying files in HDFS..."
hdfs dfs -ls /user/airpollution/data/

# Step 6: Show file sizes
echo "[6/6] File details:"
echo "    Air Quality Dataset:"
hdfs dfs -du -h /user/airpollution/data/air_quality_cleaned.csv
echo "    Mortality Dataset:"
hdfs dfs -du -h /user/airpollution/data/mortality_by_country.csv

echo ""
echo "=========================================="
echo "Data Lake Setup Complete!"
echo "=========================================="
echo ""
echo "Next step: Run Hive to create tables"
echo "  $ hive -f hive_queries.sql"
echo ""
