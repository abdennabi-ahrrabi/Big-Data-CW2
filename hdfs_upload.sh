#!/bin/bash
# ============================================
# Big Data CW2 - HDFS Upload Script
# Air Quality and Mortality Analysis
# ============================================

echo "=========================================="
echo "Starting Data Lake Setup..."
echo "=========================================="

# Step 1: Create project directory
echo "[1/7] Creating local project directory..."
mkdir -p /home/airpollution
cd /home/airpollution

# Step 2: Download datasets from GitHub
echo "[2/7] Downloading datasets from GitHub..."
wget -q https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/air_quality_cleaned.csv
wget -q https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/mortality_by_country.csv

# Verify downloads
if [ -f "air_quality_cleaned.csv" ] && [ -f "mortality_by_country.csv" ]; then
    echo "    ✓ Files downloaded successfully"
else
    echo "    ✗ Download failed!"
    exit 1
fi

# Step 3: Create HDFS directory structure (separate folder for each dataset)
echo "[3/7] Creating HDFS directory structure..."
hdfs dfs -mkdir -p /user/airpollution/data/air_quality
hdfs dfs -mkdir -p /user/airpollution/data/mortality

# Step 4: Upload CSV files to their respective HDFS folders
echo "[4/7] Uploading air quality data to HDFS..."
hdfs dfs -put -f air_quality_cleaned.csv /user/airpollution/data/air_quality/

echo "[5/7] Uploading mortality data to HDFS..."
hdfs dfs -put -f mortality_by_country.csv /user/airpollution/data/mortality/

# Step 6: Verify files in HDFS
echo "[6/7] Verifying files in HDFS..."
echo "Air Quality folder:"
hdfs dfs -ls /user/airpollution/data/air_quality/
echo ""
echo "Mortality folder:"
hdfs dfs -ls /user/airpollution/data/mortality/

# Step 7: Show file sizes
echo "[7/7] File details:"
echo "    Air Quality Dataset:"
hdfs dfs -du -h /user/airpollution/data/air_quality/
echo "    Mortality Dataset:"
hdfs dfs -du -h /user/airpollution/data/mortality/

echo ""
echo "=========================================="
echo "Data Lake Setup Complete!"
echo "=========================================="
echo ""
echo "HDFS Structure:"
echo "/user/airpollution/"
echo "└── data/"
echo "    ├── air_quality/"
echo "    │   └── air_quality_cleaned.csv"
echo "    └── mortality/"
echo "        └── mortality_by_country.csv"
echo ""
echo "Next step: Run Hive to create external tables"
echo "  $ hive -f hive_queries.sql"
echo ""
