#!/bin/bash
# ============================================================
# COM745 Big Data & Infrastructure - HDFS Setup Commands
# Air Quality and Mortality Data Lake Analysis
# ============================================================

# ============================================================
# STEP 1: CREATE PROJECT DIRECTORY (Local)
# ============================================================

echo "Creating local project directory..."
mkdir -p /home/airpollution
cd /home/airpollution

# ============================================================
# STEP 2: DOWNLOAD DATA FILES FROM GITHUB
# ============================================================

echo "Downloading air quality dataset..."
curl -L -o air_quality_cleaned.csv \
    "https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/data/air_quality_cleaned.csv"

echo "Downloading mortality dataset..."
curl -L -o mortality_by_country.csv \
    "https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/data/mortality_by_country.csv"

# ============================================================
# STEP 3: VERIFY DOWNLOADS
# ============================================================

echo "Verifying downloaded files..."
echo "File types:"
file air_quality_cleaned.csv
file mortality_by_country.csv

echo ""
echo "File sizes:"
ls -la *.csv

echo ""
echo "Preview air quality data:"
head -3 air_quality_cleaned.csv

echo ""
echo "Preview mortality data:"
head -3 mortality_by_country.csv

# ============================================================
# STEP 4: CREATE HDFS DIRECTORY STRUCTURE
# ============================================================

echo ""
echo "Creating HDFS directories..."

# Create main project directory
hdfs dfs -mkdir -p /user/airpollution/data/air_quality
hdfs dfs -mkdir -p /user/airpollution/data/mortality

echo "HDFS directories created:"
hdfs dfs -ls /user/airpollution/data/

# ============================================================
# STEP 5: UPLOAD DATA TO HDFS
# ============================================================

echo ""
echo "Uploading air quality data to HDFS..."
hdfs dfs -put air_quality_cleaned.csv /user/airpollution/data/air_quality/

echo "Uploading mortality data to HDFS..."
hdfs dfs -put mortality_by_country.csv /user/airpollution/data/mortality/

# ============================================================
# STEP 6: VERIFY HDFS UPLOADS
# ============================================================

echo ""
echo "Verifying HDFS uploads..."
echo ""
echo "Air quality folder:"
hdfs dfs -ls /user/airpollution/data/air_quality/

echo ""
echo "Mortality folder:"
hdfs dfs -ls /user/airpollution/data/mortality/

echo ""
echo "File sizes in HDFS:"
hdfs dfs -du -h /user/airpollution/data/

# ============================================================
# STEP 7: PREVIEW DATA IN HDFS
# ============================================================

echo ""
echo "Preview air quality data in HDFS:"
hdfs dfs -cat /user/airpollution/data/air_quality/air_quality_cleaned.csv | head -5

echo ""
echo "Preview mortality data in HDFS:"
hdfs dfs -cat /user/airpollution/data/mortality/mortality_by_country.csv | head -5

# ============================================================
# SETUP COMPLETE
# ============================================================

echo ""
echo "============================================"
echo "HDFS SETUP COMPLETE!"
echo "============================================"
echo ""
echo "Data uploaded to:"
echo "  - /user/airpollution/data/air_quality/"
echo "  - /user/airpollution/data/mortality/"
echo ""
echo "Next step: Run Hive queries from scripts/hive_queries.sql"
echo ""

# ============================================================
# USEFUL HDFS COMMANDS REFERENCE
# ============================================================

# List directory contents
# hdfs dfs -ls /user/airpollution/data/

# View file content
# hdfs dfs -cat /path/to/file | head -10

# Check disk usage
# hdfs dfs -du -h /user/airpollution/

# Delete file
# hdfs dfs -rm /path/to/file

# Delete directory recursively
# hdfs dfs -rm -r /path/to/directory

# Copy file from local to HDFS
# hdfs dfs -put local_file.csv /hdfs/path/

# Copy file from HDFS to local
# hdfs dfs -get /hdfs/path/file.csv local_file.csv

# Create directory
# hdfs dfs -mkdir -p /path/to/directory

# Check file exists
# hdfs dfs -test -e /path/to/file && echo "exists" || echo "not found"