# Big Data & Infrastructure - Coursework 2
## Air Quality and Mortality Data Lake Analysis

**Module:** COM745 - Big Data & Infrastructure  
**Student:** Abdennabi Ahrrabi  
**University:** Ulster University  

---

## Project Overview

This project analyzes the relationship between **air pollution levels** and **mortality rates** across different countries using a Hadoop-based data lake architecture.

### Problem Statement

Air pollution is one of the leading environmental risk factors for death globally. This project aims to:
1. Integrate air quality data from multiple cities worldwide
2. Correlate pollution levels (AQI, PM2.5) with country-level mortality rates
3. Identify which countries have the highest health impact from air pollution
4. Visualize insights using Apache Zeppelin

### Datasets Used

| Dataset | Source | Description | Size |
|---------|--------|-------------|------|
| Global Air Pollution | [Kaggle](https://www.kaggle.com/datasets/hasibalmuzdadid/global-air-pollution-dataset) | City-level AQI, PM2.5, NO2, Ozone, CO measurements | 23,463 rows |
| Mortality Rates | [World Bank](https://data.worldbank.org/indicator/SH.STA.AIRP.P5) | Country-level mortality rate attributed to air pollution (per 100,000 population) | 231 rows |

---

## Technical Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   CSV       │     │    HDFS     │     │  Hive/Spark │     │  Zeppelin   │
│   Files     │ ──▶ │  Storage    │ ──▶ │  Processing │ ──▶ │  Visualize  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Storage | HDFS (Hadoop Distributed File System) | Distributed storage for large datasets |
| Processing | Apache Hive | SQL-like queries on big data |
| Processing | Apache Spark | In-memory data processing |
| Visualization | Apache Zeppelin | Interactive notebooks and charts |
| Platform | Hortonworks Data Platform (HDP) Sandbox | Integrated Hadoop ecosystem |

### Why Hive?

**Chosen Engine:** Apache Hive

**Justification:**
- SQL-like syntax (HiveQL) - familiar and easy to use
- Excellent for batch processing and ETL operations
- Native integration with HDFS
- Supports complex joins between datasets
- Schema-on-read capability for flexible data handling
- External tables allow data to stay in HDFS without moving

**Alternative Options Considered:**

| Engine | Pros | Cons | Why Not Chosen |
|--------|------|------|----------------|
| Spark SQL | Faster (in-memory), supports ML | More complex setup | Overkill for this dataset size |
| MapReduce | Native Hadoop, very scalable | Complex Java code required | Too low-level for SQL operations |
| Pig | Good for ETL pipelines | Less SQL-like syntax | Hive more intuitive for analysis |
| Presto | Very fast interactive queries | Requires separate installation | Not pre-installed in sandbox |

---

## Implementation Steps

### Step 1: Environment Setup

```bash
# Start HDP Sandbox and access via SSH
ssh root@sandbox-hdp.hortonworks.com -p 2222
# Password: hadoop (or your configured password)
```

### Step 2: Download Datasets

```bash
# Create project directory
mkdir -p /home/airpollution
cd /home/airpollution

# Download cleaned datasets from GitHub
wget https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/air_quality_cleaned.csv

wget https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/mortality_by_country.csv

# Verify downloads
ls -la
```

### Step 3: Upload to HDFS

```bash
# Create HDFS directory structure (separate folder for each dataset)
hdfs dfs -mkdir -p /user/airpollution/data/air_quality
hdfs dfs -mkdir -p /user/airpollution/data/mortality

# Upload CSV files to their respective HDFS folders
hdfs dfs -put air_quality_cleaned.csv /user/airpollution/data/air_quality/
hdfs dfs -put mortality_by_country.csv /user/airpollution/data/mortality/

# Verify files in HDFS
hdfs dfs -ls /user/airpollution/data/air_quality/
hdfs dfs -ls /user/airpollution/data/mortality/
```

### Step 4: Create Hive External Tables

```bash
# Start Hive CLI
hive
```

#### Create Air Quality External Table

```sql
CREATE EXTERNAL TABLE air_quality (
    country STRING,
    city STRING,
    aqi_value INT,
    aqi_category STRING,
    co_aqi_value INT,
    co_aqi_category STRING,
    ozone_aqi_value INT,
    ozone_aqi_category STRING,
    no2_aqi_value INT,
    no2_aqi_category STRING,
    pm25_aqi_value INT,
    pm25_aqi_category STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/airpollution/data/air_quality/'
TBLPROPERTIES ("skip.header.line.count"="1");
```

#### Create Mortality External Table

```sql
CREATE EXTERNAL TABLE mortality (
    country_name STRING,
    country_code STRING,
    mortality_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/airpollution/data/mortality/'
TBLPROPERTIES ("skip.header.line.count"="1");
```

#### Verify Tables

```sql
SELECT * FROM air_quality LIMIT 5;
SELECT * FROM mortality LIMIT 5;
```

### Step 5: Data Analysis Queries

#### Query 1: Average AQI by Country

```sql
CREATE TABLE country_avg_aqi AS
SELECT 
    country,
    COUNT(*) as num_cities,
    ROUND(AVG(aqi_value), 2) as avg_aqi,
    ROUND(AVG(pm25_aqi_value), 2) as avg_pm25,
    ROUND(AVG(no2_aqi_value), 2) as avg_no2,
    ROUND(AVG(ozone_aqi_value), 2) as avg_ozone
FROM air_quality
GROUP BY country
ORDER BY avg_aqi DESC;
```

#### Query 2: Join Air Quality with Mortality Data

```sql
CREATE TABLE pollution_mortality_analysis AS
SELECT 
    a.country,
    a.num_cities,
    a.avg_aqi,
    a.avg_pm25,
    m.mortality_rate,
    m.country_code
FROM country_avg_aqi a
JOIN mortality m ON a.country = m.country_name
ORDER BY m.mortality_rate DESC;
```

#### Query 3: Top 10 Most Polluted Countries with Death Rates

```sql
SELECT 
    country,
    avg_aqi,
    avg_pm25,
    mortality_rate
FROM pollution_mortality_analysis
ORDER BY avg_pm25 DESC
LIMIT 10;
```

#### Query 4: Correlation Analysis - High Pollution vs High Mortality

```sql
SELECT 
    country,
    avg_pm25,
    mortality_rate,
    CASE 
        WHEN avg_pm25 > 100 AND mortality_rate > 150 THEN 'High Risk'
        WHEN avg_pm25 > 50 AND mortality_rate > 100 THEN 'Medium Risk'
        ELSE 'Lower Risk'
    END as risk_category
FROM pollution_mortality_analysis
ORDER BY mortality_rate DESC;
```

#### Query 5: AQI Category Distribution

```sql
SELECT 
    aqi_category,
    COUNT(*) as city_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM air_quality), 2) as percentage
FROM air_quality
GROUP BY aqi_category
ORDER BY city_count DESC;
```

### Step 6: Zeppelin Visualization

Access Zeppelin at: `http://sandbox-hdp.hortonworks.com:9995`

#### Create New Notebook

1. Click "Create new note"
2. Name it: "Air Pollution Analysis"
3. Select interpreter: `jdbc` or `hive`

#### Visualization Paragraphs

```sql
%jdbc(hive)
-- Top 15 Countries by Average PM2.5
SELECT country, avg_pm25, mortality_rate 
FROM pollution_mortality_analysis 
ORDER BY avg_pm25 DESC 
LIMIT 15
```

```sql
%jdbc(hive)
-- Mortality Rate Distribution
SELECT country, mortality_rate 
FROM pollution_mortality_analysis 
ORDER BY mortality_rate DESC 
LIMIT 20
```

```sql
%jdbc(hive)
-- AQI Categories Pie Chart
SELECT aqi_category, COUNT(*) as count 
FROM air_quality 
GROUP BY aqi_category
```

---

## Key Findings

### 1. Countries with Highest PM2.5 Levels
- India, Pakistan, and China have the highest average PM2.5 values
- Many cities in these countries exceed WHO safe limits

### 2. Mortality Rate Correlation
- Countries with avg PM2.5 > 100 tend to have mortality rates > 150 per 100,000
- Strong positive correlation between air pollution and health impact

### 3. AQI Category Distribution
- Majority of cities fall in "Moderate" category
- Significant portion in "Unhealthy for Sensitive Groups"

---

## Project Structure

```
Big-Data-CW2/
├── README.md                           # This file
├── air_quality_cleaned.csv             # Cleaned air quality data
├── mortality_by_country.csv            # Country mortality rates
├── hive_queries.sql                    # All Hive SQL queries
├── hdfs_upload.sh                      # HDFS upload script
└── VIDEO_SCRIPT.md                     # Video demonstration script
```

---

## HDFS Structure

```
/user/airpollution/
└── data/
    ├── air_quality/
    │   └── air_quality_cleaned.csv
    └── mortality/
        └── mortality_by_country.csv
```

---

## How to Run This Project

### Prerequisites
- Hortonworks HDP Sandbox (or equivalent Hadoop environment)
- SSH client (PuTTY on Windows)
- Web browser for Ambari and Zeppelin

### Quick Start

```bash
# 1. SSH into sandbox
ssh root@sandbox-hdp.hortonworks.com -p 2222

# 2. Create project directory and download data
mkdir -p /home/airpollution
cd /home/airpollution
wget https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/air_quality_cleaned.csv
wget https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/mortality_by_country.csv

# 3. Upload to HDFS
hdfs dfs -mkdir -p /user/airpollution/data/air_quality
hdfs dfs -mkdir -p /user/airpollution/data/mortality
hdfs dfs -put air_quality_cleaned.csv /user/airpollution/data/air_quality/
hdfs dfs -put mortality_by_country.csv /user/airpollution/data/mortality/

# 4. Run Hive setup
hive -f hive_queries.sql

# 5. Open Zeppelin for visualization
# Navigate to: http://sandbox-hdp.hortonworks.com:9995
```

---

## References

1. Kaggle - Global Air Pollution Dataset: https://www.kaggle.com/datasets/hasibalmuzdadid/global-air-pollution-dataset
2. World Bank - Air Pollution Mortality Data: https://data.worldbank.org/indicator/SH.STA.AIRP.P5
3. Apache Hive Documentation: https://hive.apache.org/
4. Apache Zeppelin Documentation: https://zeppelin.apache.org/
5. WHO Air Quality Guidelines: https://www.who.int/data/gho/data/themes/air-pollution

---

## License

This project is for educational purposes as part of Ulster University MSc coursework.

---

**Last Updated:** December 2025
