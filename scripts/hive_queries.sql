-- ============================================================
-- COM745 Big Data & Infrastructure - Hive Queries
-- Air Quality and Mortality Data Lake Analysis
-- ============================================================

-- ============================================================
-- PART 1: DATABASE SETUP
-- ============================================================

-- Create dedicated database
CREATE DATABASE IF NOT EXISTS airpollution;

-- Switch to database
USE airpollution;

-- ============================================================
-- PART 2: EXTERNAL TABLES (Read from HDFS)
-- ============================================================

-- Air Quality External Table
-- Points to: /user/airpollution/data/air_quality/
CREATE EXTERNAL TABLE IF NOT EXISTS air_quality (
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

-- Mortality External Table
-- Points to: /user/airpollution/data/mortality/
CREATE EXTERNAL TABLE IF NOT EXISTS mortality (
    country_name STRING,
    country_code STRING,
    mortality_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/airpollution/data/mortality/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- ============================================================
-- PART 3: VERIFY DATA LOADED
-- ============================================================

-- Check tables exist
SHOW TABLES;

-- Count records
SELECT COUNT(*) AS total_cities FROM air_quality;
-- Expected: 23,463

SELECT COUNT(*) AS total_countries FROM mortality;
-- Expected: 231

-- Preview data
SELECT * FROM air_quality LIMIT 5;
SELECT * FROM mortality LIMIT 5;

-- ============================================================
-- PART 4: AGGREGATION - Country Level Averages
-- ============================================================

-- Create aggregated table: Average pollution per country
CREATE TABLE IF NOT EXISTS country_avg_aqi AS
SELECT 
    country,
    COUNT(*) AS num_cities,
    ROUND(AVG(aqi_value), 2) AS avg_aqi,
    ROUND(AVG(pm25_aqi_value), 2) AS avg_pm25,
    ROUND(AVG(no2_aqi_value), 2) AS avg_no2,
    ROUND(AVG(ozone_aqi_value), 2) AS avg_ozone,
    ROUND(AVG(co_aqi_value), 2) AS avg_co
FROM air_quality
GROUP BY country;

-- Verify aggregation
SELECT COUNT(*) AS countries_with_data FROM country_avg_aqi;
-- Expected: 176

SELECT * FROM country_avg_aqi LIMIT 10;

-- ============================================================
-- PART 5: JOIN - Combine Air Quality with Mortality
-- ============================================================

-- Create joined analysis table
CREATE TABLE IF NOT EXISTS pollution_mortality_analysis AS
SELECT 
    a.country,
    a.num_cities,
    a.avg_aqi,
    a.avg_pm25,
    a.avg_no2,
    a.avg_ozone,
    a.avg_co,
    m.mortality_rate,
    m.country_code
FROM country_avg_aqi a
JOIN mortality m ON a.country = m.country_name;

-- Verify join results
SELECT COUNT(*) AS matched_countries FROM pollution_mortality_analysis;
-- Expected: 168

SELECT * FROM pollution_mortality_analysis LIMIT 10;

-- ============================================================
-- PART 6: ANALYSIS QUERIES
-- ============================================================

-- Query 1: Top 10 Most Polluted Countries (by PM2.5)
SELECT 
    country, 
    avg_pm25, 
    mortality_rate
FROM pollution_mortality_analysis
ORDER BY avg_pm25 DESC
LIMIT 10;

-- Query 2: Top 10 Countries by Mortality Rate
SELECT 
    country, 
    avg_pm25, 
    mortality_rate
FROM pollution_mortality_analysis
ORDER BY mortality_rate DESC
LIMIT 10;

-- Query 3: AQI Category Distribution
SELECT 
    aqi_category, 
    COUNT(*) AS city_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM air_quality), 2) AS percentage
FROM air_quality
GROUP BY aqi_category
ORDER BY city_count DESC;

-- Query 4: Risk Classification
SELECT 
    country, 
    avg_pm25, 
    mortality_rate,
    CASE 
        WHEN avg_pm25 > 100 AND mortality_rate > 150 THEN 'High Risk'
        WHEN avg_pm25 > 50 AND mortality_rate > 100 THEN 'Medium Risk'
        ELSE 'Lower Risk'
    END AS risk_category
FROM pollution_mortality_analysis
ORDER BY mortality_rate DESC
LIMIT 20;

-- Query 5: Count by Risk Category
SELECT 
    risk_category,
    COUNT(*) AS country_count
FROM (
    SELECT 
        country,
        CASE 
            WHEN avg_pm25 > 100 AND mortality_rate > 150 THEN 'High Risk'
            WHEN avg_pm25 > 50 AND mortality_rate > 100 THEN 'Medium Risk'
            ELSE 'Lower Risk'
        END AS risk_category
    FROM pollution_mortality_analysis
) risk_table
GROUP BY risk_category;

-- Query 6: Countries with High Pollution but Low Mortality
SELECT 
    country, 
    avg_pm25, 
    mortality_rate
FROM pollution_mortality_analysis
WHERE avg_pm25 > 100 AND mortality_rate < 100
ORDER BY avg_pm25 DESC;

-- Query 7: Countries with Low Pollution but High Mortality
SELECT 
    country, 
    avg_pm25, 
    mortality_rate
FROM pollution_mortality_analysis
WHERE avg_pm25 < 50 AND mortality_rate > 150
ORDER BY mortality_rate DESC;

-- Query 8: Regional Analysis (Countries with most cities)
SELECT 
    country, 
    num_cities, 
    avg_pm25, 
    mortality_rate
FROM pollution_mortality_analysis
ORDER BY num_cities DESC
LIMIT 15;

-- Query 9: Correlation Summary Statistics
SELECT 
    ROUND(AVG(avg_pm25), 2) AS global_avg_pm25,
    ROUND(AVG(mortality_rate), 2) AS global_avg_mortality,
    ROUND(MIN(avg_pm25), 2) AS min_pm25,
    ROUND(MAX(avg_pm25), 2) AS max_pm25,
    ROUND(MIN(mortality_rate), 2) AS min_mortality,
    ROUND(MAX(mortality_rate), 2) AS max_mortality
FROM pollution_mortality_analysis;

-- ============================================================
-- PART 7: ZEPPELIN VISUALIZATION QUERIES
-- ============================================================

-- Use these queries in Zeppelin with %jdbc(hive) interpreter

-- Zeppelin Chart 1: Bar Chart - Top 15 by PM2.5
-- %jdbc(hive)
SELECT country, avg_pm25 
FROM airpollution.pollution_mortality_analysis 
ORDER BY avg_pm25 DESC 
LIMIT 15;

-- Zeppelin Chart 2: Bar Chart - Top 15 by Mortality
-- %jdbc(hive)
SELECT country, mortality_rate 
FROM airpollution.pollution_mortality_analysis 
ORDER BY mortality_rate DESC 
LIMIT 15;

-- Zeppelin Chart 3: Pie Chart - AQI Distribution
-- %jdbc(hive)
SELECT aqi_category, COUNT(*) AS count 
FROM airpollution.air_quality 
GROUP BY aqi_category;

-- Zeppelin Chart 4: Area Chart - Risk Analysis
-- %jdbc(hive)
SELECT country, avg_pm25, mortality_rate,
CASE 
    WHEN avg_pm25 > 100 AND mortality_rate > 150 THEN 'High Risk'
    WHEN avg_pm25 > 50 AND mortality_rate > 100 THEN 'Medium Risk'
    ELSE 'Lower Risk'
END AS risk_category
FROM airpollution.pollution_mortality_analysis
ORDER BY mortality_rate DESC
LIMIT 20;

-- ============================================================
-- UTILITY QUERIES
-- ============================================================

-- Show all tables
SHOW TABLES;

-- Describe table structure
DESCRIBE air_quality;
DESCRIBE mortality;
DESCRIBE country_avg_aqi;
DESCRIBE pollution_mortality_analysis;

-- Check table locations (for External tables)
DESCRIBE FORMATTED air_quality;
DESCRIBE FORMATTED mortality;

-- ============================================================
-- CLEANUP (Use only if needed to reset)
-- ============================================================

-- DROP TABLE IF EXISTS pollution_mortality_analysis;
-- DROP TABLE IF EXISTS country_avg_aqi;
-- DROP TABLE IF EXISTS air_quality;
-- DROP TABLE IF EXISTS mortality;
-- DROP DATABASE IF EXISTS airpollution;