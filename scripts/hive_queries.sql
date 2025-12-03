-- ============================================
-- Big Data CW2 - Hive Queries
-- Air Quality and Mortality Analysis
-- ============================================

-- ============================================
-- PART 1: CREATE TABLES
-- ============================================

-- Create Air Quality Table
CREATE TABLE IF NOT EXISTS air_quality (
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
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load Air Quality Data
LOAD DATA INPATH '/user/airpollution/data/air_quality_cleaned.csv' INTO TABLE air_quality;

-- Create Mortality Table
CREATE TABLE IF NOT EXISTS mortality (
    country_name STRING,
    country_code STRING,
    mortality_rate DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load Mortality Data
LOAD DATA INPATH '/user/airpollution/data/mortality_by_country.csv' INTO TABLE mortality;

-- ============================================
-- PART 2: VERIFY DATA LOADED
-- ============================================

-- Check air quality data
SELECT * FROM air_quality LIMIT 10;

-- Check mortality data
SELECT * FROM mortality LIMIT 10;

-- Count records
SELECT COUNT(*) as total_cities FROM air_quality;
SELECT COUNT(*) as total_countries FROM mortality;

-- ============================================
-- PART 3: DATA ANALYSIS QUERIES
-- ============================================

-- Query 1: Average AQI by Country (Aggregation)
DROP TABLE IF EXISTS country_avg_aqi;
CREATE TABLE country_avg_aqi AS
SELECT 
    country,
    COUNT(*) as num_cities,
    ROUND(AVG(aqi_value), 2) as avg_aqi,
    ROUND(AVG(pm25_aqi_value), 2) as avg_pm25,
    ROUND(AVG(no2_aqi_value), 2) as avg_no2,
    ROUND(AVG(ozone_aqi_value), 2) as avg_ozone,
    ROUND(AVG(co_aqi_value), 2) as avg_co
FROM air_quality
GROUP BY country
ORDER BY avg_aqi DESC;

-- View aggregated results
SELECT * FROM country_avg_aqi LIMIT 20;

-- Query 2: Join Air Quality with Mortality Data (THE KEY JOIN)
DROP TABLE IF EXISTS pollution_mortality_analysis;
CREATE TABLE pollution_mortality_analysis AS
SELECT 
    a.country,
    a.num_cities,
    a.avg_aqi,
    a.avg_pm25,
    a.avg_no2,
    a.avg_ozone,
    m.mortality_rate,
    m.country_code
FROM country_avg_aqi a
JOIN mortality m ON a.country = m.country_name
ORDER BY m.mortality_rate DESC;

-- View joined results
SELECT * FROM pollution_mortality_analysis LIMIT 20;

-- Query 3: Top 10 Most Polluted Countries (by PM2.5) with Death Rates
SELECT 
    country,
    num_cities,
    avg_aqi,
    avg_pm25,
    mortality_rate
FROM pollution_mortality_analysis
ORDER BY avg_pm25 DESC
LIMIT 10;

-- Query 4: Top 10 Countries by Mortality Rate
SELECT 
    country,
    avg_pm25,
    avg_aqi,
    mortality_rate
FROM pollution_mortality_analysis
ORDER BY mortality_rate DESC
LIMIT 10;

-- Query 5: Risk Category Classification
SELECT 
    country,
    avg_pm25,
    mortality_rate,
    CASE 
        WHEN avg_pm25 > 100 AND mortality_rate > 150 THEN 'High Risk'
        WHEN avg_pm25 > 50 AND mortality_rate > 100 THEN 'Medium Risk'
        WHEN avg_pm25 > 50 OR mortality_rate > 50 THEN 'Moderate Risk'
        ELSE 'Lower Risk'
    END as risk_category
FROM pollution_mortality_analysis
ORDER BY mortality_rate DESC;

-- Query 6: AQI Category Distribution (for Pie Chart)
SELECT 
    aqi_category,
    COUNT(*) as city_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM air_quality), 2) as percentage
FROM air_quality
GROUP BY aqi_category
ORDER BY city_count DESC;

-- Query 7: PM2.5 Category Distribution
SELECT 
    pm25_aqi_category,
    COUNT(*) as city_count
FROM air_quality
GROUP BY pm25_aqi_category
ORDER BY city_count DESC;

-- Query 8: Countries with Most Cities in Dataset
SELECT 
    country,
    COUNT(*) as num_cities
FROM air_quality
GROUP BY country
ORDER BY num_cities DESC
LIMIT 15;

-- Query 9: Worst Cities by AQI
SELECT 
    country,
    city,
    aqi_value,
    aqi_category,
    pm25_aqi_value
FROM air_quality
ORDER BY aqi_value DESC
LIMIT 20;

-- Query 10: Correlation Summary Statistics
SELECT 
    COUNT(*) as total_countries,
    ROUND(AVG(avg_pm25), 2) as overall_avg_pm25,
    ROUND(AVG(mortality_rate), 2) as overall_avg_mortality,
    ROUND(MAX(avg_pm25), 2) as max_pm25,
    ROUND(MAX(mortality_rate), 2) as max_mortality,
    ROUND(MIN(avg_pm25), 2) as min_pm25,
    ROUND(MIN(mortality_rate), 2) as min_mortality
FROM pollution_mortality_analysis;

-- ============================================
-- PART 4: EXPORT FOR VISUALIZATION
-- ============================================

-- Create final analysis table for Zeppelin
DROP TABLE IF EXISTS final_analysis;
CREATE TABLE final_analysis AS
SELECT 
    country,
    country_code,
    num_cities,
    avg_aqi,
    avg_pm25,
    avg_no2,
    avg_ozone,
    mortality_rate,
    CASE 
        WHEN avg_pm25 > 100 AND mortality_rate > 150 THEN 'High Risk'
        WHEN avg_pm25 > 50 AND mortality_rate > 100 THEN 'Medium Risk'
        WHEN avg_pm25 > 50 OR mortality_rate > 50 THEN 'Moderate Risk'
        ELSE 'Lower Risk'
    END as risk_category
FROM pollution_mortality_analysis;

-- View final analysis
SELECT * FROM final_analysis ORDER BY mortality_rate DESC;
