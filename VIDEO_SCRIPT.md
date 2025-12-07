# Video Demonstration Script
## Big Data CW2 - Air Quality & Mortality Analysis
### Total Duration: 5 minutes

---

## BEFORE RECORDING - Preparation Checklist

- [ ] Sandbox is running and you're logged in via SSH
- [ ] Ambari shows all services GREEN (HDFS, Hive, Zeppelin)
- [ ] Data already uploaded to HDFS
- [ ] Hive tables already created
- [ ] Zeppelin notebook ready with visualizations
- [ ] Have this script open on a second monitor/printed
- [ ] Screen recording software ready (OBS, Camtasia, or Windows Game Bar)
- [ ] Microphone tested

---

## VIDEO SCRIPT

---

### PART 1: Introduction (0:00 - 0:30)
**[SHOW: Terminal with sandbox prompt]**

**SAY:**
> "Hello, in this demonstration I will show my Big Data solution for analyzing the relationship between air pollution and mortality rates across different countries.
>
> I am using the Hortonworks Hadoop Sandbox with HDFS for distributed storage, Apache Hive for data processing using External Tables, and Apache Zeppelin for visualization.
>
> I have two datasets: a global air quality dataset with over 23,000 city measurements from Kaggle, and a World Bank mortality dataset with death rates for over 200 countries."

---

### PART 2: Show Data in HDFS (0:30 - 1:00)
**[SHOW: Terminal]**

**TYPE & SAY:**
```bash
hdfs dfs -ls /user/airpollution/data/
```

> "First, let me show the data stored in HDFS. I have organized the data into separate folders - one for air quality and one for mortality data. This structure supports Hive External Tables."

**TYPE & SAY:**
```bash
hdfs dfs -ls /user/airpollution/data/air_quality/
```

> "The air quality folder contains our cleaned dataset with city-level pollution measurements."

**TYPE & SAY:**
```bash
hdfs dfs -ls /user/airpollution/data/mortality/
```

> "And the mortality folder contains country-level death rates from the World Bank."

**TYPE & SAY:**
```bash
hdfs dfs -cat /user/airpollution/data/air_quality/air_quality_cleaned.csv | head -5
```

> "The air quality data includes country, city, AQI values, and specific pollutant measurements like PM2.5 and NO2."

---

### PART 3: Show Hive Tables (1:00 - 1:45)
**[SHOW: Terminal]**

**TYPE & SAY:**
```bash
hive
```

> "Now I'll open Hive to show the External Tables I created."

**TYPE & SAY:**
```sql
SHOW TABLES;
```

> "I have created several tables: air_quality and mortality as External Tables pointing to HDFS locations, plus analysis tables for the aggregated and joined results."

**TYPE & SAY:**
```sql
DESCRIBE air_quality;
```

> "The air_quality table has columns for country, city, AQI value, and pollutant measurements."

**TYPE & SAY:**
```sql
SELECT * FROM air_quality LIMIT 5;
```

> "Here's a sample of the air quality data showing cities with their AQI and PM2.5 values."

**TYPE & SAY:**
```sql
SELECT * FROM mortality LIMIT 5;
```

> "And here's the mortality data showing countries with their death rates per 100,000 population."

---

### PART 4: Show the JOIN Query (1:45 - 2:30)
**[SHOW: Terminal - Hive]**

**SAY:**
> "The key part of my analysis is joining these two datasets. First, I aggregated air quality by country, then joined with mortality data."

**TYPE & SAY:**
```sql
SELECT * FROM country_avg_aqi LIMIT 5;
```

> "This shows the average air quality metrics per country, calculated from all cities in that country."

**TYPE & SAY:**
```sql
SELECT * FROM pollution_mortality_analysis LIMIT 10;
```

> "This joined table combines the average pollution levels with mortality rates, allowing us to analyze the correlation between air pollution and deaths."

**TYPE & SAY:**
```sql
SELECT country, avg_pm25, mortality_rate 
FROM pollution_mortality_analysis 
ORDER BY mortality_rate DESC 
LIMIT 10;
```

> "Here are the top 10 countries with the highest mortality rates from air pollution. We can see countries like Chad, Central African Republic, and Niger have death rates exceeding 200 per 100,000 people."

---

### PART 5: Show Analysis Insights (2:30 - 3:15)
**[SHOW: Terminal - Hive]**

**SAY:**
> "Let me run some analytical queries to derive insights."

**TYPE & SAY:**
```sql
SELECT country, avg_pm25, mortality_rate,
CASE 
    WHEN avg_pm25 > 100 AND mortality_rate > 150 THEN 'High Risk'
    WHEN avg_pm25 > 50 AND mortality_rate > 100 THEN 'Medium Risk'
    ELSE 'Lower Risk'
END as risk_category
FROM pollution_mortality_analysis
ORDER BY mortality_rate DESC
LIMIT 15;
```

> "I've created a risk classification based on both PM2.5 levels and mortality rates. Countries with PM2.5 above 100 and mortality above 150 are classified as High Risk."

**TYPE & SAY:**
```sql
SELECT aqi_category, COUNT(*) as city_count
FROM air_quality
GROUP BY aqi_category
ORDER BY city_count DESC;
```

> "Looking at the distribution of air quality categories, we can see how many cities fall into each category - from Good to Hazardous."

---

### PART 6: Zeppelin Visualization (3:15 - 4:30)
**[SHOW: Browser - Zeppelin at http://sandbox-hdp.hortonworks.com:9995]**

**SAY:**
> "Now let me switch to Apache Zeppelin to show the visualizations."

**[Navigate to your Zeppelin notebook]**

**SAY:**
> "I've created an interactive notebook with several visualizations."

**[SHOW: Bar Chart - Top Countries by PM2.5]**
**[Run the paragraph if needed]**

> "This bar chart shows the top 15 countries by average PM2.5 levels. We can see India, Pakistan, and Bangladesh have the highest pollution levels."

**[SHOW: Bar Chart - Mortality Rates]**

> "This chart shows mortality rates by country. African nations like Chad and Central African Republic have the highest death rates from air pollution."

**[SHOW: Pie Chart - AQI Categories]**

> "This pie chart shows the distribution of air quality across all cities. We can see that 'Moderate' is the most common category, but a significant portion falls into 'Unhealthy' categories."

**[SHOW: Scatter Plot if available - PM2.5 vs Mortality]**

> "This visualization shows the relationship between PM2.5 levels and mortality rates, demonstrating a positive correlation - higher pollution generally leads to higher death rates."

---

### PART 7: Conclusion (4:30 - 5:00)
**[SHOW: Zeppelin or Terminal]**

**SAY:**
> "In conclusion, my analysis reveals several key findings:
>
> First, there is a clear correlation between air pollution levels and mortality rates across countries.
>
> Second, South Asian countries like India and Pakistan have the highest pollution levels, while many African nations have the highest mortality rates due to combined indoor and outdoor air pollution.
>
> Third, over 40% of cities worldwide have air quality in the 'Moderate' to 'Unhealthy' range.
>
> I chose Apache Hive for this project because of its SQL-like syntax and excellent HDFS integration. Alternative options like Spark SQL or MapReduce were considered but Hive was more suitable for this batch processing task.
>
> This data lake solution using Hadoop, Hive, and Zeppelin effectively integrates multiple datasets to provide actionable health insights.
>
> Thank you for watching."

---

## COMMANDS QUICK REFERENCE (Copy-Paste Ready)

### HDFS Commands
```bash
hdfs dfs -ls /user/airpollution/data/
hdfs dfs -ls /user/airpollution/data/air_quality/
hdfs dfs -ls /user/airpollution/data/mortality/
hdfs dfs -cat /user/airpollution/data/air_quality/air_quality_cleaned.csv | head -5
```

### Hive Commands
```sql
-- Show tables
SHOW TABLES;

-- Describe table structure
DESCRIBE air_quality;

-- Sample data
SELECT * FROM air_quality LIMIT 5;
SELECT * FROM mortality LIMIT 5;

-- Aggregated data
SELECT * FROM country_avg_aqi LIMIT 5;

-- Joined analysis
SELECT * FROM pollution_mortality_analysis LIMIT 10;

-- Top countries by mortality
SELECT country, avg_pm25, mortality_rate 
FROM pollution_mortality_analysis 
ORDER BY mortality_rate DESC 
LIMIT 10;

-- Risk classification
SELECT country, avg_pm25, mortality_rate,
CASE 
    WHEN avg_pm25 > 100 AND mortality_rate > 150 THEN 'High Risk'
    WHEN avg_pm25 > 50 AND mortality_rate > 100 THEN 'Medium Risk'
    ELSE 'Lower Risk'
END as risk_category
FROM pollution_mortality_analysis
ORDER BY mortality_rate DESC
LIMIT 15;

-- AQI distribution
SELECT aqi_category, COUNT(*) as city_count
FROM air_quality
GROUP BY aqi_category
ORDER BY city_count DESC;
```

---

## ZEPPELIN PARAGRAPHS (Pre-create these before recording)

### Paragraph 1: Top Countries by PM2.5
```sql
%jdbc(hive)
SELECT country, avg_pm25 
FROM pollution_mortality_analysis 
ORDER BY avg_pm25 DESC 
LIMIT 15
```
**Chart Type:** Bar Chart

### Paragraph 2: Mortality Rates
```sql
%jdbc(hive)
SELECT country, mortality_rate 
FROM pollution_mortality_analysis 
ORDER BY mortality_rate DESC 
LIMIT 15
```
**Chart Type:** Bar Chart

### Paragraph 3: AQI Distribution
```sql
%jdbc(hive)
SELECT aqi_category, COUNT(*) as count 
FROM air_quality 
GROUP BY aqi_category
```
**Chart Type:** Pie Chart

### Paragraph 4: PM2.5 vs Mortality Scatter
```sql
%jdbc(hive)
SELECT avg_pm25, mortality_rate, country 
FROM pollution_mortality_analysis
WHERE avg_pm25 IS NOT NULL AND mortality_rate IS NOT NULL
```
**Chart Type:** Scatter Chart (X: avg_pm25, Y: mortality_rate)

---

## RECORDING TIPS

1. **Practice once** before recording
2. **Speak slowly** and clearly
3. **Pause briefly** after each command to show results
4. **Make terminal font bigger** (right-click terminal → preferences → font size 14+)
5. **Close unnecessary windows** to avoid distractions
6. **Record in 1080p** if possible
7. **Keep mouse movements smooth**
8. **If you make a mistake**, just continue - small errors are fine

---

## TIMING BREAKDOWN

| Section | Duration | Cumulative |
|---------|----------|------------|
| Introduction | 30 sec | 0:30 |
| HDFS Demo | 30 sec | 1:00 |
| Hive Tables | 45 sec | 1:45 |
| JOIN Query | 45 sec | 2:30 |
| Analysis Queries | 45 sec | 3:15 |
| Zeppelin Visuals | 75 sec | 4:30 |
| Conclusion | 30 sec | 5:00 |

---

## PRE-RECORDING CHECKLIST

Before you hit record, make sure:

- [ ] HDFS has both files in correct folders
- [ ] All Hive tables exist (air_quality, mortality, country_avg_aqi, pollution_mortality_analysis)
- [ ] Zeppelin notebook is open with all 4 paragraphs ready
- [ ] Terminal font is large enough to read
- [ ] Script is visible on second screen or printed

---

**Good luck with your recording!**
