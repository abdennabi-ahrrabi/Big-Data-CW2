# Air Quality and Mortality Data Lake Analysis

**COM745 - Big Data & Infrastructure | Ulster University**

A Hadoop-based data lake solution analyzing the correlation between global air pollution levels and mortality rates across 168 countries.

---

## Project Overview

This project demonstrates the implementation of a data lake using the Hadoop ecosystem to integrate and analyze two public health datasets:

- **Global Air Pollution Dataset** - 23,463 city-level measurements from Kaggle
- **World Bank Mortality Data** - Death rates attributed to air pollution for 231 countries

### Key Findings

| Finding | Details |
|---------|---------|
| Most Polluted Countries | Bahrain (188), Mauritania (179), Pakistan (173) - PM2.5 levels |
| Highest Mortality Rates | Central African Republic (305), Lesotho (288), Solomon Islands (281) |
| Countries Matched | 168 out of 176 (95% match rate) |
| Cities Analyzed | 23,463 across 176 countries |

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Platform | Hortonworks HDP Sandbox | Hadoop distribution |
| Storage | HDFS | Distributed file storage |
| Processing | Apache Hive | SQL-based data processing |
| Visualization | Apache Zeppelin | Interactive notebooks |

### Why Apache Hive?

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| Spark SQL | Fast in-memory processing | Complex setup, resource heavy | Overkill for dataset size |
| MapReduce | Native Hadoop, scalable | Requires Java, complex code | Too low-level |
| Apache Pig | Good for ETL pipelines | Pig Latin less intuitive | Hive more SQL-like |
| Presto | Fast interactive queries | Separate installation needed | Not included in HDP |

**Chosen: Apache Hive** - SQL-like syntax, native HDFS integration, supports External Tables

---

## Repository Structure

```
Big-Data-CW2/
├── README.md                 # This file
├── data/
│   ├── air_quality_cleaned.csv    # Cleaned air quality dataset
│   └── mortality_by_country.csv   # Cleaned mortality dataset
├── scripts/
│   ├── hdfs_commands.sh      # HDFS setup and upload commands
│   └── hive_queries.sql      # All Hive SQL queries
└── docs/
    └── architecture.md       # Technical architecture details
```

---

## Data Sources

### Dataset 1: Global Air Pollution

- **Source:** [Kaggle - Global Air Pollution Dataset](https://www.kaggle.com/datasets/hasibalmuzdadid/global-air-pollution-dataset)
- **Records:** 23,463 cities
- **Countries:** 176
- **Columns:** Country, City, AQI Value, AQI Category, CO, Ozone, NO2, PM2.5

### Dataset 2: Mortality Rate

- **Source:** [World Bank - Mortality Rate Attributed to Air Pollution](https://data.worldbank.org/indicator/SH.STA.AIRP.P5)
- **Records:** 231 countries
- **Year:** 2019
- **Metric:** Deaths per 100,000 population

### Data Preparation

1. Removed metadata rows from World Bank CSV
2. Extracted 2019 mortality column only
3. Standardized country names for matching:
   - "United States of America" → "United States"
   - "Egypt" → "Egypt, Arab Rep."
   - "Turkey" → "Turkiye"

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   SOURCE    │     │   STORAGE   │     │  PROCESSING │     │  VISUALIZE  │
│  CSV Files  │ ──► │    HDFS     │ ──► │ Apache Hive │ ──► │  Zeppelin   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

### HDFS Structure

```
/user/airpollution/
└── data/
    ├── air_quality/
    │   └── air_quality_cleaned.csv    (1.5 MB)
    └── mortality/
        └── mortality_by_country.csv   (6 KB)
```

### Hive Tables

| Table Name | Type | Description | Records |
|------------|------|-------------|---------|
| air_quality | EXTERNAL | City-level pollution data | 23,463 |
| mortality | EXTERNAL | Country mortality rates | 231 |
| country_avg_aqi | MANAGED | Aggregated by country | 176 |
| pollution_mortality_analysis | MANAGED | Joined analysis table | 168 |

---

## Quick Start

### Prerequisites

- Hortonworks HDP Sandbox (or equivalent Hadoop distribution)
- SSH access to sandbox
- Services running: HDFS, Hive, Zeppelin

### Step 1: Clone Repository

```bash
git clone https://github.com/abdennabi-ahrrabi/Big-Data-CW2.git
cd Big-Data-CW2
```

### Step 2: Upload Data to HDFS

```bash
# SSH into sandbox
ssh root@sandbox-hdp.hortonworks.com -p 2222

# Create directories
hdfs dfs -mkdir -p /user/airpollution/data/air_quality
hdfs dfs -mkdir -p /user/airpollution/data/mortality

# Download data files
curl -L -o air_quality_cleaned.csv "https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/data/air_quality_cleaned.csv"
curl -L -o mortality_by_country.csv "https://raw.githubusercontent.com/abdennabi-ahrrabi/Big-Data-CW2/main/data/mortality_by_country.csv"

# Upload to HDFS
hdfs dfs -put air_quality_cleaned.csv /user/airpollution/data/air_quality/
hdfs dfs -put mortality_by_country.csv /user/airpollution/data/mortality/
```

### Step 3: Create Hive Tables

```bash
hive -f scripts/hive_queries.sql
```

Or run queries manually in Hive CLI.

### Step 4: Open Zeppelin

Navigate to `http://sandbox-hdp.hortonworks.com:9995` and create visualizations.

---

## Sample Results

### Top 10 Most Polluted Countries (by PM2.5)

| Rank | Country | Avg PM2.5 | Mortality Rate |
|------|---------|-----------|----------------|
| 1 | Bahrain | 188.0 | 43.1 |
| 2 | Mauritania | 179.0 | 183.8 |
| 3 | Pakistan | 173.1 | 128.8 |
| 4 | Kuwait | 162.0 | 62.2 |
| 5 | United Arab Emirates | 152.7 | 70.0 |
| 6 | Senegal | 152.4 | 206.4 |
| 7 | India | 149.5 | 98.4 |
| 8 | Saudi Arabia | 149.3 | 75.7 |
| 9 | Nepal | 148.5 | 133.3 |
| 10 | Egypt | 143.2 | 77.7 |

### Top 10 Countries by Mortality Rate

| Rank | Country | Mortality Rate | Avg PM2.5 |
|------|---------|----------------|-----------|
| 1 | Central African Republic | 305.1 | 64.2 |
| 2 | Lesotho | 288.3 | 89.6 |
| 3 | Solomon Islands | 281.2 | 18.0 |
| 4 | Afghanistan | 265.7 | 96.0 |
| 5 | Vanuatu | 259.9 | 30.0 |
| 6 | Sierra Leone | 239.0 | 26.0 |
| 7 | Guinea | 238.0 | 141.3 |
| 8 | Eritrea | 237.4 | 111.0 |

### Key Insight

Countries with highest outdoor PM2.5 ≠ Countries with highest mortality. This suggests **indoor air pollution** (from cooking fuels) is a major factor in developing nations.

---

## References

1. H. Muzdadid, "Global Air Pollution Dataset," Kaggle, 2023. [Online]. Available: https://www.kaggle.com/datasets/hasibalmuzdadid/global-air-pollution-dataset

2. World Bank, "Mortality rate attributed to household and ambient air pollution," 2019. [Online]. Available: https://data.worldbank.org/indicator/SH.STA.AIRP.P5

3. Apache Software Foundation, "Apache Hive Documentation," 2023. [Online]. Available: https://hive.apache.org/

4. Apache Software Foundation, "Apache Zeppelin Documentation," 2023. [Online]. Available: https://zeppelin.apache.org/

5. World Health Organization, "WHO Global Air Quality Guidelines," 2021. [Online]. Available: https://www.who.int/publications/i/item/9789240034228

---

## Author

**Abdennabi Ahrrabi**  
MSc Computer Science  
Ulster University London Campus  
December 2025

---

## License

This project is for educational purposes as part of COM745 Big Data & Infrastructure coursework.
