# COVID-19 Global Analytics Platform using Hadoop & Spark

## Project Overview

This project builds a distributed COVID-19 analytics platform using Hadoop (HDFS + YARN) and Apache Spark in pseudo-distributed mode.

It demonstrates:
- HDFS-based distributed storage
- Spark DataFrame and RDD processing
- Spark SQL analytics
- Parquet optimization
- Window function analysis
- Data skew detection and handling
- Shuffle optimization
- Execution plan interpretation
- Resource and memory planning

Dataset:
https://www.kaggle.com/datasets/imdevskp/corona-virus-report

---

## Technology Stack

- Hadoop (HDFS + YARN)
- Apache Spark (PySpark)
- Spark SQL
- RDD API
- Parquet File Format
- Python

---

## HDFS Directory Structure

/data/covid/raw  
/data/covid/staging  
/data/covid/curated  
/data/covid/analytics  

All data is read from and written to HDFS only.

---

## Key Implementations

### 1. Data Ingestion & Optimization
- Applied explicit schema (no inferSchema)
- Handled null values
- Converted CSV to Parquet
- Compared CSV vs Parquet performance

### 2. Death Percentage Analysis
- Daily death percentage per country
- Global daily death percentage
- Continent-wise death percentage
- Top countries by deaths per capita

### 3. Infection Rate Analysis
- Confirmed cases per 1000 population
- Active cases per 1000 population
- WHO region infection ranking
- Top 10 infection countries

### 4. Recovery Efficiency
- Recovery percentage per country
- 7-day rolling recovery average (Window function)
- Fastest recovery growth
- Peak recovery day per country

### 5. Global Time-Series Analysis
- Global daily average new cases
- Spike detection using Z-score
- Peak global death date
- Month-over-Month growth rate

### 6. USA Drilldown Analysis
- County-to-state aggregation
- Top 10 affected states
- Data skew detection
- Skew impact explanation in distributed systems

### 7. RDD-Based Implementation
- Total confirmed per country
- Total deaths per country
- Death percentage using reduceByKey
- RDD vs DataFrame performance comparison

### 8. Spark SQL Implementation
- Created temporary views
- SQL-based analytics queries
- Physical plan comparison with DataFrame API

---

## Performance Optimization

### Partition Strategy
- Repartitioned by Date
- Repartitioned by Country/Region
- Compared repartition() vs coalesce()

### Data Skew Handling
- Identified skewed countries (e.g., USA)
- Implemented salting technique
- Enabled Adaptive Query Execution (AQE)

### Broadcast Join
- Used broadcast join for worldometer_data
- Verified BroadcastHashJoin using explain()

### Shuffle Optimization
- Tuned spark.sql.shuffle.partitions
- Combined filters before joins
- Avoided unnecessary wide transformations

### Caching Strategy
- Used persist(StorageLevel.MEMORY_AND_DISK)
- Evaluated when caching degrades performance

---

## Execution Mode

All Spark jobs are executed using:

spark-submit --master yarn

Hadoop runs in pseudo-distributed mode with HDFS and YARN enabled.
