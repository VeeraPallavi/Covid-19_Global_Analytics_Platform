#Task 7: USA Drilldown Analysis
#Using usa_county_wise.csv:
#1.Aggregate county data to state level.
#2.Identify top 10 affected states.
#3.Detect data skew across states.
#4.Explain skew impact in distributed systems.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
        .appName("USA Drilldown Analysis") \
        .master("yarn") \
        .getOrCreate()

spark.sparkContext.setLogLevel("Error")


usa_country_wise_df = spark.read.parquet("hdfs:///data/covid/staging/usa_country_wise.parquet")


#1.Aggregate county data to state level.
aggregate_county_data = usa_country_wise_df.groupby("Province_State","Country_Region").agg(
    avg("Lat").alias("Avg_Lat"),
    avg("Long_").alias("Avg_Long_"),
    count("iso2").alias("No.of.Countries"),
    sum("Confirmed").alias("Total affected"),
    sum("Deaths").alias("Total Deaths")
)

aggregate_county_data.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/aggregate_country_data")

aggregate_county_data.show()


#2.Identify top 10 affected states.
top_10_affected_states = aggregate_county_data.select(
    "Province_State",
    "Country_Region",
    "Total affected"
).orderBy(col("Total affected").desc()).limit(10)

top_10_affected_states.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/top_10_affected_states")

top_10_affected_states.show()


#3.Detect data skew across states.
states_df = usa_country_wise_df.groupby("Province_State").agg(
    count("*").alias("Total Records")).orderBy(col("Total Records").desc())

stats = states_df.agg(
    avg("Total Records").alias("Mean"),
    stddev("Total Records").alias("Std_dev")
)

mean = stats.collect()[0]["Mean"]
std_dev = stats.collect()[0]["Std_dev"]

data_screw = states_df.withColumn(
    "Z-Score",
    round((col("Total Records")-mean)/std_dev,2)
).filter(col("Z-Score")>2)

data_screw.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/data_screw.parquet")

data_screw.show()


#4.Explain skew impact in distributed systems.

#Data skew in distributed systems occurs when data is unevenly distributed across nodes, causing workload imbalance.
#This leads to straggler tasks, poor parallelism, increased execution time, memory pressure, and inefficient resource utilization.
