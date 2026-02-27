#Task 10: Performance Optimization (Mandatory)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder \
       .appName("Performance Optimization ") \
       .master("yarn") \
       .getOrCreate()

spark.sparkContext.setLogLevel("Error")


worldometer_data_df = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data.parquet")

full_grouped_df = spark.read.parquet("hdfs:///data/covid/staging/full_grouped.parquet")


#1.Partition Strategy
#Repartition data by Date

full_grouped_by_date = full_grouped_df.repartition("Date")

#Repartition data by Country/Region

worldometer_by_country = worldometer_data_df.repartition("Country/Region")

full_grouped_by_date.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/full_grouped_by_date.parquet")

worldometer_by_country.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/worldometer_by_country.parquet")


#2.Explain difference between:
#repartition()
#coalesce()

#repartition() performs a full shuffle to evenly distribute data and can increase or decrease partitions,
#whereas coalesce() reduces partitions without a full shuffle and is more efficient when decreasing partitions.


#2.Data Skew Handling
#1.dentify skewed countries (e.g., USA).
#2.Implement salting technique OR skew join optimization.
country_distribution = worldometer_data_df.groupBy("Country/Region") \
    .count() \
    .orderBy(col("count").desc())

country_distribution.show(10)


# Salting Technique
salted_df = worldometer_data_df.withColumn(
    "salt",
    floor(rand() * 5)
)

# Example salted aggregation
salted_agg = salted_df.groupBy("Country/Region", "salt") \
    .agg(sum("TotalCases").alias("salted_sum")) \
    .groupBy("Country/Region") \
    .agg(sum("salted_sum").alias("final_sum"))



#3. Broadcast Join Optimization
small_df = worldometer_data_df.select("Country/Region", "Population")

joined_df = full_grouped_df.join(
    broadcast(small_df),
    on="Country/Region",
    how="inner"
)

# Verify physical plan
joined_df.explain("formatted")



# 4. Shuffle Optimization
spark.conf.set("spark.sql.shuffle.partitions", 50)

print("Shuffle Partitions:",
      spark.conf.get("spark.sql.shuffle.partitions"))


#2.Avoid unnecessary wide transformations.
country_df = full_grouped_df.select(
    "Country/Region",
    "Date",
    "Confirmed",
    "Deaths",
    "Recovered"
)


# 3.Combine filters BEFORE join (reduces shuffle size)
filtered_full_grouped = country_df.filter(col("Confirmed") > 1000)

optimized_join = filtered_full_grouped.join(
    broadcast(small_df),
    on="Country/Region",
    how="inner"
)

optimized_join.explain("formatted")


#Explain why shuffle is expensive.

#shuffle is expensive because:
#It involves data redistribution across the cluster, 
#requiring network transfer, 
#disk I/O, 
#serialization/deserialization,
#synchronization barriers between stages.


#5. Cache frequently reused DataFrame

worldometer_data_df.persist(StorageLevel.MEMORY_AND_DISK)

worldometer_data_df.count()



#3.Explain when caching degrades performance.

#Caching degrades performance :
#when the dataset is too large for memory 
#when it is used only once
#when it causes memory pressure and excessive garbage collection.

