#Task 5: Recovery Efficiency
#1.Recovered percentage per country.
#2.7-day rolling recovery average (Window function).
#3.Country with fastest recovery growth.
#4.Peak recovery day per country.
#Use Spark Window functions.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark = SparkSession.builder \
        .appName("Recovery Efficiency") \
        .master("yarn") \
        .getOrCreate()

spark.sparkContext.setLogLevel("Error")


worldometer_data_df = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data.parquet")


#1.Recovered percentage per country.

recovered_percentage = worldometer_data_df.withColumn(
    "Recovered Percentage",
    round((col("TotalRecovered")/col("TotalCases"))*100,2)
)

recovered_percentage.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/recoverd_percentage.parquet")

recovered_percentage.show(5)

day_wise_df = spark.read.parquet("hdfs:///data/covid/staging/day_wise.parquet")

day_wise_df.show(5)



#2.7-day rolling recovery average (Window function).

window_spec = Window.orderBy("Date").rowsBetween(-6,0)

rolling_average = day_wise_df.withColumn(
    "Rolling Average",
    avg("Recovered").over(window_spec)
)

rolling_average.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/rolling_average.parquet")

rolling_average.show(5)

full_grouped_df = spark.read.parquet("hdfs:///data/covid/staging/full_grouped.parquet")

full_grouped_df.show(5)



#3.Country with fastest recovery growth.
window_spec = Window.partitionBy("Country/Region").orderBy("Date")

growth_df = full_grouped_df.withColumn(
    "Pre_recovered",
    lag("Recovered").over(window_spec)
).withColumn(
    "Recovery_Growth",
    col("Recovered")-col("Pre_recovered")
)

fastest_recovery = growth_df.groupBy("Country/Region").agg(
        max("Recovery_Growth").alias("max_daily_growth")
    ).orderBy(desc("max_daily_growth")).limit(1)

fastest_recovery.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/fastest_recovery.parquet")

fastest_recovery.show()



#4.Peak recovery day per country.

peak_window = Window.partitionBy("Country/Region").orderBy(col("Recovered").desc())

recovery = full_grouped_df.withColumn(
    "rank",
    row_number().over(peak_window)
)

peak_recovery = recovery.filter(
    col("rank") == 1
).select(
    "Country/Region",
    "Date",
    "Recovered"
)

peak_recovery.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/peak_recovery.parquet")
peak_recovery.show()


spark.stop()





