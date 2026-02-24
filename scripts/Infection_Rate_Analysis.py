#Task 4: Infection Rate Analysis
#Using worldometer_data:
#1.Confirmed cases per 1000 population.
#2.Active cases per 1000 population.
#3.Top 10 countries by infection rate.
#4.WHO region infection ranking.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round,desc,sum

spark = SparkSession.builder \
        .appName("Infection Rate Analysis") \
        .getOrCreate()
spark.sparkContext.setLogLevel("Error")


worldometer_data_df = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data.parquet")
covid_df = spark.read.parquet("hdfs:///data/covid/staging/covid_19_clean_complete.parquet")


#1.Confirmed cases per 1000 population.
confirmed_cases_df = worldometer_data_df.withColumn(
    "Confirmed_cases",
    round((col("TotalCases")/col("Population"))*1000,2)
)

confirmed_cases_df.show()

confirmed_cases_df.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/confirmed_cases_df")


#2.Active cases per 1000 population.
active_cases_df = worldometer_data_df.withColumn(
    "Active Cases per 1000",
    round((col("ActiveCases")/col("Population"))*1000,2)
)

active_cases_df.show()

active_cases_df.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/active_cases_df")


#3.Top 10 countries by infection rate.

top_10_infection_rate = confirmed_cases_df.orderBy(desc("Confirmed_cases")).limit(10)

top_10_infection_rate.show()

top_10_infection_rate.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/top_10_infection_rate.parquet")


#4.WHO region infection ranking.
who_making = covid_df.select(
    "Country/Region",
    col("WHO Region").alias("WHO_Region")
).distinct()

joined_df = worldometer_data_df.join(
    who_making,
    on = "Country/Region",
    how = "inner"
)

who_rank = joined_df.groupBy("WHO_Region").agg(
    sum("TotalCases").alias("Total_Cases"),
    sum("Population").alias("Total Population")
).withColumn(
    "Confirmed Cases",
    round((col("Total_Cases")/col("Total Population"))*1000,2)
).orderBy(desc("Confirmed Cases"))

who_rank.show()

who_rank.write.mode("overwrite").parquet("hdfs:///data/clean/analytics/who_rank.parquet")
