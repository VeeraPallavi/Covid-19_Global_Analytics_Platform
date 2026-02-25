#Task 6: Global Time-Series Analysis
#Using day_wise.csv:
#1.Global daily average new cases.
#2.Detect spike days using Z-score.
#3.Identify peak death date globally.
#4.Month-over-Month death growth rate.


from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *


spark = SparkSession.builder \
        .appName("Global Time-Series Analysis") \
        .master("yarn") \
        .getOrCreate()

spark.sparkContext.setLogLevel("Error")


day_wise_df = spark.read.parquet("hdfs:///data/covid/staging/day_wise.parquet")


#1.Global daily average new cases.
window_spec = Window.orderBy("Date").rowsBetween(Window.unboundedPreceding,0)

daily_average = day_wise_df.withColumn(
    "Daily_Average",
    round(avg("New Cases").over(window_spec),2)
)

daily_average.write.mode("overwrite").parquet("hdfs:///data/covid/aalytics/daily_average.parquet")

daily_average.select("Date","New Cases","Daily_Average").show()


#2.Detect spike days using Z-score.
global_df = day_wise_df.groupBy("Date").agg(
    sum("New Cases").alias("Global_NewCases")
)

stats = global_df.agg(
    avg("Global_NewCases").alias("Mean"),
    stddev("Global_NewCases").alias("Std_dev")
)
mean = stats.collect()[0]["Mean"]
std_dev = stats.collect()[0]["Std_dev"]

z_score = global_df.withColumn(
    "Z-Score",
    round((col("Global_NewCases")-mean)/std_dev,2)
)

spike_days = z_score.filter(
    abs(col("Z-Score"))>2).select(
        "Date",
        "Global_NewCases",
        "Z-Score"
    )

spike_days.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/spike_days.parquet")

spike_days.show()


#3.Identify peak death date globally.
global_death = day_wise_df.groupby("Date").agg(
    sum("New Deaths").alias("Global_NewDeaths")
)

peak_death = global_death.orderBy(col("Global_NewDeaths").desc())

peak_death.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/peak_deah.parquet")

peak_death.show(1)


#4.Month-over-Month death growth rate.
day_df = day_wise_df.withColumn("Month",month("Date")).withColumn("Year",year("Date"))

monthly_deaths = day_df.groupby("Month","year").agg(
    sum("New Deaths").alias("Monthly_deaths")
).orderBy("Month","year")

window_spec = Window.orderBy("Month","year")

death_rate = monthly_deaths.withColumn(
    "Previous_Month_Death",
    lag("Monthly_deaths").over(window_spec)
)

death_growth_rate = death_rate.withColumn(
    "death_growth_rate",
    round(((col("Monthly_deaths")-col("Previous_Month_Death"))/col("Previous_Month_Death"))*100,2)
)

death_growth_rate.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/death_growth_rate.parquet")

death_growth_rate.show()


spark.stop()





