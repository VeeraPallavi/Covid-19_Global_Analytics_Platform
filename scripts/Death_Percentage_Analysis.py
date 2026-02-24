#Task 3: Death Percentage Analysis
#Using full_grouped.csv:
#1.Compute daily death percentage per country:
 #Deaths / Confirmed * 100
#2.Compute global daily death percentage.
#3.Compute continent-wise death percentage (join with worldometer_data).
#4.Identify:
    #Country with highest death percentage
    #Top 10 countries by deaths per capita
#All results must be written to HDFS under /data/covid/analytics.


from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col,sum,round,max,desc


spark = SparkSession.builder \
        .appName("Covid Death Pecentage Analysis")\
        .getOrCreate()

full_grouped_df = spark.read.parquet("hdfs:///data/covid/staging/full_grouped.parquet")
worldometer_data_df = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data.parquet")

# 1. Daily death percentage per country

country_daily_death_percentage = full_grouped_df.withColumn(
    "death_percentage",
    when(col("Confirmed") > 0,(col("Deaths") / col("Confirmed")) * 100)
    .otherwise(0)
)

#Save to HDFS 
country_daily_death_percentage.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/country_daily_dath_percentage.parquet")

country_daily_death_percentage.select("Date","Country/Region","Confirmed","Deaths","death_percentage").show()


#2.Compute global daily death percentage
global_daily_death_percentage = full_grouped_df.groupby("Date").agg(
    sum("Deaths").alias("total_deaths"),
    sum("Confirmed").alias("total_confirmed")
).withColumn(
    "global_death_percentage",
    when (col("total_confirmed") > 0, round((col("total_deaths")/col("total_confirmed")*100),2))
    .otherwise(0)
)

global_daily_death_percentage.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/global_daily_death_percenatge.parquet")

global_daily_death_percentage.select("Date","global_death_percentage").show()


#3.Compute continent-wise death percentage (join with worldometer_data).
continent_df = full_grouped_df.join(
    worldometer_data_df,
    on = "Country/Region",
    how = "left"
)
continent_death_percentage = continent_df.groupby("Continent","Date").agg(
    sum("Deaths").alias("Total_Deaths"),
    sum("Confirmed").alias("Total_Confirmed")
).withColumn(
    "Continent_death_percentage",
    when(col("Total_Confirmed")>0 , round((col("Total_Deaths")/col("Total_Confirmed"))*100,2)).
        otherwise(0)
)

continent_death_percentage.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/continent_death_percentage.parquet")

continent_death_percentage.show()


#Country with highest death percentage
country_highest_death_percentage = (
    country_daily_death_percentage
        .groupby("Country/Region")
        .agg(max("death_percentage").alias("max_death_percentage"))
        .orderBy("max_death_percentage", ascending = False)
        .limit(1)
).show()


#Top 10 countries by deaths per capita
deaths_per_capita = continent_df.groupBy("Country/Region", "Population").agg(
    max("Deaths").alias("total_deaths")
).withColumn(
    "deaths_per_capita",
    col("total_deaths") / col("Population")
).orderBy(desc("deaths_per_capita")).limit(10).show()






