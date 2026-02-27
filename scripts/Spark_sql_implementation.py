#Task 9: Spark SQL Implementation
#1.Create temporary views.
#2.Write SQL queries for:
#Top 10 infection countries
#Death percentage ranking
#Rolling 7-day average
#3.Compare physical plans with DataFrame API.


from pyspark.sql import SparkSession


spark = SparkSession.builder \
        .appName("Spark SQL Implementation") \
        .master("yarn") \
        .getOrCreate()

spark.sparkContext.setLogLevel("Error")

df = spark.read.parquet("hdfs:///data/covid/staging/full_grouped.parquet")


#1.Create temporary views.
df.createOrReplaceTempView("covid_data")


#2.Top 10 infection countries

top10 = spark.sql("""
    SELECT `Country/Region`,SUM(`Confirmed`) AS `Total_Confimed`
    FROM covid_data
    GROUP BY `Country/Region`
    ORDER BY SUM(`Confirmed`) DESC
    LIMIT 10
    """)

top10.show()


#2.Death percentage ranking
death_percentage = spark.sql("""
    SELECT `Country/Region`,
    (SUM(`Deaths`)/SUM(`Confirmed`))*100 AS `Death_Percntage`
    FROM covid_data
    GROUP BY `Country/Region`
    ORDER BY (SUM(`Deaths`)/SUM(`Confirmed`))*100 DESC
    """)
death_percentage.show()


#2.Rolling 7-day average
rolling = spark.sql("""
    SELECT Date,
    round(AVG(`New cases`) OVER (
        PARTITION BY `Country/Region`
        ORDER BY Date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_avg
    FROM covid_data
    ORDER BY `Country/Region`, Date
    """)
rolling.show()


#3.Compare physical plans with DataFrame API.
top10.explain()

df.groupBy("Country/Region") \
  .sum("Confirmed") \
  .orderBy("sum(Confirmed)", ascending=False) \
  .limit(10) \
  .explain()


spark.stop()




