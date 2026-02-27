#Task 8: RDD-Based Implementation
#Using RDD API:
#1.Calculate total confirmed per country.
#2.Calculate total deaths per country.
#3.Compute death percentage using reduceByKey.
#4.Compare RDD performance vs DataFrame.
#5.Explain:
#Why reduceByKey is preferred over groupByKey
#When RDD should be avoided


from pyspark.sql import SparkSession

spark = SparkSession.builder \
       .appName("RDD Implementation") \
       .master("yarn") \
       .getOrCreate()

spark.sparkContext.setLogLevel("Error")


country_wise_df = spark.read.parquet("hdfs:///data/covid/staging/country_wise_latest.parquet")


#1.Calculate total confirmed per country.

rdd = country_wise_df.select("Country/Regoion","Confirmed","Deaths").rdd.map(lambda r : (r["Country/Regoion"],(int(r["Confirmed"]),int(r["Deaths"]))))

total_confirmed = rdd.map(lambda x :(x[0],x[1][0])).reduceByKey(lambda a,b : a+b)

total_confirmed_df = total_confirmed.toDF(["Country/Region","Total_Confirmed"])

total_confirmed_df.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/total_confirmed_rdd.parquet")

total_confirmed_df.show(5)


#2.Calculate total deaths per country.

total_deaths = rdd.map(lambda x : (x[0],x[1][1])).reduceByKey(lambda a, b : a + b)

total_deaths_df = total_deaths.toDF(["Country/Region","Total_Deaths"])

total_deaths_df.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/total_deaths_rdd.parquet")

total_deaths_df.show(5)


#3.Compute death percentage using reduceByKey.
rdd_joined = total_confirmed.join(total_deaths)

death_percentage = rdd.map(lambda x : (
    x[0], 
    x[1][0],
    x[1][1],
    round((x[1][1]/x[1][0])*100,2) if x[1][0] > 0 else 0
))

death_percentage_df = death_percentage.toDF(["Country/Region","Total_Confirmed","Total_Deaths","Death_Percentage"])

death_percentage_df.write.mode("overwrite").parquet("hdfs:///data/covid/analytics/death_percentage_rdd.parquet")

death_percentage_df.show(5)

