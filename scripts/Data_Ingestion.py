#Task 2: Data Ingestion & Optimization
#1.Read all raw CSV files from HDFS.
#2.Apply proper schema instead of inferSchema.
#3.Handle null values.
#4.Convert raw CSV files into Parquet format.
#5.Store them in /data/covid/staging.
#6.Compare CSV vs Parquet:
    #File size
    #Read performance
    #Execution plan
#Explain why Parquet performs better.


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType,LongType,DateType


spark = SparkSession.builder \
        .appName("COVID ANALYTICS") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Apply proper schema instead of inferSchema.
country_wise_schema = StructType ([
    StructField("Country/Regoion",StringType(),True),
    StructField("Confirmed",LongType(),True),
    StructField("Deaths",LongType(),True),
    StructField("Recovered", LongType(),True),
    StructField("Active",LongType(),True),
    StructField("New cases",LongType(),True),
    StructField("New deaths",LongType(),True),
    StructField("New recovred",LongType(),True),
    StructField("Deaths/100 Cases",DoubleType(),True),
    StructField("Recovered/100 Cases",DoubleType(),True),
    StructField("Deaths/100 Recovered",DoubleType(),True),
    StructField("Confirmed last week",LongType(),True),
    StructField("1 week change",LongType(),True),
    StructField("1 week % increase",DoubleType(),True),
    StructField("WHO Region",StringType(),True)])

#Read all raw CSV files from HDFS.
country_wise_df = spark.read.csv(
    "hdfs:///data/covid/raw/country_wise_latest.csv",
    header=True,
    schema=country_wise_schema
)

#Handle null values.
numerical_cols = ["Confirmed","Deaths","Recovered","Active","New cases","New deaths","New recovred","Deaths/100 Cases","Recovered/100 Cases","Deaths/100 Recovered","Confirmed last week","1 week change","1 week % increase"]

country_wise_df = country_wise_df.fillna(0,subset=numerical_cols)
country_wise_df = country_wise_df.fillna("Unknown")

#Store them in /data/covid/staging.
hdfs_path = "hdfs:///data/covid/staging/country_wise_latest.parquet"

#Convert raw CSV files into Parquet format.
country_wise_df.write.mode("overwrite").parquet(hdfs_path)

country_wise_df.show(5)



covid_19_clean_complete_schema = StructType([
    StructField("Province/State",StringType(),True),
    StructField("Country/Region",StringType(),True),
    StructField("Lat",DoubleType(),True),
    StructField("Long",DoubleType(),True),
    StructField("Date",DateType(),True),
    StructField("Confirmed",IntegerType(),True),
    StructField("Deaths",IntegerType(),True),
    StructField("Recovered",IntegerType(),True),
    StructField("Active",IntegerType(),True),
    StructField("WHO Region",StringType(),True)
])

covid_19_clean_complete_df = spark.read.csv( 
            "hdfs:///data/covid/raw/covid_19_clean_complete.csv" ,
            header = True ,
            schema = covid_19_clean_complete_schema)

covid_19_clean_complete_df.show(5)

numerical_cols = ["Lat","Long","Confirmed","Deaths","Recovered","Active"]

covid_19_clean_complete_df = covid_19_clean_complete_df.fillna(0,subset = numerical_cols)
covid_19_clean_complete_df = covid_19_clean_complete_df.fillna("Unknown")

hdfs_path = "hdfs:///data/covid/staging/covid_19_clean_complete.parquet"
covid_19_clean_complete_df.write.mode("overwrite").parquet(hdfs_path)

covid_19_clean_complete_df.show(5)



day_wise_schema = StructType([
    StructField("Date",DateType(),True),
    StructField("Confirmed",LongType(),True),
    StructField("Deaths",LongType(),True),
    StructField("Recovered",LongType(),True),
    StructField("Active",LongType(),True),
    StructField("New cases",LongType(),True),
    StructField("New deaths",LongType(),True),
    StructField("New recovered",LongType(),True),
    StructField("Deaths/100 Cases",DoubleType(),True),
    StructField("Recovered/100 Cases",DoubleType(),True),
    StructField("Deaths/100 Recovered",DoubleType(),True),
    StructField("No. of countries",LongType(),True)
])

day_wise_df = spark.read.csv(
    "hdfs:///data/covid/raw/day_wise.csv",
    header = True,
    schema = day_wise_schema
)

String_cols = ["Date"]

day_wise_df = day_wise_df.fillna("Unknown",subset = String_cols)
day_wise_df = day_wise_df.fillna(0)

hdfs_path = "hdfs:///data/covid/staging/day_wise.parquet"
day_wise_df.write.mode("overwrite").parquet(hdfs_path)

day_wise_df.show(5)



full_grouped_schema = StructType([
    StructField("Date",DateType(),True),
    StructField("Country/Region",StringType(),True),
    StructField("Confirmed",IntegerType(),True),
    StructField("Deaths",IntegerType(),True),
    StructField("Recovered",IntegerType(),True),
    StructField("Active",IntegerType(),True),
    StructField("New cases",IntegerType(),True),
    StructField("New deaths",IntegerType(),True),
    StructField("New recovered",IntegerType(),True),
    StructField("WHO Region",StringType(),True)
])

full_grouped_df = spark.read.csv(
    "hdfs:///data/covid/raw/full_grouped.csv",
    header = True,
    schema = full_grouped_schema
)

numerical_cols = ["Confirmed","Deaths","Recovered","Active","New cases","New deaths","New recovered"]

full_grouped_df = full_grouped_df.fillna(0,subset = numerical_cols)
full_grouped_df = full_grouped_df.fillna("Unknown")

hdfs_path = "hdfs:///data/covid/staging/full_grouped.parquet"
full_grouped_df.write.mode("overwrite").parquet(hdfs_path)

full_grouped_df.show(5)



usa_country_wise_schema = StructType([
    StructField("UID",LongType(),True),
    StructField("iso2",StringType(),True),
    StructField("iso3",StringType(),True),
    StructField("code3",IntegerType(),True),
    StructField("FIPS",IntegerType(),True),
    StructField("Admin2",StringType(),True),
    StructField("Province_State",StringType(),True),
    StructField("Country_Region",StringType(),True),
    StructField("Lat",DoubleType(),True),
    StructField("Long_",DoubleType(),True),
    StructField("Combined_Key",StringType(),True),
    StructField("Date",DateType(),True),
    StructField("Confirmed",IntegerType(),True),
    StructField("Deaths",IntegerType(),True)
])

usa_country_wise_df = spark.read.csv(
    "hdfs:///data/covid/raw/usa_county_wise.csv",
    header = True,
    schema = usa_country_wise_schema
)

numerical_cols = ["UID","code3","FIPS","Lat","Long_","Confirmed","Deaths"]

usa_country_wise_df = usa_country_wise_df.fillna(0,numerical_cols)
usa_country_wise_df = usa_country_wise_df.fillna("Unknown")

hdfs_path = "hdfs:///data/covid/staging/usa_country_wise.parquet"
usa_country_wise_df.write.mode("overwrite").parquet(hdfs_path)

usa_country_wise_df.show(5)



worldometer_data_schema = StructType([
    StructField("Country/Region",StringType(),True),
    StructField("Continent",StringType(),True),
    StructField("Population",LongType(),True),
    StructField("TotalCases",LongType(),True),
    StructField("NewCases",LongType(),True),
    StructField("TotalDeaths",LongType(),True),
    StructField("NewDeaths",LongType(),True),
    StructField("TotalRecovered",LongType(),True),
    StructField("NewRecovered",LongType(),True),
    StructField("ActiveCases",LongType(),True),
    StructField("Serious,Critical",LongType(),True),
    StructField("Tot Cases/1M pop",LongType(),True),
    StructField("Deaths/1M pop",LongType(),True),
    StructField("TotalTests",LongType(),True),
    StructField("Tests/1M pop",LongType(),True),
    StructField("WHO Region",StringType(),True)
])

worldometer_data_df = spark.read.csv(
    "hdfs:///data/covid/raw/worldometer_data.csv",
    header = True,
    schema = worldometer_data_schema
)

String_cols = ["Country/Region","Continent","WHO Region"]

worldometer_data_df = worldometer_data_df.fillna("Unknown",String_cols)
worldometer_data_df = worldometer_data_df.fillna(0)

hdfs_path = "hdfs:///data/covid/staging/worldometer_data.parquet"
worldometer_data_df.write.mode("overwrite").parquet(hdfs_path)

worldometer_data_df.show(5)
