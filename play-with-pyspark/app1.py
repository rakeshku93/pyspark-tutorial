
# import pandas as pd

# survey_df=pd.read_csv("./source/survey_input.csv")
# survey_count_by_country_df=survey_df[survey_df["Age"]<40] \
#     .groupby("Country") \
#     .size() \
#     .reset_index(name="Count")

# print(survey_count_by_country_df)


# --------------------------------------------

from pyspark.sql import SparkSession
from pyspark import SparkConf


conf = SparkConf() \
    .setAppName("app1") \
    .setMaster("local[3]") # 3 cores , 1 for driver & 2 for executors
    
spark = SparkSession \
    .builder \
    .config(conf=conf)\
    .getOrCreate()

# Load the data ( from any source & any format of data )

survey_df=spark.read \
.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("./source/survey_input.csv") 

# survey_df.printSchema()
# survey_df.show()

# get number of partiotions
# print(survey_df.rdd.getNumPartitions())

# repartition the data
survey_df=survey_df.repartition(2)

# get number of partiotions
# print(survey_df.rdd.getNumPartitions())

# record count per partition
# print(survey_df.rdd.glom().map(len).collect())


# Transformation

# in 2 ways we can write the transformation
# 1. DataFrame API
# 2. SQL

# DataFrame API

result_df=survey_df \
    .select("Age","Country") \
    .where("Age < 40") \
    .groupBy("Country") \
    .count() 

# Write the output to the console | language-list | file | db | any other sink

# result_df.show()

python_list=result_df.collect()
# for row in python_list:
#     print(row["Country"], row["count"])

# result_df.write \
#     .format("csv") \
#     .mode("overwrite") \
#     .option("header", "true") \
#     .save("./output/survey_output")


input("Press Enter to continue...")


spark.stop()