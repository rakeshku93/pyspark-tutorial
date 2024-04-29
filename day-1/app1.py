
from pyspark.sql import SparkSession
from pyspark import SparkConf

"""
first spark application
"""

if __name__ == "__main__":

    conf=SparkConf()
    conf.setAppName("app1")
    conf.setMaster("local[3]")
    
    spark=SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    
    # Print spark version
    print("Spark Version: ", spark.version)

    # Read data from source
    survey_df=spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("../source/sample_input.csv") 
    
    # Print schema
    # survey_df.printSchema()

    # Process data
    country_count_df=survey_df \
    .filter("Age<40") \
    .select("Age","Country") \
    .groupBy("Country") \
    .count() 

    # Write result
    country_count_df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", True) \
    .save("../sink/age_less_than_40_by_country")

    # Stop spark session
    spark.stop()