from pyspark.sql import SparkSession
from pyspark import SparkConf

'''
    Example : Spark Application with Dataframe and SQL
'''

if __name__ == "__main__":

    conf=SparkConf().setAppName("pyspark-app").setMaster("local[3]")

    # Create a SparkSession
    spark = SparkSession \
        .builder\
        .config(conf=conf) \
        .getOrCreate()

    # Read
    survey_df = spark.read.csv(
        "./source/survey_input.csv", 
        header=True, 
        inferSchema=True)
    
    # Repartition
    survey_df = survey_df.repartition(2)

    # Tranformation

    # DataFrame API ( option-1 )

    # country_count_df=survey_df\
    # .filter("Age<40") \
    # .select("Age","Country") \
    # .groupBy("Country") \
    # .count()

    # ANSI SQL ( option-2 )

    # to run SQL queries we need to create a temporary view ( table ) on top of the dataframe
    # this view is temporary and will be available only for the lifetime of the spark session
    

    survey_df.createOrReplaceTempView("survey_tbl")
    
    country_count_df = \
    spark.sql("SELECT Country, count(*) as count FROM survey_tbl WHERE Age < 40 GROUP BY Country")

    # Write
    country_count_df.show()
    # country_count_df.write.csv("output/country_count", mode="overwrite", header=True)

    input("Press Enter to continue...")

    # Stop the SparkSession
    spark.stop()
