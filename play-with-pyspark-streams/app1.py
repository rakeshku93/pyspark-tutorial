
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# ---------------------------------------------------
# Example 1: Spark Streaming Application:  Word Count
# ---------------------------------------------------


spark = SparkSession \
        .builder \
        .appName("demo1") \
        .master("local[3]") \
        .getOrCreate()

# Read
lines_df=spark.readStream \
.format("socket") \
.option("host", "localhost") \
.option("port", 9999) \
.load() 


lines_df.printSchema()


# Transform
words_df=lines_df.select(explode(split(lines_df.value, " ")).alias("word"))
word_count_df=words_df.groupBy("word").count()


# Write
query=word_count_df.writeStream \
.outputMode("append") \
.format("console") \
.option("checkpointLocation", "chk-point-dir") \
.start()


query.awaitTermination() # Wait for the termination signal