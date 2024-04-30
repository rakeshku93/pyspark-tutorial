from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import spark_partition_id



'''
Example : Working with DataFrameWriter i.e sink
'''


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("pyspark-app") \
        .getOrCreate()
    

    # Read 
    flightTimeParquetDF=spark.read \
        .format("parquet") \
        .option("mode", "FAILFAST") \
        .load("source/flight-time.parquet")
    
    print("Num of partitions before re-parttiion: ", flightTimeParquetDF.rdd.getNumPartitions())
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()


    # Repartition
    flightTimeParquetDF = flightTimeParquetDF.repartition(2)

    print("Num of partitions after re-parttiion: ", flightTimeParquetDF.rdd.getNumPartitions())
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()
  