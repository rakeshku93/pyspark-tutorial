from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import spark_partition_id
from pyspark import SparkConf


'''
Example : Working with DataFrameWriter i.e sink  ( MySQL )
'''


if __name__ == "__main__":

    conf=SparkConf()
    conf.setAppName("pyspark-app")
    conf.setMaster("local[3]")
    
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0") \
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


    # Transformation
    df=flightTimeParquetDF.select("ORIGIN").distinct()
    df.show(5)



    # Write ( file | json | jdbc | orc | parquet | csv | text )
    df=df.coalesce(1)

    df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/flightdb") \
        .option("dbtable", "flights") \
        .option("user", "root") \
        .option("password", "root1234") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .save()

    spark.stop()