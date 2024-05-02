from pyspark.sql import SparkSession

# --------------------------------------------------------------
# Example 8:  Creating DataFrame From Spark Table
# --------------------------------------------------------------


spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("pyspark-app") \
    .enableHiveSupport() \
    .getOrCreate()

# Read
DF=spark.sql("SELECT * FROM AIRLINE_DB.flight_data_tbl")

# Transformations
# ...

# Write
DF.show(truncate=False)

spark.stop()