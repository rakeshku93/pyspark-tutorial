from pyspark.sql import SparkSession


#----------------------------------------------------------
# Example 18 : Bucket Join
#----------------------------------------------------------

spark = SparkSession \
    .builder \
    .appName("Shuffle Join Demo") \
    .enableHiveSupport() \
    .master("local[3]") \
    .getOrCreate()

df1 = spark.read.json("source/d1/")
df2 = spark.read.json("source/d2/")


spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
spark.sql("USE MY_DB")

df1.coalesce(1).write \
    .bucketBy(3, "id") \
    .mode("overwrite") \
    .saveAsTable("MY_DB.flight_data1")

df2.coalesce(1).write \
    .bucketBy(3, "id") \
    .mode("overwrite") \
    .saveAsTable("MY_DB.flight_data2")


df3 = spark.read.table("MY_DB.flight_data1")
df4 = spark.read.table("MY_DB.flight_data2")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
join_expr = df3.id == df4.id
join_df = df3.join(df4, join_expr, "inner")

join_df.collect()
input("press a key to stop...")
