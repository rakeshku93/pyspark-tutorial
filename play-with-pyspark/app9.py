

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import to_date, col


spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("app9") \
    .getOrCreate()

my_schema = StructType([
    StructField("ID", StringType()),
    StructField("EventDate", StringType())])


my_rows = [
            Row("123", "04/05/2020"), 
            Row("124", "4/5/2020"),
            Row("125", "04/5/2020"), 
            Row("126", "4/05/2020")
        ]

my_rdd = spark.sparkContext.parallelize(my_rows, 2)
my_df = spark.createDataFrame(my_rdd, my_schema)


my_df.printSchema()
my_df.show(truncate=False)

new_df = my_df.withColumn("EventDate", to_date(col("EventDate"), "M/d/y"))


new_df.printSchema()
new_df.show(truncate=False)