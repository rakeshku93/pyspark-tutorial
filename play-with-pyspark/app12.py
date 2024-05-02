from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


# --------------------------------------------------------------
# Example 12:  Working with DataFrame * PySpark Functions
# --------------------------------------------------------------


   
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("LogFileDemo") \
    .getOrCreate()

data_list = [
("Ravi", "28", "1", "2002"),
("Abdul", "23", "5", "81"),  # 1981
("John", "12", "12", "6"),  # 2006
("Rosy", "7", "8", "63"),  # 1963
("Abdul", "23", "5", "81") # 1981
]  
raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
raw_df.printSchema()

final_df=raw_df \
    .withColumn("day", col("day").cast(IntegerType())) \
    .withColumn("month", col("month").cast(IntegerType())) \
    .withColumn("year", col("year").cast(IntegerType())) \
    .withColumn("year", when(col("year") < 21, col("year") + 2000)\
                .when(col("year") < 100, col("year") + 1900)\
                .otherwise(col("year"))) \
    .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year),'d/M/y')")) \
    .drop("day", "month", "year") \
    .dropDuplicates(["name","dob"]) \
    .sort(col("dob").desc())

final_df.show(10, False)
    


spark.stop()