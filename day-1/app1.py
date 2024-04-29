
from pyspark.sql import SparkSession

"""
First PySpark Application
"""

if __name__ == "__main__":
    
    spark=SparkSession \
        .builder \
        .appName("app1") \
        .master("local[1]") \
        .getOrCreate()
    
    # Print spark version
    print("Spark Version: ", spark.version)

    spark.stop()