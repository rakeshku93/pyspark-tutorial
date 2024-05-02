from pyspark.sql import *
from pyspark.sql import functions as f


#--------------------------------------------------------------
# Example 14:  Working with Grouping Functions
#--------------------------------------------------------------


spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("LogFileDemo") \
    .getOrCreate()


invoice_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("source/invoices.csv")

invoice_df.printSchema()


NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
InvoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")


exSummary_df=invoice_df \
.withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
.where("year(InvoiceDate) == 2010") \
.withColumn("WeekNumber", f.weekofyear("InvoiceDate")) \
.groupBy("Country","WeekNumber") \
.agg(NumInvoices, TotalQuantity, InvoiceValue) \
.sort(f.col("WeekNumber").desc())


exSummary_df.show(100, truncate=False)

exSummary_df.coalesce(1) \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .save("sink/summary")

spark.stop()