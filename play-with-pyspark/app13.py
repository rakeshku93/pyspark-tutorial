from pyspark.sql import *
from pyspark.sql import functions as f


# --------------------------------------------------------------
# Example 13:  Working with Aggregate Functions
# --------------------------------------------------------------

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


# --------------------------------------------------------------

# invoice_df \
# .select(f.count("*").alias("Count *"),
#         f.sum("Quantity").alias("Total Quantity"),
#         f.avg("UnitPrice").alias("Avg Price"),
#         f.countDistinct("InvoiceNo").alias("Count Distinct InvoiceNo"),
#         ) \
# .show(truncate=False)

# --------------------------------------------------------------

# invoice_df \
# .selectExpr("count(*) as `Count *`",
#             "sum(Quantity) as `Total Quantity`",
#             "avg(UnitPrice) as `Avg Price`",
#             "count(distinct InvoiceNo) as `Count Distinct InvoiceNo`") \
# .show(truncate=False)

# --------------------------------------------------------------


# invoice_df.createOrReplaceTempView("sales")


# summary_sql = spark.sql("""
#     SELECT Country, InvoiceNo,
#         sum(Quantity) as TotalQuantity,
#         round(sum(Quantity*UnitPrice),2) as InvoiceValue
#     FROM sales
#     GROUP BY Country, InvoiceNo""")

# summary_sql.show()


# --------------------------------------------------------------


summary_df = invoice_df \
.groupBy("Country", "InvoiceNo") \
.agg(f.sum("Quantity").alias("TotalQuantity"),
        f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
        f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
        )

summary_df.show()

spark.stop()

# --------------------------------------------------------------
