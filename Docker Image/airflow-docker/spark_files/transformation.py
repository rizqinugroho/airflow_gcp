#transformation.py
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("My PySpark code") \
    .getOrCreate()

df = spark.read.options(header='true', inferSchema='true').csv("gs://hive-example/final-project/orders.csv")

df.printSchema()
df.write.mode("Overwrite").parquet("gs://hive-example/final-project/raw/orders.parquet")

df.createOrReplaceTempView("sales")
highestPriceUnitDF = spark.sql("select * from sales where UnitPrice >= 3.0")
highestPriceUnitDF.write.mode("Overwrite").parquet("gs://hive-example/final-project/output/highest_prices.parquet")