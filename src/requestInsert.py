from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
import pandas as pd
from pymongo import MongoClient
import certifi

# Initialize Spark session
spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

df = spark.read.option("multiline", "true").json("/Users/ankitkhati/Learning/Projects/PyProjects/Project-Initial/configs/extractConfig.json")
driversDf = df.select(col("drivers.*"))
flattenedDriversDf = driversDf.withColumn("options", explode(col("options"))).alias("options")

# flattenedDriversDf = driversDf.select(
#     col("url"),
#     col("frequency"),
#     col("cdc"),
#     col("type"),
#     explode(col("options")).alias("option")
# )

# Show the contents of the DataFrame
flattenedDriversDf.show(truncate=False)

# Print the schema of the DataFrame to understand its structure
flattenedDriversDf.printSchema()