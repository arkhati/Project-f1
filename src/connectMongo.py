from pyspark.sql import SparkSession
import pandas as pd
from pymongo import MongoClient
import certifi

# Initialize Spark session
spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

# Sample data
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]

# Create PySpark DataFrame
df = spark.createDataFrame(data, columns)

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df.toPandas()
CONNECTION_STRING = "mongodb+srv://app_f1:FwQIhLJ7yJ3Dr7YP@ankitsclusterfreetier.krk8uv7.mongodb.net/"


# MongoDB connection
client = MongoClient(CONNECTION_STRING, tlsCAFile=certifi.where())
db = client["mongodbVSCodePlaygroundDB"]
collection = db["sample_age"]

# Insert data into MongoDB
collection.insert_many(pandas_df.to_dict("records"))

print("Data inserted successfully into MongoDB!")