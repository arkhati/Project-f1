from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import col, explode

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Read and Flatten Drivers Section with Schema") \
    .getOrCreate()

# Define the schema for the "options" array
options_schema = ArrayType(
    StructType([
        StructField("driver_number", IntegerType(), True)
    ])
)

# Define the schema for the "drivers" section
drivers_schema = StructType([
    StructField("url", StringType(), True),
    StructField("frequency", StringType(), True),
    StructField("cdc", StringType(), True),
    StructField("type", StringType(), True),
    StructField("options", options_schema, True)
])

# Define the schema for the entire JSON file
schema = StructType([
    StructField("drivers", drivers_schema, True),
    StructField("intervals", StructType([
        StructField("url", StringType(), True),
        StructField("frequency", StringType(), True),
        StructField("cdc", StringType(), True),
        StructField("type", StringType(), True)
    ]), True)
])

# Path to your local multiline JSON file
json_file_path = "/Users/ankitkhati/Learning/Projects/PyProjects/Project-Initial/configs/extractConfig.json"

# Read the JSON file with the schema and the multiline option enabled
df = spark.read.option("multiline", "true").schema(schema).json(json_file_path)

# Select the "drivers" section
drivers_df = df.select(col("drivers.*"))

# Explode the "options" array to flatten it
flattened_drivers_df = drivers_df.withColumn("option", explode(col("options"))).drop("options")

# Select the desired columns, including the driver_number from the flattened options
result_df = flattened_drivers_df.select(
    col("url"),
    col("frequency"),
    col("cdc"),
    col("type"),
    col("option.driver_number").alias("driver_number")
)

# Show the contents of the result DataFrame
result_df.show(truncate=False)

# Print the schema of the result DataFrame
result_df.printSchema()