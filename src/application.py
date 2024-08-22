import requests, json
import pyspark

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

response = requests.get("https://api.openf1.org/v1/sessions")
df_response_text = spark.sparkContext.parallelize([response.text])
df_json = spark.read.json(df_response_text)

# df_json.show()
df_json.printSchema()




# def read_api(url: str):
#     normalized_data = dict()
#     data = requests.get(api_url).json() 
#     normalized_data["_data"] = data # Normalize payload to handle array situtations
#     return json.dumps(normalized_data)

# api_url = r"https://api.openf1.org/v1/car_data"

# # Read data into Data Frame
# # Create payload rdd
# payload = json.loads(read_api(api_url))
# payload_rdd = spark.sparkContext.parallelize([payload])
# # Read from JSON
# df = spark.read.json(payload_rdd)
# #df.select("_data").printSchema()
# df.show()
