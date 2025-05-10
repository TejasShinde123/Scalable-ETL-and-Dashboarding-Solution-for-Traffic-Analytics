from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Traffic Data Reader") \
    .getOrCreate()



# Path to the desktop (works for Windows, macOS, and Linux)
# desktop = os.path.join(os.path.expanduser('~'), 'Desktop')
file_path_1 = os.path.join('C:/Users/Ravindra/Desktop/API Data Extraction Traffic Data/downloaded_data/dft_traffic_counts_raw_counts.csv')
file_path_2 = os.path.join('C:/Users/Ravindra/Desktop/API Data Extraction Traffic Data/downloaded_data/local_authority_traffic.csv')
file_path_3 = os.path.join('C:/Users/Ravindra/Desktop/API Data Extraction Traffic Data/downloaded_data/region_traffic_by_road_type.csv')
file_path_4 = os.path.join('C:/Users/Ravindra/Desktop/API Data Extraction Traffic Data/downloaded_data/region_traffic_by_vehicle_type.csv')

'''# Read the CSV file with header
# Added inferschema true by me
# df = spark.read.option("header", "true")\
#     .option("inferschema","true")\
#     .csv([file_path_1, file_path_2, file_path_3, file_path_4]) '''  

df = spark.read.option("header", "true")\
    .option("inferschema","true")\
    .csv(file_path_3)   

df.select(count("year").alias("Count")).show()
df.show()
df.printSchema()

# from pyspark.sql.functions import sum, desc

# # Total traffic per road
# traffic_per_road = df.groupBy("road_name") \
#     .agg(sum("all_motor_vehicles").alias("total_traffic")) \
#     .orderBy(desc("total_traffic"))

# traffic_per_road.show(10)  # Top 10 highest traffic roads











# Stop the Spark session
spark.stop()