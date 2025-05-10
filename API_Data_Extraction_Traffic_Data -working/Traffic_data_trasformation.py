import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round, col

# Optional: ensure WinUtils path if running on Windows
# os.environ["HADOOP_HOME"] = r"C:\hadoop"

# Step 1: Start Spark Session
spark = SparkSession.builder \
   .appName("Traffic Data Transformation")\
   .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
   .getOrCreate()

# Step 2: Load the CSV file
file_path = r"C:/Users/Ravindra/Desktop/API_Data_Extraction_Traffic_Data/downloaded_data/region_traffic_by_road_type.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# Step 3: Data Transformation
transformed_df = (
    df.groupBy("year", "region_name", "road_category_name")
      .agg(
          sum("link_length_km").alias("total_link_km"),
          sum("all_motor_vehicles").alias("total_vehicles")
      )
      .withColumn("vehicles_per_km", round(col("total_vehicles") / col("total_link_km"), 2))
)

# Step 4: Preview the result
transformed_df.orderBy(col("vehicles_per_km").desc()).show(truncate=False)

# Step 5: Export to CSV via pandas fallback to avoid WinUtils errors
# Collect to driver memory (small summary)
pdf = transformed_df.toPandas()
output_file = r"C:/Users/Ravindra/Desktop/API_Data_Extraction_Traffic_Data/raw/traffic_analysis.csv"
pdf.to_csv(output_file, index=False)
print(f"Exported {len(pdf)} rows to {output_file}")

# Step 6: Stop the Spark session
spark.stop()
