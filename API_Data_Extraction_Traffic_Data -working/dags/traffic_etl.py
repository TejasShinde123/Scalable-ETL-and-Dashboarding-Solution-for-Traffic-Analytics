from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import shutil, glob, subprocess, os, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round, col
import json
from datetime import datetime, timedelta

BASE = "/opt/airflow/dags"
RAW = f"{BASE}/raw"
ARCHIVE = f"{BASE}/archive"
TRANSFORMED = f"{BASE}/transformed"
GENERATOR = f"{BASE}/generate_synthetic_traffic.py"

# Ensure directories exist on DAG initialization
for dir_path in [RAW, ARCHIVE, TRANSFORMED]:
    os.makedirs(dir_path, exist_ok=True)

def generate_data():
    """Generate data and ensure it lands in the raw directory"""
    try:
        # Run generator (assuming it creates files in current directory)
        subprocess.run(["python", GENERATOR], check=True)
        
        # Move generated files to raw directory
        for file in glob.glob(f"{BASE}/traffic_raw_data*"):
            shutil.move(file, RAW)
            
    except Exception as e:
        raise Exception(f"Data generation failed: {str(e)}")

def convert_json_to_csv():
    """Convert JSON to CSV with error handling"""
    try:
        json_file = f"{RAW}/traffic_raw_data.json"
        if os.path.exists(json_file):
            with open(json_file) as f:
                data = json.load(f)
            
            import pandas as pd
            df = pd.json_normalize(data)
            
            # Create timestamped CSV filename
            csv_file = f"{RAW}/traffic_raw_data_{int(time.time())}.csv"
            df.to_csv(csv_file, index=False)
            os.remove(json_file)
            
    except Exception as e:
        raise Exception(f"JSON to CSV conversion failed: {str(e)}")

def run_spark_etl():
    """Process data with Spark and handle archiving"""
    try:
        files = glob.glob(f"{RAW}/*.csv")
        if not files:
            print("No CSV files found to process")
            return
            
        spark = SparkSession.builder.appName("Traffic ETL").getOrCreate()
        
        # Read and process data
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(files)
        
        # Debug info
        print("=== Input Data Schema ===")
        df.printSchema()
        print("=== Sample Data ===")
        df.show(5)
        
        # Transformation
        transformed = df.groupBy("region", "road_name").agg(sum("count").alias("total_count"))
        
        # Write output with timestamp
        output_file = f"{TRANSFORMED}/traffic_summary_{int(time.time())}.csv"
        transformed.toPandas().to_csv(output_file, index=False)
        
        # Archive processed files
        for f in files:
            archive_path = os.path.join(ARCHIVE, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(f)}")
            shutil.move(f, archive_path)
            
    except Exception as e:
        raise Exception(f"ETL processing failed: {str(e)}")
        
    finally:
        if 'spark' in locals():
            spark.stop()

with DAG(
    dag_id="traffic_incremental_etl",
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    t1 = PythonOperator(
        task_id="generate_synthetic_data",
        python_callable=generate_data
    )

    t2 = FileSensor(
        task_id="wait_for_raw",
        fs_conn_id="fs_default",
        filepath='/opt/airflow/dags/raw/traffic_raw_data.json',
        poke_interval=30,
        timeout=60*60,  # 1 hour timeout
        mode="poke"
    )

    t3 = PythonOperator(
        task_id="convert_json_to_csv",
        python_callable=convert_json_to_csv
    )

    t4 = PythonOperator(
        task_id="run_spark_etl",
        python_callable=run_spark_etl
    )

    t1 >> t2 >> t3 >> t4