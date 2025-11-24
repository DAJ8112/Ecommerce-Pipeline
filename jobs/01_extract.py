"""
Job 1: Extract data from BigQuery to Cloud Storage (Raw Layer)
"""

from pyspark.sql import SparkSession

from config.pipeline_config import (
    PROJECT_ID,
    RAW_LAYER,
    SOURCE_DATASET,
    TABLES_TO_EXTRACT,
)


def create_spark_session():
    """Create Spark session with BigQuery connector"""
    return SparkSession.builder \
        .appName("Extract_BigQuery_to_Raw") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
        .getOrCreate()


def extract_table(spark, table_name):
    """Extract a single table from BigQuery and save to GCS as Parquet"""
    
    print(f"Extracting table: {table_name}")
    
    # Read from BigQuery
    df = spark.read \
        .format("bigquery") \
        .option("table", f"{SOURCE_DATASET}.{table_name}") \
        .option("parentProject", PROJECT_ID) \
        .load()
    
    # Get row count for logging
    count = df.count()
    print(f"  - Rows extracted: {count}")
    
    # Write to raw layer as Parquet
    output_path = f"{RAW_LAYER}/{table_name}"
    df.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"  - Saved to: {output_path}")
    
    return count


def main():
    print("=" * 50)
    print("STARTING EXTRACTION JOB")
    print("=" * 50)
    
    spark = create_spark_session()
    
    extraction_summary = {}
    
    for table in TABLES_TO_EXTRACT:
        count = extract_table(spark, table)
        extraction_summary[table] = count
    
    print("\n" + "=" * 50)
    print("EXTRACTION COMPLETE")
    print("=" * 50)
    for table, count in extraction_summary.items():
        print(f"  {table}: {count} rows")
    
    spark.stop()


if __name__ == "__main__":
    main()