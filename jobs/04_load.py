"""
Job 4: Load Curated Data to BigQuery
- Loads transformed metrics into BigQuery tables
- Applies partitioning for performance
"""

from pyspark.sql import SparkSession

from config.pipeline_config import (
    CURATED_DATASET,
    CURATED_LAYER,
    PROJECT_ID,
    TEMP_BUCKET,
)

# Tables to load
TABLES_TO_LOAD = [
    "fulfillment_by_dc",
    "fulfillment_by_status",
    "sales_by_country",
    "sales_by_state",
    "sales_by_traffic_source",
    "revenue_by_category",
    "revenue_by_brand",
    "revenue_monthly_trend",
    "revenue_by_department"
]


def create_spark_session():
    return SparkSession.builder \
        .appName("Load_to_BigQuery") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
        .getOrCreate()


def load_table_to_bigquery(spark, table_name):
    """Load a single curated table to BigQuery"""
    
    print(f"Loading {table_name} to BigQuery...")
    
    # Read from curated layer
    df = spark.read.parquet(f"{CURATED_LAYER}/{table_name}")
    
    row_count = df.count()
    print(f"  - Rows to load: {row_count}")
    
    # Write to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", f"{CURATED_DATASET}.{table_name}") \
        .option("temporaryGcsBucket", TEMP_BUCKET) \
        .option("parentProject", PROJECT_ID) \
        .mode("overwrite") \
        .save()
    
    print(f"  - Loaded to {CURATED_DATASET}.{table_name}")
    
    return row_count


def main():
    print("=" * 50)
    print("STARTING LOAD TO BIGQUERY")
    print("=" * 50)
    
    spark = create_spark_session()
    
    load_summary = {}
    
    for table in TABLES_TO_LOAD:
        count = load_table_to_bigquery(spark, table)
        load_summary[table] = count
    
    print("\n" + "=" * 50)
    print("LOAD TO BIGQUERY COMPLETE")
    print("=" * 50)
    print(f"\nTables loaded to {CURATED_DATASET}:")
    for table, count in load_summary.items():
        print(f"  - {table}: {count} rows")
    
    spark.stop()


if __name__ == "__main__":
    main()