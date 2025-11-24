"""
Job 2: Data Cleaning and Validation
- Validates data quality
- Cleans records
- Quarantines bad data
- Generates quality report
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnull, count, lit, current_timestamp, concat_ws
from pyspark.sql.types import StringType
import json
from datetime import datetime

from config.pipeline_config import (
    CLEANED_LAYER,
    METADATA_LAYER,
    QUARANTINE_LAYER,
    RAW_LAYER,
)


def create_spark_session():
    return SparkSession.builder \
        .appName("Clean_and_Validate") \
        .getOrCreate()


def validate_orders(df):
    """
    Validate orders table
    Returns: (clean_df, quarantine_df, stats)
    """
    print("Validating orders...")
    
    # Define validation rules
    # A record fails if ANY of these conditions are true
    invalid_condition = (
        isnull(col("order_id")) |
        isnull(col("user_id")) |
        isnull(col("created_at")) |
        (col("num_of_item") <= 0)
    )
    
    # Tag each record with failure reason
    df_with_validation = df.withColumn(
        "validation_errors",
        concat_ws(", ",
            when(isnull(col("order_id")), lit("missing_order_id")),
            when(isnull(col("user_id")), lit("missing_user_id")),
            when(isnull(col("created_at")), lit("missing_created_at")),
            when(col("num_of_item") <= 0, lit("invalid_num_of_item"))
        )
    )
    
    # Split into clean and quarantine
    clean_df = df_with_validation.filter(~invalid_condition).drop("validation_errors")
    quarantine_df = df_with_validation.filter(invalid_condition)
    
    # Calculate stats
    total = df.count()
    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    
    stats = {
        "table": "orders",
        "total_records": total,
        "clean_records": clean_count,
        "quarantined_records": quarantine_count,
        "pass_rate": round((clean_count / total) * 100, 2) if total > 0 else 0
    }
    
    print(f"  - Total: {total}, Clean: {clean_count}, Quarantined: {quarantine_count}")
    
    return clean_df, quarantine_df, stats


def validate_order_items(df):
    """
    Validate order_items table
    Returns: (clean_df, quarantine_df, stats)
    """
    print("Validating order_items...")
    
    invalid_condition = (
        isnull(col("id")) |
        isnull(col("order_id")) |
        isnull(col("product_id")) |
        isnull(col("sale_price")) |
        (col("sale_price") < 0)
    )
    
    df_with_validation = df.withColumn(
        "validation_errors",
        concat_ws(", ",
            when(isnull(col("id")), lit("missing_id")),
            when(isnull(col("order_id")), lit("missing_order_id")),
            when(isnull(col("product_id")), lit("missing_product_id")),
            when(isnull(col("sale_price")), lit("missing_sale_price")),
            when(col("sale_price") < 0, lit("negative_sale_price"))
        )
    )
    
    clean_df = df_with_validation.filter(~invalid_condition).drop("validation_errors")
    quarantine_df = df_with_validation.filter(invalid_condition)
    
    total = df.count()
    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    
    stats = {
        "table": "order_items",
        "total_records": total,
        "clean_records": clean_count,
        "quarantined_records": quarantine_count,
        "pass_rate": round((clean_count / total) * 100, 2) if total > 0 else 0
    }
    
    print(f"  - Total: {total}, Clean: {clean_count}, Quarantined: {quarantine_count}")
    
    return clean_df, quarantine_df, stats


def validate_users(df):
    """
    Validate users table
    Returns: (clean_df, quarantine_df, stats)
    """
    print("Validating users...")
    
    invalid_condition = (
        isnull(col("id")) |
        isnull(col("email")) |
        isnull(col("created_at")) |
        (col("age") < 0) |
        (col("age") > 120)
    )
    
    df_with_validation = df.withColumn(
        "validation_errors",
        concat_ws(", ",
            when(isnull(col("id")), lit("missing_id")),
            when(isnull(col("email")), lit("missing_email")),
            when(isnull(col("created_at")), lit("missing_created_at")),
            when(col("age") < 0, lit("negative_age")),
            when(col("age") > 120, lit("unrealistic_age"))
        )
    )
    
    clean_df = df_with_validation.filter(~invalid_condition).drop("validation_errors")
    quarantine_df = df_with_validation.filter(invalid_condition)
    
    total = df.count()
    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    
    stats = {
        "table": "users",
        "total_records": total,
        "clean_records": clean_count,
        "quarantined_records": quarantine_count,
        "pass_rate": round((clean_count / total) * 100, 2) if total > 0 else 0
    }
    
    print(f"  - Total: {total}, Clean: {clean_count}, Quarantined: {quarantine_count}")
    
    return clean_df, quarantine_df, stats


def validate_products(df):
    """
    Validate products table
    Returns: (clean_df, quarantine_df, stats)
    """
    print("Validating products...")
    
    invalid_condition = (
        isnull(col("id")) |
        isnull(col("cost")) |
        isnull(col("retail_price")) |
        (col("cost") < 0) |
        (col("retail_price") < 0)
    )
    
    df_with_validation = df.withColumn(
        "validation_errors",
        concat_ws(", ",
            when(isnull(col("id")), lit("missing_id")),
            when(isnull(col("cost")), lit("missing_cost")),
            when(isnull(col("retail_price")), lit("missing_retail_price")),
            when(col("cost") < 0, lit("negative_cost")),
            when(col("retail_price") < 0, lit("negative_retail_price"))
        )
    )
    
    clean_df = df_with_validation.filter(~invalid_condition).drop("validation_errors")
    quarantine_df = df_with_validation.filter(invalid_condition)
    
    total = df.count()
    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()
    
    stats = {
        "table": "products",
        "total_records": total,
        "clean_records": clean_count,
        "quarantined_records": quarantine_count,
        "pass_rate": round((clean_count / total) * 100, 2) if total > 0 else 0
    }
    
    print(f"  - Total: {total}, Clean: {clean_count}, Quarantined: {quarantine_count}")
    
    return clean_df, quarantine_df, stats


def save_quality_report(spark, all_stats):
    """Save quality report as JSON to metadata layer"""
    
    report = {
        "pipeline_run": datetime.now().isoformat(),
        "validation_results": all_stats,
        "overall_summary": {
            "total_tables_processed": len(all_stats),
            "total_records_processed": sum(s["total_records"] for s in all_stats),
            "total_clean_records": sum(s["clean_records"] for s in all_stats),
            "total_quarantined_records": sum(s["quarantined_records"] for s in all_stats),
            "overall_pass_rate": round(
                (sum(s["clean_records"] for s in all_stats) / 
                 sum(s["total_records"] for s in all_stats)) * 100, 2
            )
        }
    }
    
    # Save as JSON
    report_path = f"{METADATA_LAYER}/quality_report.json"
    
    # Convert to single-row DataFrame and save
    report_json = json.dumps(report, indent=2)
    report_df = spark.createDataFrame([(report_json,)], ["report"])
    report_df.coalesce(1).write.mode("overwrite").text(report_path)
    
    print(f"\nQuality report saved to: {report_path}")
    print(f"Overall pass rate: {report['overall_summary']['overall_pass_rate']}%")
    
    return report


def main():
    print("=" * 50)
    print("STARTING DATA CLEANING & VALIDATION JOB")
    print("=" * 50)
    
    spark = create_spark_session()
    
    all_stats = []
    
    # Process orders
    orders_df = spark.read.parquet(f"{RAW_LAYER}/orders")
    clean_orders, quarantine_orders, orders_stats = validate_orders(orders_df)
    clean_orders.write.mode("overwrite").parquet(f"{CLEANED_LAYER}/orders")
    if quarantine_orders.count() > 0:
        quarantine_orders.write.mode("overwrite").parquet(f"{QUARANTINE_LAYER}/orders")
    all_stats.append(orders_stats)
    
    # Process order_items
    order_items_df = spark.read.parquet(f"{RAW_LAYER}/order_items")
    clean_items, quarantine_items, items_stats = validate_order_items(order_items_df)
    clean_items.write.mode("overwrite").parquet(f"{CLEANED_LAYER}/order_items")
    if quarantine_items.count() > 0:
        quarantine_items.write.mode("overwrite").parquet(f"{QUARANTINE_LAYER}/order_items")
    all_stats.append(items_stats)
    
    # Process users
    users_df = spark.read.parquet(f"{RAW_LAYER}/users")
    clean_users, quarantine_users, users_stats = validate_users(users_df)
    clean_users.write.mode("overwrite").parquet(f"{CLEANED_LAYER}/users")
    if quarantine_users.count() > 0:
        quarantine_users.write.mode("overwrite").parquet(f"{QUARANTINE_LAYER}/users")
    all_stats.append(users_stats)
    
    # Process products
    products_df = spark.read.parquet(f"{RAW_LAYER}/products")
    clean_products, quarantine_products, products_stats = validate_products(products_df)
    clean_products.write.mode("overwrite").parquet(f"{CLEANED_LAYER}/products")
    if quarantine_products.count() > 0:
        quarantine_products.write.mode("overwrite").parquet(f"{QUARANTINE_LAYER}/products")
    all_stats.append(products_stats)
    
    # Distribution centers (simple pass-through, minimal validation)
    print("Processing distribution_centers (pass-through)...")
    dc_df = spark.read.parquet(f"{RAW_LAYER}/distribution_centers")
    dc_df.write.mode("overwrite").parquet(f"{CLEANED_LAYER}/distribution_centers")
    all_stats.append({
        "table": "distribution_centers",
        "total_records": dc_df.count(),
        "clean_records": dc_df.count(),
        "quarantined_records": 0,
        "pass_rate": 100.0
    })
    
    # Generate quality report
    print("\n" + "=" * 50)
    print("GENERATING QUALITY REPORT")
    print("=" * 50)
    save_quality_report(spark, all_stats)
    
    print("\n" + "=" * 50)
    print("DATA CLEANING & VALIDATION COMPLETE")
    print("=" * 50)
    
    spark.stop()


if __name__ == "__main__":
    main()