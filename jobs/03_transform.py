"""
Job 3: Business Transformations
- Order Fulfillment Efficiency
- Geographic Sales Analysis
- Revenue Analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, datediff, avg, count, sum as spark_sum, round as spark_round,
    year, month, dayofweek, when, lit, desc, dense_rank, percentile_approx
)
from pyspark.sql.window import Window

from config.pipeline_config import CLEANED_LAYER, CURATED_LAYER


def create_spark_session():
    return SparkSession.builder \
        .appName("Business_Transformations") \
        .getOrCreate()


def calculate_fulfillment_metrics(order_items_df, products_df, distribution_centers_df):
    """
    Calculate order fulfillment efficiency metrics
    """
    print("Calculating Order Fulfillment Metrics...")
    
    # First join order_items with products to get distribution_center_id
    items_with_products = order_items_df.join(
        products_df.select("id", "distribution_center_id"),
        order_items_df.product_id == products_df.id,
        "left"
    )
    
    # Then join with distribution centers
    fulfillment_df = items_with_products.join(
        distribution_centers_df.withColumnRenamed("id", "dc_id"),
        items_with_products.distribution_center_id == col("dc_id"),
        "left"
    )
    
    # Calculate time differences (in days)
    fulfillment_metrics = fulfillment_df.withColumn(
        "days_to_ship",
        datediff(col("shipped_at"), col("created_at"))
    ).withColumn(
        "days_to_deliver",
        datediff(col("delivered_at"), col("shipped_at"))
    ).withColumn(
        "total_fulfillment_days",
        datediff(col("delivered_at"), col("created_at"))
    )
    
    # Aggregate by distribution center
    dc_performance = fulfillment_metrics.groupBy("name").agg(
        count("*").alias("total_orders"),
        spark_round(avg("days_to_ship"), 2).alias("avg_days_to_ship"),
        spark_round(avg("days_to_deliver"), 2).alias("avg_days_to_deliver"),
        spark_round(avg("total_fulfillment_days"), 2).alias("avg_total_fulfillment_days"),
        spark_round(percentile_approx("total_fulfillment_days", 0.5), 2).alias("median_fulfillment_days")
    ).withColumnRenamed("name", "distribution_center")
    
    # Overall status breakdown
    status_breakdown = fulfillment_metrics.groupBy("status").agg(
        count("*").alias("order_count"),
        spark_round(avg("total_fulfillment_days"), 2).alias("avg_fulfillment_days")
    )
    
    print(f"  - Distribution center metrics calculated")
    print(f"  - Status breakdown calculated")
    
    return dc_performance, status_breakdown, fulfillment_metrics


def calculate_geographic_sales(order_items_df, users_df):
    """
    Calculate sales metrics by geography
    """
    print("Calculating Geographic Sales Analysis...")
    
    # Join order items with users to get location
    geo_sales = order_items_df.join(
        users_df.select("id", "country", "state", "city", "traffic_source"),
        order_items_df.user_id == users_df.id,
        "inner"
    )
    
    # Sales by country
    country_sales = geo_sales.groupBy("country").agg(
        count("*").alias("total_orders"),
        spark_round(spark_sum("sale_price"), 2).alias("total_revenue"),
        spark_round(avg("sale_price"), 2).alias("avg_order_value"),
        count("user_id").alias("customer_count")
    ).orderBy(desc("total_revenue"))
    
    # Sales by state (top states)
    state_sales = geo_sales.groupBy("country", "state").agg(
        count("*").alias("total_orders"),
        spark_round(spark_sum("sale_price"), 2).alias("total_revenue"),
        spark_round(avg("sale_price"), 2).alias("avg_order_value")
    ).orderBy(desc("total_revenue"))
    
    # Sales by traffic source
    traffic_source_sales = geo_sales.groupBy("traffic_source").agg(
        count("*").alias("total_orders"),
        spark_round(spark_sum("sale_price"), 2).alias("total_revenue"),
        spark_round(avg("sale_price"), 2).alias("avg_order_value")
    ).orderBy(desc("total_revenue"))
    
    print(f"  - Country sales: {country_sales.count()} countries")
    print(f"  - State sales: {state_sales.count()} state combinations")
    print(f"  - Traffic source analysis complete")
    
    return country_sales, state_sales, traffic_source_sales


def calculate_revenue_analytics(order_items_df, products_df):
    """
    Calculate revenue and profitability metrics
    """
    print("Calculating Revenue Analytics...")
    
    # Join with products for cost information
    revenue_df = order_items_df.join(
        products_df.select("id", "cost", "category", "brand", "department"),
        order_items_df.product_id == products_df.id,
        "inner"
    )
    
    # Calculate profit margin
    revenue_with_profit = revenue_df.withColumn(
        "profit",
        col("sale_price") - col("cost")
    ).withColumn(
        "profit_margin_pct",
        spark_round((col("profit") / col("sale_price")) * 100, 2)
    )
    
    # Revenue by category
    category_revenue = revenue_with_profit.groupBy("category").agg(
        count("*").alias("items_sold"),
        spark_round(spark_sum("sale_price"), 2).alias("total_revenue"),
        spark_round(spark_sum("profit"), 2).alias("total_profit"),
        spark_round(avg("profit_margin_pct"), 2).alias("avg_profit_margin_pct")
    ).orderBy(desc("total_revenue"))
    
    # Revenue by brand (top 20)
    brand_revenue = revenue_with_profit.groupBy("brand").agg(
        count("*").alias("items_sold"),
        spark_round(spark_sum("sale_price"), 2).alias("total_revenue"),
        spark_round(spark_sum("profit"), 2).alias("total_profit"),
        spark_round(avg("profit_margin_pct"), 2).alias("avg_profit_margin_pct")
    ).orderBy(desc("total_revenue")).limit(50)
    
    # Monthly revenue trend
    monthly_revenue = revenue_with_profit.withColumn(
        "year", year("created_at")
    ).withColumn(
        "month", month("created_at")
    ).groupBy("year", "month").agg(
        count("*").alias("items_sold"),
        spark_round(spark_sum("sale_price"), 2).alias("total_revenue"),
        spark_round(spark_sum("profit"), 2).alias("total_profit"),
        spark_round(avg("sale_price"), 2).alias("avg_sale_price")
    ).orderBy("year", "month")
    
    # Department performance
    department_revenue = revenue_with_profit.groupBy("department").agg(
        count("*").alias("items_sold"),
        spark_round(spark_sum("sale_price"), 2).alias("total_revenue"),
        spark_round(spark_sum("profit"), 2).alias("total_profit"),
        spark_round(avg("profit_margin_pct"), 2).alias("avg_profit_margin_pct")
    ).orderBy(desc("total_revenue"))
    
    print(f"  - Category revenue: {category_revenue.count()} categories")
    print(f"  - Brand revenue: top 50 brands")
    print(f"  - Monthly trends: {monthly_revenue.count()} months")
    print(f"  - Department performance calculated")
    
    return category_revenue, brand_revenue, monthly_revenue, department_revenue


def main():
    print("=" * 50)
    print("STARTING BUSINESS TRANSFORMATIONS")
    print("=" * 50)
    
    spark = create_spark_session()
    
    # Load cleaned data
    print("\nLoading cleaned data...")
    order_items_df = spark.read.parquet(f"{CLEANED_LAYER}/order_items")
    users_df = spark.read.parquet(f"{CLEANED_LAYER}/users")
    products_df = spark.read.parquet(f"{CLEANED_LAYER}/products")
    distribution_centers_df = spark.read.parquet(f"{CLEANED_LAYER}/distribution_centers")
    
    print(f"  - Order items: {order_items_df.count()} records")
    print(f"  - Users: {users_df.count()} records")
    print(f"  - Products: {products_df.count()} records")
    
    # Calculate metrics
    print("\n" + "=" * 50)
    
    # 1. Fulfillment Metrics
    dc_performance, status_breakdown, _ = calculate_fulfillment_metrics(
        order_items_df, products_df, distribution_centers_df
    )
    dc_performance.write.mode("overwrite").parquet(f"{CURATED_LAYER}/fulfillment_by_dc")
    status_breakdown.write.mode("overwrite").parquet(f"{CURATED_LAYER}/fulfillment_by_status")
    
    # 2. Geographic Sales
    country_sales, state_sales, traffic_source_sales = calculate_geographic_sales(
        order_items_df, users_df
    )
    country_sales.write.mode("overwrite").parquet(f"{CURATED_LAYER}/sales_by_country")
    state_sales.write.mode("overwrite").parquet(f"{CURATED_LAYER}/sales_by_state")
    traffic_source_sales.write.mode("overwrite").parquet(f"{CURATED_LAYER}/sales_by_traffic_source")
    
    # 3. Revenue Analytics
    category_revenue, brand_revenue, monthly_revenue, department_revenue = calculate_revenue_analytics(
        order_items_df, products_df
    )
    category_revenue.write.mode("overwrite").parquet(f"{CURATED_LAYER}/revenue_by_category")
    brand_revenue.write.mode("overwrite").parquet(f"{CURATED_LAYER}/revenue_by_brand")
    monthly_revenue.write.mode("overwrite").parquet(f"{CURATED_LAYER}/revenue_monthly_trend")
    department_revenue.write.mode("overwrite").parquet(f"{CURATED_LAYER}/revenue_by_department")
    
    print("\n" + "=" * 50)
    print("BUSINESS TRANSFORMATIONS COMPLETE")
    print("=" * 50)
    print(f"\nCurated datasets saved to: {CURATED_LAYER}/")
    print("  - fulfillment_by_dc")
    print("  - fulfillment_by_status")
    print("  - sales_by_country")
    print("  - sales_by_state")
    print("  - sales_by_traffic_source")
    print("  - revenue_by_category")
    print("  - revenue_by_brand")
    print("  - revenue_monthly_trend")
    print("  - revenue_by_department")
    
    spark.stop()


if __name__ == "__main__":
    main()