import findspark
findspark.init("/opt/spark")

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.17.0-openjdk-amd64"

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, timedelta
import random

print("=" * 60)
print("PART 2: DATAFRAMES AND SQL OPERATIONS")
print("=" * 60)
print()

# Initialize Spark Session
try:
    spark = SparkSession.builder \
        .appName("Lab3_DataFrames") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("Spark Session initialized successfully")
    
except Exception as e:
    print(f"Error initializing Spark session: {str(e)}")
    exit(1)

# Create and Manipulate DataFrames
print("Task 2.1: Create and Manipulate DataFrames")
print("-" * 60)

try:    
    # create customer IDs in format CUST0001, CUST0002, etc.
    customers = [f"CUST{i:04d}" for i in range(1, 101)]
    
    # define product catalog with corresponding categories
    products = ["Laptop", "Phone", "Tablet", "Watch", "Headphones"]
    categories = ["Electronics", "Electronics", "Electronics", "Accessories", "Accessories"]
    
    # set base date for generating random transaction dates throughout 2023
    base_date = datetime(2023, 1, 1)
    
    print("Generating 10,000 e-commerce transactions...")
    
    # build list of transaction dictionaries
    transactions = []
    for i in range(10000):
        product = random.choice(products)
        
        transactions.append({
            "transaction_id": i + 1,
            "customer_id": random.choice(customers),
            "product": product,
            # get category corresponding to the selected product
            "category": categories[products.index(product)],
            # generate random price between $50 and $2000
            "price": round(random.uniform(50, 2000), 2),
            # generate random quantity between 1 and 5
            "quantity": random.randint(1, 5),
            # generate random date within 2023
            "date": (base_date + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d")
        })
    
    df = spark.createDataFrame(transactions)

    print("DataFrame created successfully with 10,000 records")

    # display the schema to understand data types and structure
    print("DataFrame Schema:")
    df.printSchema()
    
    # show first 5 rows as a preview
    print("Sample data (first 5 rows):")
    df.show(5)
    
    # save DataFrame in Parquet format for efficient storage and later use
    # parquet is a columnar storage format that provides good compression
    # mode("overwrite") replaces existing data if the file already exists
    print("Saving DataFrame to Parquet format...")
    
    output_path = os.path.abspath("transactions.parquet")
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved to {output_path}")
    
except Exception as e:
    print(f"Error in Task 2.1: {str(e)}")
    print("Unable to proceed with remaining tasks.")
    spark.stop()
    exit(1)

# DataFrame Operations
print("Task 2.2: DataFrame Operations")
print("-" * 60)

try:    
    # calculate total transaction amount (price × quantity)
    df_enriched = df.withColumn("total_amount", F.col("price") * F.col("quantity"))
    
    # extract month number from date string (1-12)
    df_enriched = df_enriched.withColumn("month", F.month(F.col("date")))
    
    # extract quarter from date (1-4)
    df_enriched = df_enriched.withColumn("quarter", F.quarter(F.col("date")))
    
    print("Added calculated columns: total_amount, month, quarter")
    
    print("Analyzing monthly sales by category...")
    
    monthly_sales = df_enriched.groupBy("month", "category") \
        .agg(
            # Sum total revenue for each group
            F.sum("total_amount").alias("total_sales"),
            # Count number of transactions
            F.count("transaction_id").alias("num_transactions"),
            # Count unique customers (to track customer engagement)
            F.countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy("month", "category")
    
    print("Monthly Sales Summary:")
    monthly_sales.show()
    print()
    
except Exception as e:
    print(f"Error in Task 2.2: {str(e)}")
    print("Continuing with next task...")

# SQL Queries
print("Task 2.3: SQL Queries")
print("-" * 60)

try:
    # register DataFrame as a temporary SQL view
    # this allows us to query the DataFrame using SQL syntax
    df_enriched.createOrReplaceTempView("transactions")
    print("Registered 'transactions' as SQL temporary view")
    
    print("Querying top 10 products by revenue...")
    
    top_products = spark.sql("""
        SELECT
            product,
            category,
            COUNT(*) as purchase_count,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value
        FROM transactions
        GROUP BY product, category
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    
    print("Top Products by Revenue:")
    top_products.show()
    print()
    
except Exception as e:
    print(f"Error in Task 2.3: {str(e)}")

# Cleanup
print("=" * 60)
print("PART 2 COMPLETED")
print("=" * 60)
print("Spark UI available at: http://localhost:4040")

try:
    input("Press Enter to stop Spark session...")
except KeyboardInterrupt:
    print("\nReceived interrupt signal...")

try:
    spark.stop()
    print("Spark session stopped successfully")
except Exception as e:
    print(f"Warning: Error stopping Spark session: {str(e)}")