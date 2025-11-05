import findspark
findspark.init("/opt/spark")

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.17.0-openjdk-amd64"

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.window import Window
import time
import os

print("=" * 60)
print("PART 3: ADVANCED SPARK FEATURES")
print("=" * 60)

# Initialize Spark Session
try:
    spark = SparkSession.builder \
        .appName("Lab3_Advanced") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    
    print("Spark Session initialized successfully")
    print()
    
except Exception as e:
    print(f"Error initializing Spark session: {str(e)}")
    exit(1)

# Load Data
try:
    if not os.path.exists("transactions.parquet"):
        print("Error: transactions.parquet not found")
        spark.stop()
        exit(1)
    
    # Load transaction data from Parquet format
    df = spark.read.parquet("transactions.parquet")
    print("Loaded transactions.parquet")
    
except Exception as e:
    print(f"Error loading data: {str(e)}")
    spark.stop()
    exit(1)

# User Defined Functions (UDFs)
print("Task 3.1: User Defined Functions")
print("-" * 60)

try:
    def categorize_price(price):
        if price < 100:
            return "Budget"
        elif price < 500:
            return "Mid-range"
        else:
            return "Premium"

    # StringType() specifies the return type of the UDF
    categorize_udf = F.udf(categorize_price, StringType())
    
    # This adds a "price_category" column to each row
    df_with_category = df.withColumn("price_category",
                                      categorize_udf(F.col("price")))
    
    print("Applied price categorization UDF")
    print("\nSample results with price categories:")
    df_with_category.select("product", "price", "price_category").show(10)
    
except Exception as e:
    print(f"Error in Task 3.1: {str(e)}")
    print("Continuing with next task...")

# Broadcast Variables and Accumulators
print("Task 3.2: Broadcast Variables and Accumulators")
print("-" * 60)

try:
    print("Setting up broadcast variable for product costs...")
    
    # dictionary mapping products to their wholesale costs
    product_costs = {
        "Laptop": 800,
        "Phone": 400,
        "Tablet": 300,
        "Watch": 150,
        "Headphones": 50
    }
    
    broadcast_costs = sc.broadcast(product_costs)
    
    def calculate_profit(product, price):
        # access broadcast variable's value
        cost = broadcast_costs.value.get(product, 0)
        return price - cost
    
    # register the profit calculation as a UDF
    profit_udf = F.udf(calculate_profit, FloatType())
    
    # apply profit calculation to DataFrame
    df_with_profit = df.withColumn("profit",
                                    profit_udf(F.col("product"), F.col("price")))
    
    print("Applied profit calculation using broadcast variable")
    print("\nSample results with profit margins:")
    df_with_profit.select("product", "price", "profit").show(10)
    
    print("Setting up accumulators for data quality monitoring...")
    
    # Create accumulator to count records with data quality issues
    error_count = sc.accumulator(0)
    # Create accumulator to count total processed records
    processed_count = sc.accumulator(0)
    
    def process_record(row):
        # increment processed counter for every record
        processed_count.add(1)
        
        # check for data quality issues (negative prices)
        if row.price < 0:
            error_count.add(1)
            return None
        
        return row
    
    # convert DataFrame to RDD to apply the function
    df_rdd = df.rdd
    
    # apply processing function to each record
    processed_rdd = df_rdd.map(process_record)
    
    # trigger action to actually execute the processing
    processed_rdd.count()
    
    # Display accumulator results
    print("Data quality check completed")
    print(f"Total records processed: {processed_count.value}")
    print(f"Records with errors: {error_count.value}")
    
except Exception as e:
    print(f"Error in Task 3.2: {str(e)}")
    print("Continuing with next task...")

# Performance Optimization
print("Task 3.3: Performance Optimization")
print("-" * 60)

try:
    # Proper partitioning is crucial for parallelism and performance
    print("1. Partition Optimization:")
    print("-" * 40)
    
    # Check current number of partitions
    original_partitions = df.rdd.getNumPartitions()
    print(f"Original number of partitions: {original_partitions}")
    
    # Test performance before repartitioning
    start = time.time()
    result_before = df.groupBy("customer_id").agg(F.sum("price").alias("total")).count()
    time_before = time.time() - start
    
    # Repartition data by customer_id for better locality
    # This groups related data together, reducing shuffle during aggregations
    df_optimized = df.repartition(10, "customer_id")
    print(f"After repartitioning: {df_optimized.rdd.getNumPartitions()} partitions")
    
    # Test performance after repartitioning
    start = time.time()
    result_after = df_optimized.groupBy("customer_id").agg(F.sum("price").alias("total")).count()
    time_after = time.time() - start
    
    print(f"Execution time before: {time_before:.4f} seconds")
    print(f"Execution time after: {time_after:.4f} seconds")
    
    if time_after < time_before:
        improvement = ((time_before - time_after) / time_before * 100)
        print(f"Performance improvement: {improvement:.2f}%")
    else:
        print("Note: For small datasets, repartitioning overhead may exceed benefits")
    print()
    
    # Cache frequently accessed DataFrames in memory to avoid recomputation
    print("2. Caching Impact:")
    print("-" * 40)
    
    # Create enriched DataFrame with calculated column
    df_enriched = df.withColumn("total_amount", F.col("price") * F.col("quantity"))
    
    # Measure performance without caching
    # Multiple operations will recompute the DataFrame each time
    start = time.time()
    count1 = df_enriched.groupBy("product").agg(F.sum("total_amount")).count()
    time_no_cache_1 = time.time() - start
    
    start = time.time()
    count2 = df_enriched.groupBy("category").agg(F.avg("total_amount")).count()
    time_no_cache_2 = time.time() - start
    
    time_no_cache_total = time_no_cache_1 + time_no_cache_2
    
    # now test with caching enabled
    df_cached = df_enriched.cache()
    # teigger caching by performing an action
    df_cached.count()
    
    # Subsequent operations will use cached data
    start = time.time()
    count1_cached = df_cached.groupBy("product").agg(F.sum("total_amount")).count()
    time_cached_1 = time.time() - start
    
    start = time.time()
    count2_cached = df_cached.groupBy("category").agg(F.avg("total_amount")).count()
    time_cached_2 = time.time() - start
    
    time_cached_total = time_cached_1 + time_cached_2
    
    print(f"Total time without caching: {time_no_cache_total:.4f} seconds")
    print(f"Total time with caching: {time_cached_total:.4f} seconds")
    
    if time_cached_total < time_no_cache_total:
        speedup = time_no_cache_total / time_cached_total
        print(f"Caching speedup: {speedup:.2f}x faster")
    print()
    
    # Combining multiple aggregations in one operation is more efficient
    print("3. Efficient Aggregation Pattern:")
    print("-" * 40)
    
    # combine multiple aggregations in one groupBy operation
    # This processes the data in a single pass
    result_good = df.groupBy("product").agg(
        F.count("*").alias("count"),
        F.sum("price").alias("total_price")
    )
    
    print("Combined aggregations in single operation (efficient)")
    result_good.show()
    
    # Apply filters as early as possible to reduce data volume
    print("4. Predicate Pushdown:")
    print("-" * 40)
    
    # Without predicate pushdown: load all data then filter
    start = time.time()
    df_all = spark.read.parquet("transactions.parquet")
    df_filtered_after = df_all.filter(F.col("date") >= "2023-06-01") \
                               .filter(F.col("category") == "Electronics")
    count_after = df_filtered_after.count()
    time_without_pushdown = time.time() - start
    
    # With predicate pushdown: filter while reading
    # Spark can push filters down to the data source for better performance
    start = time.time()
    df_filtered = spark.read.parquet("transactions.parquet") \
        .filter(F.col("date") >= "2023-06-01") \
        .filter(F.col("category") == "Electronics")
    count_with = df_filtered.count()
    time_with_pushdown = time.time() - start
    
    print(f"Time without pushdown: {time_without_pushdown:.4f} seconds")
    print(f"Time with pushdown: {time_with_pushdown:.4f} seconds")
    print(f"Filtered result count: {count_with} rows")
    
    if time_with_pushdown < time_without_pushdown:
        improvement = ((time_without_pushdown - time_with_pushdown) / time_without_pushdown * 100)
        print(f"Performance improvement: {improvement:.2f}%")
    
except Exception as e:
    print(f"Error in Task 3.3: {str(e)}")

# Cleanup
print("=" * 60)
print("PART 3 COMPLETED")
print("=" * 60)

# Keep Spark session running for UI exploration
try:
    input("Press Enter to stop Spark session...")
except KeyboardInterrupt:
    print("\nReceived interrupt signal...")

try:
    spark.stop()
    print("Spark session stopped successfully")
except Exception as e:
    print(f"Warning: Error stopping Spark session: {str(e)}")