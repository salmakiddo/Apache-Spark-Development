import findspark
findspark.init("/opt/spark")

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.17.0-openjdk-amd64"


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import os

print("=" * 60)
print("PART 4: SPARK UI AND DEBUGGING")
print("=" * 60)
print()

# Initialize Spark Session
try:
    spark = SparkSession.builder \
        .appName("Lab3_SparkUI") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("Spark Session initialized successfully")
    print()
    
except Exception as e:
    print(f"Error initializing Spark session: {str(e)}")
    exit(1)

# Access Spark UI
print("Task 4.1: Access Spark UI")
print("-" * 60)

try:
    print("Spark UI is available at: http://localhost:4040")
    print("In the Spark UI, you can explore:")
    print("  - Jobs: Overview of all Spark jobs")
    print("  - Stages: Breakdown of each job into stages")
    print("  - Storage: Information about cached RDDs and DataFrames")
    print("  - Environment: Configuration settings")
    print("  - Executors: Details about executor resources")
    print("  - SQL: Query execution details for DataFrames")
    
    def complex_analysis():
        # Read the parquet file created in Part 2
        df = spark.read.parquet("transactions.parquet")
        
        # Add calculated columns for analysis
        # total_amount: revenue per transaction
        # month: extracted from date for temporal analysis
        df = df.withColumn("total_amount", F.col("price") * F.col("quantity")) \
               .withColumn("month", F.month(F.col("date")))
        
        # Filter for transactions above $100
        # This reduces the dataset size for focused analysis
        filtered = df.filter(F.col("price") > 100)
        
        # Aggregate spending by customer and month
        # This answers: "How much does each customer spend monthly?"
        aggregated = filtered.groupBy("customer_id", "month") \
            .agg(F.sum("total_amount").alias("monthly_spend"))
        
        # Define window for comparing across months per customer
        # Partition by customer, order by month chronologically
        window = Window.partitionBy("customer_id").orderBy("month")
        
        # Add previous month's spending using lag window function
        # This enables month-over-month growth analysis
        with_lag = aggregated.withColumn("prev_month_spend",
                                          F.lag("monthly_spend").over(window))
        
        # Collect results (this triggers execution)
        return with_lag.collect()
    
    # Check if required data file exists
    if not os.path.exists("transactions.parquet"):
        print("⚠ Warning: transactions.parquet not found")
        print("Please run dataframe_operations.py (Part 2) first")
        print("Skipping complex analysis...")
    else:
        print("Executing complex analysis job...")
        print("Monitor the job execution in Spark UI at http://localhost:4040")
        
        # Run the complex analysis
        result = complex_analysis()
        
        print(f"Analysis completed successfully")
        print(f"Processed {len(result)} customer-month combinations")
    
except Exception as e:
    print(f"Error in Task 4.1: {str(e)}")
    print("Continuing with next task...")

# Analyze Execution Plan
print("Task 4.2: Analyze Execution Plan")
print("-" * 60)

try:
    # Check if data file exists
    if not os.path.exists("transactions.parquet"):
        print("Warning: transactions.parquet not found")
        print("Skipping execution plan analysis...")
    else:
        print("Creating query for execution plan analysis...")
        
        # Load data from parquet
        df = spark.read.parquet("transactions.parquet")
        
        # Add calculated column
        df = df.withColumn("total_amount", F.col("price") * F.col("quantity"))
        
        # Create a query with filter and aggregation
        # This query will be optimized by Catalyst optimizer
        plan_df = df.filter(F.col("price") > 100) \
            .groupBy("product") \
            .agg(F.sum("total_amount"))
        
        print("=" * 60)
        print("EXECUTION PLAN ANALYSIS")
        print("=" * 60)
        
        print("The following shows how Spark optimizes your query:")
        print("-" * 60)
        plan_df.explain(True)
        
        # Programmatically access the execution plan
        print("Programmatic Access to Plans:")
        print("-" * 60)
        
        try:
            print("Logical Plan:")
            print(plan_df._jdf.queryExecution().logical())
            
            print("Physical Plan:")
            print(plan_df._jdf.queryExecution().sparkPlan())
        except Exception as e:
            print(f"Note: Unable to access internal plan representation: {str(e)}")
            print("This is normal and doesn't affect functionality")
            print()
    
except Exception as e:
    print(f"Error in Task 4.2: {str(e)}")
    print()

# Keep Spark UI Available
print("=" * 60)
print("PART 4 COMPLETED")
print("=" * 60)
print()
print("The Spark UI remains available at: http://localhost:4040")

try:
    input("Press Enter to stop Spark session and close UI...")
except KeyboardInterrupt:
    print("\nReceived interrupt signal...")

# Cleanup
print("Shutting down Spark session...")

try:
    spark.stop()
    print("Spark session stopped successfully")
    print("Spark UI is no longer available")
except Exception as e:
    print(f"Warning: Error stopping Spark session: {str(e)}")