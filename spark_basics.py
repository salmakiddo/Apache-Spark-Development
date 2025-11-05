import findspark
findspark.init("/opt/spark")

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.17.0-openjdk-amd64"

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time
import random
import os

print("=" * 60)
print("PART 1: SPARK SESSION AND BASIC OPERATIONS")
print("=" * 60)

print("Task 1.1: Initialize Spark Session")
print("-" * 60)

try:
    # Create a Spark session with specific configurations
    spark = SparkSession.builder \
        .appName("Lab3_SparkBasics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()
    
    # SparkContext is used for RDD operations
    sc = spark.sparkContext
    
    # Set log level to WARN to reduce console output clutter
    sc.setLogLevel("WARN")

    print("Spark Session created successfully")
    print(f"Application Name: {spark.sparkContext.appName}")
    print(f"Spark Version: {spark.version}")
    
    # Test the Spark session with a simple calculation
    # Create an RDD with numbers from 1 to 1000
    numbers = sc.parallelize(range(1, 1001))
    
    # Calculate the sum (expected result: 500500)
    result_sum = numbers.sum()
    print(f"Sum of 1 to 1000: {result_sum}")
    
    # Check how many partitions Spark created for this RDD
    # Partitions determine how data is distributed across the cluster
    num_partitions = numbers.getNumPartitions()
    print(f"Number of partitions: {num_partitions}")
    print()
    
except Exception as e:
    print(f"Error initializing Spark session: {str(e)}")
    print("Please check your Spark installation and configuration.")
    exit(1)

# Word Count
print("Task 1.2: RDD Operations - Word Count")
print("-" * 60)

try:
    # Check if sample_text.txt exists
    text_file_path = "sample_text-1.txt"
    
    if os.path.exists(text_file_path):
        # Read text file into an RDD
        # Each line of the file becomes an element in the RDD
        text_rdd = sc.textFile(text_file_path)
        print(f"✓ Loaded text from {text_file_path}")
    else:
        # If file doesn't exist, create sample text data
        print(f"⚠ {text_file_path} not found, using sample data instead")
        sample_text = [
            "Apache Spark is a unified analytics engine for large-scale data processing",
            "Spark provides high-level APIs in Java, Scala, Python and R",
            "Spark offers over 80 high-level operators that make it easy to build parallel apps",
            "You can use Spark interactively from the Scala, Python, R, and SQL shells",
            "Spark runs on Hadoop, Apache Mesos, Kubernetes, standalone, or in the cloud",
            "It can access diverse data sources including HDFS, Alluxio, Apache Cassandra",
            "Spark provides a faster and more general data processing platform",
            "Apache Spark is a powerful open-source processing engine"
        ]
        text_rdd = sc.parallelize(sample_text)
    
    # Split each line into individual words
    # flatMap flattens the result, so instead of getting a list of lists,
    # we get a single flat list of all words
    words = text_rdd.flatMap(lambda line: line.split())
    
    # Create a tuple (word, 1) for each word - this is the classic MapReduce pattern
    word_pairs = words.map(lambda word: (word.lower().strip(".,"), 1))

    # Aggregate the counts for each unique word
    word_counts = word_pairs.reduceByKey(lambda a, b: a + b)
    
    # This triggers the actual computation and brings results back to driver
    results = word_counts.collect()
    
    # Display the top 10 most frequent words
    print("\nTop 10 most frequent words:")
    for word, count in sorted(results, key=lambda x: x[1], reverse=True)[:10]:
        print(f"{word}: {count}")
    
    print()
    
except Exception as e:
    print(f"Error during RDD operations: {str(e)}")
    print("Continuing with next task...")

# RDD vs DataFrame
print("Task 1.3: Performance Comparison - RDD vs DataFrame")
print("-" * 60)

try:
    # Generate sample data for testing
    data_size = 100000
    keys = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']
    
    print(f"Generating {data_size} records for performance testing...")
    # Create list of tuples: (key, value)
    data = [(random.choice(keys), random.randint(1, 100)) for _ in range(data_size)]
    print("✓ Data generation complete")
    print()
    
    # Method 1: Using RDD
    print("Method 1: Using RDD")
    print("Performing groupByKey and calculating averages...")
    
    start = time.time()
    rdd = sc.parallelize(data)
    result_rdd = rdd.groupByKey().mapValues(lambda x: sum(x) / len(list(x))).collect()
    rdd_time = time.time() - start
    
    print(f"Execution time: {rdd_time:.4f} seconds")
    print(f"Example results: {sorted(result_rdd)[:3]}")
    print()
    
    # Method 2: Using DataFrame
    print("Method 2: Using DataFrame")
    print("Performing groupBy with avg aggregation...")
    
    start = time.time()
    df = spark.createDataFrame(data, ["key", "value"])
    result_df = df.groupBy("key").avg("value").collect()
    df_time = time.time() - start
    
    print(f"  Execution time: {df_time:.4f} seconds")
    print(f"  Sample results: {sorted(result_df, key=lambda x: x[0])[:3]}")
    print()
    
    # Compare performance and display results
    print("Performance Analysis:")
    print("-" * 40)
    print(f"  RDD time:       {rdd_time:.4f} seconds")
    print(f"  DataFrame time: {df_time:.4f} seconds")
    
    # Determine which method was faster
    if df_time < rdd_time:
        speedup = rdd_time / df_time
        print(f"  DataFrame is {speedup:.2f}x faster")
    else:
        speedup = df_time / rdd_time
        print(f"  RDD is {speedup:.2f}x faster")
    
    print("Note: In local mode with moderate datasets, RDD may be faster due to")
    print("DataFrame overhead. DataFrames excel in distributed environments with")
    print("large-scale data where Catalyst optimization provides significant benefits.")
    
except Exception as e:
    print(f"Error during performance comparison: {str(e)}")

# Cleanup and Spark UI Exploration
print("=" * 60)
print("PART 1 COMPLETED")
print("=" * 60)
print("Spark UI available at: http://localhost:4040")
print("Press Ctrl+C to stop the Spark session when you're done exploring the UI")

# Keep Spark session running for UI exploration
try:
    input("Press Enter to stop Spark session...")
except KeyboardInterrupt:
    print("\nReceived interrupt signal...")

# Stop the Spark session to free up resources
try:
    spark.stop()
    print("Spark session stopped successfully")
except Exception as e:
    print(f"Warning: Error stopping Spark session: {str(e)}")