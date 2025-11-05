# Apache Spark Development

Complete implementation of Apache Spark lab covering RDDs, DataFrames, SQL operations, advanced features, and a sales dashboard.

## Quick Start

## Prerequisites

- Java 17 or higher
- Python 3.8+
- Apache Spark 4.0.1

**Note:** Ensure `JAVA_HOME` is set correctly:
```bash
# Check Java installation
java -version

# If needed, set JAVA_HOME (adjust path to your Java installation)
export JAVA_HOME=/path/to/your/jdk
export PATH=$JAVA_HOME/bin:$PATH
```

**For Spark:**
```bash
# Set SPARK_HOME if needed
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
```

### 2. Setup
```bash
# Clone the repository
git clone https://github.com/salmakiddo/Apache-Spark-Development.git
cd Apache-Spark-Development

# Create virtual environment
python3 -m venv spark_env
source spark_env/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Run the Labs

**Run in this order:**
```bash
# Part 1: Basics (RDD vs DataFrame)
python spark_basics.py

# Part 2: DataFrames and SQL (creates transactions.parquet)
python dataframe_operations.py

# Part 3: Advanced Features (UDFs, broadcast, accumulators)
python advanced_features.py

# Part 4: Spark UI and Debugging
python spark_ui_debugging.py

# Part 5: Sales Dashboard (generates reports)
python sales_dashboard.py
```

**Note:** Press Enter after each script to stop the Spark session.

## Viewing Spark UI

While any script is running:

**Local machine:** Open http://localhost:4040

**Remote VM (GCP/AWS):** 
```bash
# On local machine terminal:
ssh -L 4040:localhost:4040 your-username@your-vm-ip

# Then open: http://localhost:4040
```

## Output Files

Part 2 creates:
- `transactions.parquet/` - Transaction data (required by Parts 3-5)

Part 5 creates:
- `kpis.json` - Key performance indicators
- `top_products_output/` - Top 5 products analysis
- `monthly_sales_output/` - Monthly trends
- `customer_segments_output/` - Customer segmentation

Extract CSV files:
```bash
cp top_products_output/part-*.csv top_products.csv
cp monthly_sales_output/part-*.csv monthly_sales.csv
cp customer_segments_output/part-*.csv customer_segments.csv
```

## Troubleshooting

**"transactions.parquet not found"**
- Run Part 2 first: `python dataframe_operations.py`

**Permission errors**
```bash
sudo chown -R $(whoami):$(whoami) .
chmod 644 *.py
```

**Port 4040 in use**
```bash
# Kill existing Spark sessions
ps aux | grep spark
kill <process_id>
```

## What Each Part Does

- **Part 1:** Spark session initialization, RDD operations, performance comparison
- **Part 2:** DataFrame creation, SQL queries, data aggregations
- **Part 3:** UDFs, broadcast variables, accumulators, optimization techniques
- **Part 4:** Execution plan analysis, Spark UI exploration
- **Part 5:** Complete analytics dashboard with business metrics

## Lab Results

Performance improvements achieved:
- Partition optimization: 63.84% faster
- Predicate pushdown: 55.35% faster
- Caching: 1.02x speedup (small dataset)

## Technologies Used

- Apache Spark 4.0.1
- PySpark
- Python 3.8+
- Parquet format
- Spark SQL
