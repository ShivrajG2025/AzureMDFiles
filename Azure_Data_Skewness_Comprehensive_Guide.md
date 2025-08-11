# Azure Data Skewness - Comprehensive Guide
## Complete Analysis of Data Skewness Issues and Solutions in Azure

---

### Table of Contents

1. [Introduction to Data Skewness](#introduction-to-data-skewness)
2. [Types of Data Skewness](#types-of-data-skewness)
3. [Impact on Azure Services](#impact-on-azure-services)
4. [Detection and Measurement](#detection-and-measurement)
5. [Data Skewness in Azure Databricks](#data-skewness-in-azure-databricks)
6. [Data Skewness in Azure Synapse Analytics](#data-skewness-in-azure-synapse-analytics)
7. [Data Skewness in Azure Data Factory](#data-skewness-in-azure-data-factory)
8. [Mitigation Strategies](#mitigation-strategies)
9. [Advanced Techniques](#advanced-techniques)
10. [Monitoring and Alerting](#monitoring-and-alerting)
11. [Best Practices](#best-practices)
12. [Real-World Case Studies](#real-world-case-studies)
13. [Performance Optimization](#performance-optimization)
14. [Conclusion](#conclusion)

---

## Introduction to Data Skewness

Data skewness is a critical performance issue in distributed computing environments where data is unevenly distributed across partitions, nodes, or processing units. In Azure's big data ecosystem, data skewness can significantly impact the performance of Spark jobs, SQL queries, and data processing pipelines.

### What is Data Skewness?

```python
# Example of skewed data distribution
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Simulate skewed data
def create_skewed_dataset():
    """
    Create a dataset with intentional skewness to demonstrate the problem
    """
    
    # Highly skewed customer data (80% of data belongs to 20% of customers)
    np.random.seed(42)
    
    # Create customer IDs with Pareto distribution (80-20 rule)
    total_customers = 1000
    total_transactions = 100000
    
    # 80% of transactions from 20% of customers (power law distribution)
    high_volume_customers = np.random.choice(
        range(1, int(total_customers * 0.2)), 
        size=int(total_transactions * 0.8),
        replace=True
    )
    
    # 20% of transactions from remaining 80% of customers
    low_volume_customers = np.random.choice(
        range(int(total_customers * 0.2), total_customers),
        size=int(total_transactions * 0.2),
        replace=True
    )
    
    # Combine customer IDs
    all_customer_ids = np.concatenate([high_volume_customers, low_volume_customers])
    np.random.shuffle(all_customer_ids)
    
    # Create transaction data
    skewed_data = pd.DataFrame({
        'transaction_id': range(1, total_transactions + 1),
        'customer_id': all_customer_ids,
        'transaction_amount': np.random.uniform(10, 1000, total_transactions),
        'transaction_date': pd.date_range('2023-01-01', periods=total_transactions, freq='5min'),
        'product_category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home'], total_transactions),
        'region': np.random.choice(['North', 'South', 'East', 'West'], total_transactions)
    })
    
    return skewed_data

# Analyze skewness
def analyze_data_skewness(df):
    """
    Analyze the degree of skewness in the dataset
    """
    
    # Customer transaction count distribution
    customer_txn_count = df.groupby('customer_id').size().reset_index(name='transaction_count')
    
    # Calculate skewness metrics
    skewness_stats = {
        'total_customers': customer_txn_count['customer_id'].nunique(),
        'total_transactions': len(df),
        'avg_transactions_per_customer': customer_txn_count['transaction_count'].mean(),
        'median_transactions_per_customer': customer_txn_count['transaction_count'].median(),
        'max_transactions_per_customer': customer_txn_count['transaction_count'].max(),
        'min_transactions_per_customer': customer_txn_count['transaction_count'].min(),
        'std_transactions_per_customer': customer_txn_count['transaction_count'].std()
    }
    
    # Pareto analysis (80-20 rule)
    customer_txn_sorted = customer_txn_count.sort_values('transaction_count', ascending=False)
    customer_txn_sorted['cumulative_transactions'] = customer_txn_sorted['transaction_count'].cumsum()
    customer_txn_sorted['cumulative_pct'] = (customer_txn_sorted['cumulative_transactions'] / 
                                           customer_txn_sorted['transaction_count'].sum() * 100)
    
    # Find what percentage of customers account for 80% of transactions
    customers_80pct = customer_txn_sorted[customer_txn_sorted['cumulative_pct'] <= 80]
    pct_customers_80pct_txns = (len(customers_80pct) / len(customer_txn_sorted)) * 100
    
    skewness_stats['pct_customers_80pct_transactions'] = pct_customers_80pct_txns
    
    return skewness_stats, customer_txn_count

# Example usage
skewed_df = create_skewed_dataset()
skewness_stats, customer_distribution = analyze_data_skewness(skewed_df)

print("=== DATA SKEWNESS ANALYSIS ===")
for key, value in skewness_stats.items():
    print(f"{key}: {value:.2f}")

# Visualize skewness
def visualize_skewness(customer_distribution):
    """
    Create visualizations to show data skewness
    """
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    
    # 1. Histogram of transactions per customer
    axes[0, 0].hist(customer_distribution['transaction_count'], bins=50, alpha=0.7)
    axes[0, 0].set_title('Distribution of Transactions per Customer')
    axes[0, 0].set_xlabel('Number of Transactions')
    axes[0, 0].set_ylabel('Number of Customers')
    
    # 2. Top 20 customers by transaction count
    top_customers = customer_distribution.nlargest(20, 'transaction_count')
    axes[0, 1].bar(range(len(top_customers)), top_customers['transaction_count'])
    axes[0, 1].set_title('Top 20 Customers by Transaction Count')
    axes[0, 1].set_xlabel('Customer Rank')
    axes[0, 1].set_ylabel('Transaction Count')
    
    # 3. Cumulative distribution (Pareto chart)
    customer_sorted = customer_distribution.sort_values('transaction_count', ascending=False)
    customer_sorted['cumulative_pct'] = (customer_sorted['transaction_count'].cumsum() / 
                                        customer_sorted['transaction_count'].sum() * 100)
    
    axes[1, 0].plot(range(len(customer_sorted)), customer_sorted['cumulative_pct'])
    axes[1, 0].axhline(y=80, color='r', linestyle='--', label='80% line')
    axes[1, 0].set_title('Cumulative Transaction Distribution (Pareto Analysis)')
    axes[1, 0].set_xlabel('Customer Rank')
    axes[1, 0].set_ylabel('Cumulative % of Transactions')
    axes[1, 0].legend()
    
    # 4. Box plot showing distribution
    axes[1, 1].boxplot(customer_distribution['transaction_count'])
    axes[1, 1].set_title('Box Plot of Transactions per Customer')
    axes[1, 1].set_ylabel('Transaction Count')
    
    plt.tight_layout()
    plt.savefig('data_skewness_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

# Execute visualization
visualize_skewness(customer_distribution)
```

### Skewness Characteristics

```python
# Mathematical definition of skewness
def calculate_statistical_skewness(data):
    """
    Calculate statistical measures of skewness
    """
    
    from scipy import stats
    
    skewness_metrics = {
        'pearson_skewness_1': (data.mean() - data.mode().iloc[0]) / data.std(),
        'pearson_skewness_2': 3 * (data.mean() - data.median()) / data.std(),
        'scipy_skewness': stats.skew(data),
        'kurtosis': stats.kurtosis(data),
        'coefficient_of_variation': data.std() / data.mean(),
        'gini_coefficient': calculate_gini_coefficient(data)
    }
    
    return skewness_metrics

def calculate_gini_coefficient(data):
    """
    Calculate Gini coefficient to measure inequality
    """
    
    sorted_data = np.sort(data)
    n = len(sorted_data)
    cumsum = np.cumsum(sorted_data)
    
    return (n + 1 - 2 * np.sum(cumsum) / cumsum[-1]) / n

# Apply to our customer transaction data
customer_txn_counts = customer_distribution['transaction_count']
skewness_metrics = calculate_statistical_skewness(customer_txn_counts)

print("\n=== STATISTICAL SKEWNESS METRICS ===")
for metric, value in skewness_metrics.items():
    print(f"{metric}: {value:.4f}")

# Interpretation guide
skewness_interpretation = {
    "scipy_skewness": {
        "< -1": "Highly left-skewed (negative skew)",
        "-1 to -0.5": "Moderately left-skewed", 
        "-0.5 to 0.5": "Approximately symmetric",
        "0.5 to 1": "Moderately right-skewed",
        "> 1": "Highly right-skewed (positive skew)"
    },
    "gini_coefficient": {
        "0": "Perfect equality (no skewness)",
        "0.1-0.3": "Low inequality",
        "0.3-0.5": "Moderate inequality", 
        "0.5-0.7": "High inequality",
        "> 0.7": "Very high inequality (severe skewness)"
    }
}

print(f"\nInterpretation:")
scipy_skew = skewness_metrics['scipy_skewness']
gini_coeff = skewness_metrics['gini_coefficient']

if scipy_skew > 1:
    print(f"Data is highly right-skewed (scipy skewness: {scipy_skew:.4f})")
elif scipy_skew > 0.5:
    print(f"Data is moderately right-skewed (scipy skewness: {scipy_skew:.4f})")
else:
    print(f"Data skewness is within normal range (scipy skewness: {scipy_skew:.4f})")

if gini_coeff > 0.7:
    print(f"Very high inequality detected (Gini coefficient: {gini_coeff:.4f})")
elif gini_coeff > 0.5:
    print(f"High inequality detected (Gini coefficient: {gini_coeff:.4f})")
else:
    print(f"Moderate inequality (Gini coefficient: {gini_coeff:.4f})")
```

---

## Types of Data Skewness

### 1. Partition Skewness (Data Skew)

```python
# Example of partition skewness in Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def demonstrate_partition_skewness():
    """
    Demonstrate how partition skewness affects Spark performance
    """
    
    spark = SparkSession.builder \
        .appName("DataSkewnessDemo") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Create skewed dataset
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("transaction_id", LongType(), True),
        StructField("amount", DoubleType(), True),
        StructField("region", StringType(), True),
        StructField("date", DateType(), True)
    ])
    
    # Simulate highly skewed data - 90% of data in one region
    import random
    from datetime import date, timedelta
    
    skewed_data = []
    total_records = 1000000
    
    for i in range(total_records):
        # 90% of records go to "NORTH" region (hot partition)
        if i < total_records * 0.9:
            region = "NORTH"
            customer_id = random.randint(1, 100)  # Limited customers in hot region
        else:
            # Remaining 10% distributed among other regions
            region = random.choice(["SOUTH", "EAST", "WEST"])
            customer_id = random.randint(101, 10000)
        
        record = (
            customer_id,
            i + 1,
            random.uniform(10.0, 1000.0),
            region,
            date(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        )
        skewed_data.append(record)
    
    # Create DataFrame
    df = spark.createDataFrame(skewed_data, schema)
    
    # Partition by region (this will create skewed partitions)
    df_partitioned = df.repartition(col("region"))
    
    print("=== PARTITION SKEWNESS ANALYSIS ===")
    
    # Analyze partition sizes
    partition_stats = df_partitioned.rdd.mapPartitionsWithIndex(
        lambda idx, iterator: [(idx, sum(1 for _ in iterator))]
    ).collect()
    
    print("Partition sizes:")
    for partition_id, size in partition_stats:
        print(f"Partition {partition_id}: {size:,} records")
    
    # Show the impact on aggregation
    print("\n=== AGGREGATION PERFORMANCE IMPACT ===")
    
    # This aggregation will be slow due to skewed partitions
    start_time = time.time()
    region_summary = df_partitioned.groupBy("region") \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy(desc("transaction_count"))
    
    region_summary.show()
    end_time = time.time()
    
    print(f"Aggregation time with skewed partitions: {end_time - start_time:.2f} seconds")
    
    return df, df_partitioned

# Execute partition skewness demo
df_original, df_skewed = demonstrate_partition_skewness()
```

### 2. Join Skewness

```python
def demonstrate_join_skewness():
    """
    Demonstrate join skewness and its performance impact
    """
    
    spark = SparkSession.builder.getOrCreate()
    
    # Create two datasets with skewed join keys
    
    # Dataset 1: Customer transactions (highly skewed by customer_id)
    customers_data = []
    total_transactions = 500000
    
    # Create Pareto distribution for customers
    # 80% of transactions belong to 20% of customers
    vip_customers = list(range(1, 201))  # Top 200 VIP customers
    regular_customers = list(range(201, 10001))  # Regular customers
    
    for i in range(total_transactions):
        if i < total_transactions * 0.8:  # 80% transactions from VIP customers
            customer_id = random.choice(vip_customers)
        else:  # 20% from regular customers
            customer_id = random.choice(regular_customers)
        
        customers_data.append((
            i + 1,  # transaction_id
            customer_id,
            random.uniform(100, 5000),  # amount
            random.choice(["Online", "Store", "Mobile"])  # channel
        ))
    
    transactions_df = spark.createDataFrame(
        customers_data,
        ["transaction_id", "customer_id", "amount", "channel"]
    )
    
    # Dataset 2: Customer details (uniform distribution)
    customer_details = []
    for customer_id in range(1, 10001):
        customer_details.append((
            customer_id,
            f"Customer_{customer_id}",
            random.choice(["Gold", "Silver", "Bronze"]),
            random.choice(["North", "South", "East", "West"]),
            random.randint(25, 65)  # age
        ))
    
    customers_df = spark.createDataFrame(
        customer_details,
        ["customer_id", "customer_name", "tier", "region", "age"]
    )
    
    print("=== JOIN SKEWNESS ANALYSIS ===")
    
    # Analyze join key distribution
    print("Transaction distribution by customer:")
    txn_by_customer = transactions_df.groupBy("customer_id") \
        .count() \
        .orderBy(desc("count"))
    
    print("Top 10 customers by transaction count:")
    txn_by_customer.show(10)
    
    # Perform skewed join
    print("\n=== PERFORMING SKEWED JOIN ===")
    start_time = time.time()
    
    # This join will be slow due to skewed keys
    joined_df = transactions_df.join(customers_df, "customer_id", "inner")
    
    # Force execution
    result_count = joined_df.count()
    end_time = time.time()
    
    print(f"Join completed: {result_count:,} records")
    print(f"Join time with skewed keys: {end_time - start_time:.2f} seconds")
    
    # Show join result summary
    join_summary = joined_df.groupBy("tier", "region") \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        ) \
        .orderBy(desc("transaction_count"))
    
    join_summary.show()
    
    return transactions_df, customers_df, joined_df

# Execute join skewness demo
txn_df, cust_df, joined_result = demonstrate_join_skewness()
```

### 3. Aggregation Skewness

```python
def demonstrate_aggregation_skewness():
    """
    Demonstrate aggregation skewness in group-by operations
    """
    
    spark = SparkSession.builder.getOrCreate()
    
    # Create dataset with skewed group-by keys
    sales_data = []
    total_sales = 1000000
    
    # Simulate seasonal sales pattern with extreme skewness
    # 70% of sales in December (holiday season)
    # 20% in November
    # 10% distributed across other months
    
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    
    for i in range(total_sales):
        rand = random.random()
        
        if rand < 0.7:  # 70% in December
            month = "Dec"
        elif rand < 0.9:  # 20% in November
            month = "Nov"
        else:  # 10% in other months
            month = random.choice(months[:-2])  # Exclude Nov and Dec
        
        sales_data.append((
            i + 1,  # sale_id
            month,
            random.choice(["Electronics", "Clothing", "Books", "Home"]),  # category
            random.choice(["Online", "Store"]),  # channel
            random.uniform(10, 500),  # amount
            random.randint(1, 1000)  # customer_id
        ))
    
    sales_df = spark.createDataFrame(
        sales_data,
        ["sale_id", "month", "category", "channel", "amount", "customer_id"]
    )
    
    print("=== AGGREGATION SKEWNESS ANALYSIS ===")
    
    # Show data distribution by month
    monthly_distribution = sales_df.groupBy("month") \
        .count() \
        .orderBy(desc("count"))
    
    print("Sales distribution by month:")
    monthly_distribution.show()
    
    # Perform aggregation that will be skewed
    print("\n=== PERFORMING SKEWED AGGREGATION ===")
    start_time = time.time()
    
    # Complex aggregation with skewed group-by key
    monthly_summary = sales_df.groupBy("month", "category", "channel") \
        .agg(
            count("*").alias("sale_count"),
            sum("amount").alias("total_sales"),
            avg("amount").alias("avg_sale_amount"),
            countDistinct("customer_id").alias("unique_customers"),
            min("amount").alias("min_sale"),
            max("amount").alias("max_sale"),
            stddev("amount").alias("stddev_sale")
        ) \
        .orderBy(desc("sale_count"))
    
    # Force execution and measure time
    result_count = monthly_summary.count()
    monthly_summary.show(20)
    end_time = time.time()
    
    print(f"Aggregation completed: {result_count} summary records")
    print(f"Aggregation time with skewed groups: {end_time - start_time:.2f} seconds")
    
    # Analyze partition distribution during aggregation
    print("\n=== PARTITION ANALYSIS DURING AGGREGATION ===")
    
    # Check how data is distributed across partitions
    partition_distribution = sales_df.rdd.mapPartitionsWithIndex(
        lambda idx, iterator: [(idx, sum(1 for _ in iterator))]
    ).collect()
    
    print("Original partition distribution:")
    for partition_id, size in partition_distribution:
        print(f"Partition {partition_id}: {size:,} records")
    
    return sales_df, monthly_summary

# Execute aggregation skewness demo
sales_data, monthly_agg = demonstrate_aggregation_skewness()
```

---

## Impact on Azure Services

### Azure Databricks Performance Impact

```python
def analyze_databricks_skewness_impact():
    """
    Analyze how data skewness impacts Azure Databricks performance
    """
    
    # Databricks-specific configurations for handling skewness
    databricks_configs = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true", 
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0"
    }
    
    print("=== AZURE DATABRICKS SKEWNESS IMPACT ===")
    
    # Performance metrics affected by skewness
    performance_impacts = {
        "task_execution_time": {
            "description": "Uneven task execution times",
            "skewed_impact": "Some tasks take 10x longer than others",
            "symptoms": [
                "Long-running stages with few active tasks",
                "Resource underutilization",
                "Job completion time determined by slowest task"
            ],
            "measurement": "Check Spark UI stage timeline"
        },
        "memory_usage": {
            "description": "Uneven memory distribution",
            "skewed_impact": "Hot partitions cause OOM errors",
            "symptoms": [
                "OutOfMemoryError on specific executors",
                "Excessive garbage collection",
                "Spilling to disk on hot partitions"
            ],
            "measurement": "Monitor executor memory usage in Spark UI"
        },
        "shuffle_operations": {
            "description": "Inefficient data shuffling",
            "skewed_impact": "Network bottlenecks and disk I/O spikes",
            "symptoms": [
                "High shuffle read/write times",
                "Network saturation on specific nodes",
                "Disk space issues on hot executors"
            ],
            "measurement": "Analyze shuffle metrics in Spark UI"
        },
        "cluster_utilization": {
            "description": "Poor cluster resource utilization",
            "skewed_impact": "Most nodes idle while few are overloaded",
            "symptoms": [
                "Low CPU utilization across cluster",
                "Uneven disk usage",
                "Auto-scaling inefficiency"
            ],
            "measurement": "Monitor cluster metrics in Databricks"
        }
    }
    
    # Cost implications
    cost_implications = {
        "compute_waste": {
            "description": "Paying for idle compute resources",
            "cost_factor": "2-5x higher compute costs",
            "example": "10-node cluster with only 2 nodes actively processing"
        },
        "job_duration": {
            "description": "Longer job execution times",
            "cost_factor": "Proportional increase in DBU consumption",
            "example": "4-hour job could complete in 1 hour with proper data distribution"
        },
        "auto_scaling": {
            "description": "Ineffective auto-scaling decisions",
            "cost_factor": "Over-provisioning due to misleading metrics",
            "example": "Scaling up when only partition skewness exists"
        }
    }
    
    return databricks_configs, performance_impacts, cost_implications

# Databricks monitoring queries
def create_databricks_monitoring_queries():
    """
    SQL queries to monitor skewness in Databricks
    """
    
    monitoring_queries = {
        "partition_size_analysis": """
        -- Analyze partition sizes for a table
        SELECT 
            input_file_name() as file_path,
            count(*) as record_count,
            round(sum(length(column_name)) / 1024 / 1024, 2) as size_mb
        FROM your_table_name
        GROUP BY input_file_name()
        ORDER BY record_count DESC
        """,
        
        "skewed_join_detection": """
        -- Detect skewed join keys
        WITH join_key_stats AS (
            SELECT 
                join_key,
                count(*) as frequency,
                count(*) * 100.0 / sum(count(*)) OVER() as percentage
            FROM your_table_name
            GROUP BY join_key
        )
        SELECT 
            join_key,
            frequency,
            percentage,
            CASE 
                WHEN percentage > 10 THEN 'HIGHLY_SKEWED'
                WHEN percentage > 5 THEN 'MODERATELY_SKEWED'
                ELSE 'NORMAL'
            END as skew_level
        FROM join_key_stats
        ORDER BY frequency DESC
        LIMIT 20
        """,
        
        "data_distribution_analysis": """
        -- Analyze data distribution across partitions
        SELECT 
            partition_column,
            count(*) as record_count,
            min(date_column) as min_date,
            max(date_column) as max_date,
            approx_count_distinct(other_column) as distinct_values
        FROM your_partitioned_table
        GROUP BY partition_column
        ORDER BY record_count DESC
        """
    }
    
    return monitoring_queries

# Execute Databricks analysis
databricks_configs, perf_impacts, cost_impacts = analyze_databricks_skewness_impact()
monitoring_queries = create_databricks_monitoring_queries()

print("Recommended Databricks configurations for skewness handling:")
for config, value in databricks_configs.items():
    print(f"  {config}: {value}")
```

### Azure Synapse Analytics Impact

```python
def analyze_synapse_skewness_impact():
    """
    Analyze data skewness impact in Azure Synapse Analytics
    """
    
    print("=== AZURE SYNAPSE ANALYTICS SKEWNESS IMPACT ===")
    
    # Synapse-specific skewness issues
    synapse_skewness_issues = {
        "distribution_skew": {
            "description": "Uneven data distribution across compute nodes",
            "causes": [
                "Poor hash distribution key selection",
                "Natural data skewness in distribution column",
                "Replicated tables that are too large"
            ],
            "impact": [
                "Some compute nodes overloaded while others idle",
                "Query performance degradation",
                "Resource waste and higher costs"
            ],
            "detection_query": """
            -- Detect distribution skew in Synapse
            SELECT 
                distribution_id,
                count(*) as row_count,
                count(*) * 100.0 / sum(count(*)) OVER() as percentage
            FROM sys.dm_pdw_nodes_db_partition_stats pnps
            INNER JOIN sys.dm_pdw_nodes_tables pnt
                ON pnps.object_id = pnt.object_id
                AND pnps.pdw_node_id = pnt.pdw_node_id
            WHERE pnt.name = 'your_table_name'
            GROUP BY distribution_id
            ORDER BY row_count DESC
            """
        },
        
        "compute_skew": {
            "description": "Uneven compute resource utilization",
            "causes": [
                "Skewed data leading to uneven workload",
                "Complex queries with skewed predicates",
                "Join operations on skewed keys"
            ],
            "impact": [
                "Long-running queries with poor parallelism",
                "DWU underutilization",
                "Increased query costs"
            ],
            "detection_query": """
            -- Monitor compute skew
            SELECT 
                r.request_id,
                r.command,
                r.start_time,
                r.end_time,
                datediff(ms, r.start_time, r.end_time) as duration_ms,
                s.distribution_id,
                s.row_count,
                s.used_memory_kb
            FROM sys.dm_pdw_exec_requests r
            JOIN sys.dm_pdw_request_steps rs ON r.request_id = rs.request_id
            JOIN sys.dm_pdw_sql_requests sr ON rs.request_id = sr.request_id
            JOIN sys.dm_pdw_nodes_db_partition_stats s ON sr.spid = s.pdw_node_id
            WHERE r.status = 'Running'
            ORDER BY duration_ms DESC
            """
        },
        
        "storage_skew": {
            "description": "Uneven storage utilization across distributions",
            "causes": [
                "Large fact tables with poor distribution",
                "Temporal data with date-based skewness",
                "Categorical data with uneven distribution"
            ],
            "impact": [
                "Storage hotspots",
                "I/O bottlenecks",
                "Backup and maintenance issues"
            ],
            "detection_query": """
            -- Analyze storage distribution
            SELECT 
                t.name as table_name,
                p.distribution_policy_desc,
                SUM(s.row_count) as total_rows,
                MAX(s.row_count) as max_distribution_rows,
                MIN(s.row_count) as min_distribution_rows,
                (MAX(s.row_count) * 1.0 / NULLIF(MIN(s.row_count), 0)) as skew_ratio
            FROM sys.tables t
            JOIN sys.pdw_table_distribution_properties p ON t.object_id = p.object_id
            JOIN sys.dm_pdw_table_partition_stats s ON t.object_id = s.object_id
            GROUP BY t.name, p.distribution_policy_desc
            HAVING (MAX(s.row_count) * 1.0 / NULLIF(MIN(s.row_count), 0)) > 2
            ORDER BY skew_ratio DESC
            """
        }
    }
    
    # Synapse optimization strategies
    synapse_optimization_strategies = {
        "distribution_key_selection": {
            "strategy": "Choose optimal distribution keys",
            "implementation": [
                "Select high cardinality columns",
                "Avoid columns with natural skewness", 
                "Consider composite distribution keys",
                "Use round-robin for small tables"
            ],
            "example_ddl": """
            -- Good distribution key selection
            CREATE TABLE sales_fact (
                sale_id BIGINT,
                customer_id INT,
                product_id INT,
                sale_date DATE,
                amount DECIMAL(10,2)
            )
            WITH (
                DISTRIBUTION = HASH(customer_id), -- High cardinality, even distribution
                CLUSTERED COLUMNSTORE INDEX
            )
            """
        },
        
        "table_partitioning": {
            "strategy": "Implement effective partitioning",
            "implementation": [
                "Partition by date for temporal data",
                "Align partitions with query patterns",
                "Avoid over-partitioning",
                "Consider partition elimination"
            ],
            "example_ddl": """
            -- Effective partitioning strategy
            CREATE TABLE sales_fact (
                sale_id BIGINT,
                customer_id INT,
                sale_date DATE,
                amount DECIMAL(10,2)
            )
            WITH (
                DISTRIBUTION = HASH(customer_id),
                PARTITION (sale_date RANGE RIGHT FOR VALUES 
                    ('2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01'))
            )
            """
        },
        
        "query_optimization": {
            "strategy": "Optimize queries for skewed data",
            "implementation": [
                "Use appropriate join hints",
                "Implement pre-aggregation",
                "Use materialized views",
                "Apply query result caching"
            ],
            "example_sql": """
            -- Optimized query for skewed joins
            SELECT /*+ USE_HINT('REDISTRIBUTE_JOIN') */
                c.customer_name,
                SUM(s.amount) as total_sales
            FROM sales_fact s
            JOIN customer_dim c ON s.customer_id = c.customer_id
            WHERE s.sale_date >= '2023-01-01'
            GROUP BY c.customer_name
            OPTION (LABEL = 'Skew-optimized sales query')
            """
        }
    }
    
    return synapse_skewness_issues, synapse_optimization_strategies

# Execute Synapse analysis
synapse_issues, synapse_optimizations = analyze_synapse_skewness_impact()

print("Synapse skewness detection and optimization strategies loaded.")
```

---

## Detection and Measurement

### Automated Skewness Detection Framework

```python
class DataSkewnessDetector:
    """
    Comprehensive framework for detecting and measuring data skewness
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def detect_partition_skewness(self, df, threshold_ratio=5.0):
        """
        Detect partition skewness in a DataFrame
        """
        
        # Get partition sizes
        partition_sizes = df.rdd.mapPartitionsWithIndex(
            lambda idx, iterator: [(idx, sum(1 for _ in iterator))]
        ).collect()
        
        if not partition_sizes:
            return {"skewed": False, "reason": "No partitions found"}
        
        sizes = [size for _, size in partition_sizes]
        max_size = max(sizes)
        min_size = min(sizes) if min(sizes) > 0 else 1
        avg_size = sum(sizes) / len(sizes)
        
        skew_ratio = max_size / min_size if min_size > 0 else float('inf')
        
        analysis = {
            "skewed": skew_ratio > threshold_ratio,
            "skew_ratio": skew_ratio,
            "max_partition_size": max_size,
            "min_partition_size": min_size,
            "avg_partition_size": avg_size,
            "total_partitions": len(partition_sizes),
            "partition_details": partition_sizes,
            "recommendation": self._get_partition_skew_recommendation(skew_ratio, sizes)
        }
        
        return analysis
    
    def detect_key_skewness(self, df, key_column, threshold_percentage=10.0):
        """
        Detect skewness in key distribution (for joins/aggregations)
        """
        
        total_records = df.count()
        
        # Analyze key distribution
        key_distribution = df.groupBy(key_column) \
            .count() \
            .withColumn("percentage", (col("count") * 100.0 / total_records)) \
            .orderBy(desc("count"))
        
        # Get top keys and their statistics
        top_keys = key_distribution.limit(20).collect()
        
        # Calculate skewness metrics
        key_counts = [row["count"] for row in top_keys]
        
        if not key_counts:
            return {"skewed": False, "reason": "No keys found"}
        
        max_count = max(key_counts)
        total_count = sum(key_counts)
        max_percentage = (max_count / total_records) * 100
        
        # Check for hot keys
        hot_keys = [row for row in top_keys if row["percentage"] > threshold_percentage]
        
        analysis = {
            "skewed": len(hot_keys) > 0,
            "max_key_percentage": max_percentage,
            "hot_keys_count": len(hot_keys),
            "hot_keys": hot_keys[:10],  # Top 10 hot keys
            "total_unique_keys": df.select(key_column).distinct().count(),
            "gini_coefficient": self._calculate_gini_coefficient([row["count"] for row in top_keys]),
            "recommendation": self._get_key_skew_recommendation(max_percentage, len(hot_keys))
        }
        
        return analysis
    
    def detect_temporal_skewness(self, df, date_column, granularity="day"):
        """
        Detect temporal data skewness
        """
        
        # Extract time components based on granularity
        if granularity == "hour":
            time_component = hour(col(date_column))
        elif granularity == "day":
            time_component = dayofweek(col(date_column))
        elif granularity == "month":
            time_component = month(col(date_column))
        elif granularity == "year":
            time_component = year(col(date_column))
        else:
            time_component = date_trunc(granularity, col(date_column))
        
        # Analyze temporal distribution
        temporal_distribution = df.withColumn("time_component", time_component) \
            .groupBy("time_component") \
            .count() \
            .orderBy(desc("count"))
        
        temporal_stats = temporal_distribution.collect()
        
        if not temporal_stats:
            return {"skewed": False, "reason": "No temporal data found"}
        
        counts = [row["count"] for row in temporal_stats]
        max_count = max(counts)
        min_count = min(counts)
        avg_count = sum(counts) / len(counts)
        
        # Calculate coefficient of variation
        std_dev = (sum((x - avg_count) ** 2 for x in counts) / len(counts)) ** 0.5
        cv = std_dev / avg_count if avg_count > 0 else 0
        
        analysis = {
            "skewed": cv > 0.5,  # Threshold for temporal skewness
            "coefficient_of_variation": cv,
            "max_period_count": max_count,
            "min_period_count": min_count,
            "avg_period_count": avg_count,
            "temporal_distribution": temporal_stats,
            "recommendation": self._get_temporal_skew_recommendation(cv, granularity)
        }
        
        return analysis
    
    def comprehensive_skewness_analysis(self, df, key_columns=None, date_column=None):
        """
        Perform comprehensive skewness analysis
        """
        
        analysis_results = {
            "table_info": {
                "total_records": df.count(),
                "total_columns": len(df.columns),
                "partition_count": df.rdd.getNumPartitions()
            }
        }
        
        # 1. Partition skewness analysis
        print("Analyzing partition skewness...")
        analysis_results["partition_skewness"] = self.detect_partition_skewness(df)
        
        # 2. Key skewness analysis
        if key_columns:
            analysis_results["key_skewness"] = {}
            for key_col in key_columns:
                print(f"Analyzing key skewness for column: {key_col}")
                analysis_results["key_skewness"][key_col] = self.detect_key_skewness(df, key_col)
        
        # 3. Temporal skewness analysis
        if date_column:
            print("Analyzing temporal skewness...")
            analysis_results["temporal_skewness"] = {
                "daily": self.detect_temporal_skewness(df, date_column, "day"),
                "monthly": self.detect_temporal_skewness(df, date_column, "month"),
                "hourly": self.detect_temporal_skewness(df, date_column, "hour")
            }
        
        # 4. Overall assessment
        analysis_results["overall_assessment"] = self._generate_overall_assessment(analysis_results)
        
        return analysis_results
    
    def _calculate_gini_coefficient(self, values):
        """Calculate Gini coefficient for inequality measurement"""
        if not values or len(values) == 0:
            return 0
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        cumsum = 0
        
        for i, value in enumerate(sorted_values):
            cumsum += value * (n - i)
        
        return (2 * cumsum) / (n * sum(sorted_values)) - (n + 1) / n
    
    def _get_partition_skew_recommendation(self, skew_ratio, sizes):
        """Generate recommendations for partition skewness"""
        if skew_ratio <= 2:
            return "Partition distribution is acceptable"
        elif skew_ratio <= 5:
            return "Consider repartitioning with better key or increasing partition count"
        else:
            return "Severe partition skewness detected. Immediate repartitioning required"
    
    def _get_key_skew_recommendation(self, max_percentage, hot_keys_count):
        """Generate recommendations for key skewness"""
        if max_percentage <= 5:
            return "Key distribution is acceptable"
        elif max_percentage <= 15:
            return "Consider salting technique or broadcast join for hot keys"
        else:
            return "Severe key skewness. Implement advanced skew handling techniques"
    
    def _get_temporal_skew_recommendation(self, cv, granularity):
        """Generate recommendations for temporal skewness"""
        if cv <= 0.3:
            return f"Temporal distribution at {granularity} level is acceptable"
        elif cv <= 0.7:
            return f"Consider partitioning by {granularity} for better distribution"
        else:
            return f"Severe temporal skewness at {granularity} level. Review data collection patterns"
    
    def _generate_overall_assessment(self, analysis_results):
        """Generate overall skewness assessment"""
        issues = []
        
        if analysis_results.get("partition_skewness", {}).get("skewed", False):
            issues.append("Partition skewness detected")
        
        if analysis_results.get("key_skewness"):
            for key, analysis in analysis_results["key_skewness"].items():
                if analysis.get("skewed", False):
                    issues.append(f"Key skewness in column: {key}")
        
        if analysis_results.get("temporal_skewness"):
            for granularity, analysis in analysis_results["temporal_skewness"].items():
                if analysis.get("skewed", False):
                    issues.append(f"Temporal skewness at {granularity} level")
        
        if not issues:
            severity = "LOW"
            recommendation = "Data distribution is acceptable. Continue monitoring."
        elif len(issues) <= 2:
            severity = "MEDIUM" 
            recommendation = "Some skewness detected. Consider optimization techniques."
        else:
            severity = "HIGH"
            recommendation = "Multiple skewness issues detected. Immediate optimization required."
        
        return {
            "severity": severity,
            "issues_detected": issues,
            "recommendation": recommendation,
            "priority": "High" if severity == "HIGH" else "Medium" if severity == "MEDIUM" else "Low"
        }

# Usage example
def run_skewness_detection_example():
    """
    Run comprehensive skewness detection on sample data
    """
    
    spark = SparkSession.builder.getOrCreate()
    
    # Create sample skewed dataset
    sample_data = create_skewed_dataset()  # Using function from earlier
    df = spark.createDataFrame(sample_data)
    
    # Initialize detector
    detector = DataSkewnessDetector(spark)
    
    # Run comprehensive analysis
    results = detector.comprehensive_skewness_analysis(
        df=df,
        key_columns=["customer_id", "region"],
        date_column="transaction_date"
    )
    
    # Print results
    print("=== COMPREHENSIVE SKEWNESS ANALYSIS RESULTS ===")
    
    print(f"Total Records: {results['table_info']['total_records']:,}")
    print(f"Total Partitions: {results['table_info']['partition_count']}")
    
    if results["partition_skewness"]["skewed"]:
        print(f"\n⚠️  PARTITION SKEWNESS DETECTED")
        print(f"Skew Ratio: {results['partition_skewness']['skew_ratio']:.2f}")
        print(f"Recommendation: {results['partition_skewness']['recommendation']}")
    
    if results.get("key_skewness"):
        for key, analysis in results["key_skewness"].items():
            if analysis["skewed"]:
                print(f"\n⚠️  KEY SKEWNESS DETECTED in {key}")
                print(f"Max Key Percentage: {analysis['max_key_percentage']:.2f}%")
                print(f"Hot Keys Count: {analysis['hot_keys_count']}")
    
    print(f"\n=== OVERALL ASSESSMENT ===")
    print(f"Severity: {results['overall_assessment']['severity']}")
    print(f"Issues: {', '.join(results['overall_assessment']['issues_detected'])}")
    print(f"Recommendation: {results['overall_assessment']['recommendation']}")
    
    return results

# Execute skewness detection
detection_results = run_skewness_detection_example()
```

---

## Mitigation Strategies

### 1. Salting Technique

```python
def implement_salting_technique():
    """
    Implement salting technique to handle key skewness
    """
    
    spark = SparkSession.builder.getOrCreate()
    
    print("=== SALTING TECHNIQUE FOR SKEWNESS MITIGATION ===")
    
    # Create highly skewed dataset
    skewed_data = []
    total_records = 1000000
    
    # 80% of records have the same key (extreme skewness)
    hot_key = "HOT_CUSTOMER_123"
    
    for i in range(total_records):
        if i < total_records * 0.8:
            customer_key = hot_key
        else:
            customer_key = f"CUSTOMER_{i % 1000}"
        
        skewed_data.append((
            i,
            customer_key,
            random.uniform(100, 1000),
            random.choice(["A", "B", "C", "D"])
        ))
    
    original_df = spark.createDataFrame(
        skewed_data,
        ["transaction_id", "customer_key", "amount", "category"]
    )
    
    print("Original data distribution:")
    original_df.groupBy("customer_key").count().orderBy(desc("count")).show(10)
    
    # Method 1: Simple Salting
    def apply_simple_salting(df, salt_range=100):
        """
        Apply simple salting by adding random salt to keys
        """
        
        # Add salt to the key
        salted_df = df.withColumn(
            "salt", 
            (rand() * salt_range).cast("int")
        ).withColumn(
            "salted_key",
            concat(col("customer_key"), lit("_"), col("salt"))
        )
        
        return salted_df
    
    # Method 2: Intelligent Salting (salt only hot keys)
    def apply_intelligent_salting(df, hot_key_threshold=10000, salt_range=50):
        """
        Apply salting only to keys that exceed threshold
        """
        
        # Identify hot keys
        key_counts = df.groupBy("customer_key").count()
        hot_keys = key_counts.filter(col("count") > hot_key_threshold) \
                            .select("customer_key") \
                            .rdd.map(lambda row: row[0]).collect()
        
        print(f"Identified {len(hot_keys)} hot keys for salting")
        
        # Apply conditional salting
        salted_df = df.withColumn(
            "salt",
            when(col("customer_key").isin(hot_keys), 
                 (rand() * salt_range).cast("int"))
            .otherwise(lit(0))
        ).withColumn(
            "salted_key",
            when(col("salt") > 0,
                 concat(col("customer_key"), lit("_"), col("salt")))
            .otherwise(col("customer_key"))
        )
        
        return salted_df
    
    # Method 3: Two-Phase Aggregation with Salting
    def perform_salted_aggregation(df, salt_range=50):
        """
        Perform aggregation using salting technique
        """
        
        # Phase 1: Salt and pre-aggregate
        salted_df = df.withColumn(
            "salt", 
            (rand() * salt_range).cast("int")
        ).withColumn(
            "salted_key",
            concat(col("customer_key"), lit("_"), col("salt"))
        )
        
        # Pre-aggregation with salted keys
        pre_agg = salted_df.groupBy("salted_key", "customer_key", "category") \
            .agg(
                sum("amount").alias("sum_amount"),
                count("*").alias("count_transactions"),
                avg("amount").alias("avg_amount")
            )
        
        # Phase 2: Final aggregation (remove salt)
        final_agg = pre_agg.groupBy("customer_key", "category") \
            .agg(
                sum("sum_amount").alias("total_amount"),
                sum("count_transactions").alias("total_transactions"),
                avg("avg_amount").alias("average_amount")
            )
        
        return final_agg
    
    # Apply different salting techniques
    print("\n1. Simple Salting:")
    simple_salted = apply_simple_salting(original_df)
    print("Salted key distribution (sample):")
    simple_salted.groupBy("salted_key").count().orderBy(desc("count")).show(10)
    
    print("\n2. Intelligent Salting:")
    intelligent_salted = apply_intelligent_salting(original_df)
    print("Intelligently salted key distribution:")
    intelligent_salted.groupBy("salted_key").count().orderBy(desc("count")).show(10)
    
    print("\n3. Two-Phase Aggregation with Salting:")
    start_time = time.time()
    
    # Original aggregation (slow due to skewness)
    original_agg = original_df.groupBy("customer_key", "category") \
        .agg(
            sum("amount").alias("total_amount"),
            count("*").alias("total_transactions"),
            avg("amount").alias("average_amount")
        )
    
    original_count = original_agg.count()
    original_time = time.time() - start_time
    
    print(f"Original aggregation: {original_count} results in {original_time:.2f} seconds")
    
    # Salted aggregation (faster)
    start_time = time.time()
    salted_agg = perform_salted_aggregation(original_df)
    salted_count = salted_agg.count()
    salted_time = time.time() - start_time
    
    print(f"Salted aggregation: {salted_count} results in {salted_time:.2f} seconds")
    print(f"Performance improvement: {(original_time / salted_time):.2f}x faster")
    
    return original_df, simple_salted, intelligent_salted, salted_agg

# Execute salting techniques
original_data, simple_salt, intelligent_salt, salted_result = implement_salting_technique()
```

### 2. Broadcast Join Optimization

```python
def implement_broadcast_join_optimization():
    """
    Implement broadcast join to handle skewed join scenarios
    """
    
    spark = SparkSession.builder.getOrCreate()
    
    print("=== BROADCAST JOIN OPTIMIZATION ===")
    
    # Create large fact table with skewed keys
    large_fact_data = []
    total_fact_records = 5000000
    
    # Create extreme skewness: 90% of records reference same dimension keys
    hot_product_ids = [1, 2, 3, 4, 5]  # Hot products
    cold_product_ids = list(range(6, 10001))  # Cold products
    
    for i in range(total_fact_records):
        if i < total_fact_records * 0.9:  # 90% hot keys
            product_id = random.choice(hot_product_ids)
        else:  # 10% cold keys
            product_id = random.choice(cold_product_ids)
        
        large_fact_data.append((
            i + 1,  # transaction_id
            product_id,
            random.randint(1, 100000),  # customer_id
            random.uniform(10, 500),  # amount
            random.choice(["Online", "Store", "Mobile"])  # channel
        ))
    
    fact_df = spark.createDataFrame(
        large_fact_data,
        ["transaction_id", "product_id", "customer_id", "amount", "channel"]
    )
    
    # Create small dimension table
    dimension_data = []
    for product_id in range(1, 10001):
        dimension_data.append((
            product_id,
            f"Product_{product_id}",
            random.choice(["Electronics", "Clothing", "Books", "Home"]),
            random.uniform(5, 1000)  # price
        ))
    
    dimension_df = spark.createDataFrame(
        dimension_data,
        ["product_id", "product_name", "category", "price"]
    )
    
    print(f"Fact table size: {fact_df.count():,} records")
    print(f"Dimension table size: {dimension_df.count():,} records")
    
    # Analyze join key skewness
    print("\nJoin key distribution in fact table:")
    fact_df.groupBy("product_id").count().orderBy(desc("count")).show(10)
    
    # Method 1: Regular join (will be slow due to skewness)
    def perform_regular_join():
        print("\n1. Regular Join Performance:")
        start_time = time.time()
        
        regular_join = fact_df.join(dimension_df, "product_id", "inner") \
            .select("transaction_id", "product_name", "category", "amount", "channel")
        
        result_count = regular_join.count()
        regular_time = time.time() - start_time
        
        print(f"Regular join: {result_count:,} results in {regular_time:.2f} seconds")
        return regular_join, regular_time
    
    # Method 2: Broadcast join (faster for small dimension)
    def perform_broadcast_join():
        print("\n2. Broadcast Join Performance:")
        start_time = time.time()
        
        # Force broadcast of dimension table
        broadcast_join = fact_df.join(
            broadcast(dimension_df), 
            "product_id", 
            "inner"
        ).select("transaction_id", "product_name", "category", "amount", "channel")
        
        result_count = broadcast_join.count()
        broadcast_time = time.time() - start_time
        
        print(f"Broadcast join: {result_count:,} results in {broadcast_time:.2f} seconds")
        return broadcast_join, broadcast_time
    
    # Method 3: Adaptive broadcast join with hot key handling
    def perform_adaptive_broadcast_join():
        print("\n3. Adaptive Broadcast Join with Hot Key Handling:")
        
        # Identify hot keys
        key_distribution = fact_df.groupBy("product_id").count()
        hot_keys = key_distribution.filter(col("count") > 100000) \
                                 .select("product_id") \
                                 .rdd.map(lambda row: row[0]).collect()
        
        print(f"Identified {len(hot_keys)} hot keys: {hot_keys}")
        
        # Split fact table into hot and cold partitions
        hot_fact = fact_df.filter(col("product_id").isin(hot_keys))
        cold_fact = fact_df.filter(~col("product_id").isin(hot_keys))
        
        # Split dimension table accordingly
        hot_dimension = dimension_df.filter(col("product_id").isin(hot_keys))
        cold_dimension = dimension_df.filter(~col("product_id").isin(hot_keys))
        
        start_time = time.time()
        
        # Broadcast join for both hot and cold data
        hot_join = hot_fact.join(broadcast(hot_dimension), "product_id", "inner")
        cold_join = cold_fact.join(broadcast(cold_dimension), "product_id", "inner")
        
        # Union results
        adaptive_join = hot_join.union(cold_join) \
            .select("transaction_id", "product_name", "category", "amount", "channel")
        
        result_count = adaptive_join.count()
        adaptive_time = time.time() - start_time
        
        print(f"Adaptive broadcast join: {result_count:,} results in {adaptive_time:.2f} seconds")
        return adaptive_join, adaptive_time
    
    # Method 4: Bucketed join for very large dimensions
    def create_bucketed_tables():
        print("\n4. Bucketed Join Setup:")
        
        # Create bucketed fact table
        fact_df.write \
            .bucketBy(200, "product_id") \
            .sortBy("product_id") \
            .option("path", "/tmp/bucketed_fact") \
            .saveAsTable("bucketed_fact_table", mode="overwrite")
        
        # Create bucketed dimension table
        dimension_df.write \
            .bucketBy(200, "product_id") \
            .sortBy("product_id") \
            .option("path", "/tmp/bucketed_dimension") \
            .saveAsTable("bucketed_dimension_table", mode="overwrite")
        
        print("Bucketed tables created successfully")
        
        # Perform bucketed join
        start_time = time.time()
        
        bucketed_join = spark.sql("""
            SELECT f.transaction_id, d.product_name, d.category, f.amount, f.channel
            FROM bucketed_fact_table f
            JOIN bucketed_dimension_table d ON f.product_id = d.product_id
        """)
        
        result_count = bucketed_join.count()
        bucketed_time = time.time() - start_time
        
        print(f"Bucketed join: {result_count:,} results in {bucketed_time:.2f} seconds")
        return bucketed_join, bucketed_time
    
    # Execute different join strategies
    regular_result, regular_time = perform_regular_join()
    broadcast_result, broadcast_time = perform_broadcast_join()
    adaptive_result, adaptive_time = perform_adaptive_broadcast_join()
    
    # Performance comparison
    print("\n=== PERFORMANCE COMPARISON ===")
    print(f"Regular Join:     {regular_time:.2f} seconds (baseline)")
    print(f"Broadcast Join:   {broadcast_time:.2f} seconds ({regular_time/broadcast_time:.2f}x faster)")
    print(f"Adaptive Join:    {adaptive_time:.2f} seconds ({regular_time/adaptive_time:.2f}x faster)")
    
    # Recommendations
    print("\n=== BROADCAST JOIN RECOMMENDATIONS ===")
    dimension_size_mb = dimension_df.count() * 0.001  # Rough estimate
    
    if dimension_size_mb < 200:
        print("✅ Dimension table is small enough for broadcast join")
    elif dimension_size_mb < 500:
        print("⚠️  Dimension table is medium-sized. Consider broadcast with caution")
    else:
        print("❌ Dimension table too large for broadcast. Consider bucketing or other strategies")
    
    broadcast_recommendations = {
        "when_to_use": [
            "Dimension table < 200MB",
            "Join key is highly skewed",
            "Sufficient executor memory available",
            "Network bandwidth is adequate"
        ],
        "when_not_to_use": [
            "Dimension table > 2GB",
            "Limited executor memory",
            "Multiple large broadcasts in same job",
            "Network is a bottleneck"
        ],
        "optimization_tips": [
            "Cache dimension table if used multiple times",
            "Use columnar formats (Parquet) for dimensions",
            "Consider partitioning large fact tables",
            "Monitor broadcast timeout settings"
        ]
    }
    
    return broadcast_recommendations

# Execute broadcast join optimization
broadcast_recommendations = implement_broadcast_join_optimization()
```

### 3. Adaptive Query Execution (AQE)

```python
def configure_adaptive_query_execution():
    """
    Configure and demonstrate Adaptive Query Execution for skewness handling
    """
    
    print("=== ADAPTIVE QUERY EXECUTION CONFIGURATION ===")
    
    # AQE configuration for skewness handling
    aqe_configs = {
        # Enable AQE
        "spark.sql.adaptive.enabled": "true",
        
        # Coalesce partitions
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "200",
        
        # Skew join handling
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        
        # Local shuffle reader
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        
        # Dynamic partition pruning
        "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
        "spark.sql.optimizer.dynamicPartitionPruning.useStats": "true",
        "spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio": "0.5",
        
        # Runtime filter
        "spark.sql.optimizer.runtime.bloomFilter.enabled": "true",
        "spark.sql.optimizer.runtime.bloomFilter.expectedNumItems": "1000000",
        "spark.sql.optimizer.runtime.bloomFilter.maxNumBits": "67108864",
        
        # Statistics collection
        "spark.sql.statistics.histogram.enabled": "true",
        "spark.sql.statistics.histogram.numBins": "254"
    }
    
    # Apply configurations
    spark = SparkSession.builder.getOrCreate()
    
    for config_key, config_value in aqe_configs.items():
        spark.conf.set(config_key, config_value)
        print(f"Set {config_key} = {config_value}")
    
    # AQE features explanation
    aqe_features = {
        "dynamically_coalescing_partitions": {
            "description": "Reduces number of partitions after shuffle operations",
            "benefit": "Eliminates small partitions and reduces task overhead",
            "example": "200 partitions reduced to 50 based on actual data size"
        },
        "dynamically_switching_join_strategies": {
            "description": "Changes join strategy based on runtime statistics",
            "benefit": "Converts sort-merge join to broadcast join when appropriate",
            "example": "Switch to broadcast join when dimension table is smaller than expected"
        },
        "dynamically_optimizing_skew_joins": {
            "description": "Splits skewed partitions during join operations",
            "benefit": "Prevents single task from processing all skewed data",
            "example": "Hot partition split into multiple smaller partitions"
        },
        "dynamically_pruning_partitions": {
            "description": "Prunes partitions based on runtime filter information",
            "benefit": "Reduces data scanning and improves performance",
            "example": "Skip partitions that don't match join conditions"
        }
    }
    
    # Monitoring AQE effectiveness
    def monitor_aqe_effectiveness():
        """
        Monitor AQE effectiveness through Spark metrics
        """
        
        monitoring_queries = {
            "partition_coalescing": """
            -- Check partition coalescing in Spark UI
            -- Look for "AQEShuffleRead" in physical plan
            -- Monitor "Coalesced Partition Num" vs "Original Partition Num"
            """,
            
            "join_strategy_changes": """
            -- Monitor join strategy changes
            -- Look for "BroadcastHashJoin" replacing "SortMergeJoin" in plans
            -- Check "AdaptiveSparkPlan" in query execution
            """,
            
            "skew_join_optimization": """
            -- Monitor skew join handling
            -- Look for "OptimizeSkewedJoin" in adaptive execution
            -- Check partition size distribution in Spark UI
            """,
            
            "dynamic_partition_pruning": """
            -- Monitor partition pruning effectiveness
            -- Check "PartitionFilters" in query plans
            -- Monitor "Pruned Partitions" vs "Total Partitions"
            """
        }
        
        return monitoring_queries
    
    # AQE best practices
    aqe_best_practices = {
        "configuration_tuning": {
            "skewedPartitionFactor": {
                "description": "Threshold for detecting skewed partitions",
                "recommendation": "Start with 5, adjust based on workload",
                "impact": "Lower values detect skewness more aggressively"
            },
            "skewedPartitionThresholdInBytes": {
                "description": "Minimum size for skewed partition detection",
                "recommendation": "Set to 256MB for most workloads",
                "impact": "Prevents optimization of small partitions"
            },
            "advisoryPartitionSizeInBytes": {
                "description": "Target partition size for coalescing",
                "recommendation": "128MB-256MB based on cluster size",
                "impact": "Affects number of tasks and parallelism"
            }
        },
        "monitoring_guidelines": {
            "spark_ui_metrics": [
                "Check AQE plan changes in SQL tab",
                "Monitor partition size distribution",
                "Review task execution times",
                "Analyze shuffle read/write metrics"
            ],
            "performance_indicators": [
                "Reduced task execution time variance",
                "Better cluster resource utilization",
                "Fewer long-running tasks",
                "Improved overall job performance"
            ]
        },
        "troubleshooting": {
            "aqe_not_triggering": [
                "Verify AQE is enabled in Spark configuration",
                "Check if data size meets thresholds",
                "Ensure statistics are available for tables",
                "Review query patterns for AQE applicability"
            ],
            "performance_regression": [
                "Monitor overhead of plan re-optimization",
                "Check if frequent plan changes occur",
                "Verify memory settings are adequate",
                "Consider disabling specific AQE features"
            ]
        }
    }
    
    return aqe_configs, aqe_features, monitor_aqe_effectiveness(), aqe_best_practices

# Execute AQE configuration
aqe_config, aqe_features, aqe_monitoring, aqe_practices = configure_adaptive_query_execution()

print("\nAQE configuration completed successfully!")
print("Monitor Spark UI for AQE optimizations during query execution.")
```

---

## Best Practices

### Comprehensive Best Practices Framework

```python
def create_skewness_best_practices_framework():
    """
    Create comprehensive framework for handling data skewness
    """
    
    best_practices_framework = {
        "prevention_strategies": {
            "data_modeling": {
                "principles": [
                    "Choose distribution keys with high cardinality",
                    "Avoid natural skewness in partition keys",
                    "Consider composite keys for better distribution",
                    "Design tables with query patterns in mind"
                ],
                "implementation": {
                    "good_distribution_key": """
                    -- Good: High cardinality, even distribution
                    CREATE TABLE sales_fact
                    USING DELTA
                    PARTITIONED BY (date_partition)
                    TBLPROPERTIES (
                        'delta.autoOptimize.optimizeWrite' = 'true',
                        'delta.autoOptimize.autoCompact' = 'true'
                    )
                    AS SELECT 
                        customer_id,  -- High cardinality
                        product_id,
                        sale_date,
                        date_format(sale_date, 'yyyy-MM') as date_partition,
                        amount
                    FROM raw_sales
                    """,
                    "poor_distribution_key": """
                    -- Poor: Low cardinality, natural skewness
                    CREATE TABLE sales_fact
                    USING DELTA  
                    PARTITIONED BY (region)  -- Only 4-5 regions, uneven distribution
                    AS SELECT *
                    FROM raw_sales
                    """
                }
            },
            
            "partitioning_strategy": {
                "principles": [
                    "Use time-based partitioning for temporal data",
                    "Avoid over-partitioning (< 1GB per partition)",
                    "Consider multi-level partitioning carefully",
                    "Align partitions with query patterns"
                ],
                "implementation": {
                    "time_based_partitioning": """
                    -- Effective time-based partitioning
                    CREATE TABLE events
                    USING DELTA
                    PARTITIONED BY (
                        year(event_timestamp) as event_year,
                        month(event_timestamp) as event_month
                    )
                    AS SELECT 
                        event_id,
                        user_id,
                        event_type,
                        event_timestamp,
                        properties
                    FROM raw_events
                    WHERE event_timestamp >= '2023-01-01'
                    """,
                    "hash_based_partitioning": """
                    -- Hash-based partitioning for even distribution
                    CREATE TABLE user_activities  
                    USING DELTA
                    PARTITIONED BY (hash(user_id) % 100 as user_bucket)
                    AS SELECT 
                        user_id,
                        activity_type,
                        activity_timestamp,
                        metadata
                    FROM raw_activities
                    """
                }
            }
        },
        
        "detection_strategies": {
            "monitoring_framework": {
                "automated_detection": """
                -- Automated skewness detection query
                WITH partition_stats AS (
                    SELECT 
                        partition_col,
                        count(*) as record_count,
                        count(*) * 100.0 / sum(count(*)) OVER() as percentage
                    FROM your_table
                    GROUP BY partition_col
                ),
                skewness_analysis AS (
                    SELECT 
                        max(record_count) as max_partition_size,
                        min(record_count) as min_partition_size,
                        avg(record_count) as avg_partition_size,
                        stddev(record_count) as stddev_partition_size,
                        count(*) as total_partitions
                    FROM partition_stats
                )
                SELECT 
                    *,
                    max_partition_size / NULLIF(min_partition_size, 0) as skew_ratio,
                    stddev_partition_size / NULLIF(avg_partition_size, 0) as coefficient_of_variation,
                    CASE 
                        WHEN max_partition_size / NULLIF(min_partition_size, 0) > 10 THEN 'HIGH_SKEW'
                        WHEN max_partition_size / NULLIF(min_partition_size, 0) > 5 THEN 'MEDIUM_SKEW'
                        ELSE 'LOW_SKEW'
                    END as skewness_level
                FROM skewness_analysis
                """,
                
                "performance_monitoring": """
                -- Monitor task execution time variance
                SELECT 
                    application_id,
                    stage_id,
                    max(task_duration_ms) as max_task_duration,
                    min(task_duration_ms) as min_task_duration,
                    avg(task_duration_ms) as avg_task_duration,
                    stddev(task_duration_ms) as stddev_task_duration,
                    max(task_duration_ms) / NULLIF(min(task_duration_ms), 0) as task_skew_ratio
                FROM spark_task_metrics
                GROUP BY application_id, stage_id
                HAVING max(task_duration_ms) / NULLIF(min(task_duration_ms), 0) > 5
                ORDER BY task_skew_ratio DESC
                """
            },
            
            "alerting_thresholds": {
                "partition_skew": {
                    "low_threshold": 2.0,
                    "medium_threshold": 5.0,
                    "high_threshold": 10.0,
                    "action": "Investigate partitioning strategy"
                },
                "task_duration_skew": {
                    "low_threshold": 3.0,
                    "medium_threshold": 5.0,
                    "high_threshold": 10.0,
                    "action": "Review data distribution and processing logic"
                },
                "memory_usage_skew": {
                    "threshold": "80% memory usage on single executor",
                    "action": "Check for hot partitions and memory leaks"
                }
            }
        },
        
        "mitigation_strategies": {
            "immediate_fixes": {
                "repartitioning": {
                    "description": "Quick fix for partition skewness",
                    "code_example": """
                    # Repartition skewed DataFrame
                    df_repartitioned = df.repartition(200, col("better_partition_key"))
                    
                    # Or use coalesce for reducing partitions
                    df_coalesced = df.coalesce(50)
                    
                    # Hash-based repartitioning for even distribution
                    df_hash_partitioned = df.repartition(
                        100, 
                        hash(col("skewed_key")) % 100
                    )
                    """,
                    "when_to_use": "Temporary fix, immediate performance improvement needed"
                },
                
                "broadcast_join": {
                    "description": "Handle skewed joins with small dimension tables",
                    "code_example": """
                    from pyspark.sql.functions import broadcast
                    
                    # Force broadcast of small dimension table
                    result = large_fact_df.join(
                        broadcast(small_dimension_df),
                        "join_key",
                        "inner"
                    )
                    """,
                    "when_to_use": "Dimension table < 2GB, join key highly skewed"
                },
                
                "salting": {
                    "description": "Add randomness to skewed keys",
                    "code_example": """
                    # Add salt to skewed keys
                    salted_df = df.withColumn(
                        "salt", 
                        (rand() * 100).cast("int")
                    ).withColumn(
                        "salted_key",
                        concat(col("skewed_key"), lit("_"), col("salt"))
                    )
                    
                    # Two-phase aggregation
                    phase1 = salted_df.groupBy("salted_key", "other_cols").agg(...)
                    phase2 = phase1.groupBy("original_key", "other_cols").agg(...)
                    """,
                    "when_to_use": "Extreme key skewness, aggregation operations"
                }
            },
            
            "long_term_solutions": {
                "data_architecture_redesign": {
                    "strategies": [
                        "Implement proper data lake architecture",
                        "Use Delta Lake for ACID transactions and optimization",
                        "Design star/snowflake schema appropriately",
                        "Implement data tiering (hot/warm/cold)"
                    ],
                    "implementation_example": """
                    # Delta Lake with optimization
                    df.write \
                        .format("delta") \
                        .partitionBy("date_partition") \
                        .option("delta.autoOptimize.optimizeWrite", "true") \
                        .option("delta.autoOptimize.autoCompact", "true") \
                        .save("/path/to/delta/table")
                    
                    # Z-ordering for better data locality
                    spark.sql("OPTIMIZE delta.`/path/to/delta/table` ZORDER BY (commonly_filtered_column)")
                    """
                },
                
                "workload_optimization": {
                    "strategies": [
                        "Implement incremental processing",
                        "Use materialized views for common aggregations",
                        "Optimize query patterns",
                        "Implement proper caching strategy"
                    ],
                    "implementation_example": """
                    # Incremental processing with Delta Lake
                    from delta.tables import DeltaTable
                    
                    # Read only new data since last processing
                    last_processed_version = get_last_processed_version()
                    
                    new_data = spark.read.format("delta") \
                        .option("versionAsOf", last_processed_version) \
                        .load("/path/to/source/table") \
                        .filter(col("processing_timestamp") > last_processed_time)
                    
                    # Process only incremental data
                    processed_data = process_incremental_data(new_data)
                    
                    # Merge into target table
                    target_table = DeltaTable.forPath(spark, "/path/to/target/table")
                    target_table.alias("target").merge(
                        processed_data.alias("source"),
                        "target.key = source.key"
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                    """
                }
            }
        },
        
        "performance_optimization": {
            "spark_configuration": {
                "memory_optimization": {
                    "spark.executor.memory": "8g",
                    "spark.executor.memoryFraction": "0.8", 
                    "spark.executor.cores": "4",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.sql.adaptive.skewJoin.enabled": "true"
                },
                "shuffle_optimization": {
                    "spark.sql.shuffle.partitions": "200",
                    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                    "spark.sql.adaptive.localShuffleReader.enabled": "true"
                }
            },
            
            "query_optimization": {
                "techniques": [
                    "Use columnar storage formats (Parquet, Delta)",
                    "Implement predicate pushdown",
                    "Use appropriate join strategies",
                    "Leverage statistics for cost-based optimization"
                ],
                "example_optimized_query": """
                -- Optimized query with proper filtering and hints
                SELECT /*+ BROADCAST(d) */
                    f.customer_id,
                    d.customer_name,
                    sum(f.amount) as total_sales
                FROM sales_fact f
                JOIN customer_dim d ON f.customer_id = d.customer_id
                WHERE f.sale_date >= current_date() - interval 30 days
                    AND d.customer_status = 'ACTIVE'
                GROUP BY f.customer_id, d.customer_name
                HAVING sum(f.amount) > 1000
                ORDER BY total_sales DESC
                LIMIT 100
                """
            }
        }
    }
    
    return best_practices_framework

# Create and display best practices
best_practices = create_skewness_best_practices_framework()

print("=== DATA SKEWNESS BEST PRACTICES FRAMEWORK ===")
print("Comprehensive framework for preventing, detecting, and mitigating data skewness created successfully!")
```

---

## Conclusion

Data skewness is a critical challenge in Azure's distributed computing environment that can severely impact performance, cost, and resource utilization. This comprehensive guide provides:

### Key Takeaways

1. **Understanding is Crucial**: Recognizing different types of skewness (partition, key, temporal) is the first step to effective mitigation.

2. **Detection Before Mitigation**: Implement automated monitoring to detect skewness early before it impacts production workloads.

3. **Multiple Mitigation Strategies**: No single solution fits all scenarios. Combine techniques like salting, broadcast joins, and AQE based on specific use cases.

4. **Prevention is Better**: Design data architecture and partitioning strategies upfront to minimize skewness occurrence.

5. **Continuous Monitoring**: Implement ongoing monitoring and alerting to catch skewness issues as data patterns evolve.

### Implementation Roadmap

```python
implementation_roadmap = {
    "phase_1_assessment": {
        "duration": "1-2 weeks",
        "activities": [
            "Analyze current data distribution patterns",
            "Identify skewed tables and queries", 
            "Measure performance impact",
            "Establish baseline metrics"
        ]
    },
    "phase_2_quick_wins": {
        "duration": "2-4 weeks",
        "activities": [
            "Enable Adaptive Query Execution",
            "Implement broadcast joins for small dimensions",
            "Apply salting to highly skewed aggregations",
            "Optimize Spark configurations"
        ]
    },
    "phase_3_systematic_improvements": {
        "duration": "1-3 months",
        "activities": [
            "Redesign partitioning strategies",
            "Implement automated skewness detection",
            "Optimize data lake architecture",
            "Establish monitoring and alerting"
        ]
    },
    "phase_4_advanced_optimization": {
        "duration": "3-6 months", 
        "activities": [
            "Implement advanced techniques (bucketing, Z-ordering)",
            "Optimize end-to-end data pipelines",
            "Establish data governance practices",
            "Continuous performance optimization"
        ]
    }
}
```

### Expected Benefits

- **Performance Improvement**: 2-10x faster query execution times
- **Cost Reduction**: 20-50% reduction in compute costs through better resource utilization  
- **Reliability**: Reduced job failures and more predictable performance
- **Scalability**: Better handling of growing data volumes and user concurrency

This guide serves as both a learning resource and practical implementation handbook for addressing data skewness challenges in Azure environments.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*