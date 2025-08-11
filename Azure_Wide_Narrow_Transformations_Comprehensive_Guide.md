# Azure Wide & Narrow Transformations - Comprehensive Guide
## Complete Analysis of Spark Transformations in Azure Databricks and Synapse Analytics

---

### Table of Contents

1. [Introduction to Transformations](#introduction-to-transformations)
2. [Narrow Transformations](#narrow-transformations)
3. [Wide Transformations](#wide-transformations)
4. [Performance Implications](#performance-implications)
5. [Azure Databricks Examples](#azure-databricks-examples)
6. [Azure Synapse Analytics Examples](#azure-synapse-analytics-examples)
7. [Optimization Strategies](#optimization-strategies)
8. [Real-World Use Cases](#real-world-use-cases)
9. [Monitoring and Debugging](#monitoring-and-debugging)
10. [Best Practices](#best-practices)
11. [Advanced Patterns](#advanced-patterns)
12. [Conclusion](#conclusion)

---

## Introduction to Transformations

In Apache Spark (used in Azure Databricks and Azure Synapse Analytics), transformations are operations that create a new RDD, DataFrame, or Dataset from an existing one. Understanding the difference between wide and narrow transformations is crucial for optimizing Spark applications and understanding their execution behavior.

### Fundamental Concepts

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Initialize Spark session for Azure Databricks
spark = SparkSession.builder \
    .appName("WideNarrowTransformationsDemo") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

def create_sample_data():
    """
    Create sample datasets to demonstrate transformations
    """
    
    # Sales data with different regions and products
    sales_data = [
        (1, "Product_A", "North", 100, "2024-01-01"),
        (2, "Product_B", "South", 150, "2024-01-01"),
        (3, "Product_A", "East", 200, "2024-01-01"),
        (4, "Product_C", "West", 120, "2024-01-01"),
        (5, "Product_B", "North", 180, "2024-01-02"),
        (6, "Product_A", "South", 90, "2024-01-02"),
        (7, "Product_C", "East", 250, "2024-01-02"),
        (8, "Product_B", "West", 130, "2024-01-02")
    ]
    
    sales_schema = StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("region", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("sale_date", StringType(), True)
    ])
    
    sales_df = spark.createDataFrame(sales_data, sales_schema)
    
    # Customer data
    customer_data = [
        (101, "Alice", "North", "Premium"),
        (102, "Bob", "South", "Standard"),
        (103, "Charlie", "East", "Premium"),
        (104, "Diana", "West", "Standard"),
        (105, "Eve", "North", "Premium")
    ]
    
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("tier", StringType(), True)
    ])
    
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    return sales_df, customer_df

# Create sample data
sales_df, customer_df = create_sample_data()

print("=== SAMPLE DATA CREATED ===")
print("Sales Data:")
sales_df.show()
print("Customer Data:")
customer_df.show()
```

### Transformation Categories

```python
def explain_transformation_categories():
    """
    Explain the fundamental difference between wide and narrow transformations
    """
    
    transformation_concepts = {
        "narrow_transformations": {
            "definition": "Operations where each input partition contributes to only one output partition",
            "characteristics": [
                "No data shuffling across partitions",
                "Fast execution (pipelined)",
                "No network communication between partitions",
                "Maintains data locality"
            ],
            "execution_model": "Pipelined execution within the same stage",
            "examples": ["map", "filter", "select", "withColumn", "drop"]
        },
        
        "wide_transformations": {
            "definition": "Operations where each input partition contributes to multiple output partitions",
            "characteristics": [
                "Requires data shuffling across partitions",
                "Slower execution due to network I/O",
                "Creates stage boundaries",
                "May cause data skew issues"
            ],
            "execution_model": "Requires shuffle and creates new stage",
            "examples": ["groupBy", "join", "orderBy", "distinct", "repartition"]
        }
    }
    
    return transformation_concepts

# Get transformation concepts
concepts = explain_transformation_categories()
print("Transformation concepts loaded successfully!")
```

---

## Narrow Transformations

Narrow transformations are operations where each input partition contributes to only one output partition. They don't require data movement across the cluster.

### Core Narrow Transformations

```python
def demonstrate_narrow_transformations():
    """
    Demonstrate various narrow transformations with examples
    """
    
    print("=== NARROW TRANSFORMATIONS DEMO ===")
    
    # 1. SELECT - Column selection
    print("1. SELECT Transformation:")
    selected_df = sales_df.select("product", "region", "amount")
    print("Original columns:", sales_df.columns)
    print("Selected columns:", selected_df.columns)
    selected_df.show(5)
    
    # 2. FILTER/WHERE - Row filtering
    print("\n2. FILTER Transformation:")
    filtered_df = sales_df.filter(col("amount") > 150)
    print("Filtered sales > 150:")
    filtered_df.show()
    
    # 3. WITHCOLUMN - Add/modify columns
    print("\n3. WITHCOLUMN Transformation:")
    enhanced_df = sales_df.withColumn("amount_category", 
                                    when(col("amount") > 150, "High")
                                    .when(col("amount") > 100, "Medium")
                                    .otherwise("Low")) \
                          .withColumn("tax", col("amount") * 0.1) \
                          .withColumn("total_with_tax", col("amount") + col("tax"))
    
    enhanced_df.show()
    
    # 4. MAP - Transform each row
    print("\n4. MAP-like Transformation using withColumn:")
    mapped_df = sales_df.withColumn("product_upper", upper(col("product"))) \
                       .withColumn("region_lower", lower(col("region"))) \
                       .withColumn("amount_doubled", col("amount") * 2)
    
    mapped_df.show()
    
    # 5. DROP - Remove columns
    print("\n5. DROP Transformation:")
    dropped_df = enhanced_df.drop("tax", "amount_category")
    print("Columns after dropping:", dropped_df.columns)
    
    # 6. CAST - Data type conversion
    print("\n6. CAST Transformation:")
    casted_df = sales_df.withColumn("amount_double", col("amount").cast("double")) \
                       .withColumn("sale_date_timestamp", to_timestamp(col("sale_date"), "yyyy-MM-dd"))
    
    casted_df.printSchema()
    
    # 7. String operations (all narrow)
    print("\n7. String Operations:")
    string_ops_df = sales_df.withColumn("product_length", length(col("product"))) \
                           .withColumn("product_substring", substring(col("product"), 1, 4)) \
                           .withColumn("region_concat", concat(lit("Region_"), col("region")))
    
    string_ops_df.show()
    
    return {
        "selected": selected_df,
        "filtered": filtered_df,
        "enhanced": enhanced_df,
        "mapped": mapped_df,
        "string_ops": string_ops_df
    }

# Execute narrow transformations demo
narrow_results = demonstrate_narrow_transformations()
```

### Advanced Narrow Transformations

```python
def advanced_narrow_transformations():
    """
    Advanced narrow transformation patterns
    """
    
    print("=== ADVANCED NARROW TRANSFORMATIONS ===")
    
    # 1. Complex conditional logic
    print("1. Complex Conditional Logic:")
    complex_conditions_df = sales_df.withColumn(
        "sales_performance",
        when((col("region") == "North") & (col("amount") > 150), "Excellent")
        .when((col("region") == "South") & (col("amount") > 120), "Good") 
        .when(col("amount") > 100, "Average")
        .otherwise("Below Average")
    ).withColumn(
        "bonus_eligible",
        when(col("sales_performance").isin(["Excellent", "Good"]), True)
        .otherwise(False)
    )
    
    complex_conditions_df.show()
    
    # 2. Array operations (narrow when operating on existing arrays)
    print("\n2. Array Operations:")
    # Create sample data with arrays
    array_data = [
        (1, "Product_A", ["feature1", "feature2", "feature3"]),
        (2, "Product_B", ["feature1", "feature4"]),
        (3, "Product_C", ["feature2", "feature3", "feature5"])
    ]
    
    array_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("features", ArrayType(StringType()), True)
    ])
    
    array_df = spark.createDataFrame(array_data, array_schema)
    
    # Array transformations (narrow)
    array_transformed_df = array_df.withColumn("feature_count", size(col("features"))) \
                                  .withColumn("first_feature", col("features")[0]) \
                                  .withColumn("has_feature1", array_contains(col("features"), "feature1"))
    
    array_transformed_df.show(truncate=False)
    
    # 3. JSON operations (narrow when parsing existing JSON columns)
    print("\n3. JSON Operations:")
    # Create sample data with JSON
    json_data = [
        (1, '{"name": "Alice", "age": 30, "city": "New York"}'),
        (2, '{"name": "Bob", "age": 25, "city": "Los Angeles"}'),
        (3, '{"name": "Charlie", "age": 35, "city": "Chicago"}')
    ]
    
    json_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("user_data", StringType(), True)
    ])
    
    json_df = spark.createDataFrame(json_data, json_schema)
    
    # JSON parsing (narrow)
    json_parsed_df = json_df.withColumn("parsed_data", from_json(col("user_data"), 
                                       StructType([
                                           StructField("name", StringType(), True),
                                           StructField("age", IntegerType(), True),
                                           StructField("city", StringType(), True)
                                       ]))) \
                           .withColumn("user_name", col("parsed_data.name")) \
                           .withColumn("user_age", col("parsed_data.age")) \
                           .withColumn("user_city", col("parsed_data.city"))
    
    json_parsed_df.show()
    
    # 4. Window functions (narrow when using ROWS BETWEEN)
    print("\n4. Window Functions (Narrow variant):")
    from pyspark.sql.window import Window
    
    # ROWS BETWEEN creates narrow transformation
    window_spec = Window.partitionBy("region").orderBy("amount").rowsBetween(-1, 1)
    
    windowed_df = sales_df.withColumn("running_avg", 
                                    avg(col("amount")).over(window_spec)) \
                         .withColumn("prev_amount", 
                                   lag(col("amount"), 1).over(Window.partitionBy("region").orderBy("amount"))) \
                         .withColumn("next_amount",
                                   lead(col("amount"), 1).over(Window.partitionBy("region").orderBy("amount")))
    
    windowed_df.show()
    
    return {
        "complex_conditions": complex_conditions_df,
        "array_ops": array_transformed_df,
        "json_ops": json_parsed_df,
        "windowed": windowed_df
    }

# Execute advanced narrow transformations
advanced_narrow_results = advanced_narrow_transformations()
```

### Performance Characteristics of Narrow Transformations

```python
def analyze_narrow_transformation_performance():
    """
    Analyze performance characteristics of narrow transformations
    """
    
    print("=== NARROW TRANSFORMATION PERFORMANCE ANALYSIS ===")
    
    # Create larger dataset for performance testing
    large_data = []
    for i in range(100000):
        large_data.append((
            i,
            f"Product_{i % 10}",
            f"Region_{i % 4}",
            (i % 1000) + 100,
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
        ))
    
    large_schema = StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("region", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("sale_date", StringType(), True)
    ])
    
    large_df = spark.createDataFrame(large_data, large_schema)
    
    print(f"Created dataset with {large_df.count():,} records")
    print(f"Number of partitions: {large_df.rdd.getNumPartitions()}")
    
    # Chain multiple narrow transformations
    start_time = time.time()
    
    chained_narrow_df = large_df.select("product", "region", "amount", "sale_date") \
                               .filter(col("amount") > 200) \
                               .withColumn("amount_category", 
                                         when(col("amount") > 800, "High")
                                         .when(col("amount") > 500, "Medium")
                                         .otherwise("Low")) \
                               .withColumn("tax", col("amount") * 0.1) \
                               .withColumn("total", col("amount") + col("tax")) \
                               .withColumn("product_upper", upper(col("product"))) \
                               .withColumn("region_lower", lower(col("region")))
    
    # Force execution
    result_count = chained_narrow_df.count()
    narrow_time = time.time() - start_time
    
    print(f"Chained narrow transformations completed:")
    print(f"  - Result count: {result_count:,}")
    print(f"  - Execution time: {narrow_time:.2f} seconds")
    print(f"  - Records per second: {result_count / narrow_time:,.0f}")
    
    # Show execution plan
    print("\nExecution Plan for Narrow Transformations:")
    chained_narrow_df.explain(True)
    
    return {
        "result_count": result_count,
        "execution_time": narrow_time,
        "throughput": result_count / narrow_time
    }

# Analyze narrow transformation performance
narrow_perf = analyze_narrow_transformation_performance()
```

---

## Wide Transformations

Wide transformations require data to be shuffled across partitions, creating stage boundaries in the Spark execution plan.

### Core Wide Transformations

```python
def demonstrate_wide_transformations():
    """
    Demonstrate various wide transformations with examples
    """
    
    print("=== WIDE TRANSFORMATIONS DEMO ===")
    
    # 1. GROUPBY - Aggregation operations
    print("1. GROUPBY Transformation:")
    grouped_df = sales_df.groupBy("region", "product") \
                        .agg(
                            sum("amount").alias("total_sales"),
                            avg("amount").alias("avg_sales"),
                            count("*").alias("transaction_count"),
                            min("amount").alias("min_sales"),
                            max("amount").alias("max_sales")
                        )
    
    print("Sales summary by region and product:")
    grouped_df.show()
    
    # 2. JOIN - Combining datasets
    print("\n2. JOIN Transformation:")
    joined_df = sales_df.join(customer_df, "region", "inner") \
                       .select("sale_id", "product", "amount", "customer_name", "tier")
    
    print("Sales with customer information:")
    joined_df.show()
    
    # 3. DISTINCT - Remove duplicates
    print("\n3. DISTINCT Transformation:")
    distinct_regions_df = sales_df.select("region").distinct()
    print("Distinct regions:")
    distinct_regions_df.show()
    
    distinct_products_df = sales_df.select("product", "region").distinct()
    print("Distinct product-region combinations:")
    distinct_products_df.show()
    
    # 4. ORDERBY/SORT - Global sorting
    print("\n4. ORDERBY Transformation:")
    sorted_df = sales_df.orderBy(desc("amount"), asc("region"))
    print("Sales sorted by amount (desc) and region (asc):")
    sorted_df.show()
    
    # 5. REPARTITION - Change partitioning
    print("\n5. REPARTITION Transformation:")
    original_partitions = sales_df.rdd.getNumPartitions()
    repartitioned_df = sales_df.repartition(4, "region")
    new_partitions = repartitioned_df.rdd.getNumPartitions()
    
    print(f"Original partitions: {original_partitions}")
    print(f"New partitions: {new_partitions}")
    
    # Check partition distribution
    partition_counts = repartitioned_df.rdd.mapPartitionsWithIndex(
        lambda idx, iterator: [(idx, sum(1 for _ in iterator))]
    ).collect()
    
    print("Records per partition after repartitioning by region:")
    for partition_id, count in partition_counts:
        print(f"  Partition {partition_id}: {count} records")
    
    return {
        "grouped": grouped_df,
        "joined": joined_df,
        "distinct_regions": distinct_regions_df,
        "distinct_products": distinct_products_df,
        "sorted": sorted_df,
        "repartitioned": repartitioned_df
    }

# Execute wide transformations demo
wide_results = demonstrate_wide_transformations()
```

### Advanced Wide Transformations

```python
def advanced_wide_transformations():
    """
    Advanced wide transformation patterns
    """
    
    print("=== ADVANCED WIDE TRANSFORMATIONS ===")
    
    # 1. Complex aggregations with multiple grouping sets
    print("1. Complex Aggregations:")
    complex_agg_df = sales_df.groupBy("region") \
                           .agg(
                               sum("amount").alias("total_sales"),
                               countDistinct("product").alias("unique_products"),
                               collect_list("product").alias("product_list"),
                               collect_set("product").alias("unique_product_set"),
                               first("sale_date").alias("first_sale_date"),
                               last("sale_date").alias("last_sale_date")
                           )
    
    complex_agg_df.show(truncate=False)
    
    # 2. Pivot operations
    print("\n2. PIVOT Transformation:")
    pivot_df = sales_df.groupBy("region") \
                      .pivot("product") \
                      .agg(sum("amount"))
    
    print("Sales pivoted by product:")
    pivot_df.show()
    
    # 3. Multiple joins with different strategies
    print("\n3. Multiple JOIN Strategies:")
    
    # Create additional dataset for complex joins
    product_info_data = [
        ("Product_A", "Electronics", 50.0),
        ("Product_B", "Clothing", 30.0),
        ("Product_C", "Books", 20.0)
    ]
    
    product_schema = StructType([
        StructField("product", StringType(), True),
        StructField("category", StringType(), True),
        StructField("cost", DoubleType(), True)
    ])
    
    product_df = spark.createDataFrame(product_info_data, product_schema)
    
    # Complex multi-table join
    complex_join_df = sales_df.alias("s") \
                            .join(customer_df.alias("c"), col("s.region") == col("c.region"), "inner") \
                            .join(product_df.alias("p"), col("s.product") == col("p.product"), "left") \
                            .select(
                                col("s.sale_id"),
                                col("s.product"),
                                col("s.region"), 
                                col("s.amount"),
                                col("c.customer_name"),
                                col("c.tier"),
                                col("p.category"),
                                col("p.cost")
                            ) \
                            .withColumn("profit", col("amount") - col("cost"))
    
    print("Complex multi-table join result:")
    complex_join_df.show()
    
    # 4. Window functions with RANGE BETWEEN (wide transformation)
    print("\n4. Window Functions (Wide variant):")
    from pyspark.sql.window import Window
    
    # RANGE BETWEEN creates wide transformation due to sorting requirement
    window_spec = Window.partitionBy("region").orderBy("amount").rangeBetween(-50, 50)
    
    windowed_wide_df = sales_df.withColumn("range_avg", 
                                         avg(col("amount")).over(window_spec)) \
                             .withColumn("range_count",
                                       count(col("amount")).over(window_spec)) \
                             .withColumn("cumulative_sum",
                                       sum(col("amount")).over(
                                           Window.partitionBy("region")
                                                .orderBy("amount")
                                                .rangeBetween(Window.unboundedPreceding, Window.currentRow)
                                       ))
    
    windowed_wide_df.show()
    
    # 5. Set operations
    print("\n5. Set Operations:")
    
    # Create two datasets for set operations
    sales_2024_q1 = sales_df.filter(col("sale_date").startswith("2024-01"))
    sales_2024_q2 = sales_df.withColumn("sale_date", lit("2024-04-01")) \
                           .withColumn("amount", col("amount") * 1.1)
    
    # Union (wide transformation when schemas differ)
    union_df = sales_2024_q1.union(sales_2024_q2)
    print("Union of Q1 and Q2 sales:")
    union_df.show()
    
    # Intersect (wide transformation)
    intersect_df = sales_2024_q1.select("product", "region") \
                               .intersect(sales_2024_q2.select("product", "region"))
    print("Intersect of Q1 and Q2 product-region combinations:")
    intersect_df.show()
    
    # Except (wide transformation)
    except_df = sales_2024_q1.select("product", "region") \
                            .except_(sales_2024_q2.select("product", "region"))
    print("Q1 product-region combinations not in Q2:")
    except_df.show()
    
    return {
        "complex_agg": complex_agg_df,
        "pivot": pivot_df,
        "complex_join": complex_join_df,
        "windowed_wide": windowed_wide_df,
        "union": union_df,
        "intersect": intersect_df,
        "except": except_df
    }

# Execute advanced wide transformations
advanced_wide_results = advanced_wide_transformations()
```

### Performance Characteristics of Wide Transformations

```python
def analyze_wide_transformation_performance():
    """
    Analyze performance characteristics of wide transformations
    """
    
    print("=== WIDE TRANSFORMATION PERFORMANCE ANALYSIS ===")
    
    # Create larger dataset for performance testing
    large_data = []
    for i in range(500000):  # Larger dataset for wide transformations
        large_data.append((
            i,
            f"Product_{i % 100}",  # More products for better grouping
            f"Region_{i % 20}",    # More regions
            (i % 1000) + 100,
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
        ))
    
    large_schema = StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("region", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("sale_date", StringType(), True)
    ])
    
    large_df = spark.createDataFrame(large_data, large_schema)
    
    print(f"Created dataset with {large_df.count():,} records")
    print(f"Number of partitions: {large_df.rdd.getNumPartitions()}")
    
    # Test different wide transformations
    performance_results = {}
    
    # 1. GroupBy performance
    print("\n1. Testing GROUPBY Performance:")
    start_time = time.time()
    
    groupby_result = large_df.groupBy("region", "product") \
                           .agg(
                               sum("amount").alias("total_sales"),
                               count("*").alias("transaction_count"),
                               avg("amount").alias("avg_sales")
                           )
    
    groupby_count = groupby_result.count()
    groupby_time = time.time() - start_time
    
    print(f"  - Groups created: {groupby_count:,}")
    print(f"  - Execution time: {groupby_time:.2f} seconds")
    
    performance_results["groupby"] = {
        "count": groupby_count,
        "time": groupby_time
    }
    
    # 2. Join performance
    print("\n2. Testing JOIN Performance:")
    
    # Create dimension table for join
    dimension_data = [(f"Product_{i}", f"Category_{i % 10}", i * 10) 
                     for i in range(100)]
    dim_schema = StructType([
        StructField("product", StringType(), True),
        StructField("category", StringType(), True),
        StructField("base_price", IntegerType(), True)
    ])
    
    dimension_df = spark.createDataFrame(dimension_data, dim_schema)
    
    start_time = time.time()
    
    join_result = large_df.join(dimension_df, "product", "inner")
    join_count = join_result.count()
    join_time = time.time() - start_time
    
    print(f"  - Joined records: {join_count:,}")
    print(f"  - Execution time: {join_time:.2f} seconds")
    
    performance_results["join"] = {
        "count": join_count,
        "time": join_time
    }
    
    # 3. Distinct performance
    print("\n3. Testing DISTINCT Performance:")
    start_time = time.time()
    
    distinct_result = large_df.select("product", "region").distinct()
    distinct_count = distinct_result.count()
    distinct_time = time.time() - start_time
    
    print(f"  - Distinct combinations: {distinct_count:,}")
    print(f"  - Execution time: {distinct_time:.2f} seconds")
    
    performance_results["distinct"] = {
        "count": distinct_count,
        "time": distinct_time
    }
    
    # 4. OrderBy performance
    print("\n4. Testing ORDERBY Performance:")
    start_time = time.time()
    
    # Take only top 1000 to avoid excessive sorting time
    ordered_result = large_df.orderBy(desc("amount")).limit(1000)
    ordered_count = ordered_result.count()
    orderby_time = time.time() - start_time
    
    print(f"  - Ordered records: {ordered_count:,}")
    print(f"  - Execution time: {orderby_time:.2f} seconds")
    
    performance_results["orderby"] = {
        "count": ordered_count,
        "time": orderby_time
    }
    
    # Show execution plans for comparison
    print("\n5. Execution Plans:")
    print("GroupBy execution plan:")
    groupby_result.explain(True)
    
    return performance_results

# Analyze wide transformation performance
wide_perf = analyze_wide_transformation_performance()
```

---

## Performance Implications

### Stage Boundaries and Execution Plans

```python
def analyze_execution_stages():
    """
    Analyze how wide and narrow transformations affect execution stages
    """
    
    print("=== EXECUTION STAGE ANALYSIS ===")
    
    # Create a complex pipeline mixing narrow and wide transformations
    complex_pipeline = sales_df.select("product", "region", "amount", "sale_date") \
                              .filter(col("amount") > 100) \
                              .withColumn("tax", col("amount") * 0.1) \
                              .groupBy("region") \
                              .agg(sum("amount").alias("total_sales"), 
                                   avg("amount").alias("avg_sales")) \
                              .withColumn("sales_category", 
                                        when(col("total_sales") > 500, "High")
                                        .otherwise("Low")) \
                              .orderBy(desc("total_sales"))
    
    print("Complex pipeline execution plan:")
    complex_pipeline.explain(True)
    
    # Show the stages created
    print("\nStage Breakdown:")
    print("Stage 1: select, filter, withColumn (narrow) + groupBy (wide - shuffle)")
    print("Stage 2: withColumn (narrow) + orderBy (wide - shuffle)")
    
    # Compare narrow-only vs wide-including pipelines
    print("\n=== PERFORMANCE COMPARISON ===")
    
    # Narrow-only pipeline
    start_time = time.time()
    narrow_only = sales_df.select("product", "region", "amount") \
                         .filter(col("amount") > 100) \
                         .withColumn("tax", col("amount") * 0.1) \
                         .withColumn("total", col("amount") + col("tax")) \
                         .withColumn("category", 
                                   when(col("amount") > 150, "High").otherwise("Low"))
    
    narrow_count = narrow_only.count()
    narrow_time = time.time() - start_time
    
    print(f"Narrow-only pipeline: {narrow_count} records in {narrow_time:.2f}s")
    
    # Wide-including pipeline
    start_time = time.time()
    wide_including = sales_df.select("product", "region", "amount") \
                           .filter(col("amount") > 100) \
                           .groupBy("region") \
                           .agg(sum("amount").alias("total_sales")) \
                           .orderBy(desc("total_sales"))
    
    wide_count = wide_including.count()
    wide_time = time.time() - start_time
    
    print(f"Wide-including pipeline: {wide_count} records in {wide_time:.2f}s")
    print(f"Performance difference: {wide_time / narrow_time:.2f}x slower")
    
    return {
        "narrow_time": narrow_time,
        "wide_time": wide_time,
        "performance_ratio": wide_time / narrow_time
    }

# Analyze execution stages
stage_analysis = analyze_execution_stages()
```

### Shuffle Operations Deep Dive

```python
def analyze_shuffle_operations():
    """
    Deep dive into shuffle operations caused by wide transformations
    """
    
    print("=== SHUFFLE OPERATIONS ANALYSIS ===")
    
    # Enable detailed Spark metrics
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # Create dataset with known skew
    skewed_data = []
    for i in range(10000):
        # Create skew: 80% of data in one region
        if i < 8000:
            region = "North"  # Hot partition
        else:
            region = f"Region_{i % 10}"  # Distributed
        
        skewed_data.append((
            i,
            f"Product_{i % 5}",
            region,
            (i % 100) + 100
        ))
    
    skewed_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("region", StringType(), True),
        StructField("amount", IntegerType(), True)
    ])
    
    skewed_df = spark.createDataFrame(skewed_data, skewed_schema)
    
    print(f"Created skewed dataset with {skewed_df.count():,} records")
    
    # Analyze partition distribution before shuffle
    print("\nPartition distribution before shuffle:")
    partition_counts_before = skewed_df.rdd.mapPartitionsWithIndex(
        lambda idx, iterator: [(idx, sum(1 for _ in iterator))]
    ).collect()
    
    for partition_id, count in partition_counts_before:
        print(f"  Partition {partition_id}: {count} records")
    
    # Perform groupBy (causes shuffle)
    start_time = time.time()
    grouped_skewed = skewed_df.groupBy("region") \
                            .agg(
                                sum("amount").alias("total_sales"),
                                count("*").alias("record_count")
                            )
    
    # Force execution and measure time
    result = grouped_skewed.collect()
    shuffle_time = time.time() - start_time
    
    print(f"\nShuffle operation completed in {shuffle_time:.2f} seconds")
    print("Results:")
    for row in result:
        print(f"  {row['region']}: {row['record_count']} records, ${row['total_sales']} total")
    
    # Show shuffle metrics in execution plan
    print("\nExecution plan with shuffle details:")
    grouped_skewed.explain(True)
    
    # Demonstrate shuffle optimization techniques
    print("\n=== SHUFFLE OPTIMIZATION TECHNIQUES ===")
    
    # 1. Pre-partitioning to avoid shuffle
    print("1. Pre-partitioning optimization:")
    pre_partitioned_df = skewed_df.repartition(col("region"))
    
    start_time = time.time()
    optimized_grouped = pre_partitioned_df.groupBy("region") \
                                        .agg(sum("amount").alias("total_sales"))
    
    optimized_result = optimized_grouped.collect()
    optimized_time = time.time() - start_time
    
    print(f"   Pre-partitioned groupBy: {optimized_time:.2f} seconds")
    print(f"   Performance improvement: {shuffle_time / optimized_time:.2f}x")
    
    # 2. Broadcast join to avoid shuffle
    print("\n2. Broadcast join optimization:")
    
    # Small dimension table
    small_dim_data = [
        ("North", "Northern Region", 1.1),
        ("South", "Southern Region", 1.0),
        ("East", "Eastern Region", 1.05),
        ("West", "Western Region", 0.95)
    ]
    
    dim_schema = StructType([
        StructField("region", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("multiplier", DoubleType(), True)
    ])
    
    dim_df = spark.createDataFrame(small_dim_data, dim_schema)
    
    # Regular join (causes shuffle)
    start_time = time.time()
    regular_join = skewed_df.join(dim_df, "region", "inner")
    regular_join_count = regular_join.count()
    regular_join_time = time.time() - start_time
    
    # Broadcast join (avoids shuffle on large table)
    start_time = time.time()
    broadcast_join = skewed_df.join(broadcast(dim_df), "region", "inner")
    broadcast_join_count = broadcast_join.count()
    broadcast_join_time = time.time() - start_time
    
    print(f"   Regular join: {regular_join_time:.2f} seconds")
    print(f"   Broadcast join: {broadcast_join_time:.2f} seconds")
    print(f"   Performance improvement: {regular_join_time / broadcast_join_time:.2f}x")
    
    return {
        "shuffle_time": shuffle_time,
        "optimized_time": optimized_time,
        "regular_join_time": regular_join_time,
        "broadcast_join_time": broadcast_join_time
    }

# Analyze shuffle operations
shuffle_analysis = analyze_shuffle_operations()
```

---

## Azure Databricks Examples

### Databricks-Specific Optimizations

```python
def databricks_specific_examples():
    """
    Azure Databricks specific examples and optimizations
    """
    
    print("=== AZURE DATABRICKS SPECIFIC EXAMPLES ===")
    
    # Databricks Auto Optimization configurations
    databricks_configs = {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.databricks.io.cache.enabled": "true",
        "spark.databricks.photon.enabled": "true"  # If Photon is available
    }
    
    # Apply configurations
    for config, value in databricks_configs.items():
        try:
            spark.conf.set(config, value)
            print(f"Set {config} = {value}")
        except Exception as e:
            print(f"Could not set {config}: {e}")
    
    # Delta Lake operations with wide and narrow transformations
    print("\n=== DELTA LAKE OPERATIONS ===")
    
    # Create Delta table path
    delta_table_path = "/tmp/delta/sales_table"
    
    # Write initial data as Delta table
    sales_df.write.format("delta") \
           .mode("overwrite") \
           .option("overwriteSchema", "true") \
           .save(delta_table_path)
    
    print(f"Created Delta table at: {delta_table_path}")
    
    # Read Delta table and perform transformations
    delta_df = spark.read.format("delta").load(delta_table_path)
    
    # Narrow transformations on Delta table
    delta_narrow = delta_df.select("product", "region", "amount") \
                          .filter(col("amount") > 120) \
                          .withColumn("premium_sale", col("amount") > 200)
    
    print("Delta narrow transformations result:")
    delta_narrow.show()
    
    # Wide transformations on Delta table
    delta_wide = delta_df.groupBy("region") \
                        .agg(
                            sum("amount").alias("total_sales"),
                            countDistinct("product").alias("unique_products")
                        ) \
                        .orderBy(desc("total_sales"))
    
    print("Delta wide transformations result:")
    delta_wide.show()
    
    # Delta merge operation (wide transformation)
    print("\n=== DELTA MERGE OPERATION ===")
    
    # Create updates data
    updates_data = [
        (9, "Product_D", "North", 300, "2024-01-03"),
        (10, "Product_A", "Central", 150, "2024-01-03")
    ]
    
    updates_df = spark.createDataFrame(updates_data, sales_df.schema)
    
    # Perform merge (wide transformation due to join)
    from delta.tables import DeltaTable
    
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    
    delta_table.alias("target").merge(
        updates_df.alias("source"),
        "target.sale_id = source.sale_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print("Merge operation completed")
    
    # Show updated table
    updated_delta_df = spark.read.format("delta").load(delta_table_path)
    print("Updated Delta table:")
    updated_delta_df.show()
    
    # Databricks-specific performance monitoring
    print("\n=== DATABRICKS PERFORMANCE MONITORING ===")
    
    # Enable query execution metrics
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
    # Complex query with both narrow and wide transformations
    complex_query = f"""
    SELECT 
        region,
        product,
        SUM(amount) as total_sales,
        AVG(amount) as avg_sales,
        COUNT(*) as transaction_count,
        MAX(amount) as max_sale,
        MIN(amount) as min_sale,
        CASE 
            WHEN SUM(amount) > 300 THEN 'High Performance'
            WHEN SUM(amount) > 150 THEN 'Medium Performance'
            ELSE 'Low Performance'
        END as performance_category
    FROM delta.`{delta_table_path}`
    WHERE amount > 100
    GROUP BY region, product
    HAVING COUNT(*) > 0
    ORDER BY total_sales DESC
    """
    
    start_time = time.time()
    complex_result = spark.sql(complex_query)
    result_count = complex_result.count()
    complex_time = time.time() - start_time
    
    print(f"Complex query executed: {result_count} results in {complex_time:.2f} seconds")
    complex_result.show()
    
    # Show execution plan
    print("\nComplex query execution plan:")
    complex_result.explain(True)
    
    return {
        "delta_table_path": delta_table_path,
        "complex_query_time": complex_time,
        "result_count": result_count
    }

# Execute Databricks-specific examples
databricks_results = databricks_specific_examples()
```

### Photon Engine Optimizations

```python
def photon_engine_optimizations():
    """
    Demonstrate Photon engine optimizations for transformations
    """
    
    print("=== PHOTON ENGINE OPTIMIZATIONS ===")
    
    # Enable Photon if available
    try:
        spark.conf.set("spark.databricks.photon.enabled", "true")
        spark.conf.set("spark.databricks.photon.scan.enabled", "true")
        print("Photon engine enabled")
    except:
        print("Photon engine not available in this environment")
    
    # Create larger dataset for Photon performance testing
    photon_data = []
    for i in range(1000000):  # 1M records
        photon_data.append((
            i,
            f"Product_{i % 1000}",
            f"Region_{i % 50}",
            f"Customer_{i % 10000}",
            (i % 10000) + 100,
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
        ))
    
    photon_schema = StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("region", StringType(), True),
        StructField("customer", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("sale_date", StringType(), True)
    ])
    
    photon_df = spark.createDataFrame(photon_data, photon_schema)
    
    print(f"Created dataset with {photon_df.count():,} records for Photon testing")
    
    # Test Photon-optimized operations
    photon_operations = {
        "narrow_transformations": {
            "operation": lambda df: df.select("product", "region", "amount")
                                     .filter(col("amount") > 500)
                                     .withColumn("tax", col("amount") * 0.08)
                                     .withColumn("total", col("amount") + col("tax")),
            "description": "Select, filter, withColumn operations"
        },
        
        "aggregation": {
            "operation": lambda df: df.groupBy("region", "product")
                                     .agg(sum("amount").alias("total_sales"),
                                          avg("amount").alias("avg_sales"),
                                          count("*").alias("count")),
            "description": "GroupBy aggregation"
        },
        
        "join": {
            "operation": lambda df: df.join(
                df.select("region").distinct().withColumnRenamed("region", "r"),
                col("region") == col("r"),
                "inner"
            ),
            "description": "Join operation"
        },
        
        "window_functions": {
            "operation": lambda df: df.withColumn(
                "running_total",
                sum("amount").over(Window.partitionBy("region").orderBy("sale_id"))
            ),
            "description": "Window function"
        }
    }
    
    # Execute operations and measure performance
    for op_name, op_config in photon_operations.items():
        print(f"\nTesting {op_name}: {op_config['description']}")
        
        start_time = time.time()
        result_df = op_config["operation"](photon_df)
        result_count = result_df.count()
        execution_time = time.time() - start_time
        
        print(f"  Result count: {result_count:,}")
        print(f"  Execution time: {execution_time:.2f} seconds")
        print(f"  Throughput: {result_count / execution_time:,.0f} records/second")
    
    return photon_df

# Execute Photon optimizations (if available)
try:
    photon_df = photon_engine_optimizations()
except Exception as e:
    print(f"Photon optimization demo failed: {e}")
```

---

## Azure Synapse Analytics Examples

### Synapse-Specific Transformations

```python
def synapse_specific_examples():
    """
    Azure Synapse Analytics specific transformation examples
    """
    
    print("=== AZURE SYNAPSE ANALYTICS EXAMPLES ===")
    
    # Synapse-specific configurations
    synapse_configs = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.synapse.enabled": "true",
        "spark.sql.parquet.columnarReaderBatchSize": "4096",
        "spark.sql.parquet.enableVectorizedReader": "true"
    }
    
    # Apply configurations
    for config, value in synapse_configs.items():
        try:
            spark.conf.set(config, value)
            print(f"Set {config} = {value}")
        except Exception as e:
            print(f"Could not set {config}: {e}")
    
    # Create Synapse-optimized data structure
    print("\n=== SYNAPSE DATA LAKE INTEGRATION ===")
    
    # Simulate reading from Azure Data Lake Gen2
    adls_path = "abfss://container@account.dfs.core.windows.net/data/sales/"
    
    # For demo purposes, we'll use our existing data
    # In real Synapse, you would read from ADLS Gen2
    synapse_df = sales_df.withColumn("year", lit(2024)) \
                        .withColumn("month", lit(1)) \
                        .withColumn("processing_timestamp", current_timestamp())
    
    print("Synapse DataFrame with partitioning columns:")
    synapse_df.show()
    
    # Narrow transformations optimized for Synapse
    print("\n=== SYNAPSE NARROW TRANSFORMATIONS ===")
    
    synapse_narrow = synapse_df.select("product", "region", "amount", "year", "month") \
                              .filter((col("amount") > 100) & (col("year") == 2024)) \
                              .withColumn("amount_usd", col("amount")) \
                              .withColumn("amount_eur", col("amount") * 0.85) \
                              .withColumn("tax_rate", 
                                        when(col("region") == "North", 0.08)
                                        .when(col("region") == "South", 0.06)
                                        .otherwise(0.07)) \
                              .withColumn("tax_amount", col("amount") * col("tax_rate")) \
                              .withColumn("net_amount", col("amount") - col("tax_amount"))
    
    print("Synapse narrow transformations result:")
    synapse_narrow.show()
    
    # Wide transformations with Synapse optimizations
    print("\n=== SYNAPSE WIDE TRANSFORMATIONS ===")
    
    # Complex aggregation with multiple dimensions
    synapse_wide = synapse_df.groupBy("region", "product", "year", "month") \
                           .agg(
                               sum("amount").alias("total_sales"),
                               avg("amount").alias("avg_sales"),
                               count("*").alias("transaction_count"),
                               min("amount").alias("min_sale"),
                               max("amount").alias("max_sale"),
                               stddev("amount").alias("stddev_sales"),
                               collect_list("sale_id").alias("sale_ids")
                           ) \
                           .withColumn("sales_variance", col("stddev_sales") ** 2) \
                           .withColumn("coefficient_of_variation", 
                                     col("stddev_sales") / col("avg_sales"))
    
    print("Synapse wide transformations result:")
    synapse_wide.show()
    
    # Synapse SQL Pool integration example
    print("\n=== SYNAPSE SQL POOL INTEGRATION ===")
    
    # Create temporary view for SQL access
    synapse_df.createOrReplaceTempView("sales_temp")
    
    # Complex SQL query mixing narrow and wide operations
    synapse_sql_query = """
    WITH sales_enhanced AS (
        -- Narrow transformations in CTE
        SELECT 
            product,
            region,
            amount,
            amount * 1.1 as amount_with_markup,
            CASE 
                WHEN amount > 200 THEN 'Premium'
                WHEN amount > 100 THEN 'Standard'
                ELSE 'Basic'
            END as sale_category,
            year,
            month
        FROM sales_temp
        WHERE amount > 50
    ),
    regional_summary AS (
        -- Wide transformations for aggregation
        SELECT 
            region,
            sale_category,
            COUNT(*) as transaction_count,
            SUM(amount_with_markup) as total_sales,
            AVG(amount_with_markup) as avg_sales,
            MIN(amount_with_markup) as min_sale,
            MAX(amount_with_markup) as max_sale
        FROM sales_enhanced
        GROUP BY region, sale_category
    )
    -- Final selection with ranking (wide transformation)
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_sales DESC) as rank_in_region,
        total_sales / SUM(total_sales) OVER (PARTITION BY region) as pct_of_region_sales
    FROM regional_summary
    ORDER BY region, total_sales DESC
    """
    
    synapse_sql_result = spark.sql(synapse_sql_query)
    print("Synapse SQL query result:")
    synapse_sql_result.show()
    
    # Demonstrate Synapse-specific performance features
    print("\n=== SYNAPSE PERFORMANCE FEATURES ===")
    
    # Partition elimination demonstration
    partitioned_query = """
    SELECT region, SUM(amount) as total_sales
    FROM sales_temp
    WHERE year = 2024 AND month = 1  -- Partition elimination
    GROUP BY region
    """
    
    start_time = time.time()
    partition_result = spark.sql(partitioned_query)
    partition_count = partition_result.count()
    partition_time = time.time() - start_time
    
    print(f"Partition elimination query: {partition_count} results in {partition_time:.2f}s")
    partition_result.show()
    
    # Show execution plan
    print("\nSynapse SQL execution plan:")
    partition_result.explain(True)
    
    return {
        "synapse_narrow": synapse_narrow,
        "synapse_wide": synapse_wide,
        "sql_result": synapse_sql_result,
        "partition_time": partition_time
    }

# Execute Synapse-specific examples
try:
    synapse_results = synapse_specific_examples()
except Exception as e:
    print(f"Synapse examples failed: {e}")
```

---

## Optimization Strategies

### Comprehensive Optimization Framework

```python
def create_optimization_framework():
    """
    Create comprehensive framework for optimizing wide and narrow transformations
    """
    
    optimization_strategies = {
        "narrow_transformation_optimizations": {
            "pipelining": {
                "description": "Chain multiple narrow transformations for pipeline execution",
                "benefits": ["Single pass through data", "Memory efficient", "CPU cache friendly"],
                "implementation": """
                # Good: Chained narrow transformations
                optimized_df = df.select("col1", "col2", "col3") \\
                                .filter(col("col1") > 100) \\
                                .withColumn("derived1", col("col2") * 2) \\
                                .withColumn("derived2", col("col3") + 10) \\
                                .withColumn("category", when(col("col1") > 200, "High").otherwise("Low"))
                
                # Avoid: Separate operations forcing multiple passes
                temp1 = df.select("col1", "col2", "col3")
                temp2 = temp1.filter(col("col1") > 100)  # Separate action
                temp3 = temp2.withColumn("derived1", col("col2") * 2)  # Separate action
                """,
                "example": """
                def optimized_narrow_pipeline(df):
                    return df.select("product", "region", "amount", "sale_date") \\
                            .filter(col("amount") > 100) \\
                            .withColumn("tax", col("amount") * 0.08) \\
                            .withColumn("total", col("amount") + col("tax")) \\
                            .withColumn("quarter", quarter(to_date(col("sale_date")))) \\
                            .withColumn("profit_margin", 
                                      when(col("total") > 200, "High")
                                      .when(col("total") > 100, "Medium")
                                      .otherwise("Low"))
                """
            },
            
            "column_pruning": {
                "description": "Select only necessary columns early in the pipeline",
                "benefits": ["Reduced memory usage", "Faster serialization", "Better cache utilization"],
                "implementation": """
                # Good: Early column pruning
                result = df.select("needed_col1", "needed_col2", "needed_col3") \\
                          .filter(condition) \\
                          .withColumn("derived", expr)
                
                # Avoid: Late column pruning
                result = df.filter(condition) \\
                          .withColumn("derived", expr) \\
                          .select("needed_col1", "needed_col2", "derived")  # Too late
                """
            },
            
            "predicate_pushdown": {
                "description": "Apply filters as early as possible",
                "benefits": ["Reduced data processing", "Better partition elimination", "Faster execution"],
                "implementation": """
                # Good: Early filtering
                result = df.filter(col("date") >= "2024-01-01") \\
                          .filter(col("amount") > 100) \\
                          .select("product", "amount") \\
                          .withColumn("category", when(col("amount") > 200, "High").otherwise("Low"))
                
                # Avoid: Late filtering
                result = df.select("product", "amount", "date") \\
                          .withColumn("category", when(col("amount") > 200, "High").otherwise("Low")) \\
                          .filter(col("date") >= "2024-01-01") \\
                          .filter(col("amount") > 100)
                """
            }
        },
        
        "wide_transformation_optimizations": {
            "shuffle_optimization": {
                "description": "Minimize and optimize shuffle operations",
                "techniques": {
                    "pre_partitioning": {
                        "description": "Partition data by grouping keys before aggregation",
                        "example": """
                        # Pre-partition before groupBy
                        pre_partitioned = df.repartition(col("region"))
                        result = pre_partitioned.groupBy("region").agg(sum("amount"))
                        """
                    },
                    "coalesce_partitions": {
                        "description": "Reduce partition count after wide operations",
                        "example": """
                        # Coalesce after groupBy to reduce small partitions
                        result = df.groupBy("region").agg(sum("amount")).coalesce(10)
                        """
                    },
                    "bucketing": {
                        "description": "Pre-bucket tables for efficient joins",
                        "example": """
                        # Create bucketed table
                        df.write.bucketBy(10, "join_key").saveAsTable("bucketed_table")
                        """
                    }
                }
            },
            
            "join_optimization": {
                "description": "Optimize join operations for better performance",
                "strategies": {
                    "broadcast_joins": {
                        "description": "Broadcast small tables to avoid shuffle",
                        "when_to_use": "Small dimension tables (<200MB)",
                        "example": """
                        # Broadcast small dimension table
                        result = large_fact.join(broadcast(small_dim), "key")
                        """
                    },
                    "bucketed_joins": {
                        "description": "Use bucketed tables for sort-merge joins",
                        "when_to_use": "Both tables are large and frequently joined",
                        "example": """
                        # Both tables bucketed on join key
                        result = bucketed_table1.join(bucketed_table2, "join_key")
                        """
                    },
                    "join_reordering": {
                        "description": "Reorder joins for optimal execution",
                        "example": """
                        # Good: Join smaller result first
                        result = large_table.join(small_table1, "key1") \\
                                          .join(small_table2, "key2")
                        
                        # Consider: Filter before join
                        filtered = large_table.filter(condition)
                        result = filtered.join(dimension_table, "key")
                        """
                    }
                }
            },
            
            "aggregation_optimization": {
                "description": "Optimize aggregation operations",
                "techniques": {
                    "partial_aggregation": {
                        "description": "Enable partial aggregation for better performance",
                        "config": "spark.sql.execution.arrow.pyspark.enabled=true"
                    },
                    "skew_handling": {
                        "description": "Handle skewed aggregations",
                        "example": """
                        # Salt technique for skewed keys
                        salted_df = df.withColumn("salt", (rand() * 100).cast("int")) \\
                                     .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))
                        
                        # Two-phase aggregation
                        phase1 = salted_df.groupBy("salted_key", "other_cols").agg(sum("amount"))
                        phase2 = phase1.groupBy("key", "other_cols").agg(sum("sum(amount)"))
                        """
                    }
                }
            }
        },
        
        "general_optimization_principles": {
            "caching_strategy": {
                "description": "Cache frequently accessed DataFrames",
                "when_to_cache": [
                    "DataFrame used multiple times",
                    "Expensive computations",
                    "After wide transformations"
                ],
                "cache_levels": {
                    "MEMORY_ONLY": "Fast access, limited by memory",
                    "MEMORY_AND_DISK": "Spills to disk when memory full",
                    "DISK_ONLY": "Slower but handles large datasets"
                },
                "example": """
                # Cache after expensive wide transformation
                expensive_result = df.groupBy("key").agg(complex_aggregations)
                expensive_result.cache()
                
                # Use cached result multiple times
                result1 = expensive_result.filter(condition1)
                result2 = expensive_result.filter(condition2)
                """
            },
            
            "adaptive_query_execution": {
                "description": "Enable AQE for automatic optimizations",
                "configurations": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.sql.adaptive.skewJoin.enabled": "true",
                    "spark.sql.adaptive.localShuffleReader.enabled": "true"
                },
                "benefits": [
                    "Automatic partition coalescing",
                    "Dynamic join strategy selection",
                    "Skew join optimization",
                    "Local shuffle reader optimization"
                ]
            }
        }
    }
    
    return optimization_strategies

# Create optimization framework
optimization_framework = create_optimization_framework()
print("Optimization framework created successfully!")
```

### Practical Optimization Examples

```python
def demonstrate_practical_optimizations():
    """
    Demonstrate practical optimization techniques with before/after comparisons
    """
    
    print("=== PRACTICAL OPTIMIZATION DEMONSTRATIONS ===")
    
    # Create larger dataset for meaningful performance comparisons
    large_data = []
    for i in range(100000):
        large_data.append((
            i,
            f"Product_{i % 100}",
            f"Region_{i % 20}",
            f"Category_{i % 10}",
            (i % 1000) + 100,
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
        ))
    
    large_schema = StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("region", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("sale_date", StringType(), True)
    ])
    
    large_df = spark.createDataFrame(large_data, large_schema)
    print(f"Created dataset with {large_df.count():,} records")
    
    # Optimization 1: Column Pruning and Predicate Pushdown
    print("\n1. COLUMN PRUNING AND PREDICATE PUSHDOWN OPTIMIZATION")
    
    # Unoptimized version
    start_time = time.time()
    unoptimized = large_df.withColumn("tax", col("amount") * 0.08) \
                         .withColumn("total", col("amount") + col("tax")) \
                         .filter(col("amount") > 500) \
                         .filter(col("region").isin(["Region_1", "Region_2", "Region_3"])) \
                         .select("product", "region", "total")
    
    unoptimized_count = unoptimized.count()
    unoptimized_time = time.time() - start_time
    
    # Optimized version
    start_time = time.time()
    optimized = large_df.filter(col("amount") > 500) \
                       .filter(col("region").isin(["Region_1", "Region_2", "Region_3"])) \
                       .select("product", "region", "amount") \
                       .withColumn("tax", col("amount") * 0.08) \
                       .withColumn("total", col("amount") + col("tax")) \
                       .select("product", "region", "total")
    
    optimized_count = optimized.count()
    optimized_time = time.time() - start_time
    
    print(f"Unoptimized: {unoptimized_count:,} records in {unoptimized_time:.2f}s")
    print(f"Optimized: {optimized_count:,} records in {optimized_time:.2f}s")
    print(f"Performance improvement: {unoptimized_time / optimized_time:.2f}x")
    
    # Optimization 2: Shuffle Optimization
    print("\n2. SHUFFLE OPTIMIZATION")
    
    # Unoptimized aggregation
    start_time = time.time()
    unoptimized_agg = large_df.groupBy("region", "category") \
                             .agg(sum("amount").alias("total_sales")) \
                             .orderBy(desc("total_sales"))
    
    unoptimized_agg_count = unoptimized_agg.count()
    unoptimized_agg_time = time.time() - start_time
    
    # Optimized with pre-partitioning
    start_time = time.time()
    pre_partitioned = large_df.repartition(col("region"))
    optimized_agg = pre_partitioned.groupBy("region", "category") \
                                  .agg(sum("amount").alias("total_sales")) \
                                  .orderBy(desc("total_sales"))
    
    optimized_agg_count = optimized_agg.count()
    optimized_agg_time = time.time() - start_time
    
    print(f"Unoptimized aggregation: {unoptimized_agg_count:,} records in {unoptimized_agg_time:.2f}s")
    print(f"Optimized aggregation: {optimized_agg_count:,} records in {optimized_agg_time:.2f}s")
    print(f"Performance improvement: {unoptimized_agg_time / optimized_agg_time:.2f}x")
    
    # Optimization 3: Caching Strategy
    print("\n3. CACHING STRATEGY OPTIMIZATION")
    
    # Create expensive computation
    expensive_computation = large_df.filter(col("amount") > 200) \
                                   .groupBy("product", "region") \
                                   .agg(
                                       sum("amount").alias("total_sales"),
                                       avg("amount").alias("avg_sales"),
                                       count("*").alias("transaction_count")
                                   )
    
    # Without caching - multiple uses
    start_time = time.time()
    
    result1 = expensive_computation.filter(col("total_sales") > 1000).count()
    result2 = expensive_computation.filter(col("avg_sales") > 300).count()
    result3 = expensive_computation.orderBy(desc("total_sales")).limit(10).count()
    
    no_cache_time = time.time() - start_time
    
    # With caching - multiple uses
    expensive_computation.cache()  # Cache the expensive computation
    
    start_time = time.time()
    
    cached_result1 = expensive_computation.filter(col("total_sales") > 1000).count()
    cached_result2 = expensive_computation.filter(col("avg_sales") > 300).count()
    cached_result3 = expensive_computation.orderBy(desc("total_sales")).limit(10).count()
    
    cached_time = time.time() - start_time
    
    print(f"Without caching: {no_cache_time:.2f}s")
    print(f"With caching: {cached_time:.2f}s")
    print(f"Performance improvement: {no_cache_time / cached_time:.2f}x")
    
    # Clean up cache
    expensive_computation.unpersist()
    
    return {
        "column_pruning_improvement": unoptimized_time / optimized_time,
        "shuffle_optimization_improvement": unoptimized_agg_time / optimized_agg_time,
        "caching_improvement": no_cache_time / cached_time
    }

# Execute practical optimizations
optimization_results = demonstrate_practical_optimizations()
```

---

## Best Practices

### Comprehensive Best Practices Guide

```python
def create_best_practices_guide():
    """
    Create comprehensive best practices guide for wide and narrow transformations
    """
    
    best_practices = {
        "narrow_transformation_best_practices": {
            "design_principles": {
                "chain_operations": {
                    "principle": "Chain multiple narrow transformations together",
                    "reasoning": "Enables pipeline execution and reduces memory overhead",
                    "example": """
                    # Good: Chained operations
                    result = df.select("col1", "col2") \\
                              .filter(col("col1") > 100) \\
                              .withColumn("derived", col("col2") * 2) \\
                              .withColumn("category", when(col("col1") > 200, "High").otherwise("Low"))
                    
                    # Avoid: Separate operations
                    temp1 = df.select("col1", "col2")
                    temp2 = temp1.filter(col("col1") > 100)
                    temp3 = temp2.withColumn("derived", col("col2") * 2)
                    result = temp3.withColumn("category", when(col("col1") > 200, "High").otherwise("Low"))
                    """
                },
                
                "early_filtering": {
                    "principle": "Apply filters as early as possible in the pipeline",
                    "reasoning": "Reduces data volume for subsequent operations",
                    "example": """
                    # Good: Early filtering
                    result = df.filter(col("status") == "active") \\
                              .filter(col("amount") > 100) \\
                              .select("id", "name", "amount") \\
                              .withColumn("tax", col("amount") * 0.08)
                    
                    # Avoid: Late filtering
                    result = df.select("id", "name", "amount", "status") \\
                              .withColumn("tax", col("amount") * 0.08) \\
                              .filter(col("status") == "active") \\
                              .filter(col("amount") > 100)
                    """
                },
                
                "column_selection": {
                    "principle": "Select only required columns early",
                    "reasoning": "Reduces memory usage and improves serialization performance",
                    "example": """
                    # Good: Early column selection
                    result = df.select("id", "amount", "date") \\
                              .filter(col("amount") > 100) \\
                              .withColumn("tax", col("amount") * 0.08)
                    
                    # Avoid: Late column selection
                    result = df.filter(col("amount") > 100) \\
                              .withColumn("tax", col("amount") * 0.08) \\
                              .select("id", "amount", "date", "tax")
                    """
                }
            },
            
            "performance_tips": {
                "avoid_udfs": {
                    "tip": "Use built-in functions instead of UDFs when possible",
                    "reasoning": "Built-in functions are optimized and don't require Python/JVM serialization",
                    "example": """
                    # Good: Built-in function
                    df.withColumn("upper_name", upper(col("name")))
                    
                    # Avoid: UDF (unless necessary)
                    from pyspark.sql.functions import udf
                    upper_udf = udf(lambda x: x.upper() if x else None, StringType())
                    df.withColumn("upper_name", upper_udf(col("name")))
                    """
                },
                
                "use_when_instead_of_udf": {
                    "tip": "Use when/otherwise for conditional logic",
                    "example": """
                    # Good: Built-in when/otherwise
                    df.withColumn("category", 
                                when(col("amount") > 1000, "High")
                                .when(col("amount") > 500, "Medium")
                                .otherwise("Low"))
                    
                    # Avoid: UDF for simple conditions
                    def categorize(amount):
                        if amount > 1000: return "High"
                        elif amount > 500: return "Medium"
                        else: return "Low"
                    
                    categorize_udf = udf(categorize, StringType())
                    df.withColumn("category", categorize_udf(col("amount")))
                    """
                }
            }
        },
        
        "wide_transformation_best_practices": {
            "shuffle_minimization": {
                "pre_aggregate": {
                    "principle": "Pre-aggregate data when possible",
                    "example": """
                    # Good: Pre-aggregate before join
                    aggregated = large_table.groupBy("key").agg(sum("amount").alias("total"))
                    result = aggregated.join(dimension_table, "key")
                    
                    # Avoid: Join then aggregate (more data shuffled)
                    joined = large_table.join(dimension_table, "key")
                    result = joined.groupBy("key").agg(sum("amount").alias("total"))
                    """
                },
                
                "partition_awareness": {
                    "principle": "Be aware of data partitioning for joins and aggregations",
                    "example": """
                    # Good: Partition by join key before join
                    partitioned_df = df.repartition(col("join_key"))
                    result = partitioned_df.join(other_df, "join_key")
                    
                    # Good: Use same partitioning for multiple operations
                    partitioned = df.repartition(col("region"))
                    agg1 = partitioned.groupBy("region").agg(sum("sales"))
                    agg2 = partitioned.groupBy("region", "product").agg(avg("price"))
                    """
                }
            },
            
            "join_optimization": {
                "broadcast_small_tables": {
                    "principle": "Broadcast small dimension tables",
                    "threshold": "< 200MB for broadcast",
                    "example": """
                    from pyspark.sql.functions import broadcast
                    
                    # Good: Broadcast small dimension
                    result = large_fact.join(broadcast(small_dimension), "key")
                    
                    # Monitor broadcast size
                    small_dimension.cache()
                    print(f"Dimension size: {small_dimension.count()} rows")
                    """
                },
                
                "join_order": {
                    "principle": "Join smaller tables first",
                    "example": """
                    # Good: Join order from smallest to largest
                    result = large_table \\
                            .join(small_table1, "key1") \\
                            .join(medium_table2, "key2") \\
                            .join(largest_table3, "key3")
                    
                    # Consider filtering before joins
                    filtered_large = large_table.filter(important_condition)
                    result = filtered_large.join(dimension_table, "key")
                    """
                }
            },
            
            "aggregation_optimization": {
                "avoid_collect": {
                    "principle": "Avoid collect() on large datasets",
                    "reasoning": "collect() brings all data to driver, causing memory issues",
                    "example": """
                    # Good: Use take() or limit() for sampling
                    sample_data = df.limit(100).collect()
                    
                    # Good: Use write operations for large results
                    df.groupBy("key").agg(sum("amount")).write.parquet("output_path")
                    
                    # Avoid: collect() on large aggregations
                    all_data = df.groupBy("key").agg(sum("amount")).collect()  # Don't do this
                    """
                },
                
                "handle_skew": {
                    "principle": "Handle data skew in aggregations",
                    "techniques": ["Salting", "Two-phase aggregation", "Custom partitioning"],
                    "example": """
                    # Salting technique for skewed keys
                    def handle_skewed_aggregation(df, skewed_column, salt_range=100):
                        # Add salt
                        salted_df = df.withColumn("salt", (rand() * salt_range).cast("int")) \\
                                     .withColumn("salted_key", concat(col(skewed_column), lit("_"), col("salt")))
                        
                        # Phase 1: Aggregate with salt
                        phase1 = salted_df.groupBy("salted_key", "other_cols").agg(sum("amount").alias("sum_amount"))
                        
                        # Phase 2: Remove salt and final aggregation
                        phase2 = phase1.withColumn("original_key", split(col("salted_key"), "_")[0]) \\
                                       .groupBy("original_key", "other_cols").agg(sum("sum_amount").alias("total"))
                        
                        return phase2
                    """
                }
            }
        },
        
        "general_best_practices": {
            "monitoring_and_debugging": {
                "use_explain": {
                    "principle": "Always check execution plans for complex queries",
                    "example": """
                    # Check execution plan
                    complex_df.explain(True)  # Shows all plan details
                    complex_df.explain()      # Shows physical plan only
                    """
                },
                
                "monitor_stages": {
                    "principle": "Monitor Spark UI for stage performance",
                    "what_to_check": [
                        "Stage duration and task distribution",
                        "Shuffle read/write metrics",
                        "Task execution time variance",
                        "Memory usage patterns"
                    ]
                }
            },
            
            "resource_management": {
                "appropriate_caching": {
                    "principle": "Cache strategically, not everything",
                    "when_to_cache": [
                        "DataFrame used multiple times",
                        "After expensive wide transformations",
                        "Intermediate results in iterative algorithms"
                    ],
                    "example": """
                    # Good: Cache expensive intermediate result
                    expensive_result = df.join(large_table1, "key1") \\
                                        .join(large_table2, "key2") \\
                                        .groupBy("group_key").agg(complex_aggregations)
                    
                    expensive_result.cache()
                    
                    # Use cached result multiple times
                    result1 = expensive_result.filter(condition1)
                    result2 = expensive_result.filter(condition2)
                    
                    # Don't forget to unpersist when done
                    expensive_result.unpersist()
                    """
                },
                
                "partition_sizing": {
                    "principle": "Maintain optimal partition sizes",
                    "guidelines": {
                        "target_size": "128MB - 1GB per partition",
                        "avoid_small_partitions": "< 10MB partitions are inefficient",
                        "avoid_large_partitions": "> 2GB partitions may cause memory issues"
                    },
                    "example": """
                    # Check current partition sizes
                    partition_counts = df.rdd.mapPartitionsWithIndex(
                        lambda idx, iterator: [(idx, sum(1 for _ in iterator))]
                    ).collect()
                    
                    # Repartition if needed
                    if max(count for _, count in partition_counts) / min(count for _, count in partition_counts) > 10:
                        df = df.repartition(optimal_partition_count)
                    """
                }
            }
        }
    }
    
    return best_practices

# Create best practices guide
best_practices_guide = create_best_practices_guide()
print("Best practices guide created successfully!")
```

---

## Conclusion

Understanding the difference between wide and narrow transformations is fundamental to optimizing Spark applications in Azure Databricks and Azure Synapse Analytics. This comprehensive guide has covered:

### Key Takeaways

1. **Narrow Transformations**:
   - Execute within single partitions without data shuffling
   - Enable pipeline execution for better performance
   - Include operations like `select`, `filter`, `withColumn`, `map`
   - Should be chained together for optimal performance

2. **Wide Transformations**:
   - Require data shuffling across partitions
   - Create stage boundaries in execution plans
   - Include operations like `groupBy`, `join`, `orderBy`, `distinct`
   - Need careful optimization to avoid performance bottlenecks

3. **Performance Optimization**:
   - Chain narrow transformations for pipeline execution
   - Minimize shuffle operations in wide transformations
   - Use broadcast joins for small dimension tables
   - Apply filters early and select columns strategically
   - Handle data skew with techniques like salting

4. **Azure-Specific Considerations**:
   - Leverage Azure Databricks' Photon engine for acceleration
   - Use Delta Lake for optimized storage and ACID transactions
   - Take advantage of Azure Synapse's integration with Azure Data Lake
   - Configure Adaptive Query Execution for automatic optimizations

### Best Practices Summary

- **Design**: Plan transformation pipelines with performance in mind
- **Optimization**: Use appropriate caching, partitioning, and join strategies  
- **Monitoring**: Regularly check execution plans and Spark UI metrics
- **Testing**: Performance test with realistic data volumes
- **Documentation**: Document optimization decisions for team knowledge

### Implementation Guidelines

```python
# Example of well-optimized transformation pipeline
def optimized_transformation_pipeline(df):
    """
    Example of a well-optimized transformation pipeline
    combining narrow and wide transformations efficiently
    """
    
    # Step 1: Narrow transformations (chained for pipeline execution)
    cleaned_df = df.select("product", "region", "amount", "date", "customer_id") \
                   .filter(col("amount") > 0) \
                   .filter(col("date") >= "2024-01-01") \
                   .withColumn("tax", col("amount") * 0.08) \
                   .withColumn("total", col("amount") + col("tax")) \
                   .withColumn("quarter", quarter(to_date(col("date"))))
    
    # Step 2: Wide transformation (aggregation)
    # Pre-partition by grouping key for better performance
    partitioned_df = cleaned_df.repartition(col("region"))
    
    aggregated_df = partitioned_df.groupBy("region", "quarter") \
                                 .agg(
                                     sum("total").alias("total_sales"),
                                     avg("total").alias("avg_sale"),
                                     countDistinct("customer_id").alias("unique_customers"),
                                     count("*").alias("transaction_count")
                                 )
    
    # Step 3: Final narrow transformations
    final_df = aggregated_df.withColumn("avg_sale_per_customer", 
                                       col("total_sales") / col("unique_customers")) \
                           .withColumn("sales_category",
                                     when(col("total_sales") > 10000, "High")
                                     .when(col("total_sales") > 5000, "Medium")
                                     .otherwise("Low"))
    
    return final_df

# This pipeline demonstrates:
# 1. Early filtering and column selection
# 2. Chained narrow transformations
# 3. Strategic partitioning before wide transformation
# 4. Efficient aggregation
# 5. Final narrow transformations for derived metrics
```

This guide provides the foundation for building high-performance Spark applications in Azure environments by understanding and optimizing the fundamental building blocks of data transformations.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*