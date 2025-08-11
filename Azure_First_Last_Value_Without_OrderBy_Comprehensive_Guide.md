# Azure First & Last Value Without ORDER BY - Comprehensive Guide

## Table of Contents
1. [Overview](#overview)
2. [Azure SQL Database Techniques](#azure-sql-database-techniques)
3. [Azure Synapse Analytics Methods](#azure-synapse-analytics-methods)
4. [Azure Databricks/Spark Approaches](#azure-databricksspark-approaches)
5. [Window Functions and Analytics](#window-functions-and-analytics)
6. [Performance Optimization](#performance-optimization)
7. [Real-World Examples](#real-world-examples)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)
10. [Advanced Scenarios](#advanced-scenarios)

---

## Overview

Getting first and last values without explicit ORDER BY clauses is a common requirement in Azure data services. This approach is often needed for performance optimization, when dealing with pre-sorted data, or when working with specific data structures where ordering is implicit.

### Key Scenarios

| Scenario | Use Case | Azure Service |
|----------|----------|---------------|
| **Performance Optimization** | Avoid expensive sorting operations | SQL Database, Synapse |
| **Pre-sorted Data** | Data already ordered by design | All services |
| **Streaming Data** | First/last in time-series data | Stream Analytics, Databricks |
| **Partition-based Processing** | First/last within partitions | Synapse, Databricks |
| **Index-based Retrieval** | Leverage existing indexes | SQL Database, Synapse |

### Common Challenges
- **Non-deterministic Results**: Without ORDER BY, results may be unpredictable
- **Performance Issues**: Some techniques may be slower than expected
- **Data Consistency**: Ensuring consistent results across executions
- **Concurrency Issues**: Handling concurrent data modifications

---

## Azure SQL Database Techniques

### Method 1: Using ROW_NUMBER() with Implicit Ordering

```sql
-- Example: Get first and last records from a table without explicit ORDER BY
-- Assumes data has natural ordering or is pre-sorted

-- Create sample table
CREATE TABLE SalesData (
    SalesId INT IDENTITY(1,1) PRIMARY KEY,
    ProductId INT,
    SaleAmount DECIMAL(10,2),
    SaleDate DATETIME2,
    CustomerId INT,
    RegionId INT
);

-- Insert sample data
INSERT INTO SalesData (ProductId, SaleAmount, SaleDate, CustomerId, RegionId) VALUES
(1, 1500.00, '2024-01-01 10:00:00', 101, 1),
(2, 2300.00, '2024-01-02 11:30:00', 102, 1),
(1, 1800.00, '2024-01-03 09:15:00', 103, 2),
(3, 3200.00, '2024-01-04 14:45:00', 104, 2),
(2, 1900.00, '2024-01-05 16:20:00', 105, 1),
(1, 2100.00, '2024-01-06 12:10:00', 106, 3);

-- Method 1: Using ROW_NUMBER() to get first and last records
WITH NumberedRows AS (
    SELECT 
        SalesId,
        ProductId,
        SaleAmount,
        SaleDate,
        CustomerId,
        RegionId,
        ROW_NUMBER() OVER (PARTITION BY RegionId ORDER BY (SELECT NULL)) AS RowNum,
        COUNT(*) OVER (PARTITION BY RegionId) AS TotalRows
    FROM SalesData
)
SELECT 
    RegionId,
    'First' AS Position,
    SalesId,
    ProductId,
    SaleAmount,
    SaleDate,
    CustomerId
FROM NumberedRows
WHERE RowNum = 1

UNION ALL

SELECT 
    RegionId,
    'Last' AS Position,
    SalesId,
    ProductId,
    SaleAmount,
    SaleDate,
    CustomerId
FROM NumberedRows
WHERE RowNum = TotalRows;
```

### Method 2: Using MIN/MAX with Correlated Subqueries

```sql
-- Method 2: Using MIN/MAX to identify first/last records
-- This works when you have a unique identifier that represents order

SELECT 
    RegionId,
    'First' AS Position,
    s1.SalesId,
    s1.ProductId,
    s1.SaleAmount,
    s1.SaleDate,
    s1.CustomerId
FROM SalesData s1
WHERE s1.SalesId = (
    SELECT MIN(s2.SalesId) 
    FROM SalesData s2 
    WHERE s2.RegionId = s1.RegionId
)

UNION ALL

SELECT 
    RegionId,
    'Last' AS Position,
    s1.SalesId,
    s1.ProductId,
    s1.SaleAmount,
    s1.SaleDate,
    s1.CustomerId
FROM SalesData s1
WHERE s1.SalesId = (
    SELECT MAX(s2.SalesId) 
    FROM SalesData s2 
    WHERE s2.RegionId = s1.RegionId
);
```

### Method 3: Using FIRST_VALUE and LAST_VALUE Window Functions

```sql
-- Method 3: Using FIRST_VALUE and LAST_VALUE without explicit ORDER BY
-- Uses default frame specification

WITH FirstLastValues AS (
    SELECT 
        RegionId,
        SalesId,
        ProductId,
        SaleAmount,
        SaleDate,
        CustomerId,
        FIRST_VALUE(SalesId) OVER (PARTITION BY RegionId) AS FirstSalesId,
        FIRST_VALUE(ProductId) OVER (PARTITION BY RegionId) AS FirstProductId,
        FIRST_VALUE(SaleAmount) OVER (PARTITION BY RegionId) AS FirstSaleAmount,
        LAST_VALUE(SalesId) OVER (PARTITION BY RegionId ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LastSalesId,
        LAST_VALUE(ProductId) OVER (PARTITION BY RegionId ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LastProductId,
        LAST_VALUE(SaleAmount) OVER (PARTITION BY RegionId ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LastSaleAmount
    FROM SalesData
)
SELECT DISTINCT
    RegionId,
    FirstSalesId,
    FirstProductId,
    FirstSaleAmount,
    LastSalesId,
    LastProductId,
    LastSaleAmount
FROM FirstLastValues
ORDER BY RegionId;
```

### Advanced SQL Database Techniques

```sql
-- Method 4: Using CROSS APPLY for complex scenarios
SELECT DISTINCT
    s.RegionId,
    first_record.SalesId AS FirstSalesId,
    first_record.SaleAmount AS FirstSaleAmount,
    last_record.SalesId AS LastSalesId,
    last_record.SaleAmount AS LastSaleAmount
FROM SalesData s
CROSS APPLY (
    SELECT TOP 1 SalesId, SaleAmount
    FROM SalesData s1
    WHERE s1.RegionId = s.RegionId
) first_record
CROSS APPLY (
    SELECT TOP 1 SalesId, SaleAmount
    FROM SalesData s2
    WHERE s2.RegionId = s.RegionId
    ORDER BY SalesId DESC
) last_record;

-- Method 5: Using Common Table Expressions with ranking
WITH RankedData AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY RegionId ORDER BY (SELECT NULL)) AS FirstRank,
        RANK() OVER (PARTITION BY RegionId ORDER BY SalesId DESC) AS LastRank
    FROM SalesData
),
FirstRecords AS (
    SELECT RegionId, SalesId, SaleAmount, 'First' AS Position
    FROM RankedData
    WHERE FirstRank = 1
),
LastRecords AS (
    SELECT RegionId, SalesId, SaleAmount, 'Last' AS Position
    FROM RankedData
    WHERE LastRank = 1
)
SELECT * FROM FirstRecords
UNION ALL
SELECT * FROM LastRecords
ORDER BY RegionId, Position;

-- Method 6: Using EXISTS for performance optimization
SELECT 
    RegionId,
    SalesId,
    SaleAmount,
    'First' AS Position
FROM SalesData s1
WHERE NOT EXISTS (
    SELECT 1 
    FROM SalesData s2 
    WHERE s2.RegionId = s1.RegionId 
    AND s2.SalesId < s1.SalesId
)

UNION ALL

SELECT 
    RegionId,
    SalesId,
    SaleAmount,
    'Last' AS Position
FROM SalesData s1
WHERE NOT EXISTS (
    SELECT 1 
    FROM SalesData s2 
    WHERE s2.RegionId = s1.RegionId 
    AND s2.SalesId > s1.SalesId
);
```

---

## Azure Synapse Analytics Methods

### Synapse-Specific Optimizations

```sql
-- Synapse Method 1: Using distribution keys for performance
-- Assumes table is distributed on RegionId

-- Create distributed table in Synapse
CREATE TABLE SalesDataSynapse (
    SalesId INT IDENTITY(1,1),
    ProductId INT,
    SaleAmount DECIMAL(10,2),
    SaleDate DATETIME2,
    CustomerId INT,
    RegionId INT
)
WITH (
    DISTRIBUTION = HASH(RegionId),
    CLUSTERED COLUMNSTORE INDEX
);

-- Insert data (same as previous example)

-- Optimized query for Synapse using distribution
WITH DistributedFirstLast AS (
    SELECT 
        RegionId,
        SalesId,
        SaleAmount,
        ROW_NUMBER() OVER (PARTITION BY RegionId ORDER BY (SELECT NULL)) AS RowNum,
        COUNT(*) OVER (PARTITION BY RegionId) AS TotalRows
    FROM SalesDataSynapse
)
SELECT 
    RegionId,
    MIN(CASE WHEN RowNum = 1 THEN SalesId END) AS FirstSalesId,
    MIN(CASE WHEN RowNum = 1 THEN SaleAmount END) AS FirstSaleAmount,
    MIN(CASE WHEN RowNum = TotalRows THEN SalesId END) AS LastSalesId,
    MIN(CASE WHEN RowNum = TotalRows THEN SaleAmount END) AS LastSaleAmount
FROM DistributedFirstLast
GROUP BY RegionId;
```

### Synapse Serverless SQL Pool Approach

```sql
-- Synapse Serverless: Working with external data
-- Example using OPENROWSET

WITH ExternalData AS (
    SELECT 
        RegionId,
        SalesId,
        SaleAmount,
        SaleDate
    FROM OPENROWSET(
        BULK 'https://yourstorageaccount.dfs.core.windows.net/data/sales/*.parquet',
        FORMAT = 'PARQUET'
    ) AS sales_data
),
FirstLastExternal AS (
    SELECT 
        RegionId,
        SalesId,
        SaleAmount,
        FIRST_VALUE(SalesId) OVER (PARTITION BY RegionId) AS FirstSalesId,
        FIRST_VALUE(SaleAmount) OVER (PARTITION BY RegionId) AS FirstSaleAmount,
        LAST_VALUE(SalesId) OVER (
            PARTITION BY RegionId 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS LastSalesId,
        LAST_VALUE(SaleAmount) OVER (
            PARTITION BY RegionId 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS LastSaleAmount
    FROM ExternalData
)
SELECT DISTINCT
    RegionId,
    FirstSalesId,
    FirstSaleAmount,
    LastSalesId,
    LastSaleAmount
FROM FirstLastExternal;
```

---

## Azure Databricks/Spark Approaches

### PySpark Methods for First/Last Values

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FirstLastValueWithoutOrderBy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

class FirstLastValueProcessor:
    """
    Comprehensive processor for getting first/last values without explicit ORDER BY
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
    
    def create_sample_data(self):
        """
        Create sample sales data for demonstration
        """
        schema = StructType([
            StructField("sales_id", IntegerType(), False),
            StructField("product_id", IntegerType(), True),
            StructField("sale_amount", DoubleType(), True),
            StructField("sale_date", TimestampType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("region_id", IntegerType(), True)
        ])
        
        data = [
            (1, 1, 1500.00, "2024-01-01 10:00:00", 101, 1),
            (2, 2, 2300.00, "2024-01-02 11:30:00", 102, 1),
            (3, 1, 1800.00, "2024-01-03 09:15:00", 103, 2),
            (4, 3, 3200.00, "2024-01-04 14:45:00", 104, 2),
            (5, 2, 1900.00, "2024-01-05 16:20:00", 105, 1),
            (6, 1, 2100.00, "2024-01-06 12:10:00", 106, 3),
            (7, 3, 2800.00, "2024-01-07 13:25:00", 107, 3),
            (8, 2, 2500.00, "2024-01-08 15:40:00", 108, 2)
        ]
        
        df = self.spark.createDataFrame(data, schema)
        df = df.withColumn("sale_date", to_timestamp(col("sale_date"), "yyyy-MM-dd HH:mm:ss"))
        
        return df
    
    def method1_row_number_approach(self, df, partition_col="region_id"):
        """
        Method 1: Using row_number() without explicit ORDER BY
        """
        print("🔢 Method 1: Row Number Approach")
        
        # Create window specification without ORDER BY (uses default ordering)
        window_spec = Window.partitionBy(partition_col)
        
        # Add row numbers and total count
        df_with_row_numbers = df.withColumn(
            "row_num", 
            row_number().over(window_spec)
        ).withColumn(
            "total_rows", 
            count("*").over(window_spec)
        )
        
        # Get first and last records
        first_records = df_with_row_numbers.filter(col("row_num") == 1) \
                                          .withColumn("position", lit("First"))
        
        last_records = df_with_row_numbers.filter(col("row_num") == col("total_rows")) \
                                         .withColumn("position", lit("Last"))
        
        result = first_records.union(last_records) \
                             .select("region_id", "position", "sales_id", "sale_amount", "sale_date") \
                             .orderBy("region_id", "position")
        
        print("📊 Results:")
        result.show()
        
        return result
    
    def method2_first_last_value_functions(self, df, partition_col="region_id"):
        """
        Method 2: Using first() and last() functions
        """
        print("🎯 Method 2: First/Last Value Functions")
        
        # Using first() and last() functions
        result = df.groupBy(partition_col).agg(
            first("sales_id").alias("first_sales_id"),
            first("sale_amount").alias("first_sale_amount"),
            first("sale_date").alias("first_sale_date"),
            last("sales_id").alias("last_sales_id"),
            last("sale_amount").alias("last_sale_amount"),
            last("sale_date").alias("last_sale_date"),
            count("*").alias("total_records")
        ).orderBy(partition_col)
        
        print("📊 Results:")
        result.show(truncate=False)
        
        return result
    
    def method3_window_functions_without_order(self, df, partition_col="region_id"):
        """
        Method 3: Using window functions without explicit ORDER BY
        """
        print("🪟 Method 3: Window Functions Without Order")
        
        # Define window without ORDER BY clause
        window_spec = Window.partitionBy(partition_col) \
                           .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        
        # Apply first_value and last_value
        result = df.withColumn(
            "first_sales_id", 
            first("sales_id").over(Window.partitionBy(partition_col))
        ).withColumn(
            "first_sale_amount", 
            first("sale_amount").over(Window.partitionBy(partition_col))
        ).withColumn(
            "last_sales_id", 
            last("sales_id").over(window_spec)
        ).withColumn(
            "last_sale_amount", 
            last("sale_amount").over(window_spec)
        ).select(
            partition_col,
            "first_sales_id", 
            "first_sale_amount",
            "last_sales_id", 
            "last_sale_amount"
        ).distinct().orderBy(partition_col)
        
        print("📊 Results:")
        result.show()
        
        return result
    
    def method4_collect_list_approach(self, df, partition_col="region_id"):
        """
        Method 4: Using collect_list to get first/last elements
        """
        print("📝 Method 4: Collect List Approach")
        
        # Collect all values and extract first/last
        result = df.groupBy(partition_col).agg(
            collect_list("sales_id").alias("all_sales_ids"),
            collect_list("sale_amount").alias("all_sale_amounts"),
            count("*").alias("total_records")
        ).withColumn(
            "first_sales_id", 
            col("all_sales_ids")[0]
        ).withColumn(
            "last_sales_id", 
            col("all_sales_ids")[col("total_records") - 1]
        ).withColumn(
            "first_sale_amount", 
            col("all_sale_amounts")[0]
        ).withColumn(
            "last_sale_amount", 
            col("all_sale_amounts")[col("total_records") - 1]
        ).select(
            partition_col,
            "first_sales_id",
            "first_sale_amount", 
            "last_sales_id",
            "last_sale_amount",
            "total_records"
        ).orderBy(partition_col)
        
        print("📊 Results:")
        result.show()
        
        return result
    
    def method5_rdd_approach(self, df, partition_col="region_id"):
        """
        Method 5: Using RDD operations for first/last
        """
        print("🔧 Method 5: RDD Approach")
        
        # Convert to RDD and group by partition column
        rdd_grouped = df.rdd.map(lambda row: (row[partition_col], row)) \
                           .groupByKey() \
                           .mapValues(list)
        
        # Extract first and last from each group
        def extract_first_last(group_data):
            region_id, records = group_data
            records_list = list(records)
            
            if records_list:
                first_record = records_list[0]
                last_record = records_list[-1]
                
                return (
                    region_id,
                    first_record.sales_id,
                    first_record.sale_amount,
                    last_record.sales_id,
                    last_record.sale_amount,
                    len(records_list)
                )
            else:
                return (region_id, None, None, None, None, 0)
        
        result_rdd = rdd_grouped.map(extract_first_last)
        
        # Convert back to DataFrame
        result_schema = StructType([
            StructField("region_id", IntegerType(), True),
            StructField("first_sales_id", IntegerType(), True),
            StructField("first_sale_amount", DoubleType(), True),
            StructField("last_sales_id", IntegerType(), True),
            StructField("last_sale_amount", DoubleType(), True),
            StructField("total_records", IntegerType(), True)
        ])
        
        result = self.spark.createDataFrame(result_rdd, result_schema).orderBy("region_id")
        
        print("📊 Results:")
        result.show()
        
        return result
    
    def method6_sql_approach(self, df, partition_col="region_id"):
        """
        Method 6: Using Spark SQL without explicit ORDER BY
        """
        print("🔍 Method 6: Spark SQL Approach")
        
        # Register DataFrame as temporary view
        df.createOrReplaceTempView("sales_data")
        
        sql_query = f"""
        WITH first_last_values AS (
            SELECT 
                {partition_col},
                sales_id,
                sale_amount,
                FIRST_VALUE(sales_id) OVER (PARTITION BY {partition_col}) AS first_sales_id,
                FIRST_VALUE(sale_amount) OVER (PARTITION BY {partition_col}) AS first_sale_amount,
                LAST_VALUE(sales_id) OVER (
                    PARTITION BY {partition_col} 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS last_sales_id,
                LAST_VALUE(sale_amount) OVER (
                    PARTITION BY {partition_col} 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS last_sale_amount
            FROM sales_data
        )
        SELECT DISTINCT
            {partition_col},
            first_sales_id,
            first_sale_amount,
            last_sales_id,
            last_sale_amount
        FROM first_last_values
        ORDER BY {partition_col}
        """
        
        result = self.spark.sql(sql_query)
        
        print("📊 Results:")
        result.show()
        
        return result
    
    def performance_comparison(self, df):
        """
        Compare performance of different methods
        """
        print("⚡ Performance Comparison")
        
        import time
        
        methods = [
            ("Row Number", self.method1_row_number_approach),
            ("First/Last Functions", self.method2_first_last_value_functions),
            ("Window Functions", self.method3_window_functions_without_order),
            ("Collect List", self.method4_collect_list_approach),
            ("RDD Approach", self.method5_rdd_approach),
            ("Spark SQL", self.method6_sql_approach)
        ]
        
        performance_results = []
        
        for method_name, method_func in methods:
            try:
                start_time = time.time()
                
                # Execute method
                result_df = method_func(df)
                
                # Force execution by collecting results
                result_count = result_df.count()
                
                execution_time = time.time() - start_time
                
                performance_results.append({
                    "method": method_name,
                    "execution_time_seconds": execution_time,
                    "result_count": result_count,
                    "status": "Success"
                })
                
                print(f"✅ {method_name}: {execution_time:.3f}s ({result_count} results)")
                
            except Exception as e:
                performance_results.append({
                    "method": method_name,
                    "execution_time_seconds": None,
                    "result_count": None,
                    "status": f"Error: {str(e)}"
                })
                
                print(f"❌ {method_name}: Failed - {str(e)}")
        
        # Create performance summary DataFrame
        performance_df = self.spark.createDataFrame(performance_results)
        
        print("\n📊 Performance Summary:")
        performance_df.show(truncate=False)
        
        return performance_results
    
    def advanced_first_last_with_conditions(self, df):
        """
        Advanced scenario: First/last values with conditions
        """
        print("🎯 Advanced: First/Last with Conditions")
        
        # Get first/last records where sale_amount > 2000
        high_value_sales = df.filter(col("sale_amount") > 2000)
        
        result = high_value_sales.groupBy("region_id").agg(
            first("sales_id").alias("first_high_value_sales_id"),
            first("sale_amount").alias("first_high_value_amount"),
            last("sales_id").alias("last_high_value_sales_id"),
            last("sale_amount").alias("last_high_value_amount"),
            count("*").alias("high_value_count")
        ).orderBy("region_id")
        
        print("📊 High Value Sales (> 2000) - First/Last:")
        result.show()
        
        return result

# Example usage
processor = FirstLastValueProcessor(spark)

# Create sample data
sales_df = processor.create_sample_data()

print("📋 Sample Data:")
sales_df.show()

print("\n" + "="*80)
print("TESTING DIFFERENT METHODS")
print("="*80)

# Test all methods
method1_result = processor.method1_row_number_approach(sales_df)
print("\n" + "-"*50)

method2_result = processor.method2_first_last_value_functions(sales_df)
print("\n" + "-"*50)

method3_result = processor.method3_window_functions_without_order(sales_df)
print("\n" + "-"*50)

method4_result = processor.method4_collect_list_approach(sales_df)
print("\n" + "-"*50)

method5_result = processor.method5_rdd_approach(sales_df)
print("\n" + "-"*50)

method6_result = processor.method6_sql_approach(sales_df)
print("\n" + "-"*50)

# Performance comparison
print("\n" + "="*80)
print("PERFORMANCE COMPARISON")
print("="*80)
performance_results = processor.performance_comparison(sales_df)

# Advanced scenarios
print("\n" + "="*80)
print("ADVANCED SCENARIOS")
print("="*80)
advanced_result = processor.advanced_first_last_with_conditions(sales_df)
```

### Scala Implementation for Databricks

```scala
// Scala implementation for first/last values without ORDER BY
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class FirstLastValueProcessorScala(spark: SparkSession) {
  import spark.implicits._
  
  def createSampleData(): DataFrame = {
    val data = Seq(
      (1, 1, 1500.00, "2024-01-01 10:00:00", 101, 1),
      (2, 2, 2300.00, "2024-01-02 11:30:00", 102, 1),
      (3, 1, 1800.00, "2024-01-03 09:15:00", 103, 2),
      (4, 3, 3200.00, "2024-01-04 14:45:00", 104, 2),
      (5, 2, 1900.00, "2024-01-05 16:20:00", 105, 1),
      (6, 1, 2100.00, "2024-01-06 12:10:00", 106, 3)
    ).toDF("sales_id", "product_id", "sale_amount", "sale_date", "customer_id", "region_id")
    
    data.withColumn("sale_date", to_timestamp($"sale_date", "yyyy-MM-dd HH:mm:ss"))
  }
  
  def firstLastWithoutOrderBy(df: DataFrame, partitionCol: String = "region_id"): DataFrame = {
    // Method using first() and last() functions
    df.groupBy(partitionCol).agg(
      first("sales_id").as("first_sales_id"),
      first("sale_amount").as("first_sale_amount"),
      last("sales_id").as("last_sales_id"),
      last("sale_amount").as("last_sale_amount"),
      count("*").as("total_records")
    ).orderBy(partitionCol)
  }
  
  def windowFunctionApproach(df: DataFrame, partitionCol: String = "region_id"): DataFrame = {
    val windowSpec = Window.partitionBy(partitionCol)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    df.withColumn("first_sales_id", first("sales_id").over(Window.partitionBy(partitionCol)))
      .withColumn("first_sale_amount", first("sale_amount").over(Window.partitionBy(partitionCol)))
      .withColumn("last_sales_id", last("sales_id").over(windowSpec))
      .withColumn("last_sale_amount", last("sale_amount").over(windowSpec))
      .select(partitionCol, "first_sales_id", "first_sale_amount", "last_sales_id", "last_sale_amount")
      .distinct()
      .orderBy(partitionCol)
  }
}

// Example usage
val spark = SparkSession.builder()
  .appName("FirstLastValueScala")
  .getOrCreate()

val processor = new FirstLastValueProcessorScala(spark)
val sampleData = processor.createSampleData()

println("Sample Data:")
sampleData.show()

println("First/Last using aggregation:")
processor.firstLastWithoutOrderBy(sampleData).show()

println("First/Last using window functions:")
processor.windowFunctionApproach(sampleData).show()
```

---

## Window Functions and Analytics

### Advanced Window Function Techniques

```sql
-- Advanced window function examples for first/last values

-- Example 1: Multiple partitioning levels
WITH MultiLevelPartitioning AS (
    SELECT 
        RegionId,
        ProductId,
        SalesId,
        SaleAmount,
        SaleDate,
        -- First/Last by Region
        FIRST_VALUE(SalesId) OVER (PARTITION BY RegionId) AS FirstInRegion,
        LAST_VALUE(SalesId) OVER (
            PARTITION BY RegionId 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS LastInRegion,
        -- First/Last by Region and Product
        FIRST_VALUE(SalesId) OVER (PARTITION BY RegionId, ProductId) AS FirstInRegionProduct,
        LAST_VALUE(SalesId) OVER (
            PARTITION BY RegionId, ProductId 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS LastInRegionProduct,
        -- Running totals without explicit ORDER BY
        SUM(SaleAmount) OVER (PARTITION BY RegionId) AS RegionTotal,
        COUNT(*) OVER (PARTITION BY RegionId) AS RegionCount
    FROM SalesData
)
SELECT 
    RegionId,
    ProductId,
    COUNT(*) AS RecordCount,
    MIN(FirstInRegion) AS RegionFirstSalesId,
    MAX(LastInRegion) AS RegionLastSalesId,
    MIN(FirstInRegionProduct) AS ProductFirstSalesId,
    MAX(LastInRegionProduct) AS ProductLastSalesId,
    AVG(RegionTotal) AS AvgRegionTotal
FROM MultiLevelPartitioning
GROUP BY RegionId, ProductId
ORDER BY RegionId, ProductId;

-- Example 2: Conditional first/last values
WITH ConditionalFirstLast AS (
    SELECT 
        RegionId,
        SalesId,
        SaleAmount,
        ProductId,
        -- First/Last high-value sales (> 2000)
        CASE 
            WHEN SaleAmount > 2000 THEN 
                FIRST_VALUE(SalesId) OVER (
                    PARTITION BY RegionId 
                    ORDER BY CASE WHEN SaleAmount > 2000 THEN 1 ELSE 2 END
                )
        END AS FirstHighValueSale,
        CASE 
            WHEN SaleAmount > 2000 THEN 
                LAST_VALUE(SalesId) OVER (
                    PARTITION BY RegionId 
                    ORDER BY CASE WHEN SaleAmount > 2000 THEN 1 ELSE 2 END
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
        END AS LastHighValueSale
    FROM SalesData
)
SELECT 
    RegionId,
    MIN(FirstHighValueSale) AS FirstHighValueSalesId,
    MAX(LastHighValueSale) AS LastHighValueSalesId,
    COUNT(CASE WHEN SaleAmount > 2000 THEN 1 END) AS HighValueCount
FROM ConditionalFirstLast
GROUP BY RegionId
ORDER BY RegionId;
```

### Analytical Functions for Complex Scenarios

```sql
-- Example 3: Using LAG/LEAD for first/last identification
WITH LagLeadAnalysis AS (
    SELECT 
        RegionId,
        SalesId,
        SaleAmount,
        LAG(SalesId) OVER (PARTITION BY RegionId) AS PreviousSalesId,
        LEAD(SalesId) OVER (PARTITION BY RegionId) AS NextSalesId,
        -- Identify first record (no previous)
        CASE WHEN LAG(SalesId) OVER (PARTITION BY RegionId) IS NULL THEN 'First' END AS IsFirst,
        -- Identify last record (no next)
        CASE WHEN LEAD(SalesId) OVER (PARTITION BY RegionId) IS NULL THEN 'Last' END AS IsLast
    FROM SalesData
)
SELECT 
    RegionId,
    SalesId,
    SaleAmount,
    COALESCE(IsFirst, IsLast, 'Middle') AS Position
FROM LagLeadAnalysis
WHERE IsFirst IS NOT NULL OR IsLast IS NOT NULL
ORDER BY RegionId, SalesId;

-- Example 4: Percentile-based first/last
WITH PercentileAnalysis AS (
    SELECT 
        RegionId,
        SalesId,
        SaleAmount,
        PERCENT_RANK() OVER (PARTITION BY RegionId) AS PercentRank,
        NTILE(10) OVER (PARTITION BY RegionId) AS Decile
    FROM SalesData
)
SELECT 
    RegionId,
    -- First 10% (decile 1)
    MIN(CASE WHEN Decile = 1 THEN SalesId END) AS FirstDecileSalesId,
    AVG(CASE WHEN Decile = 1 THEN SaleAmount END) AS FirstDecileAvgAmount,
    -- Last 10% (decile 10)
    MAX(CASE WHEN Decile = 10 THEN SalesId END) AS LastDecileSalesId,
    AVG(CASE WHEN Decile = 10 THEN SaleAmount END) AS LastDecileAvgAmount
FROM PercentileAnalysis
GROUP BY RegionId
ORDER BY RegionId;
```

---

## Performance Optimization

### Index-Based Optimization Strategies

```sql
-- Performance optimization techniques

-- 1. Create appropriate indexes for first/last queries
CREATE NONCLUSTERED INDEX IX_SalesData_RegionId_SalesId 
ON SalesData (RegionId, SalesId);

CREATE NONCLUSTERED INDEX IX_SalesData_RegionId_SaleDate 
ON SalesData (RegionId, SaleDate);

-- 2. Optimized query using index hints
SELECT 
    s1.RegionId,
    s1.SalesId AS FirstSalesId,
    s1.SaleAmount AS FirstSaleAmount,
    s2.SalesId AS LastSalesId,
    s2.SaleAmount AS LastSaleAmount
FROM SalesData s1 WITH (INDEX(IX_SalesData_RegionId_SalesId))
INNER JOIN (
    SELECT 
        RegionId,
        MIN(SalesId) AS MinSalesId,
        MAX(SalesId) AS MaxSalesId
    FROM SalesData WITH (INDEX(IX_SalesData_RegionId_SalesId))
    GROUP BY RegionId
) minmax ON s1.RegionId = minmax.RegionId AND s1.SalesId = minmax.MinSalesId
INNER JOIN SalesData s2 WITH (INDEX(IX_SalesData_RegionId_SalesId))
    ON minmax.RegionId = s2.RegionId AND s2.SalesId = minmax.MaxSalesId;

-- 3. Memory-optimized table approach (SQL Server/Azure SQL)
CREATE TABLE SalesDataMemoryOptimized (
    SalesId INT IDENTITY(1,1) NOT NULL PRIMARY KEY NONCLUSTERED,
    ProductId INT,
    SaleAmount DECIMAL(10,2),
    SaleDate DATETIME2,
    CustomerId INT,
    RegionId INT,
    INDEX IX_RegionId_SalesId NONCLUSTERED (RegionId, SalesId)
) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);

-- Optimized query for memory-optimized table
WITH NATIVE_COMPILATION, SCHEMABINDING
AS
BEGIN ATOMIC WITH (
    TRANSACTION ISOLATION LEVEL = SNAPSHOT,
    LANGUAGE = N'us_english'
)
    SELECT 
        RegionId,
        MIN(SalesId) AS FirstSalesId,
        MAX(SalesId) AS LastSalesId
    FROM dbo.SalesDataMemoryOptimized
    GROUP BY RegionId;
END;
```

### Databricks Performance Optimization

```python
# Performance optimization for Databricks/Spark

def optimize_first_last_performance(df, partition_col="region_id"):
    """
    Optimized approach for first/last values in large datasets
    """
    
    # 1. Cache the dataframe if it will be reused
    df.cache()
    
    # 2. Optimize partitioning
    optimized_df = df.repartition(col(partition_col))
    
    # 3. Use broadcast join for small lookup tables
    if optimized_df.count() < 1000000:  # For smaller datasets
        # Collect min/max values
        min_max_df = optimized_df.groupBy(partition_col).agg(
            min("sales_id").alias("min_sales_id"),
            max("sales_id").alias("max_sales_id")
        )
        
        # Broadcast the small lookup table
        min_max_broadcast = broadcast(min_max_df)
        
        # Join to get first records
        first_records = optimized_df.join(
            min_max_broadcast,
            (col(f"df.{partition_col}") == col(f"min_max_df.{partition_col}")) &
            (col("df.sales_id") == col("min_sales_id"))
        ).select(
            col(f"df.{partition_col}").alias(partition_col),
            col("df.sales_id").alias("first_sales_id"),
            col("df.sale_amount").alias("first_sale_amount")
        )
        
        # Join to get last records
        last_records = optimized_df.join(
            min_max_broadcast,
            (col(f"df.{partition_col}") == col(f"min_max_df.{partition_col}")) &
            (col("df.sales_id") == col("max_sales_id"))
        ).select(
            col(f"df.{partition_col}").alias(partition_col),
            col("df.sales_id").alias("last_sales_id"),
            col("df.sale_amount").alias("last_sale_amount")
        )
        
        # Combine results
        result = first_records.join(last_records, partition_col)
        
    else:  # For larger datasets
        # Use window functions with optimized partitioning
        window_spec = Window.partitionBy(partition_col)
        
        result = optimized_df.withColumn(
            "first_sales_id", first("sales_id").over(window_spec)
        ).withColumn(
            "first_sale_amount", first("sale_amount").over(window_spec)
        ).withColumn(
            "last_sales_id", 
            last("sales_id").over(
                window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            )
        ).withColumn(
            "last_sale_amount", 
            last("sale_amount").over(
                window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            )
        ).select(
            partition_col, "first_sales_id", "first_sale_amount", 
            "last_sales_id", "last_sale_amount"
        ).distinct()
    
    return result

# Example usage with performance monitoring
import time

def benchmark_first_last_methods(df):
    """
    Benchmark different methods for performance
    """
    methods = {
        "Window Functions": lambda df: df.withColumn(
            "first_val", first("sales_id").over(Window.partitionBy("region_id"))
        ).withColumn(
            "last_val", last("sales_id").over(
                Window.partitionBy("region_id")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            )
        ).select("region_id", "first_val", "last_val").distinct(),
        
        "Group By Aggregation": lambda df: df.groupBy("region_id").agg(
            first("sales_id").alias("first_val"),
            last("sales_id").alias("last_val")
        ),
        
        "Optimized Method": lambda df: optimize_first_last_performance(df)
    }
    
    results = {}
    
    for method_name, method_func in methods.items():
        start_time = time.time()
        
        try:
            result_df = method_func(df)
            count = result_df.count()  # Force execution
            
            execution_time = time.time() - start_time
            results[method_name] = {
                "execution_time": execution_time,
                "record_count": count,
                "status": "Success"
            }
            
            print(f"✅ {method_name}: {execution_time:.3f}s ({count} records)")
            
        except Exception as e:
            results[method_name] = {
                "execution_time": None,
                "record_count": None,
                "status": f"Error: {str(e)}"
            }
            
            print(f"❌ {method_name}: Failed - {str(e)}")
    
    return results

# Run benchmark
sales_df = processor.create_sample_data()
benchmark_results = benchmark_first_last_methods(sales_df)
```

---

## Real-World Examples

### Example 1: Time-Series Data Processing

```python
# Real-world example: Processing time-series sensor data

def process_sensor_data_first_last():
    """
    Process sensor data to get first and last readings per device per day
    """
    
    # Create sample sensor data
    from datetime import datetime, timedelta
    import random
    
    # Generate sample data
    base_date = datetime(2024, 1, 1)
    sensor_data = []
    
    for device_id in range(1, 6):  # 5 devices
        for day in range(10):  # 10 days
            for reading in range(24):  # 24 readings per day (hourly)
                timestamp = base_date + timedelta(days=day, hours=reading)
                temperature = 20 + random.uniform(-5, 15)  # Temperature between 15-35°C
                humidity = 50 + random.uniform(-20, 30)    # Humidity between 30-80%
                
                sensor_data.append((
                    device_id,
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    round(temperature, 2),
                    round(humidity, 2)
                ))
    
    # Create DataFrame
    schema = StructType([
        StructField("device_id", IntegerType(), False),
        StructField("timestamp", StringType(), False),
        StructField("temperature", DoubleType(), False),
        StructField("humidity", DoubleType(), False)
    ])
    
    sensor_df = spark.createDataFrame(sensor_data, schema)
    sensor_df = sensor_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    sensor_df = sensor_df.withColumn("date", to_date(col("timestamp")))
    
    print("📊 Sample Sensor Data:")
    sensor_df.show(10)
    
    # Get first and last readings per device per day without explicit ORDER BY
    daily_first_last = sensor_df.groupBy("device_id", "date").agg(
        first("timestamp").alias("first_reading_time"),
        first("temperature").alias("first_temperature"),
        first("humidity").alias("first_humidity"),
        last("timestamp").alias("last_reading_time"),
        last("temperature").alias("last_temperature"),
        last("humidity").alias("last_humidity"),
        count("*").alias("total_readings"),
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    ).orderBy("device_id", "date")
    
    print("\n📈 Daily First/Last Readings per Device:")
    daily_first_last.show(20, truncate=False)
    
    # Calculate daily temperature change (last - first)
    temperature_change = daily_first_last.withColumn(
        "temperature_change",
        col("last_temperature") - col("first_temperature")
    ).withColumn(
        "humidity_change",
        col("last_humidity") - col("first_humidity")
    )
    
    print("\n🌡️ Daily Temperature and Humidity Changes:")
    temperature_change.select(
        "device_id", "date", "temperature_change", "humidity_change",
        "total_readings", "avg_temperature", "avg_humidity"
    ).show(20)
    
    return temperature_change

# Execute sensor data processing
sensor_results = process_sensor_data_first_last()
```

### Example 2: Financial Data Analysis

```sql
-- Real-world example: Stock price analysis

-- Create sample stock data
CREATE TABLE StockPrices (
    StockId INT,
    Symbol NVARCHAR(10),
    TradeDate DATE,
    OpenPrice DECIMAL(10,2),
    ClosePrice DECIMAL(10,2),
    HighPrice DECIMAL(10,2),
    LowPrice DECIMAL(10,2),
    Volume BIGINT
);

-- Insert sample data
INSERT INTO StockPrices VALUES
(1, 'AAPL', '2024-01-01', 150.00, 155.00, 157.00, 149.00, 1000000),
(1, 'AAPL', '2024-01-02', 155.00, 158.00, 160.00, 154.00, 1200000),
(1, 'AAPL', '2024-01-03', 158.00, 156.00, 159.00, 155.00, 900000),
(2, 'GOOGL', '2024-01-01', 2800.00, 2850.00, 2870.00, 2790.00, 500000),
(2, 'GOOGL', '2024-01-02', 2850.00, 2820.00, 2860.00, 2810.00, 600000),
(2, 'GOOGL', '2024-01-03', 2820.00, 2880.00, 2890.00, 2815.00, 700000);

-- Get first and last trading prices per stock without explicit ORDER BY
WITH StockFirstLast AS (
    SELECT 
        Symbol,
        -- First trading day data (assuming data is naturally ordered)
        FIRST_VALUE(TradeDate) OVER (PARTITION BY Symbol) AS FirstTradeDate,
        FIRST_VALUE(OpenPrice) OVER (PARTITION BY Symbol) AS FirstOpenPrice,
        FIRST_VALUE(ClosePrice) OVER (PARTITION BY Symbol) AS FirstClosePrice,
        -- Last trading day data
        LAST_VALUE(TradeDate) OVER (
            PARTITION BY Symbol 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS LastTradeDate,
        LAST_VALUE(OpenPrice) OVER (
            PARTITION BY Symbol 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS LastOpenPrice,
        LAST_VALUE(ClosePrice) OVER (
            PARTITION BY Symbol 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS LastClosePrice,
        -- Additional metrics
        COUNT(*) OVER (PARTITION BY Symbol) AS TradingDays,
        AVG(ClosePrice) OVER (PARTITION BY Symbol) AS AvgClosePrice,
        MAX(HighPrice) OVER (PARTITION BY Symbol) AS PeriodHigh,
        MIN(LowPrice) OVER (PARTITION BY Symbol) AS PeriodLow
    FROM StockPrices
)
SELECT DISTINCT
    Symbol,
    FirstTradeDate,
    FirstOpenPrice,
    FirstClosePrice,
    LastTradeDate,
    LastOpenPrice,
    LastClosePrice,
    -- Calculate return
    ROUND(((LastClosePrice - FirstOpenPrice) / FirstOpenPrice) * 100, 2) AS TotalReturn,
    TradingDays,
    ROUND(AvgClosePrice, 2) AS AvgClosePrice,
    PeriodHigh,
    PeriodLow,
    ROUND(((PeriodHigh - PeriodLow) / PeriodLow) * 100, 2) AS VolatilityRange
FROM StockFirstLast
ORDER BY Symbol;
```

### Example 3: Customer Journey Analysis

```python
# Real-world example: Customer journey first/last touchpoints

def analyze_customer_journey():
    """
    Analyze customer journey to identify first and last touchpoints
    """
    
    # Create sample customer journey data
    journey_data = [
        (1, 101, "Email", "2024-01-01 09:00:00", "Campaign_A"),
        (2, 101, "Website", "2024-01-01 10:30:00", "Organic"),
        (3, 101, "Social", "2024-01-01 14:00:00", "Facebook"),
        (4, 101, "Purchase", "2024-01-01 16:45:00", "Website"),
        (5, 102, "Social", "2024-01-01 11:00:00", "Instagram"),
        (6, 102, "Website", "2024-01-01 12:15:00", "Direct"),
        (7, 102, "Email", "2024-01-01 15:30:00", "Campaign_B"),
        (8, 103, "Website", "2024-01-01 08:30:00", "Google_Ads"),
        (9, 103, "Purchase", "2024-01-01 09:15:00", "Website"),
        (10, 104, "Email", "2024-01-01 13:00:00", "Campaign_A"),
        (11, 104, "Website", "2024-01-01 13:45:00", "Email_Link"),
        (12, 104, "Social", "2024-01-01 17:30:00", "Twitter"),
        (13, 104, "Purchase", "2024-01-01 18:00:00", "Website")
    ]
    
    schema = StructType([
        StructField("touchpoint_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("channel", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("source", StringType(), False)
    ])
    
    journey_df = spark.createDataFrame(journey_data, schema)
    journey_df = journey_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    print("📊 Customer Journey Data:")
    journey_df.show()
    
    # Get first and last touchpoints per customer without explicit ORDER BY
    customer_journey_analysis = journey_df.groupBy("customer_id").agg(
        first("channel").alias("first_touchpoint_channel"),
        first("source").alias("first_touchpoint_source"),
        first("timestamp").alias("first_touchpoint_time"),
        last("channel").alias("last_touchpoint_channel"),
        last("source").alias("last_touchpoint_source"),
        last("timestamp").alias("last_touchpoint_time"),
        count("*").alias("total_touchpoints"),
        collect_list("channel").alias("journey_channels")
    )
    
    # Add journey analysis
    journey_analysis = customer_journey_analysis.withColumn(
        "journey_duration_hours",
        (unix_timestamp("last_touchpoint_time") - unix_timestamp("first_touchpoint_time")) / 3600
    ).withColumn(
        "is_converted",
        when(col("last_touchpoint_channel") == "Purchase", "Yes").otherwise("No")
    ).withColumn(
        "journey_complexity",
        when(col("total_touchpoints") == 1, "Single Touch")
        .when(col("total_touchpoints") <= 3, "Simple")
        .when(col("total_touchpoints") <= 5, "Moderate")
        .otherwise("Complex")
    )
    
    print("\n🛤️ Customer Journey Analysis:")
    journey_analysis.show(truncate=False)
    
    # Attribution analysis - first vs last touch
    attribution_analysis = journey_analysis.groupBy(
        "first_touchpoint_channel", "last_touchpoint_channel", "is_converted"
    ).agg(
        count("*").alias("customer_count"),
        avg("journey_duration_hours").alias("avg_journey_duration"),
        avg("total_touchpoints").alias("avg_touchpoints")
    ).orderBy("customer_count", ascending=False)
    
    print("\n🎯 Attribution Analysis (First vs Last Touch):")
    attribution_analysis.show()
    
    return journey_analysis, attribution_analysis

# Execute customer journey analysis
journey_results, attribution_results = analyze_customer_journey()
```

---

## Best Practices

### General Best Practices

```python
class FirstLastValueBestPractices:
    """
    Best practices for getting first/last values without ORDER BY
    """
    
    @staticmethod
    def practice_1_understand_data_order():
        """
        Practice 1: Always understand your data's natural ordering
        """
        practices = {
            "data_source_analysis": [
                "Understand how data is inserted/loaded",
                "Check if there's natural ordering (timestamps, IDs)",
                "Verify data consistency across different environments"
            ],
            "index_awareness": [
                "Know which columns have indexes",
                "Understand clustered vs non-clustered indexes",
                "Consider index scan vs index seek implications"
            ],
            "partitioning_impact": [
                "Understand how table partitioning affects ordering",
                "Consider partition elimination benefits",
                "Account for cross-partition queries"
            ]
        }
        
        return practices
    
    @staticmethod
    def practice_2_performance_considerations():
        """
        Practice 2: Performance optimization strategies
        """
        return {
            "small_datasets": {
                "recommendation": "Use simple aggregation functions",
                "methods": ["first()", "last()", "min()", "max()"],
                "avoid": ["Complex window functions", "Multiple passes"]
            },
            "large_datasets": {
                "recommendation": "Use indexed columns and partitioning",
                "methods": ["Window functions with proper frames", "Indexed lookups"],
                "optimize": ["Partition pruning", "Index seeks", "Parallel processing"]
            },
            "streaming_data": {
                "recommendation": "Leverage natural time ordering",
                "methods": ["Watermarking", "Event time processing"],
                "considerations": ["Late arriving data", "Out-of-order events"]
            }
        }
    
    @staticmethod
    def practice_3_error_handling():
        """
        Practice 3: Robust error handling
        """
        return {
            "null_handling": [
                "Use COALESCE for null values",
                "Consider NULLS FIRST/LAST behavior",
                "Handle empty result sets gracefully"
            ],
            "data_consistency": [
                "Implement data validation checks",
                "Use transactions for consistency",
                "Handle concurrent modifications"
            ],
            "result_validation": [
                "Verify expected result counts",
                "Implement sanity checks",
                "Log unexpected patterns"
            ]
        }
    
    @staticmethod
    def practice_4_testing_strategies():
        """
        Practice 4: Comprehensive testing approaches
        """
        return {
            "unit_tests": [
                "Test with empty datasets",
                "Test with single records",
                "Test with duplicate values"
            ],
            "performance_tests": [
                "Benchmark different approaches",
                "Test with various data sizes",
                "Monitor resource usage"
            ],
            "edge_cases": [
                "Test with all NULL values",
                "Test with identical timestamps",
                "Test with missing partitions"
            ]
        }

# Example implementation of best practices
def implement_robust_first_last(df, partition_col, value_col):
    """
    Robust implementation following best practices
    """
    
    try:
        # Validate inputs
        if not df or df.count() == 0:
            print("⚠️ Warning: Empty dataset provided")
            return spark.createDataFrame([], StructType([
                StructField(partition_col, StringType(), True),
                StructField("first_value", StringType(), True),
                StructField("last_value", StringType(), True),
                StructField("record_count", IntegerType(), True)
            ]))
        
        # Check if required columns exist
        if partition_col not in df.columns or value_col not in df.columns:
            raise ValueError(f"Required columns not found: {partition_col}, {value_col}")
        
        # Get first/last values with null handling
        result = df.groupBy(partition_col).agg(
            first(value_col, ignorenulls=True).alias("first_value"),
            last(value_col, ignorenulls=True).alias("last_value"),
            count("*").alias("record_count"),
            sum(when(col(value_col).isNull(), 1).otherwise(0)).alias("null_count")
        )
        
        # Add data quality indicators
        result = result.withColumn(
            "data_quality",
            when(col("null_count") == 0, "Clean")
            .when(col("null_count") < col("record_count") * 0.1, "Good")
            .when(col("null_count") < col("record_count") * 0.5, "Fair")
            .otherwise("Poor")
        )
        
        # Log results
        total_partitions = result.count()
        print(f"✅ Successfully processed {total_partitions} partitions")
        
        return result
        
    except Exception as e:
        print(f"❌ Error in first/last processing: {str(e)}")
        raise

# Example usage with error handling
try:
    sales_df = processor.create_sample_data()
    robust_result = implement_robust_first_last(sales_df, "region_id", "sale_amount")
    robust_result.show()
except Exception as e:
    print(f"Processing failed: {e}")
```

---

## Troubleshooting

### Common Issues and Solutions

```python
class FirstLastTroubleshooting:
    """
    Troubleshooting guide for first/last value operations
    """
    
    @staticmethod
    def issue_1_non_deterministic_results():
        """
        Issue 1: Results change between executions
        """
        return {
            "problem": "Getting different first/last values in different runs",
            "causes": [
                "No explicit ordering specified",
                "Parallel processing with different execution plans",
                "Data modifications between runs",
                "Non-deterministic data source ordering"
            ],
            "solutions": [
                "Add tie-breaker columns for consistent ordering",
                "Use deterministic functions",
                "Implement proper locking/isolation",
                "Cache intermediate results"
            ],
            "example_fix": """
            -- Instead of this (non-deterministic):
            SELECT FIRST_VALUE(column) OVER (PARTITION BY group_col)
            
            -- Use this (deterministic):
            SELECT FIRST_VALUE(column) OVER (
                PARTITION BY group_col 
                ORDER BY id  -- Add deterministic ordering
            )
            """
        }
    
    @staticmethod
    def issue_2_performance_problems():
        """
        Issue 2: Poor performance with large datasets
        """
        return {
            "problem": "Slow execution with large datasets",
            "causes": [
                "Full table scans",
                "Lack of appropriate indexes",
                "Inefficient window function usage",
                "Memory pressure from large partitions"
            ],
            "solutions": [
                "Create covering indexes",
                "Use partition elimination",
                "Optimize window frame specifications",
                "Consider data sampling for analysis"
            ],
            "example_optimization": """
            -- Create index for better performance
            CREATE INDEX IX_Table_PartitionCol_OrderCol 
            ON TableName (PartitionColumn, OrderColumn);
            
            -- Use optimized window frame
            LAST_VALUE(column) OVER (
                PARTITION BY group_col 
                ORDER BY order_col
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            """
        }
    
    @staticmethod
    def issue_3_null_value_handling():
        """
        Issue 3: Unexpected NULL values in results
        """
        return {
            "problem": "Getting NULL values when expecting data",
            "causes": [
                "All values in partition are NULL",
                "Incorrect NULL handling in functions",
                "Data type conversion issues",
                "Missing data in source"
            ],
            "solutions": [
                "Use COALESCE for fallback values",
                "Add NULL checks in WHERE clauses",
                "Use IGNORE NULLS in window functions",
                "Implement data validation"
            ],
            "example_fix": """
            -- Handle NULLs properly
            SELECT 
                partition_col,
                COALESCE(
                    FIRST_VALUE(column IGNORE NULLS) OVER (PARTITION BY partition_col),
                    'DEFAULT_VALUE'
                ) AS first_value
            FROM table_name
            WHERE column IS NOT NULL  -- Filter NULLs if appropriate
            """
        }
    
    @staticmethod
    def diagnostic_queries():
        """
        Diagnostic queries for troubleshooting
        """
        return {
            "data_distribution_check": """
            -- Check data distribution across partitions
            SELECT 
                partition_column,
                COUNT(*) as record_count,
                MIN(order_column) as min_value,
                MAX(order_column) as max_value,
                COUNT(DISTINCT order_column) as distinct_values
            FROM your_table
            GROUP BY partition_column
            ORDER BY record_count DESC;
            """,
            
            "null_analysis": """
            -- Analyze NULL patterns
            SELECT 
                partition_column,
                COUNT(*) as total_records,
                SUM(CASE WHEN value_column IS NULL THEN 1 ELSE 0 END) as null_count,
                CAST(SUM(CASE WHEN value_column IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as null_percentage
            FROM your_table
            GROUP BY partition_column
            ORDER BY null_percentage DESC;
            """,
            
            "performance_analysis": """
            -- Analyze query performance
            SET STATISTICS IO ON;
            SET STATISTICS TIME ON;
            
            -- Your first/last query here
            
            -- Check execution plan
            SET SHOWPLAN_ALL ON;
            -- Run your query again
            SET SHOWPLAN_ALL OFF;
            """
        }

def run_diagnostics(df, partition_col="region_id", value_col="sale_amount"):
    """
    Run diagnostic checks on DataFrame
    """
    print("🔍 Running Diagnostics...")
    
    # Check 1: Data distribution
    print("\n📊 Data Distribution Check:")
    distribution = df.groupBy(partition_col).agg(
        count("*").alias("record_count"),
        min(value_col).alias("min_value"),
        max(value_col).alias("max_value"),
        countDistinct(value_col).alias("distinct_values"),
        sum(when(col(value_col).isNull(), 1).otherwise(0)).alias("null_count")
    ).orderBy(desc("record_count"))
    
    distribution.show()
    
    # Check 2: Value patterns
    print("\n🔍 Value Pattern Analysis:")
    patterns = df.groupBy(partition_col, value_col).count().orderBy(partition_col, desc("count"))
    patterns.show(20)
    
    # Check 3: First/Last comparison
    print("\n🎯 First/Last Value Comparison:")
    comparison = df.groupBy(partition_col).agg(
        first(value_col).alias("first_value"),
        last(value_col).alias("last_value"),
        min(value_col).alias("min_value"),
        max(value_col).alias("max_value")
    ).withColumn(
        "first_equals_min", 
        when(col("first_value") == col("min_value"), "Yes").otherwise("No")
    ).withColumn(
        "last_equals_max", 
        when(col("last_value") == col("max_value"), "Yes").otherwise("No")
    )
    
    comparison.show()
    
    return {
        "distribution": distribution,
        "patterns": patterns,
        "comparison": comparison
    }

# Run diagnostics on sample data
sales_df = processor.create_sample_data()
diagnostic_results = run_diagnostics(sales_df)
```

---

## Conclusion

This comprehensive guide covers all aspects of getting first and last values without explicit ORDER BY clauses in Azure data services:

### Key Takeaways:

1. **Multiple Approaches**: Various methods available depending on platform and requirements
2. **Performance Considerations**: Choose the right method based on data size and structure
3. **Data Understanding**: Critical to understand natural data ordering
4. **Error Handling**: Implement robust error handling for production use
5. **Testing**: Comprehensive testing across different scenarios

### Recommended Implementation Strategy:

1. **Analyze Data Characteristics**: Understand your data's natural ordering
2. **Choose Appropriate Method**: Select based on performance requirements
3. **Implement Error Handling**: Add robust error handling and validation
4. **Test Thoroughly**: Test with various data scenarios and edge cases
5. **Monitor Performance**: Continuously monitor and optimize

This guide provides production-ready patterns for implementing first/last value operations efficiently across Azure data platforms.

---

*Generated on: $(date)*
*Azure First & Last Value Without ORDER BY - Comprehensive Guide*

