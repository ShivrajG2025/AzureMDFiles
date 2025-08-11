# Azure Lazy Evaluation Comprehensive Guide
## Complete Guide to Lazy Evaluation in Azure Services

---

### Table of Contents

1. [Overview](#overview)
2. [Spark Lazy Evaluation Fundamentals](#spark-lazy-evaluation-fundamentals)
3. [Azure Databricks Lazy Evaluation](#azure-databricks-lazy-evaluation)
4. [Transformations vs Actions](#transformations-vs-actions)
5. [Lazy Evaluation in Practice](#lazy-evaluation-in-practice)
6. [Query Optimization with Lazy Evaluation](#query-optimization-with-lazy-evaluation)
7. [Performance Analysis](#performance-analysis)
8. [Advanced Lazy Evaluation Patterns](#advanced-lazy-evaluation-patterns)
9. [Monitoring and Debugging](#monitoring-and-debugging)
10. [Best Practices](#best-practices)

---

## Overview

Lazy evaluation is a fundamental concept in distributed computing systems, particularly in Apache Spark and Azure Databricks. It refers to the strategy of delaying computation until the results are actually needed, enabling powerful optimizations and efficient resource utilization.

### What is Lazy Evaluation?

```json
{
  "lazy_evaluation_concept": {
    "definition": "Computation strategy where expressions are not evaluated immediately but only when their results are needed",
    "key_characteristics": {
      "deferred_execution": "Operations are recorded but not executed immediately",
      "optimization_opportunities": "Entire computation graph can be optimized before execution",
      "resource_efficiency": "Only necessary computations are performed",
      "fault_tolerance": "Operations can be replayed if needed"
    },
    "contrast_with_eager_evaluation": {
      "eager": "Operations executed immediately when called",
      "lazy": "Operations recorded and executed only when results needed"
    }
  }
}
```

### Benefits of Lazy Evaluation

```json
{
  "lazy_evaluation_benefits": {
    "performance_optimization": {
      "query_optimization": "Catalyst optimizer can analyze entire query plan",
      "predicate_pushdown": "Filters applied early to reduce data movement",
      "column_pruning": "Only required columns are processed",
      "join_optimization": "Optimal join strategies selected based on data characteristics"
    },
    "resource_efficiency": {
      "memory_management": "Data loaded only when needed",
      "compute_optimization": "Unnecessary computations avoided",
      "network_efficiency": "Reduced data shuffling through optimization",
      "storage_efficiency": "Intermediate results not stored unnecessarily"
    },
    "fault_tolerance": {
      "lineage_tracking": "Complete computation lineage maintained",
      "automatic_recovery": "Failed computations can be replayed",
      "checkpoint_optimization": "Strategic checkpointing based on computation graph"
    },
    "development_flexibility": {
      "iterative_development": "Build complex transformations incrementally",
      "debugging_capabilities": "Inspect computation plans before execution",
      "testing_efficiency": "Test transformations without executing on full data"
    }
  }
}
```

### Azure Services with Lazy Evaluation

```json
{
  "azure_services_lazy_evaluation": {
    "apache_spark_based": {
      "azure_databricks": {
        "description": "Managed Apache Spark with lazy evaluation",
        "features": ["Catalyst optimizer", "Adaptive query execution", "Delta Lake optimization"]
      },
      "azure_synapse_spark": {
        "description": "Spark pools in Azure Synapse Analytics",
        "features": ["Integrated with SQL pools", "Automatic scaling", "Lazy evaluation optimization"]
      },
      "azure_hdinsight_spark": {
        "description": "Spark clusters on Azure HDInsight",
        "features": ["Customizable Spark configurations", "Integration with Azure storage"]
      }
    },
    "related_concepts": {
      "azure_data_factory": {
        "description": "Pipeline orchestration with lazy evaluation patterns",
        "features": ["Conditional execution", "Dynamic content evaluation"]
      },
      "azure_stream_analytics": {
        "description": "Real-time analytics with lazy evaluation concepts",
        "features": ["Query optimization", "Windowing functions"]
      }
    }
  }
}
```

---

## Spark Lazy Evaluation Fundamentals

### Understanding the Computation Graph

```python
# Comprehensive demonstration of Spark lazy evaluation
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import json

class LazyEvaluationDemo:
    """Comprehensive demonstration of lazy evaluation in Spark"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("LazyEvaluationDemo") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("=== Spark Lazy Evaluation Demo Initialized ===")
    
    def demonstrate_lazy_vs_eager(self):
        """Demonstrate the difference between lazy and eager evaluation"""
        
        print("\n=== Lazy vs Eager Evaluation Demonstration ===")
        
        # Create sample dataset
        data = [(i, f"name_{i}", i * 100, f"category_{i % 5}") 
                for i in range(1, 100001)]
        
        df = self.spark.createDataFrame(data, ["id", "name", "amount", "category"])
        
        print("1. Lazy Evaluation Example:")
        print("   Building transformation chain...")
        
        # Record start time
        lazy_start = time.time()
        
        # These are all transformations - lazy evaluation
        lazy_result = df \
            .filter(col("amount") > 50000) \
            .withColumn("amount_category", 
                when(col("amount") > 80000, "High")
                .when(col("amount") > 60000, "Medium")
                .otherwise("Low")) \
            .groupBy("category", "amount_category") \
            .agg(
                count("id").alias("count"),
                avg("amount").alias("avg_amount"),
                sum("amount").alias("total_amount")
            ) \
            .orderBy("category", "amount_category")
        
        lazy_build_time = time.time() - lazy_start
        print(f"   Transformation chain built in: {lazy_build_time:.4f} seconds")
        print("   No computation performed yet - this is lazy evaluation!")
        
        # Now trigger execution with an action
        print("   Triggering execution with action...")
        action_start = time.time()
        result_count = lazy_result.count()  # This is an action - triggers execution
        action_time = time.time() - action_start
        
        print(f"   Action executed in: {action_time:.4f} seconds")
        print(f"   Result count: {result_count}")
        print(f"   Total time (build + execute): {lazy_build_time + action_time:.4f} seconds")
        
        return {
            "build_time": lazy_build_time,
            "execution_time": action_time,
            "total_time": lazy_build_time + action_time,
            "result_count": result_count
        }
    
    def demonstrate_transformation_chaining(self):
        """Demonstrate how transformations are chained lazily"""
        
        print("\n=== Transformation Chaining Demonstration ===")
        
        # Create sample e-commerce dataset
        ecommerce_data = [
            (i, f"customer_{i % 1000}", f"product_{i % 500}", 
             random.choice(["Electronics", "Clothing", "Books", "Home"]),
             random.uniform(10, 1000), 
             random.choice(["online", "store", "mobile"]),
             f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}")
            for i in range(1, 50001)
        ]
        
        schema = StructType([
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("category", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("channel", StringType(), False),
            StructField("order_date", StringType(), False)
        ])
        
        df = self.spark.createDataFrame(ecommerce_data, schema)
        
        print("Building complex transformation chain:")
        
        # Step 1: Basic transformations (lazy)
        print("  Step 1: Adding computed columns...")
        step1 = df.withColumn("order_date", to_date(col("order_date"))) \
                  .withColumn("year_month", date_format(col("order_date"), "yyyy-MM")) \
                  .withColumn("amount_tier", 
                      when(col("amount") > 500, "Premium")
                      .when(col("amount") > 200, "Standard")
                      .otherwise("Basic"))
        
        # Step 2: Filtering (lazy)
        print("  Step 2: Applying filters...")
        step2 = step1.filter(col("amount") > 50) \
                     .filter(col("category").isin(["Electronics", "Clothing"]))
        
        # Step 3: Aggregations (lazy)
        print("  Step 3: Creating aggregations...")
        step3 = step2.groupBy("category", "channel", "amount_tier", "year_month") \
                     .agg(
                         count("order_id").alias("order_count"),
                         sum("amount").alias("total_revenue"),
                         avg("amount").alias("avg_order_value"),
                         countDistinct("customer_id").alias("unique_customers")
                     )
        
        # Step 4: Additional transformations (lazy)
        print("  Step 4: Final transformations...")
        final_result = step3.withColumn("revenue_per_customer", 
                                       col("total_revenue") / col("unique_customers")) \
                           .withColumn("category_channel", 
                                     concat(col("category"), lit("_"), col("channel"))) \
                           .orderBy("year_month", "total_revenue")
        
        print("  All transformations defined - no execution yet!")
        
        # Show the logical plan
        print("\n  Logical Plan Preview:")
        print("  " + str(final_result.explain(extended=False)))
        
        # Now execute
        print("\n  Executing transformation chain...")
        execution_start = time.time()
        results = final_result.collect()
        execution_time = time.time() - execution_start
        
        print(f"  Execution completed in: {execution_time:.4f} seconds")
        print(f"  Results count: {len(results)}")
        
        # Show sample results
        print("\n  Sample Results:")
        for i, row in enumerate(results[:5]):
            print(f"    {i+1}: {row.asDict()}")
        
        return {
            "transformation_steps": 4,
            "execution_time": execution_time,
            "result_count": len(results),
            "final_dataframe": final_result
        }
    
    def demonstrate_catalyst_optimizer(self):
        """Demonstrate how Catalyst optimizer works with lazy evaluation"""
        
        print("\n=== Catalyst Optimizer Demonstration ===")
        
        # Create datasets for join optimization demo
        customers = self.spark.createDataFrame([
            (i, f"customer_{i}", f"email_{i}@example.com", 
             random.choice(["Gold", "Silver", "Bronze"]))
            for i in range(1, 10001)
        ], ["customer_id", "customer_name", "email", "tier"])
        
        orders = self.spark.createDataFrame([
            (i, i % 10000 + 1, random.uniform(50, 500), 
             f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}")
            for i in range(1, 100001)
        ], ["order_id", "customer_id", "amount", "order_date"])
        
        # Complex query that will benefit from Catalyst optimization
        print("Creating complex query for Catalyst optimization...")
        
        complex_query = orders \
            .filter(col("amount") > 100) \
            .join(customers, "customer_id") \
            .filter(col("tier").isin(["Gold", "Silver"])) \
            .withColumn("order_date", to_date(col("order_date"))) \
            .filter(col("order_date") >= "2023-06-01") \
            .groupBy("tier", "customer_name") \
            .agg(
                count("order_id").alias("order_count"),
                sum("amount").alias("total_spent"),
                avg("amount").alias("avg_order_value")
            ) \
            .filter(col("total_spent") > 1000) \
            .orderBy(desc("total_spent"))
        
        # Show different plan stages
        print("\n1. Logical Plan (before optimization):")
        complex_query.explain(mode="simple")
        
        print("\n2. Optimized Physical Plan:")
        complex_query.explain(mode="formatted")
        
        print("\n3. Executing optimized query...")
        start_time = time.time()
        optimized_results = complex_query.collect()
        execution_time = time.time() - start_time
        
        print(f"   Optimized execution time: {execution_time:.4f} seconds")
        print(f"   Results: {len(optimized_results)} rows")
        
        # Demonstrate predicate pushdown
        print("\n4. Predicate Pushdown Example:")
        
        # Query with filters that can be pushed down
        pushdown_query = orders \
            .join(customers, "customer_id") \
            .filter(col("amount") > 200) \
            .filter(col("tier") == "Gold") \
            .select("customer_name", "amount", "tier")
        
        print("   Query with predicate pushdown opportunities:")
        pushdown_query.explain(mode="simple")
        
        return {
            "optimization_enabled": True,
            "execution_time": execution_time,
            "result_count": len(optimized_results),
            "optimized_query": complex_query
        }

import random

# Initialize and run demonstrations
demo = LazyEvaluationDemo()
lazy_vs_eager_results = demo.demonstrate_lazy_vs_eager()
chaining_results = demo.demonstrate_transformation_chaining()
catalyst_results = demo.demonstrate_catalyst_optimizer()
```

### Transformation vs Action Classification

```python
class TransformationActionAnalyzer:
    """Analyze and categorize Spark operations as transformations or actions"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def demonstrate_transformations(self):
        """Demonstrate various Spark transformations (lazy operations)"""
        
        print("=== Spark Transformations (Lazy Operations) ===")
        
        # Create sample dataset
        data = [(i, f"name_{i}", i * 10, f"dept_{i % 5}") for i in range(1, 1001)]
        df = self.spark.createDataFrame(data, ["id", "name", "salary", "department"])
        
        transformations = {
            "select": {
                "description": "Select specific columns",
                "operation": lambda df: df.select("id", "name", "salary"),
                "lazy": True
            },
            "filter": {
                "description": "Filter rows based on condition",
                "operation": lambda df: df.filter(col("salary") > 500),
                "lazy": True
            },
            "withColumn": {
                "description": "Add or modify columns",
                "operation": lambda df: df.withColumn("bonus", col("salary") * 0.1),
                "lazy": True
            },
            "groupBy": {
                "description": "Group data for aggregation",
                "operation": lambda df: df.groupBy("department").agg(avg("salary")),
                "lazy": True
            },
            "orderBy": {
                "description": "Sort data by columns",
                "operation": lambda df: df.orderBy("salary"),
                "lazy": True
            },
            "join": {
                "description": "Join with another DataFrame",
                "operation": lambda df: df.join(df.select("id", "department").distinct(), "department"),
                "lazy": True
            },
            "union": {
                "description": "Union with another DataFrame",
                "operation": lambda df: df.union(df.limit(10)),
                "lazy": True
            },
            "distinct": {
                "description": "Remove duplicate rows",
                "operation": lambda df: df.select("department").distinct(),
                "lazy": True
            },
            "repartition": {
                "description": "Change number of partitions",
                "operation": lambda df: df.repartition(4),
                "lazy": True
            },
            "cache": {
                "description": "Mark DataFrame for caching",
                "operation": lambda df: df.cache(),
                "lazy": True  # Marking is lazy, actual caching happens on first action
            }
        }
        
        print("Transformations (executed lazily):")
        transformation_results = {}
        
        for name, info in transformations.items():
            print(f"\n{name.upper()}:")
            print(f"  Description: {info['description']}")
            print(f"  Lazy evaluation: {info['lazy']}")
            
            # Apply transformation (this should be very fast as it's lazy)
            start_time = time.time()
            transformed_df = info['operation'](df)
            transformation_time = time.time() - start_time
            
            print(f"  Transformation time: {transformation_time:.6f} seconds (should be very fast)")
            print(f"  Type: {type(transformed_df)}")
            
            transformation_results[name] = {
                "transformation_time": transformation_time,
                "dataframe": transformed_df,
                "is_lazy": info['lazy']
            }
        
        return transformation_results
    
    def demonstrate_actions(self):
        """Demonstrate various Spark actions (trigger execution)"""
        
        print("\n=== Spark Actions (Trigger Execution) ===")
        
        # Create sample dataset
        data = [(i, f"name_{i}", i * 10, f"dept_{i % 5}") for i in range(1, 10001)]
        df = self.spark.createDataFrame(data, ["id", "name", "salary", "department"])
        
        # Apply some transformations first
        transformed_df = df.filter(col("salary") > 500) \
                          .withColumn("bonus", col("salary") * 0.1) \
                          .groupBy("department") \
                          .agg(
                              count("id").alias("employee_count"),
                              avg("salary").alias("avg_salary"),
                              sum("bonus").alias("total_bonus")
                          )
        
        actions = {
            "collect": {
                "description": "Collect all rows to driver",
                "operation": lambda df: df.collect(),
                "returns": "List of Rows"
            },
            "count": {
                "description": "Count number of rows",
                "operation": lambda df: df.count(),
                "returns": "Integer"
            },
            "show": {
                "description": "Display rows in console",
                "operation": lambda df: df.show(5, truncate=False),
                "returns": "None (side effect)"
            },
            "take": {
                "description": "Take first n rows",
                "operation": lambda df: df.take(3),
                "returns": "List of Rows"
            },
            "first": {
                "description": "Get first row",
                "operation": lambda df: df.first(),
                "returns": "Row object"
            },
            "head": {
                "description": "Get first n rows (alias for take)",
                "operation": lambda df: df.head(2),
                "returns": "List of Rows"
            },
            "foreach": {
                "description": "Apply function to each row",
                "operation": lambda df: df.foreach(lambda row: None),  # No-op for demo
                "returns": "None (side effect)"
            },
            "write_save": {
                "description": "Write DataFrame to storage",
                "operation": lambda df: df.write.mode("overwrite").option("header", "true").csv("/tmp/demo_output"),
                "returns": "None (side effect)"
            },
            "toPandas": {
                "description": "Convert to Pandas DataFrame",
                "operation": lambda df: df.limit(100).toPandas(),  # Limit for demo
                "returns": "Pandas DataFrame"
            }
        }
        
        print("Actions (trigger immediate execution):")
        action_results = {}
        
        for name, info in actions.items():
            print(f"\n{name.upper()}:")
            print(f"  Description: {info['description']}")
            print(f"  Returns: {info['returns']}")
            
            # Execute action (this will trigger computation)
            start_time = time.time()
            try:
                result = info['operation'](transformed_df)
                execution_time = time.time() - start_time
                
                print(f"  Execution time: {execution_time:.4f} seconds")
                
                # Show result preview based on type
                if name == "collect":
                    print(f"  Result: {len(result)} rows collected")
                elif name == "count":
                    print(f"  Result: {result} rows")
                elif name == "take" or name == "head":
                    print(f"  Result: {len(result)} rows taken")
                elif name == "first":
                    print(f"  Result: {result}")
                elif name == "toPandas":
                    print(f"  Result: Pandas DataFrame with shape {result.shape}")
                else:
                    print(f"  Result: Operation completed")
                
                action_results[name] = {
                    "execution_time": execution_time,
                    "result": result if name not in ["show", "foreach", "write_save"] else "completed",
                    "triggered_execution": True
                }
                
            except Exception as e:
                print(f"  Error: {str(e)}")
                action_results[name] = {
                    "execution_time": 0,
                    "result": None,
                    "error": str(e)
                }
        
        return action_results
    
    def analyze_execution_timing(self):
        """Analyze timing differences between transformations and actions"""
        
        print("\n=== Execution Timing Analysis ===")
        
        # Create larger dataset for meaningful timing
        print("Creating large dataset...")
        large_data = [(i, f"name_{i}", i * random.randint(1, 100), 
                      f"dept_{i % 20}", f"location_{i % 10}") 
                     for i in range(1, 100001)]
        
        df = self.spark.createDataFrame(large_data, 
                                       ["id", "name", "salary", "department", "location"])
        
        # Build complex transformation chain
        print("\nBuilding complex transformation chain...")
        transform_start = time.time()
        
        complex_transformations = df \
            .filter(col("salary") > 1000) \
            .withColumn("salary_grade", 
                when(col("salary") > 8000, "A")
                .when(col("salary") > 5000, "B")
                .when(col("salary") > 2000, "C")
                .otherwise("D")) \
            .groupBy("department", "location", "salary_grade") \
            .agg(
                count("id").alias("employee_count"),
                avg("salary").alias("avg_salary"),
                min("salary").alias("min_salary"),
                max("salary").alias("max_salary")
            ) \
            .withColumn("salary_range", col("max_salary") - col("min_salary")) \
            .filter(col("employee_count") > 5) \
            .orderBy(desc("avg_salary"))
        
        transform_time = time.time() - transform_start
        print(f"Transformation chain built in: {transform_time:.6f} seconds")
        
        # Execute different actions and measure timing
        actions_to_test = [
            ("count", lambda df: df.count()),
            ("collect_limited", lambda df: df.limit(10).collect()),
            ("show", lambda df: df.show(5)),
            ("first", lambda df: df.first())
        ]
        
        execution_times = {}
        
        for action_name, action_func in actions_to_test:
            print(f"\nExecuting {action_name}...")
            action_start = time.time()
            result = action_func(complex_transformations)
            action_time = time.time() - action_start
            
            execution_times[action_name] = action_time
            print(f"{action_name} execution time: {action_time:.4f} seconds")
        
        # Summary
        print(f"\n=== Timing Summary ===")
        print(f"Transformation chain build time: {transform_time:.6f} seconds (lazy)")
        print("Action execution times (trigger computation):")
        for action, time_taken in execution_times.items():
            print(f"  {action}: {time_taken:.4f} seconds")
        
        return {
            "transformation_time": transform_time,
            "action_times": execution_times,
            "total_transformations": 8,  # Approximate count
            "dataset_size": len(large_data)
        }

# Run transformation vs action analysis
analyzer = TransformationActionAnalyzer(demo.spark)
transformation_results = analyzer.demonstrate_transformations()
action_results = analyzer.demonstrate_actions()
timing_analysis = analyzer.analyze_execution_timing()
```

---

## Azure Databricks Lazy Evaluation

### Databricks-Specific Optimizations

```python
class DatabricksLazyEvaluationDemo:
    """Demonstrate Databricks-specific lazy evaluation features"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.dbutils = None  # Would be available in Databricks environment
    
    def demonstrate_adaptive_query_execution(self):
        """Demonstrate Adaptive Query Execution (AQE) with lazy evaluation"""
        
        print("=== Adaptive Query Execution Demo ===")
        
        # Check AQE configuration
        aqe_enabled = self.spark.conf.get("spark.sql.adaptive.enabled", "false")
        print(f"Adaptive Query Execution enabled: {aqe_enabled}")
        
        # Enable AQE features
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
        
        # Create datasets with different characteristics for AQE demonstration
        large_table = self.spark.range(1000000) \
            .withColumn("category", (col("id") % 100).cast("string")) \
            .withColumn("value", rand() * 1000) \
            .withColumn("status", when(rand() > 0.7, "active").otherwise("inactive"))
        
        small_table = self.spark.range(50) \
            .withColumn("category", col("id").cast("string")) \
            .withColumn("category_name", concat(lit("Category_"), col("id"))) \
            .withColumn("priority", when(col("id") < 10, "high").otherwise("normal"))
        
        print("\n1. Join Query with AQE Optimization:")
        
        # Query that will benefit from AQE
        aqe_query = large_table \
            .filter(col("value") > 500) \
            .join(small_table, "category") \
            .groupBy("category_name", "priority", "status") \
            .agg(
                count("id").alias("record_count"),
                avg("value").alias("avg_value"),
                sum("value").alias("total_value")
            ) \
            .filter(col("record_count") > 100) \
            .orderBy(desc("total_value"))
        
        print("Query plan with AQE:")
        aqe_query.explain(mode="formatted")
        
        # Execute and measure
        start_time = time.time()
        aqe_results = aqe_query.collect()
        aqe_execution_time = time.time() - start_time
        
        print(f"AQE execution time: {aqe_execution_time:.4f} seconds")
        print(f"Results: {len(aqe_results)} rows")
        
        # Compare with AQE disabled
        print("\n2. Same query with AQE disabled:")
        self.spark.conf.set("spark.sql.adaptive.enabled", "false")
        
        start_time = time.time()
        non_aqe_results = aqe_query.collect()
        non_aqe_execution_time = time.time() - start_time
        
        print(f"Non-AQE execution time: {non_aqe_execution_time:.4f} seconds")
        
        # Re-enable AQE
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        
        improvement = ((non_aqe_execution_time - aqe_execution_time) / non_aqe_execution_time) * 100
        print(f"Performance improvement with AQE: {improvement:.1f}%")
        
        return {
            "aqe_enabled": True,
            "aqe_execution_time": aqe_execution_time,
            "non_aqe_execution_time": non_aqe_execution_time,
            "performance_improvement": improvement,
            "result_count": len(aqe_results)
        }
    
    def demonstrate_delta_lake_lazy_evaluation(self):
        """Demonstrate lazy evaluation with Delta Lake operations"""
        
        print("\n=== Delta Lake Lazy Evaluation Demo ===")
        
        try:
            from delta.tables import DeltaTable
            from delta import configure_spark_with_delta_pip
            
            # Create Delta table path
            delta_path = "/tmp/delta_lazy_demo"
            
            # Create initial Delta table
            print("1. Creating Delta table with lazy transformations:")
            
            initial_data = self.spark.range(100000) \
                .withColumn("customer_id", (col("id") % 10000).cast("string")) \
                .withColumn("product_category", 
                    when(col("id") % 4 == 0, "Electronics")
                    .when(col("id") % 4 == 1, "Clothing")
                    .when(col("id") % 4 == 2, "Books")
                    .otherwise("Home")) \
                .withColumn("purchase_amount", rand() * 1000) \
                .withColumn("purchase_date", 
                    date_add(lit("2023-01-01"), (col("id") % 365).cast("int"))) \
                .withColumn("is_premium", rand() > 0.8)
            
            # Write to Delta (this triggers execution)
            print("   Writing initial data to Delta table...")
            write_start = time.time()
            initial_data.write.format("delta").mode("overwrite").save(delta_path)
            write_time = time.time() - write_start
            print(f"   Initial write time: {write_time:.4f} seconds")
            
            # Load Delta table
            delta_df = self.spark.read.format("delta").load(delta_path)
            
            # Demonstrate lazy operations on Delta table
            print("\n2. Lazy transformations on Delta table:")
            
            lazy_operations = delta_df \
                .filter(col("purchase_amount") > 200) \
                .withColumn("amount_tier", 
                    when(col("purchase_amount") > 800, "Tier1")
                    .when(col("purchase_amount") > 500, "Tier2")
                    .when(col("purchase_amount") > 300, "Tier3")
                    .otherwise("Tier4")) \
                .withColumn("year_month", date_format(col("purchase_date"), "yyyy-MM")) \
                .groupBy("product_category", "amount_tier", "year_month") \
                .agg(
                    count("id").alias("transaction_count"),
                    sum("purchase_amount").alias("total_revenue"),
                    avg("purchase_amount").alias("avg_transaction"),
                    countDistinct("customer_id").alias("unique_customers")
                ) \
                .withColumn("revenue_per_customer", 
                    col("total_revenue") / col("unique_customers")) \
                .filter(col("transaction_count") > 10) \
                .orderBy("year_month", desc("total_revenue"))
            
            print("   Lazy operations chain built (no execution yet)")
            
            # Show optimized plan
            print("\n3. Delta Lake optimized execution plan:")
            lazy_operations.explain(mode="simple")
            
            # Execute operations
            print("\n4. Executing lazy operations on Delta table:")
            exec_start = time.time()
            delta_results = lazy_operations.collect()
            exec_time = time.time() - exec_start
            
            print(f"   Execution time: {exec_time:.4f} seconds")
            print(f"   Results: {len(delta_results)} rows")
            
            # Demonstrate Delta-specific optimizations
            print("\n5. Delta Lake optimization features:")
            
            # Z-order optimization (lazy until executed)
            print("   Creating Z-order optimization command (lazy):")
            zorder_sql = f"OPTIMIZE delta.`{delta_path}` ZORDER BY (product_category, purchase_date)"
            print(f"   SQL: {zorder_sql}")
            
            # Vacuum operation (lazy until executed)
            print("   Creating vacuum command (lazy):")
            vacuum_sql = f"VACUUM delta.`{delta_path}` RETAIN 168 HOURS"
            print(f"   SQL: {vacuum_sql}")
            
            return {
                "delta_table_path": delta_path,
                "initial_write_time": write_time,
                "lazy_execution_time": exec_time,
                "result_count": len(delta_results),
                "optimization_available": True
            }
            
        except ImportError:
            print("Delta Lake not available in this environment")
            return {"delta_available": False}
    
    def demonstrate_photon_engine_lazy_evaluation(self):
        """Demonstrate Photon engine optimization with lazy evaluation"""
        
        print("\n=== Photon Engine Lazy Evaluation Demo ===")
        
        # Check if Photon is enabled
        photon_enabled = self.spark.conf.get("spark.databricks.photon.enabled", "false")
        print(f"Photon Engine enabled: {photon_enabled}")
        
        if photon_enabled.lower() == "true":
            print("Photon engine optimizations apply to lazy evaluation:")
            
            # Create dataset optimized for Photon
            photon_data = self.spark.range(1000000) \
                .withColumn("numeric_col1", col("id") * 1.5) \
                .withColumn("numeric_col2", col("id") / 3.0) \
                .withColumn("string_col", concat(lit("item_"), col("id").cast("string"))) \
                .withColumn("category", (col("id") % 20).cast("string"))
            
            # Photon-optimized operations (lazy)
            photon_optimized = photon_data \
                .filter(col("numeric_col1") > 1000) \
                .withColumn("computed_value", col("numeric_col1") + col("numeric_col2")) \
                .groupBy("category") \
                .agg(
                    sum("computed_value").alias("total_value"),
                    avg("numeric_col1").alias("avg_numeric1"),
                    count("id").alias("record_count")
                ) \
                .filter(col("record_count") > 1000) \
                .orderBy(desc("total_value"))
            
            print("Photon-optimized query plan:")
            photon_optimized.explain(mode="simple")
            
            # Execute with Photon optimization
            start_time = time.time()
            photon_results = photon_optimized.collect()
            photon_time = time.time() - start_time
            
            print(f"Photon execution time: {photon_time:.4f} seconds")
            print(f"Results: {len(photon_results)} rows")
            
            return {
                "photon_enabled": True,
                "execution_time": photon_time,
                "result_count": len(photon_results),
                "optimization_type": "Photon vectorized execution"
            }
        else:
            print("Photon engine not enabled - using standard Spark execution")
            return {"photon_enabled": False}
    
    def demonstrate_caching_with_lazy_evaluation(self):
        """Demonstrate intelligent caching strategies with lazy evaluation"""
        
        print("\n=== Caching with Lazy Evaluation Demo ===")
        
        # Create base dataset
        base_data = self.spark.range(500000) \
            .withColumn("category", (col("id") % 10).cast("string")) \
            .withColumn("value", rand() * 1000) \
            .withColumn("timestamp", 
                current_timestamp() - expr(f"INTERVAL {col('id') % 86400} SECONDS"))
        
        print("1. Without caching - multiple actions on same transformations:")
        
        # Complex transformation that will be reused
        complex_transform = base_data \
            .filter(col("value") > 100) \
            .withColumn("value_category", 
                when(col("value") > 800, "High")
                .when(col("value") > 400, "Medium")
                .otherwise("Low")) \
            .withColumn("hour", hour(col("timestamp"))) \
            .groupBy("category", "value_category", "hour") \
            .agg(
                count("id").alias("count"),
                avg("value").alias("avg_value"),
                sum("value").alias("total_value")
            )
        
        # Multiple actions without caching
        print("   Executing multiple actions without caching:")
        
        start_time = time.time()
        count1 = complex_transform.count()
        action1_time = time.time() - start_time
        
        start_time = time.time()
        first_row = complex_transform.first()
        action2_time = time.time() - start_time
        
        start_time = time.time()
        sample_data = complex_transform.limit(10).collect()
        action3_time = time.time() - start_time
        
        total_time_no_cache = action1_time + action2_time + action3_time
        print(f"   Total time without caching: {total_time_no_cache:.4f} seconds")
        
        print("\n2. With caching - same operations:")
        
        # Cache the transformation (lazy - actual caching happens on first action)
        cached_transform = complex_transform.cache()
        
        print("   DataFrame cached (lazy - not materialized yet)")
        
        # Execute actions with caching
        start_time = time.time()
        count2 = cached_transform.count()  # First action materializes cache
        cached_action1_time = time.time() - start_time
        
        start_time = time.time()
        first_row_cached = cached_transform.first()  # Uses cache
        cached_action2_time = time.time() - start_time
        
        start_time = time.time()
        sample_data_cached = cached_transform.limit(10).collect()  # Uses cache
        cached_action3_time = time.time() - start_time
        
        total_time_cached = cached_action1_time + cached_action2_time + cached_action3_time
        
        print(f"   Total time with caching: {total_time_cached:.4f} seconds")
        
        improvement = ((total_time_no_cache - total_time_cached) / total_time_no_cache) * 100
        print(f"   Performance improvement: {improvement:.1f}%")
        
        # Show cache statistics
        print("\n3. Cache statistics:")
        print("   Cache status: Cached after first action")
        print(f"   Storage level: {cached_transform.storageLevel}")
        
        # Unpersist cache
        cached_transform.unpersist()
        print("   Cache cleared")
        
        return {
            "no_cache_time": total_time_no_cache,
            "cached_time": total_time_cached,
            "cache_improvement": improvement,
            "cache_materialization": "Lazy until first action"
        }

# Run Databricks-specific demos
databricks_demo = DatabricksLazyEvaluationDemo(demo.spark)
aqe_results = databricks_demo.demonstrate_adaptive_query_execution()
delta_results = databricks_demo.demonstrate_delta_lake_lazy_evaluation()
photon_results = databricks_demo.demonstrate_photon_engine_lazy_evaluation()
caching_results = databricks_demo.demonstrate_caching_with_lazy_evaluation()
```

---

## Query Optimization with Lazy Evaluation

### Catalyst Optimizer Deep Dive

```python
class CatalystOptimizerAnalysis:
    """Deep dive into Catalyst optimizer and lazy evaluation"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def demonstrate_optimization_phases(self):
        """Demonstrate different phases of Catalyst optimization"""
        
        print("=== Catalyst Optimizer Phases Demo ===")
        
        # Create complex dataset for optimization demonstration
        employees = self.spark.createDataFrame([
            (i, f"emp_{i}", random.randint(30000, 120000), 
             random.choice(["Engineering", "Sales", "Marketing", "HR", "Finance"]),
             random.choice(["Manager", "Senior", "Junior", "Lead"]),
             f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}")
            for i in range(1, 50001)
        ], ["emp_id", "name", "salary", "department", "level", "hire_date"])
        
        departments = self.spark.createDataFrame([
            ("Engineering", "Tech", "Building A", 1000000),
            ("Sales", "Revenue", "Building B", 800000),
            ("Marketing", "Growth", "Building C", 600000),
            ("HR", "People", "Building D", 400000),
            ("Finance", "Money", "Building E", 500000)
        ], ["dept_name", "dept_type", "location", "budget"])
        
        # Complex query for optimization analysis
        complex_query = employees \
            .filter(col("salary") > 50000) \
            .join(departments, employees.department == departments.dept_name) \
            .filter(col("budget") > 500000) \
            .withColumn("hire_date", to_date(col("hire_date"))) \
            .filter(col("hire_date") >= "2023-06-01") \
            .withColumn("salary_ratio", col("salary") / col("budget")) \
            .groupBy("dept_type", "location", "level") \
            .agg(
                count("emp_id").alias("emp_count"),
                avg("salary").alias("avg_salary"),
                sum("salary").alias("total_salary"),
                avg("salary_ratio").alias("avg_ratio")
            ) \
            .filter(col("emp_count") > 10) \
            .orderBy(desc("avg_salary"))
        
        print("1. Logical Plan Analysis:")
        print("   Raw logical plan (before optimization):")
        complex_query.explain(mode="simple")
        
        print("\n2. Optimized Logical Plan:")
        print("   After Catalyst rule-based optimization:")
        complex_query.explain(mode="extended")
        
        print("\n3. Physical Plan:")
        print("   Executable physical plan:")
        complex_query.explain(mode="formatted")
        
        # Demonstrate specific optimizations
        print("\n4. Specific Optimization Examples:")
        
        # Predicate Pushdown
        print("   a) Predicate Pushdown:")
        pushdown_example = employees \
            .join(departments, "department") \
            .filter(col("salary") > 60000) \
            .filter(col("budget") > 600000) \
            .select("name", "salary", "dept_type")
        
        print("      Filters pushed down to data sources before join")
        pushdown_example.explain(mode="simple")
        
        # Column Pruning
        print("\n   b) Column Pruning:")
        column_pruning_example = employees \
            .join(departments, "department") \
            .select("name", "salary", "location")  # Only these columns will be read
        
        print("      Only required columns read from sources")
        column_pruning_example.explain(mode="simple")
        
        # Constant Folding
        print("\n   c) Constant Folding:")
        constant_folding_example = employees \
            .withColumn("bonus", lit(1000) + lit(500)) \
            .withColumn("tax_rate", lit(0.1) * lit(1.2)) \
            .select("name", "salary", "bonus", "tax_rate")
        
        print("      Constants computed at compile time")
        constant_folding_example.explain(mode="simple")
        
        return {
            "optimization_phases": ["Analysis", "Logical Optimization", "Physical Planning", "Code Generation"],
            "optimizations_applied": ["Predicate Pushdown", "Column Pruning", "Constant Folding", "Join Reordering"],
            "query_complexity": "High - multiple joins, filters, aggregations"
        }
    
    def demonstrate_join_optimization(self):
        """Demonstrate join optimization strategies"""
        
        print("\n=== Join Optimization with Lazy Evaluation ===")
        
        # Create datasets of different sizes for join optimization
        large_table = self.spark.range(1000000) \
            .withColumn("category", (col("id") % 1000).cast("string")) \
            .withColumn("value", rand() * 1000)
        
        small_table = self.spark.range(100) \
            .withColumn("category", col("id").cast("string")) \
            .withColumn("category_info", concat(lit("Info_"), col("id")))
        
        medium_table = self.spark.range(10000) \
            .withColumn("category", (col("id") % 1000).cast("string")) \
            .withColumn("metadata", concat(lit("Meta_"), col("id")))
        
        print("1. Broadcast Join (Small table joined with large table):")
        
        # Force broadcast join
        broadcast_join = large_table.join(broadcast(small_table), "category")
        
        print("   Query plan with broadcast join:")
        broadcast_join.explain(mode="simple")
        
        # Execute and measure
        start_time = time.time()
        broadcast_result = broadcast_join.count()
        broadcast_time = time.time() - start_time
        
        print(f"   Broadcast join execution time: {broadcast_time:.4f} seconds")
        print(f"   Result count: {broadcast_result:,}")
        
        print("\n2. Sort-Merge Join (Large tables):")
        
        sort_merge_join = large_table.join(medium_table, "category")
        
        print("   Query plan with sort-merge join:")
        sort_merge_join.explain(mode="simple")
        
        # Execute and measure
        start_time = time.time()
        sort_merge_result = sort_merge_join.count()
        sort_merge_time = time.time() - start_time
        
        print(f"   Sort-merge join execution time: {sort_merge_time:.4f} seconds")
        print(f"   Result count: {sort_merge_result:,}")
        
        print("\n3. Multi-way Join Optimization:")
        
        # Complex multi-way join
        multi_join = large_table \
            .join(small_table, "category") \
            .join(medium_table, "category") \
            .select("id", "value", "category_info", "metadata")
        
        print("   Multi-way join optimization:")
        multi_join.explain(mode="formatted")
        
        # Execute multi-way join
        start_time = time.time()
        multi_result = multi_join.count()
        multi_time = time.time() - start_time
        
        print(f"   Multi-way join execution time: {multi_time:.4f} seconds")
        
        return {
            "broadcast_join_time": broadcast_time,
            "sort_merge_join_time": sort_merge_time,
            "multi_way_join_time": multi_time,
            "optimization_strategies": ["Broadcast", "Sort-Merge", "Hash", "Nested Loop"]
        }
    
    def demonstrate_aggregation_optimization(self):
        """Demonstrate aggregation optimization with lazy evaluation"""
        
        print("\n=== Aggregation Optimization Demo ===")
        
        # Create dataset for aggregation testing
        sales_data = self.spark.range(1000000) \
            .withColumn("product_id", (col("id") % 10000).cast("string")) \
            .withColumn("customer_id", (col("id") % 50000).cast("string")) \
            .withColumn("region", 
                when(col("id") % 4 == 0, "North")
                .when(col("id") % 4 == 1, "South")
                .when(col("id") % 4 == 2, "East")
                .otherwise("West")) \
            .withColumn("amount", rand() * 1000) \
            .withColumn("quantity", (rand() * 10 + 1).cast("int"))
        
        print("1. Simple Aggregation Optimization:")
        
        simple_agg = sales_data \
            .groupBy("region") \
            .agg(
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                count("id").alias("transaction_count")
            )
        
        print("   Simple aggregation plan:")
        simple_agg.explain(mode="simple")
        
        start_time = time.time()
        simple_result = simple_agg.collect()
        simple_time = time.time() - start_time
        
        print(f"   Simple aggregation time: {simple_time:.4f} seconds")
        
        print("\n2. Multi-level Aggregation:")
        
        multi_level_agg = sales_data \
            .groupBy("region", "product_id") \
            .agg(sum("amount").alias("product_total")) \
            .groupBy("region") \
            .agg(
                sum("product_total").alias("region_total"),
                count("product_id").alias("unique_products"),
                avg("product_total").alias("avg_product_total")
            )
        
        print("   Multi-level aggregation plan:")
        multi_level_agg.explain(mode="simple")
        
        start_time = time.time()
        multi_result = multi_level_agg.collect()
        multi_time = time.time() - start_time
        
        print(f"   Multi-level aggregation time: {multi_time:.4f} seconds")
        
        print("\n3. Window Function Optimization:")
        
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy("region").orderBy(desc("amount"))
        
        window_agg = sales_data \
            .withColumn("rank", row_number().over(window_spec)) \
            .withColumn("running_total", sum("amount").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow))) \
            .filter(col("rank") <= 10)
        
        print("   Window function optimization plan:")
        window_agg.explain(mode="simple")
        
        start_time = time.time()
        window_result = window_agg.collect()
        window_time = time.time() - start_time
        
        print(f"   Window function time: {window_time:.4f} seconds")
        
        return {
            "simple_aggregation_time": simple_time,
            "multi_level_aggregation_time": multi_time,
            "window_function_time": window_time,
            "optimization_features": ["Partial aggregation", "Columnar processing", "Code generation"]
        }

# Run Catalyst optimizer analysis
catalyst_analyzer = CatalystOptimizerAnalysis(demo.spark)
optimization_phases = catalyst_analyzer.demonstrate_optimization_phases()
join_optimization = catalyst_analyzer.demonstrate_join_optimization()
aggregation_optimization = catalyst_analyzer.demonstrate_aggregation_optimization()
```

---

## Best Practices

### Lazy Evaluation Best Practices

```json
{
  "lazy_evaluation_best_practices": {
    "transformation_design": {
      "chain_efficiently": {
        "principle": "Build logical transformation chains before triggering execution",
        "example": "df.filter().select().groupBy().agg() - all lazy until action",
        "benefits": ["Better optimization", "Reduced intermediate data", "Improved performance"]
      },
      "avoid_premature_actions": {
        "principle": "Minimize actions in transformation chains",
        "bad_example": "df.filter().count() followed by df.filter().collect()",
        "good_example": "df.filter().cache() followed by multiple actions",
        "impact": "Each action triggers full computation"
      },
      "use_appropriate_actions": {
        "principle": "Choose actions based on data size and requirements",
        "guidelines": {
          "small_results": "collect(), take(), first()",
          "large_results": "write to storage, forEach",
          "counting": "count() for exact count, approxCount() for estimation",
          "sampling": "sample() before collect() for large datasets"
        }
      }
    },
    "optimization_strategies": {
      "leverage_catalyst": {
        "principle": "Design queries to benefit from Catalyst optimization",
        "techniques": [
          "Use DataFrame API over RDD API",
          "Apply filters early in transformation chain",
          "Use built-in functions over UDFs when possible",
          "Structure joins to enable broadcast optimization"
        ]
      },
      "caching_strategy": {
        "principle": "Cache strategically for reused DataFrames",
        "when_to_cache": [
          "DataFrame used in multiple actions",
          "Expensive computations that will be reused",
          "Iterative algorithms",
          "Interactive data exploration"
        ],
        "cache_levels": {
          "MEMORY_ONLY": "Fast but may cause OOM",
          "MEMORY_AND_DISK": "Balanced approach",
          "DISK_ONLY": "Slow but reliable for large data"
        }
      },
      "partition_optimization": {
        "principle": "Optimize partitioning for lazy evaluation efficiency",
        "strategies": [
          "Partition by frequently filtered columns",
          "Avoid over-partitioning (too many small partitions)",
          "Use coalesce() to reduce partitions after filtering",
          "Consider repartition() for better distribution before expensive operations"
        ]
      }
    },
    "debugging_and_monitoring": {
      "query_planning": {
        "principle": "Analyze query plans before execution",
        "techniques": [
          "Use explain() to understand execution plan",
          "Check for unnecessary shuffles or broadcasts",
          "Verify predicate pushdown is working",
          "Monitor partition elimination"
        ]
      },
      "execution_monitoring": {
        "principle": "Monitor lazy evaluation execution",
        "tools": [
          "Spark UI for job and stage analysis",
          "SQL tab for query execution details",
          "Storage tab for cached DataFrame information",
          "Executors tab for resource utilization"
        ]
      }
    },
    "common_pitfalls": {
      "repeated_actions": {
        "problem": "Calling actions repeatedly on same transformation chain",
        "solution": "Cache the DataFrame after transformations, then call multiple actions",
        "example": "df.filter().cache() followed by count() and collect()"
      },
      "large_collect": {
        "problem": "Using collect() on large datasets",
        "solution": "Use write operations or limit() before collect()",
        "alternative": "Process data in chunks or use forEach for side effects"
      },
      "unnecessary_caching": {
        "problem": "Caching DataFrames that are used only once",
        "solution": "Only cache DataFrames that will be reused",
        "impact": "Wastes memory and may slow down single-use operations"
      },
      "udf_overuse": {
        "problem": "Using UDFs when built-in functions would work",
        "solution": "Prefer built-in Spark functions over UDFs",
        "reason": "Built-in functions benefit from Catalyst optimization and code generation"
      }
    }
  }
}
```

This comprehensive guide provides everything needed to understand and effectively use lazy evaluation in Azure Spark environments, covering all aspects from fundamental concepts to advanced optimization techniques and best practices.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*