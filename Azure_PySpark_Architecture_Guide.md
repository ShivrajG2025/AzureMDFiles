# Azure PySpark Stages, Tasks & Architecture Guide
## Comprehensive Guide to Spark Architecture in Azure

---

### Table of Contents

1. [Overview](#overview)
2. [Spark Architecture Fundamentals](#spark-architecture-fundamentals)
3. [Stages and Tasks Deep Dive](#stages-and-tasks-deep-dive)
4. [Azure Databricks Architecture](#azure-databricks-architecture)
5. [Azure Synapse Spark Architecture](#azure-synapse-spark-architecture)
6. [Job Execution Model](#job-execution-model)
7. [Performance Optimization](#performance-optimization)
8. [Monitoring and Debugging](#monitoring-and-debugging)
9. [Practical Examples](#practical-examples)
10. [Best Practices](#best-practices)

---

## Overview

Apache Spark is a distributed computing framework that processes large datasets across clusters of machines. Understanding Spark's architecture, particularly stages and tasks, is crucial for optimizing performance in Azure environments.

### Core Concepts

```json
{
  "spark_architecture_overview": {
    "key_components": {
      "driver_program": "Central coordinator that manages the Spark application",
      "cluster_manager": "Resource allocation and management (YARN, Kubernetes, Standalone)",
      "executors": "Worker processes that execute tasks and store data",
      "tasks": "Individual units of work sent to executors",
      "stages": "Collection of tasks that can run in parallel"
    },
    "execution_hierarchy": {
      "application": "Complete Spark program",
      "job": "Triggered by an action (collect, save, count)",
      "stage": "Set of tasks that can run without shuffling data",
      "task": "Smallest unit of work, operates on one partition"
    },
    "azure_services": [
      "Azure Databricks",
      "Azure Synapse Analytics",
      "Azure HDInsight",
      "Azure Container Instances (ACI)"
    ]
  }
}
```

### Benefits in Azure

```json
{
  "azure_spark_benefits": {
    "scalability": {
      "auto_scaling": "Dynamic resource allocation based on workload",
      "multi_node_clusters": "Scale from single node to hundreds of nodes",
      "elastic_capacity": "Pay-as-you-go scaling model"
    },
    "integration": {
      "azure_data_lake": "Native integration with ADLS Gen2",
      "azure_sql": "Direct connectivity to Azure SQL Database",
      "power_bi": "Seamless integration for analytics and reporting",
      "azure_ml": "Integration with Azure Machine Learning"
    },
    "management": {
      "managed_service": "Fully managed Spark clusters",
      "security": "Azure AD integration and enterprise security",
      "monitoring": "Built-in monitoring and logging capabilities"
    }
  }
}
```

---

## Spark Architecture Fundamentals

### Driver Program

```python
# Understanding the Spark Driver
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import json

class SparkArchitectureDemo:
    """Demonstrate Spark architecture concepts"""
    
    def __init__(self):
        # Configure Spark with detailed settings
        conf = SparkConf() \
            .setAppName("SparkArchitectureDemo") \
            .set("spark.sql.adaptive.enabled", "true") \
            .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        # Initialize Spark Session (Driver)
        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        # Get Spark Context for low-level operations
        self.sc = self.spark.sparkContext
        
        print("=== Spark Driver Information ===")
        print(f"Application ID: {self.sc.applicationId}")
        print(f"Application Name: {self.sc.appName}")
        print(f"Spark Version: {self.sc.version}")
        print(f"Master: {self.sc.master}")
        print(f"Default Parallelism: {self.sc.defaultParallelism}")
        print(f"Driver Host: {self.sc.getConf().get('spark.driver.host', 'Unknown')}")
        print(f"Driver Port: {self.sc.getConf().get('spark.driver.port', 'Unknown')}")
    
    def analyze_cluster_resources(self):
        """Analyze cluster resources and executor information"""
        
        print("\n=== Cluster Resource Analysis ===")
        
        # Get executor information
        executor_infos = self.sc.statusTracker().getExecutorInfos()
        
        total_cores = 0
        total_memory = 0
        
        for executor in executor_infos:
            print(f"Executor ID: {executor.executorId}")
            print(f"  Host: {executor.host}")
            print(f"  Port: {executor.port}")
            print(f"  Total Cores: {executor.totalCores}")
            print(f"  Max Tasks: {executor.maxTasks}")
            print(f"  Active Tasks: {executor.activeTasks}")
            print(f"  Failed Tasks: {executor.failedTasks}")
            print(f"  Completed Tasks: {executor.completedTasks}")
            print(f"  Memory Used: {executor.memoryUsed} bytes")
            print(f"  Max Memory: {executor.maxMemory} bytes")
            print(f"  Disk Used: {executor.diskUsed} bytes")
            print("-" * 40)
            
            total_cores += executor.totalCores
            total_memory += executor.maxMemory
        
        print(f"Total Cluster Cores: {total_cores}")
        print(f"Total Cluster Memory: {total_memory / (1024**3):.2f} GB")
        print(f"Number of Executors: {len(executor_infos)}")
        
        return {
            "total_cores": total_cores,
            "total_memory_gb": total_memory / (1024**3),
            "num_executors": len(executor_infos),
            "executor_details": [
                {
                    "executor_id": e.executorId,
                    "host": e.host,
                    "cores": e.totalCores,
                    "memory_gb": e.maxMemory / (1024**3)
                } for e in executor_infos
            ]
        }
    
    def demonstrate_rdd_partitioning(self):
        """Demonstrate RDD partitioning and task distribution"""
        
        print("\n=== RDD Partitioning Demo ===")
        
        # Create RDD with different partition strategies
        data = list(range(1, 10001))  # 10,000 numbers
        
        # Default partitioning
        rdd_default = self.sc.parallelize(data)
        print(f"Default partitions: {rdd_default.getNumPartitions()}")
        print(f"Partitioner: {rdd_default.partitioner}")
        
        # Custom partitioning
        rdd_custom = self.sc.parallelize(data, numSlices=8)
        print(f"Custom partitions (8): {rdd_custom.getNumPartitions()}")
        
        # Analyze partition distribution
        partition_sizes = rdd_custom.mapPartitionsWithIndex(
            lambda idx, iterator: [(idx, sum(1 for _ in iterator))]
        ).collect()
        
        print("Partition distribution:")
        for partition_id, size in partition_sizes:
            print(f"  Partition {partition_id}: {size} elements")
        
        # Demonstrate glom() to see actual partition contents
        partition_contents = rdd_custom.glom().collect()
        print(f"First partition sample: {partition_contents[0][:10]}...")
        
        return {
            "default_partitions": rdd_default.getNumPartitions(),
            "custom_partitions": rdd_custom.getNumPartitions(),
            "partition_sizes": dict(partition_sizes)
        }
    
    def analyze_dataframe_partitioning(self):
        """Analyze DataFrame partitioning strategies"""
        
        print("\n=== DataFrame Partitioning Analysis ===")
        
        # Create sample DataFrame
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
        from pyspark.sql.functions import col, rand, when, date_add, lit
        from datetime import date, timedelta
        import random
        
        # Generate sample data
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("department", StringType(), False),
            StructField("salary", IntegerType(), False),
            StructField("hire_date", DateType(), False)
        ])
        
        departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"]
        sample_data = []
        
        for i in range(1, 10001):
            sample_data.append((
                i,
                f"Employee_{i}",
                random.choice(departments),
                random.randint(40000, 120000),
                date(2020, 1, 1) + timedelta(days=random.randint(0, 1000))
            ))
        
        df = self.spark.createDataFrame(sample_data, schema)
        
        print(f"Original DataFrame partitions: {df.rdd.getNumPartitions()}")
        
        # Different partitioning strategies
        partitioning_strategies = {
            "repartition_4": df.repartition(4),
            "repartition_by_department": df.repartition(col("department")),
            "repartition_by_department_4": df.repartition(4, col("department")),
            "coalesce_2": df.coalesce(2)
        }
        
        results = {}
        
        for strategy_name, partitioned_df in partitioning_strategies.items():
            num_partitions = partitioned_df.rdd.getNumPartitions()
            
            # Analyze partition sizes
            partition_sizes = partitioned_df.rdd.mapPartitionsWithIndex(
                lambda idx, iterator: [(idx, sum(1 for _ in iterator))]
            ).collect()
            
            results[strategy_name] = {
                "num_partitions": num_partitions,
                "partition_sizes": dict(partition_sizes),
                "min_partition_size": min(size for _, size in partition_sizes),
                "max_partition_size": max(size for _, size in partition_sizes)
            }
            
            print(f"\n{strategy_name}:")
            print(f"  Partitions: {num_partitions}")
            print(f"  Partition sizes: {dict(partition_sizes)}")
            print(f"  Size range: {results[strategy_name]['min_partition_size']} - {results[strategy_name]['max_partition_size']}")
        
        return results

# Initialize demo
demo = SparkArchitectureDemo()
cluster_info = demo.analyze_cluster_resources()
rdd_info = demo.demonstrate_rdd_partitioning()
df_info = demo.analyze_dataframe_partitioning()
```

### Executors and Tasks

```python
class TaskExecutionAnalyzer:
    """Analyze task execution patterns and performance"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.sc = spark_session.sparkContext
    
    def analyze_task_execution(self, operation_name, operation_func):
        """Analyze task execution for a given operation"""
        
        print(f"\n=== Task Execution Analysis: {operation_name} ===")
        
        # Get initial status
        status_tracker = self.sc.statusTracker()
        initial_job_ids = set(status_tracker.getJobIdsForGroup(None) or [])
        
        # Execute operation
        start_time = time.time()
        result = operation_func()
        end_time = time.time()
        
        # Get job information after execution
        final_job_ids = set(status_tracker.getJobIdsForGroup(None) or [])
        new_job_ids = final_job_ids - initial_job_ids
        
        execution_info = {
            "operation_name": operation_name,
            "execution_time_seconds": end_time - start_time,
            "job_ids": list(new_job_ids),
            "stages": [],
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0
        }
        
        # Analyze each job
        for job_id in new_job_ids:
            job_info = status_tracker.getJobInfo(job_id)
            if job_info:
                print(f"Job ID: {job_id}")
                print(f"  Status: {job_info.status}")
                print(f"  Stage IDs: {job_info.stageIds}")
                
                # Analyze stages
                for stage_id in job_info.stageIds:
                    stage_info = status_tracker.getStageInfo(stage_id)
                    if stage_info:
                        stage_data = {
                            "stage_id": stage_id,
                            "name": stage_info.name,
                            "num_tasks": stage_info.numTasks,
                            "num_active_tasks": stage_info.numActiveTasks,
                            "num_complete_tasks": stage_info.numCompleteTasks,
                            "num_failed_tasks": stage_info.numFailedTasks,
                            "submission_time": stage_info.submissionTime,
                            "completion_time": stage_info.completionTime
                        }
                        
                        execution_info["stages"].append(stage_data)
                        execution_info["total_tasks"] += stage_info.numTasks
                        execution_info["successful_tasks"] += stage_info.numCompleteTasks
                        execution_info["failed_tasks"] += stage_info.numFailedTasks
                        
                        print(f"  Stage {stage_id}: {stage_info.name}")
                        print(f"    Tasks: {stage_info.numTasks} total, {stage_info.numCompleteTasks} completed, {stage_info.numFailedTasks} failed")
                        print(f"    Active Tasks: {stage_info.numActiveTasks}")
                        
                        if stage_info.submissionTime and stage_info.completionTime:
                            stage_duration = (stage_info.completionTime - stage_info.submissionTime) / 1000.0
                            print(f"    Duration: {stage_duration:.2f} seconds")
                            stage_data["duration_seconds"] = stage_duration
        
        print(f"Total execution time: {execution_info['execution_time_seconds']:.2f} seconds")
        print(f"Total tasks: {execution_info['total_tasks']}")
        print(f"Successful tasks: {execution_info['successful_tasks']}")
        print(f"Failed tasks: {execution_info['failed_tasks']}")
        
        return execution_info, result
    
    def demonstrate_wide_vs_narrow_transformations(self):
        """Demonstrate the difference between wide and narrow transformations"""
        
        print("\n=== Wide vs Narrow Transformations ===")
        
        # Create sample data
        data = [(i, f"name_{i}", i % 10, i * 100) for i in range(1, 10001)]
        df = self.spark.createDataFrame(data, ["id", "name", "category", "value"])
        
        # Cache the DataFrame to ensure fair comparison
        df.cache()
        df.count()  # Trigger caching
        
        # Narrow transformation examples
        narrow_transformations = {
            "filter": lambda: df.filter(col("value") > 50000).count(),
            "select": lambda: df.select("id", "name", "value").count(),
            "withColumn": lambda: df.withColumn("value_doubled", col("value") * 2).count(),
            "map": lambda: df.rdd.map(lambda row: (row.id, row.value * 2)).count()
        }
        
        # Wide transformation examples
        wide_transformations = {
            "groupBy": lambda: df.groupBy("category").count().count(),
            "orderBy": lambda: df.orderBy("value").count(),
            "distinct": lambda: df.distinct().count(),
            "join": lambda: df.alias("a").join(
                df.select("id", "category").alias("b"), 
                col("a.id") == col("b.id")
            ).count()
        }
        
        results = {"narrow": {}, "wide": {}}
        
        print("\nNarrow Transformations (no shuffle):")
        for name, operation in narrow_transformations.items():
            execution_info, result = self.analyze_task_execution(f"narrow_{name}", operation)
            results["narrow"][name] = execution_info
        
        print("\nWide Transformations (require shuffle):")
        for name, operation in wide_transformations.items():
            execution_info, result = self.analyze_task_execution(f"wide_{name}", operation)
            results["wide"][name] = execution_info
        
        return results
    
    def analyze_shuffle_operations(self):
        """Analyze shuffle operations in detail"""
        
        print("\n=== Shuffle Operations Analysis ===")
        
        # Create data that will require shuffling
        large_data = [(i, f"key_{i % 1000}", i * 10, f"value_{i}") for i in range(1, 50001)]
        df = self.spark.createDataFrame(large_data, ["id", "key", "amount", "description"])
        
        shuffle_operations = {
            "groupBy_aggregation": lambda: df.groupBy("key").agg(
                {"amount": "sum", "id": "count"}
            ).collect(),
            
            "window_function": lambda: df.select(
                "*",
                row_number().over(Window.partitionBy("key").orderBy("amount"))
            ).collect(),
            
            "join_operation": lambda: df.alias("a").join(
                df.groupBy("key").agg(avg("amount").alias("avg_amount")).alias("b"),
                "key"
            ).collect(),
            
            "distinct_operation": lambda: df.select("key", "amount").distinct().collect(),
            
            "repartition_operation": lambda: df.repartition(10, "key").count()
        }
        
        shuffle_results = {}
        
        for operation_name, operation in shuffle_operations.items():
            print(f"\n--- {operation_name} ---")
            execution_info, result = self.analyze_task_execution(operation_name, operation)
            shuffle_results[operation_name] = execution_info
            
            # Analyze shuffle metrics if available
            for stage in execution_info["stages"]:
                if "shuffle" in stage["name"].lower() or "exchange" in stage["name"].lower():
                    print(f"    *** Shuffle detected in stage {stage['stage_id']} ***")
        
        return shuffle_results

# Usage example
import time
from pyspark.sql.functions import col, avg, row_number
from pyspark.sql.window import Window

analyzer = TaskExecutionAnalyzer(demo.spark)
transformation_analysis = analyzer.demonstrate_wide_vs_narrow_transformations()
shuffle_analysis = analyzer.analyze_shuffle_operations()
```

---

## Stages and Tasks Deep Dive

### Stage Creation and Execution

```python
class StageAnalysisFramework:
    """Comprehensive framework for analyzing Spark stages"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.sc = spark_session.sparkContext
    
    def create_stage_visualization(self, job_description="Custom Job"):
        """Create a detailed visualization of stage execution"""
        
        print(f"\n=== Stage Visualization: {job_description} ===")
        
        # Create complex operation that will generate multiple stages
        from pyspark.sql.functions import col, sum as spark_sum, avg, count, max as spark_max
        
        # Generate sample data
        data1 = [(i, f"dept_{i % 5}", i * 100, f"2023-{(i % 12) + 1:02d}-01") 
                for i in range(1, 10001)]
        data2 = [(i % 5, f"dept_{i % 5}", f"Department {i % 5}", "Active") 
                for i in range(5)]
        
        df_employees = self.spark.createDataFrame(
            data1, ["emp_id", "dept_code", "salary", "hire_date"]
        )
        df_departments = self.spark.createDataFrame(
            data2, ["dept_id", "dept_code", "dept_name", "status"]
        )
        
        # Complex query that will create multiple stages
        def complex_operation():
            # Stage 1: Filter and transform employees
            filtered_employees = df_employees.filter(col("salary") > 50000) \
                .withColumn("salary_category", 
                    when(col("salary") > 80000, "High")
                    .when(col("salary") > 60000, "Medium")
                    .otherwise("Low"))
            
            # Stage 2: Aggregation (will cause shuffle)
            dept_stats = filtered_employees.groupBy("dept_code", "salary_category") \
                .agg(
                    count("emp_id").alias("employee_count"),
                    avg("salary").alias("avg_salary"),
                    spark_max("salary").alias("max_salary")
                )
            
            # Stage 3: Join operation (will cause shuffle)
            result = dept_stats.join(df_departments, "dept_code") \
                .select(
                    "dept_name",
                    "salary_category",
                    "employee_count",
                    "avg_salary",
                    "max_salary"
                ) \
                .orderBy("dept_name", "salary_category")
            
            return result.collect()
        
        # Execute and analyze
        status_tracker = self.sc.statusTracker()
        initial_job_ids = set(status_tracker.getJobIdsForGroup(None) or [])
        
        start_time = time.time()
        result = complex_operation()
        end_time = time.time()
        
        final_job_ids = set(status_tracker.getJobIdsForGroup(None) or [])
        new_job_ids = final_job_ids - initial_job_ids
        
        # Detailed stage analysis
        stage_details = []
        
        for job_id in new_job_ids:
            job_info = status_tracker.getJobInfo(job_id)
            if job_info:
                for stage_id in job_info.stageIds:
                    stage_info = status_tracker.getStageInfo(stage_id)
                    if stage_info:
                        stage_detail = {
                            "job_id": job_id,
                            "stage_id": stage_id,
                            "stage_name": stage_info.name,
                            "num_tasks": stage_info.numTasks,
                            "num_partitions": stage_info.numTasks,  # Usually equal
                            "parent_stage_ids": list(stage_info.parentIds) if stage_info.parentIds else [],
                            "submission_time": stage_info.submissionTime,
                            "completion_time": stage_info.completionTime,
                            "is_shuffle_stage": "shuffle" in stage_info.name.lower() or "exchange" in stage_info.name.lower()
                        }
                        
                        if stage_info.submissionTime and stage_info.completionTime:
                            stage_detail["duration_ms"] = stage_info.completionTime - stage_info.submissionTime
                            stage_detail["duration_seconds"] = stage_detail["duration_ms"] / 1000.0
                        
                        stage_details.append(stage_detail)
        
        # Sort stages by submission time
        stage_details.sort(key=lambda x: x.get("submission_time", 0))
        
        # Print stage execution flow
        print("Stage Execution Flow:")
        print("=" * 80)
        
        for i, stage in enumerate(stage_details):
            prefix = "├─" if i < len(stage_details) - 1 else "└─"
            shuffle_indicator = " [SHUFFLE]" if stage["is_shuffle_stage"] else ""
            duration_info = f" ({stage.get('duration_seconds', 0):.2f}s)" if stage.get('duration_seconds') else ""
            
            print(f"{prefix} Stage {stage['stage_id']}: {stage['stage_name']}{shuffle_indicator}{duration_info}")
            print(f"│   Tasks: {stage['num_tasks']}, Partitions: {stage['num_partitions']}")
            
            if stage["parent_stage_ids"]:
                print(f"│   Dependencies: {stage['parent_stage_ids']}")
            
            if i < len(stage_details) - 1:
                print("│")
        
        total_duration = end_time - start_time
        print(f"\nTotal Execution Time: {total_duration:.2f} seconds")
        print(f"Number of Stages: {len(stage_details)}")
        print(f"Total Tasks: {sum(stage['num_tasks'] for stage in stage_details)}")
        
        return {
            "total_duration": total_duration,
            "stage_count": len(stage_details),
            "total_tasks": sum(stage['num_tasks'] for stage in stage_details),
            "stages": stage_details,
            "result_count": len(result)
        }
    
    def analyze_task_distribution(self):
        """Analyze how tasks are distributed across executors"""
        
        print("\n=== Task Distribution Analysis ===")
        
        # Create operation with known partitioning
        data = list(range(1, 100001))  # 100,000 numbers
        rdd = self.sc.parallelize(data, numSlices=16)  # 16 partitions
        
        # Operation that will create tasks
        def task_heavy_operation():
            return rdd.map(lambda x: x * x) \
                     .filter(lambda x: x % 2 == 0) \
                     .map(lambda x: (x % 10, x)) \
                     .reduceByKey(lambda a, b: a + b) \
                     .collect()
        
        # Monitor task execution
        status_tracker = self.sc.statusTracker()
        executor_infos_before = status_tracker.getExecutorInfos()
        
        start_time = time.time()
        result = task_heavy_operation()
        end_time = time.time()
        
        executor_infos_after = status_tracker.getExecutorInfos()
        
        print("Task Distribution Across Executors:")
        print("-" * 60)
        
        for executor in executor_infos_after:
            tasks_completed_delta = 0
            tasks_failed_delta = 0
            
            # Find corresponding before state
            for before_executor in executor_infos_before:
                if before_executor.executorId == executor.executorId:
                    tasks_completed_delta = executor.completedTasks - before_executor.completedTasks
                    tasks_failed_delta = executor.failedTasks - before_executor.failedTasks
                    break
            
            print(f"Executor {executor.executorId} ({executor.host}):")
            print(f"  Tasks completed in this operation: {tasks_completed_delta}")
            print(f"  Tasks failed in this operation: {tasks_failed_delta}")
            print(f"  Total cores: {executor.totalCores}")
            print(f"  Active tasks: {executor.activeTasks}")
            print(f"  Memory usage: {executor.memoryUsed / (1024**2):.1f} MB / {executor.maxMemory / (1024**2):.1f} MB")
            print()
        
        return {
            "execution_time": end_time - start_time,
            "result_count": len(result),
            "executor_task_distribution": [
                {
                    "executor_id": e.executorId,
                    "host": e.host,
                    "completed_tasks": e.completedTasks,
                    "failed_tasks": e.failedTasks,
                    "active_tasks": e.activeTasks,
                    "total_cores": e.totalCores
                } for e in executor_infos_after
            ]
        }
    
    def demonstrate_stage_boundaries(self):
        """Demonstrate what causes stage boundaries"""
        
        print("\n=== Stage Boundary Demonstration ===")
        
        # Create sample DataFrame
        data = [(i, f"category_{i % 10}", i * 100, f"item_{i}") 
                for i in range(1, 10001)]
        df = self.spark.createDataFrame(data, ["id", "category", "price", "name"])
        
        operations = {
            "narrow_only": {
                "description": "Only narrow transformations (single stage)",
                "operation": lambda: df.filter(col("price") > 500)
                                     .select("id", "category", "price")
                                     .withColumn("discounted_price", col("price") * 0.9)
                                     .count()
            },
            
            "with_groupby": {
                "description": "GroupBy operation (creates stage boundary)",
                "operation": lambda: df.groupBy("category")
                                     .agg(avg("price").alias("avg_price"))
                                     .count()
            },
            
            "with_orderby": {
                "description": "OrderBy operation (creates stage boundary)",
                "operation": lambda: df.orderBy("price").count()
            },
            
            "with_join": {
                "description": "Join operation (creates stage boundary)",
                "operation": lambda: df.alias("a").join(
                    df.groupBy("category").agg(avg("price").alias("cat_avg")).alias("b"),
                    "category"
                ).count()
            },
            
            "with_repartition": {
                "description": "Repartition operation (creates stage boundary)",
                "operation": lambda: df.repartition(8, "category").count()
            }
        }
        
        boundary_analysis = {}
        
        for op_name, op_info in operations.items():
            print(f"\n--- {op_name.upper()}: {op_info['description']} ---")
            
            # Analyze stages for this operation
            status_tracker = self.sc.statusTracker()
            initial_job_ids = set(status_tracker.getJobIdsForGroup(None) or [])
            
            start_time = time.time()
            result = op_info["operation"]()
            end_time = time.time()
            
            final_job_ids = set(status_tracker.getJobIdsForGroup(None) or [])
            new_job_ids = final_job_ids - initial_job_ids
            
            stage_count = 0
            shuffle_stages = 0
            
            for job_id in new_job_ids:
                job_info = status_tracker.getJobInfo(job_id)
                if job_info:
                    stage_count += len(job_info.stageIds)
                    for stage_id in job_info.stageIds:
                        stage_info = status_tracker.getStageInfo(stage_id)
                        if stage_info and ("shuffle" in stage_info.name.lower() or "exchange" in stage_info.name.lower()):
                            shuffle_stages += 1
            
            boundary_analysis[op_name] = {
                "description": op_info["description"],
                "execution_time": end_time - start_time,
                "stage_count": stage_count,
                "shuffle_stages": shuffle_stages,
                "result": result
            }
            
            print(f"Stages created: {stage_count}")
            print(f"Shuffle stages: {shuffle_stages}")
            print(f"Execution time: {end_time - start_time:.3f} seconds")
            print(f"Result: {result}")
        
        return boundary_analysis

# Usage example
from pyspark.sql.functions import when, avg

stage_analyzer = StageAnalysisFramework(demo.spark)
stage_viz_result = stage_analyzer.create_stage_visualization("Complex Analytics Query")
task_dist_result = stage_analyzer.analyze_task_distribution()
boundary_result = stage_analyzer.demonstrate_stage_boundaries()
```

---

## Azure Databricks Architecture

### Databricks Cluster Configuration

```python
class DatabricksArchitectureManager:
    """Manage and analyze Databricks cluster architecture"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.sc = spark_session.sparkContext
    
    def analyze_databricks_cluster(self):
        """Analyze current Databricks cluster configuration"""
        
        print("=== Databricks Cluster Analysis ===")
        
        # Get cluster configuration
        conf = self.spark.sparkContext.getConf()
        
        cluster_info = {
            "cluster_id": conf.get("spark.databricks.clusterUsageTags.clusterId", "Unknown"),
            "cluster_name": conf.get("spark.databricks.clusterUsageTags.clusterName", "Unknown"),
            "spark_version": conf.get("spark.databricks.clusterUsageTags.sparkVersion", "Unknown"),
            "node_type": conf.get("spark.databricks.clusterUsageTags.clusterNodeType", "Unknown"),
            "driver_node_type": conf.get("spark.databricks.clusterUsageTags.clusterDriverNodeType", "Unknown"),
            "autoscaling": {
                "enabled": conf.get("spark.databricks.cluster.profile", "").startswith("serverless"),
                "min_workers": conf.get("spark.databricks.clusterUsageTags.clusterMinWorkers", "Unknown"),
                "max_workers": conf.get("spark.databricks.clusterUsageTags.clusterMaxWorkers", "Unknown")
            },
            "runtime_version": conf.get("spark.databricks.clusterUsageTags.clusterRuntimeVersion", "Unknown")
        }
        
        # Get executor information
        executor_infos = self.sc.statusTracker().getExecutorInfos()
        
        print(f"Cluster ID: {cluster_info['cluster_id']}")
        print(f"Cluster Name: {cluster_info['cluster_name']}")
        print(f"Spark Version: {cluster_info['spark_version']}")
        print(f"Runtime Version: {cluster_info['runtime_version']}")
        print(f"Node Type: {cluster_info['node_type']}")
        print(f"Driver Node Type: {cluster_info['driver_node_type']}")
        print(f"Number of Executors: {len(executor_infos) - 1}")  # Exclude driver
        
        # Analyze executor configuration
        worker_executors = [e for e in executor_infos if e.executorId != "driver"]
        
        if worker_executors:
            total_cores = sum(e.totalCores for e in worker_executors)
            total_memory = sum(e.maxMemory for e in worker_executors)
            
            print(f"Total Worker Cores: {total_cores}")
            print(f"Total Worker Memory: {total_memory / (1024**3):.2f} GB")
            print(f"Cores per Executor: {worker_executors[0].totalCores}")
            print(f"Memory per Executor: {worker_executors[0].maxMemory / (1024**3):.2f} GB")
        
        return cluster_info
    
    def demonstrate_databricks_optimizations(self):
        """Demonstrate Databricks-specific optimizations"""
        
        print("\n=== Databricks Optimizations Demo ===")
        
        # Check if Photon is enabled
        photon_enabled = self.spark.conf.get("spark.databricks.photon.enabled", "false").lower() == "true"
        print(f"Photon Engine: {'Enabled' if photon_enabled else 'Disabled'}")
        
        # Check Delta optimizations
        delta_optimizations = {
            "auto_optimize": self.spark.conf.get("spark.databricks.delta.autoOptimize.enabled", "false"),
            "optimized_writes": self.spark.conf.get("spark.databricks.delta.autoOptimize.optimizeWrite", "false"),
            "auto_compact": self.spark.conf.get("spark.databricks.delta.autoOptimize.autoCompact", "false")
        }
        
        print("Delta Lake Optimizations:")
        for opt_name, opt_value in delta_optimizations.items():
            status = "Enabled" if opt_value.lower() == "true" else "Disabled"
            print(f"  {opt_name}: {status}")
        
        # Adaptive Query Execution settings
        aqe_settings = {
            "enabled": self.spark.conf.get("spark.sql.adaptive.enabled", "false"),
            "coalesce_partitions": self.spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled", "false"),
            "skew_join": self.spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "false"),
            "local_shuffle_reader": self.spark.conf.get("spark.sql.adaptive.localShuffleReader.enabled", "false")
        }
        
        print("Adaptive Query Execution:")
        for setting_name, setting_value in aqe_settings.items():
            status = "Enabled" if setting_value.lower() == "true" else "Disabled"
            print(f"  {setting_name}: {status}")
        
        return {
            "photon_enabled": photon_enabled,
            "delta_optimizations": delta_optimizations,
            "aqe_settings": aqe_settings
        }
    
    def create_performance_comparison(self):
        """Create performance comparison between different configurations"""
        
        print("\n=== Performance Comparison ===")
        
        # Create test dataset
        from pyspark.sql.functions import rand, when, col
        
        # Generate large dataset for testing
        test_df = self.spark.range(1000000) \
            .withColumn("category", (col("id") % 100).cast("string")) \
            .withColumn("value", rand() * 1000) \
            .withColumn("status", when(col("value") > 500, "high").otherwise("low"))
        
        # Cache the dataset
        test_df.cache()
        test_df.count()  # Trigger caching
        
        # Test different operations
        operations = {
            "simple_aggregation": lambda: test_df.groupBy("category").count().collect(),
            "complex_aggregation": lambda: test_df.groupBy("category", "status")
                                                 .agg({"value": "avg", "id": "count"})
                                                 .collect(),
            "join_operation": lambda: test_df.alias("a").join(
                test_df.groupBy("category").agg(avg("value").alias("avg_val")).alias("b"),
                "category"
            ).count(),
            "window_function": lambda: test_df.select(
                "*",
                row_number().over(Window.partitionBy("category").orderBy("value"))
            ).count()
        }
        
        performance_results = {}
        
        for op_name, operation in operations.items():
            print(f"\nTesting {op_name}...")
            
            # Warm up
            operation()
            
            # Measure performance
            times = []
            for i in range(3):  # Run 3 times for average
                start_time = time.time()
                result = operation()
                end_time = time.time()
                times.append(end_time - start_time)
            
            avg_time = sum(times) / len(times)
            performance_results[op_name] = {
                "average_time": avg_time,
                "times": times,
                "min_time": min(times),
                "max_time": max(times)
            }
            
            print(f"  Average time: {avg_time:.3f} seconds")
            print(f"  Range: {min(times):.3f} - {max(times):.3f} seconds")
        
        return performance_results

# Initialize Databricks analyzer
databricks_analyzer = DatabricksArchitectureManager(demo.spark)
cluster_analysis = databricks_analyzer.analyze_databricks_cluster()
optimization_analysis = databricks_analyzer.demonstrate_databricks_optimizations()
performance_comparison = databricks_analyzer.create_performance_comparison()
```

### Delta Lake Integration

```python
class DeltaLakeArchitectureDemo:
    """Demonstrate Delta Lake architecture and optimization"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.delta_table_path = "/tmp/delta-table-demo"
    
    def create_delta_table_with_optimization(self):
        """Create and optimize Delta Lake table"""
        
        print("=== Delta Lake Architecture Demo ===")
        
        from delta.tables import DeltaTable
        from pyspark.sql.functions import col, current_timestamp, rand
        
        # Generate sample data
        sample_data = self.spark.range(100000) \
            .withColumn("department", (col("id") % 10).cast("string")) \
            .withColumn("salary", (rand() * 50000 + 50000).cast("int")) \
            .withColumn("hire_date", current_timestamp()) \
            .withColumn("is_active", (rand() > 0.1))
        
        # Write as Delta table with partitioning
        print("Creating Delta table with partitioning...")
        sample_data.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("department") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true") \
            .save(self.delta_table_path)
        
        # Load as Delta table
        delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
        
        # Demonstrate ACID properties
        print("\nDemonstrating ACID properties...")
        
        # Concurrent operations
        def update_operation():
            delta_table.update(
                condition=col("salary") < 60000,
                set={"salary": col("salary") * 1.1}
            )
        
        def insert_operation():
            new_data = self.spark.range(10) \
                .withColumn("department", lit("11")) \
                .withColumn("salary", lit(75000)) \
                .withColumn("hire_date", current_timestamp()) \
                .withColumn("is_active", lit(True))
            
            delta_table.alias("target").merge(
                new_data.alias("source"),
                "target.id = source.id"
            ).whenNotMatchedInsertAll().execute()
        
        # Execute operations
        start_time = time.time()
        update_operation()
        insert_operation()
        end_time = time.time()
        
        print(f"ACID operations completed in {end_time - start_time:.3f} seconds")
        
        # Show table history
        print("\nTable History:")
        history_df = delta_table.history(5)
        history_df.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
        
        # Optimize table
        print("\nOptimizing Delta table...")
        optimize_start = time.time()
        self.spark.sql(f"OPTIMIZE delta.`{self.delta_table_path}` ZORDER BY (salary)")
        optimize_end = time.time()
        
        print(f"Optimization completed in {optimize_end - optimize_start:.3f} seconds")
        
        # Vacuum old files
        print("\nVacuuming old files...")
        vacuum_start = time.time()
        self.spark.sql(f"VACUUM delta.`{self.delta_table_path}` RETAIN 0 HOURS")
        vacuum_end = time.time()
        
        print(f"Vacuum completed in {vacuum_end - vacuum_start:.3f} seconds")
        
        return {
            "table_path": self.delta_table_path,
            "record_count": delta_table.toDF().count(),
            "partition_count": len(delta_table.toDF().select("department").distinct().collect()),
            "acid_operation_time": end_time - start_time,
            "optimize_time": optimize_end - optimize_start,
            "vacuum_time": vacuum_end - vacuum_start
        }
    
    def demonstrate_time_travel(self):
        """Demonstrate Delta Lake time travel capabilities"""
        
        print("\n=== Delta Lake Time Travel Demo ===")
        
        delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
        
        # Get current version
        current_version = delta_table.history(1).select("version").collect()[0]["version"]
        print(f"Current version: {current_version}")
        
        # Read different versions
        versions_to_check = min(3, current_version + 1)
        
        for version in range(max(0, current_version - versions_to_check + 1), current_version + 1):
            print(f"\nVersion {version}:")
            version_df = self.spark.read.format("delta") \
                .option("versionAsOf", version) \
                .load(self.delta_table_path)
            
            count = version_df.count()
            avg_salary = version_df.agg(avg("salary")).collect()[0][0]
            
            print(f"  Record count: {count}")
            print(f"  Average salary: ${avg_salary:.2f}")
        
        # Demonstrate restore
        if current_version > 0:
            restore_version = max(0, current_version - 1)
            print(f"\nRestoring to version {restore_version}...")
            
            restore_start = time.time()
            self.spark.sql(f"RESTORE delta.`{self.delta_table_path}` TO VERSION AS OF {restore_version}")
            restore_end = time.time()
            
            print(f"Restore completed in {restore_end - restore_start:.3f} seconds")
        
        return {
            "versions_analyzed": versions_to_check,
            "current_version": current_version,
            "restore_time": restore_end - restore_start if current_version > 0 else 0
        }

# Usage example
from pyspark.sql.functions import lit, avg

delta_demo = DeltaLakeArchitectureDemo(demo.spark)
delta_optimization_result = delta_demo.create_delta_table_with_optimization()
time_travel_result = delta_demo.demonstrate_time_travel()
```

---

## Performance Optimization

### Optimization Strategies

```json
{
  "pyspark_optimization_strategies": {
    "data_partitioning": {
      "techniques": [
        "Partition by frequently filtered columns",
        "Use appropriate partition size (128MB - 1GB)",
        "Avoid over-partitioning (too many small files)",
        "Consider data skew when partitioning"
      ],
      "implementation": {
        "repartition": "df.repartition(num_partitions, 'column')",
        "coalesce": "df.coalesce(num_partitions)",
        "partition_by": "df.write.partitionBy('column').save(path)"
      }
    },
    "caching_strategies": {
      "when_to_cache": [
        "DataFrame used multiple times",
        "Expensive computations",
        "Iterative algorithms",
        "Interactive analysis"
      ],
      "storage_levels": {
        "MEMORY_ONLY": "Fast access, may cause OOM",
        "MEMORY_AND_DISK": "Balanced approach",
        "DISK_ONLY": "Slower but reliable",
        "MEMORY_ONLY_SER": "Serialized, saves space"
      }
    },
    "join_optimization": {
      "broadcast_join": "For small tables (<200MB)",
      "sort_merge_join": "For large tables with sorted data",
      "bucket_join": "Pre-partition data by join keys",
      "skew_handling": "Use salting or broadcast for skewed joins"
    },
    "sql_optimization": {
      "predicate_pushdown": "Filter early in the pipeline",
      "column_pruning": "Select only needed columns",
      "constant_folding": "Spark optimizes constant expressions",
      "cost_based_optimizer": "Use table statistics for optimization"
    }
  }
}
```

### Performance Monitoring Framework

```python
class PerformanceMonitoringFramework:
    """Comprehensive performance monitoring for PySpark applications"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.sc = spark_session.sparkContext
        self.metrics_history = []
    
    def capture_performance_metrics(self, operation_name, operation_func):
        """Capture detailed performance metrics for an operation"""
        
        print(f"\n=== Performance Metrics: {operation_name} ===")
        
        # Get initial metrics
        status_tracker = self.sc.statusTracker()
        initial_executor_infos = status_tracker.getExecutorInfos()
        initial_job_ids = set(status_tracker.getJobIdsForGroup(None) or [])
        
        # Capture JVM metrics if available
        initial_jvm_metrics = self._capture_jvm_metrics()
        
        # Execute operation
        start_time = time.time()
        start_cpu_time = time.process_time()
        
        result = operation_func()
        
        end_time = time.time()
        end_cpu_time = time.process_time()
        
        # Get final metrics
        final_executor_infos = status_tracker.getExecutorInfos()
        final_job_ids = set(status_tracker.getJobIdsForGroup(None) or [])
        new_job_ids = final_job_ids - initial_job_ids
        final_jvm_metrics = self._capture_jvm_metrics()
        
        # Calculate metrics
        wall_time = end_time - start_time
        cpu_time = end_cpu_time - start_cpu_time
        
        # Analyze job and stage metrics
        job_metrics = self._analyze_job_metrics(new_job_ids)
        executor_metrics = self._analyze_executor_metrics(initial_executor_infos, final_executor_infos)
        
        performance_report = {
            "operation_name": operation_name,
            "timestamp": time.time(),
            "timing": {
                "wall_time_seconds": wall_time,
                "cpu_time_seconds": cpu_time,
                "cpu_efficiency": (cpu_time / wall_time) * 100 if wall_time > 0 else 0
            },
            "job_metrics": job_metrics,
            "executor_metrics": executor_metrics,
            "resource_usage": {
                "memory_delta_mb": (final_jvm_metrics["heap_used"] - initial_jvm_metrics["heap_used"]) / (1024**2),
                "gc_time_delta_ms": final_jvm_metrics["gc_time"] - initial_jvm_metrics["gc_time"]
            }
        }
        
        # Print summary
        self._print_performance_summary(performance_report)
        
        # Store in history
        self.metrics_history.append(performance_report)
        
        return performance_report, result
    
    def _capture_jvm_metrics(self):
        """Capture JVM metrics"""
        try:
            # Access JVM metrics through Spark's status tracker
            jvm = self.sc._jvm
            runtime = jvm.java.lang.Runtime.getRuntime()
            gc_beans = jvm.java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()
            
            total_gc_time = sum(bean.getCollectionTime() for bean in gc_beans)
            
            return {
                "heap_used": runtime.totalMemory() - runtime.freeMemory(),
                "heap_total": runtime.totalMemory(),
                "heap_max": runtime.maxMemory(),
                "gc_time": total_gc_time
            }
        except:
            return {"heap_used": 0, "heap_total": 0, "heap_max": 0, "gc_time": 0}
    
    def _analyze_job_metrics(self, job_ids):
        """Analyze metrics for executed jobs"""
        status_tracker = self.sc.statusTracker()
        
        job_metrics = {
            "total_jobs": len(job_ids),
            "total_stages": 0,
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 0
        }
        
        for job_id in job_ids:
            job_info = status_tracker.getJobInfo(job_id)
            if job_info:
                job_metrics["total_stages"] += len(job_info.stageIds)
                
                for stage_id in job_info.stageIds:
                    stage_info = status_tracker.getStageInfo(stage_id)
                    if stage_info:
                        job_metrics["total_tasks"] += stage_info.numTasks
                        job_metrics["successful_tasks"] += stage_info.numCompleteTasks
                        job_metrics["failed_tasks"] += stage_info.numFailedTasks
        
        return job_metrics
    
    def _analyze_executor_metrics(self, initial_infos, final_infos):
        """Analyze executor metrics delta"""
        
        executor_metrics = {
            "total_executors": len(final_infos),
            "task_deltas": [],
            "memory_usage_mb": []
        }
        
        for final_executor in final_infos:
            initial_executor = next(
                (e for e in initial_infos if e.executorId == final_executor.executorId),
                None
            )
            
            if initial_executor:
                task_delta = final_executor.completedTasks - initial_executor.completedTasks
                executor_metrics["task_deltas"].append(task_delta)
            
            executor_metrics["memory_usage_mb"].append(
                final_executor.memoryUsed / (1024**2)
            )
        
        if executor_metrics["task_deltas"]:
            executor_metrics["avg_tasks_per_executor"] = sum(executor_metrics["task_deltas"]) / len(executor_metrics["task_deltas"])
            executor_metrics["max_tasks_per_executor"] = max(executor_metrics["task_deltas"])
            executor_metrics["min_tasks_per_executor"] = min(executor_metrics["task_deltas"])
        
        if executor_metrics["memory_usage_mb"]:
            executor_metrics["avg_memory_usage_mb"] = sum(executor_metrics["memory_usage_mb"]) / len(executor_metrics["memory_usage_mb"])
            executor_metrics["max_memory_usage_mb"] = max(executor_metrics["memory_usage_mb"])
        
        return executor_metrics
    
    def _print_performance_summary(self, report):
        """Print performance summary"""
        
        print(f"Operation: {report['operation_name']}")
        print(f"Wall Time: {report['timing']['wall_time_seconds']:.3f} seconds")
        print(f"CPU Time: {report['timing']['cpu_time_seconds']:.3f} seconds")
        print(f"CPU Efficiency: {report['timing']['cpu_efficiency']:.1f}%")
        print(f"Jobs: {report['job_metrics']['total_jobs']}")
        print(f"Stages: {report['job_metrics']['total_stages']}")
        print(f"Tasks: {report['job_metrics']['total_tasks']} (Success: {report['job_metrics']['successful_tasks']}, Failed: {report['job_metrics']['failed_tasks']})")
        
        if report['executor_metrics'].get('avg_tasks_per_executor'):
            print(f"Avg Tasks per Executor: {report['executor_metrics']['avg_tasks_per_executor']:.1f}")
            print(f"Task Distribution: {report['executor_metrics']['min_tasks_per_executor']} - {report['executor_metrics']['max_tasks_per_executor']}")
        
        print(f"Memory Delta: {report['resource_usage']['memory_delta_mb']:.1f} MB")
        print(f"GC Time Delta: {report['resource_usage']['gc_time_delta_ms']} ms")
    
    def generate_performance_report(self):
        """Generate comprehensive performance report"""
        
        if not self.metrics_history:
            print("No performance metrics available")
            return
        
        print("\n=== Performance Report Summary ===")
        print("=" * 60)
        
        # Overall statistics
        total_operations = len(self.metrics_history)
        total_wall_time = sum(m['timing']['wall_time_seconds'] for m in self.metrics_history)
        total_cpu_time = sum(m['timing']['cpu_time_seconds'] for m in self.metrics_history)
        avg_cpu_efficiency = sum(m['timing']['cpu_efficiency'] for m in self.metrics_history) / total_operations
        
        print(f"Total Operations: {total_operations}")
        print(f"Total Wall Time: {total_wall_time:.3f} seconds")
        print(f"Total CPU Time: {total_cpu_time:.3f} seconds")
        print(f"Average CPU Efficiency: {avg_cpu_efficiency:.1f}%")
        
        # Top slowest operations
        slowest_ops = sorted(self.metrics_history, 
                           key=lambda x: x['timing']['wall_time_seconds'], 
                           reverse=True)[:5]
        
        print(f"\nTop 5 Slowest Operations:")
        for i, op in enumerate(slowest_ops, 1):
            print(f"{i}. {op['operation_name']}: {op['timing']['wall_time_seconds']:.3f}s")
        
        # Resource usage patterns
        memory_deltas = [m['resource_usage']['memory_delta_mb'] for m in self.metrics_history]
        gc_times = [m['resource_usage']['gc_time_delta_ms'] for m in self.metrics_history]
        
        print(f"\nResource Usage:")
        print(f"Average Memory Delta: {sum(memory_deltas) / len(memory_deltas):.1f} MB")
        print(f"Average GC Time: {sum(gc_times) / len(gc_times):.1f} ms")
        
        return {
            "total_operations": total_operations,
            "total_wall_time": total_wall_time,
            "total_cpu_time": total_cpu_time,
            "avg_cpu_efficiency": avg_cpu_efficiency,
            "slowest_operations": [(op['operation_name'], op['timing']['wall_time_seconds']) for op in slowest_ops],
            "avg_memory_delta": sum(memory_deltas) / len(memory_deltas),
            "avg_gc_time": sum(gc_times) / len(gc_times)
        }

# Usage example
perf_monitor = PerformanceMonitoringFramework(demo.spark)

# Test different operations
def test_aggregation():
    data = demo.spark.range(1000000)
    return data.groupBy((col("id") % 100).alias("group")).count().collect()

def test_join():
    df1 = demo.spark.range(100000).withColumn("key", col("id") % 1000)
    df2 = demo.spark.range(1000).withColumn("value", col("id") * 10)
    return df1.join(df2, df1.key == df2.id).count()

def test_window_function():
    data = demo.spark.range(100000).withColumn("category", col("id") % 10)
    return data.select("*", row_number().over(Window.partitionBy("category").orderBy("id"))).count()

# Monitor performance
agg_metrics, agg_result = perf_monitor.capture_performance_metrics("Aggregation Test", test_aggregation)
join_metrics, join_result = perf_monitor.capture_performance_metrics("Join Test", test_join)
window_metrics, window_result = perf_monitor.capture_performance_metrics("Window Function Test", test_window_function)

# Generate final report
final_report = perf_monitor.generate_performance_report()
```

---

## Best Practices

### Implementation Guidelines

```json
{
  "pyspark_best_practices": {
    "cluster_configuration": [
      "Size executors appropriately (2-5 cores per executor)",
      "Configure memory settings (executor memory, driver memory)",
      "Enable dynamic allocation for variable workloads",
      "Use appropriate node types for workload characteristics",
      "Monitor resource utilization and adjust accordingly"
    ],
    "code_optimization": [
      "Minimize data shuffling operations",
      "Use broadcast joins for small lookup tables",
      "Cache frequently accessed DataFrames",
      "Avoid collect() on large datasets",
      "Use column pruning and predicate pushdown",
      "Prefer DataFrame API over RDD API for better optimization"
    ],
    "data_management": [
      "Partition data by frequently filtered columns",
      "Use appropriate file formats (Parquet, Delta)",
      "Implement proper data lifecycle management",
      "Monitor and optimize table statistics",
      "Use Z-ordering for better query performance"
    ],
    "monitoring_and_debugging": [
      "Monitor Spark UI for job execution details",
      "Use structured logging for better observability",
      "Implement custom metrics for business logic",
      "Set up alerts for job failures and performance degradation",
      "Regularly review and optimize slow-running jobs"
    ],
    "azure_specific": [
      "Use Azure Data Lake Storage Gen2 for optimal performance",
      "Enable auto-scaling in Databricks for cost optimization",
      "Integrate with Azure Monitor for centralized logging",
      "Use Azure Key Vault for secure credential management",
      "Implement proper network security with VNet integration"
    ]
  }
}
```

This comprehensive guide provides everything needed to understand and optimize PySpark architecture, stages, and tasks in Azure environments, covering all aspects from fundamental concepts to advanced performance optimization techniques.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*