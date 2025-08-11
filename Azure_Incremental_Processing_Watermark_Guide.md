# Azure Incremental Data Processing with Watermarks Guide
## Complete Implementation Guide for ADF and Databricks

---

### Table of Contents

1. [Overview](#overview)
2. [Watermark Concepts and Strategies](#watermark-concepts-and-strategies)
3. [Azure Data Factory Implementation](#azure-data-factory-implementation)
4. [Azure Databricks Implementation](#azure-databricks-implementation)
5. [Watermark Dataset Management](#watermark-dataset-management)
6. [Advanced Patterns](#advanced-patterns)
7. [Error Handling and Recovery](#error-handling-and-recovery)
8. [Performance Optimization](#performance-optimization)
9. [Monitoring and Alerting](#monitoring-and-alerting)
10. [Best Practices](#best-practices)

---

## Overview

Incremental data processing is crucial for efficient data pipelines that handle large volumes of data. Instead of processing entire datasets, incremental processing only handles new or modified data since the last successful run, using watermarks to track progress.

### Architecture Overview

```json
{
  "incremental_processing_architecture": {
    "components": {
      "watermark_store": "Azure SQL Database / Delta Lake Table",
      "source_systems": ["SQL Server", "Oracle", "MySQL", "REST APIs", "Files"],
      "processing_engines": ["Azure Data Factory", "Azure Databricks"],
      "target_systems": ["Azure Data Lake", "Azure SQL DW", "Cosmos DB"],
      "monitoring": ["Azure Monitor", "Log Analytics", "Custom Dashboards"]
    },
    "data_flow": "Source -> Watermark Check -> Incremental Extract -> Transform -> Load -> Update Watermark",
    "watermark_types": ["Timestamp-based", "ID-based", "Hybrid", "File-based"]
  }
}
```

### Benefits of Incremental Processing

```json
{
  "benefits": {
    "performance": {
      "reduced_processing_time": "90% faster than full load",
      "lower_resource_consumption": "Minimal compute and storage usage",
      "improved_concurrency": "Multiple pipelines can run simultaneously"
    },
    "cost_optimization": {
      "compute_savings": "Pay only for incremental data processing",
      "storage_efficiency": "Reduced data movement and storage costs",
      "network_bandwidth": "Minimal data transfer requirements"
    },
    "reliability": {
      "faster_recovery": "Quick restart from last watermark",
      "data_consistency": "Atomic watermark updates ensure consistency",
      "error_isolation": "Failures affect only incremental batches"
    }
  }
}
```

---

## Watermark Concepts and Strategies

### Timestamp-Based Watermarks

```sql
-- Example watermark table structure
CREATE TABLE WatermarkTable (
    TableName NVARCHAR(255) PRIMARY KEY,
    WatermarkValue DATETIME2,
    LastUpdateTime DATETIME2 DEFAULT GETDATE(),
    ProcessedRecords BIGINT DEFAULT 0,
    Status NVARCHAR(50) DEFAULT 'Active'
);

-- Initialize watermarks for source tables
INSERT INTO WatermarkTable (TableName, WatermarkValue) VALUES
('Sales.Orders', '1900-01-01 00:00:00'),
('HR.Employees', '1900-01-01 00:00:00'),
('Inventory.Products', '1900-01-01 00:00:00');

-- Sample source table with timestamp column
CREATE TABLE Sales.Orders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    OrderDate DATETIME2,
    TotalAmount DECIMAL(10,2),
    LastModifiedDate DATETIME2 DEFAULT GETDATE(),
    Status NVARCHAR(50)
);
```

### ID-Based Watermarks

```sql
-- ID-based watermark table
CREATE TABLE IDWatermarkTable (
    TableName NVARCHAR(255) PRIMARY KEY,
    LastProcessedID BIGINT,
    LastUpdateTime DATETIME2 DEFAULT GETDATE(),
    ProcessedRecords BIGINT DEFAULT 0
);

-- Initialize ID-based watermarks
INSERT INTO IDWatermarkTable (TableName, LastProcessedID) VALUES
('Sales.OrderDetails', 0),
('Log.ApplicationEvents', 0),
('Audit.UserActivities', 0);

-- Sample source table with auto-increment ID
CREATE TABLE Sales.OrderDetails (
    OrderDetailID BIGINT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    CreatedDate DATETIME2 DEFAULT GETDATE()
);
```

### Hybrid Watermarks

```sql
-- Hybrid watermark combining timestamp and ID
CREATE TABLE HybridWatermarkTable (
    TableName NVARCHAR(255) PRIMARY KEY,
    LastTimestamp DATETIME2,
    LastProcessedID BIGINT,
    LastUpdateTime DATETIME2 DEFAULT GETDATE(),
    WatermarkType NVARCHAR(50) -- 'TIMESTAMP', 'ID', 'HYBRID'
);

-- Stored procedure for hybrid watermark management
CREATE PROCEDURE UpdateHybridWatermark
    @TableName NVARCHAR(255),
    @NewTimestamp DATETIME2,
    @NewID BIGINT
AS
BEGIN
    UPDATE HybridWatermarkTable 
    SET LastTimestamp = @NewTimestamp,
        LastProcessedID = @NewID,
        LastUpdateTime = GETDATE()
    WHERE TableName = @TableName;
    
    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO HybridWatermarkTable (TableName, LastTimestamp, LastProcessedID, WatermarkType)
        VALUES (@TableName, @NewTimestamp, @NewID, 'HYBRID');
    END
END;
```

---

## Azure Data Factory Implementation

### Watermark Dataset Configuration

```json
{
  "name": "WatermarkDataset",
  "properties": {
    "type": "AzureSqlTable",
    "linkedServiceName": {
      "referenceName": "AzureSqlDatabase",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "schema": "dbo",
      "table": "WatermarkTable"
    }
  }
}
```

### Source Dataset with Parameters

```json
{
  "name": "SourceDataset",
  "properties": {
    "type": "SqlServerTable",
    "linkedServiceName": {
      "referenceName": "OnPremisesSqlServer",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "schema": "Sales",
      "table": "Orders"
    },
    "parameters": {
      "watermarkValue": {
        "type": "String"
      },
      "tableName": {
        "type": "String"
      }
    }
  }
}
```

### Incremental Copy Pipeline

```json
{
  "name": "IncrementalCopyPipeline",
  "properties": {
    "parameters": {
      "sourceTableName": {
        "type": "String",
        "defaultValue": "Sales.Orders"
      },
      "watermarkColumnName": {
        "type": "String",
        "defaultValue": "LastModifiedDate"
      }
    },
    "activities": [
      {
        "name": "GetOldWatermark",
        "type": "Lookup",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT WatermarkValue FROM WatermarkTable WHERE TableName = '@{pipeline().parameters.sourceTableName}'"
          },
          "dataset": {
            "referenceName": "WatermarkDataset",
            "type": "DatasetReference"
          }
        }
      },
      {
        "name": "GetNewWatermark",
        "type": "Lookup",
        "dependsOn": [
          {
            "activity": "GetOldWatermark",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "source": {
            "type": "SqlServerSource",
            "sqlReaderQuery": "SELECT MAX(@{pipeline().parameters.watermarkColumnName}) as NewWatermarkValue FROM @{pipeline().parameters.sourceTableName}"
          },
          "dataset": {
            "referenceName": "SourceDataset",
            "type": "DatasetReference"
          }
        }
      },
      {
        "name": "CopyIncrementalData",
        "type": "Copy",
        "dependsOn": [
          {
            "activity": "GetNewWatermark",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "inputs": [
          {
            "referenceName": "SourceDataset",
            "type": "DatasetReference"
          }
        ],
        "outputs": [
          {
            "referenceName": "SinkDataset",
            "type": "DatasetReference"
          }
        ],
        "typeProperties": {
          "source": {
            "type": "SqlServerSource",
            "sqlReaderQuery": "SELECT * FROM @{pipeline().parameters.sourceTableName} WHERE @{pipeline().parameters.watermarkColumnName} > '@{activity('GetOldWatermark').output.firstRow.WatermarkValue}' AND @{pipeline().parameters.watermarkColumnName} <= '@{activity('GetNewWatermark').output.firstRow.NewWatermarkValue}'"
          },
          "sink": {
            "type": "DelimitedTextSink",
            "storeSettings": {
              "type": "AzureBlobFSWriteSettings",
              "copyBehavior": "PreserveHierarchy"
            },
            "formatSettings": {
              "type": "DelimitedTextWriteSettings",
              "quoteAllText": true,
              "fileExtension": ".csv"
            }
          },
          "enableStaging": false,
          "parallelCopies": 4,
          "dataIntegrationUnits": 8
        }
      },
      {
        "name": "UpdateWatermark",
        "type": "SqlServerStoredProcedure",
        "dependsOn": [
          {
            "activity": "CopyIncrementalData",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "storedProcedureName": "UpdateWatermarkValue",
          "storedProcedureParameters": {
            "TableName": {
              "value": "@{pipeline().parameters.sourceTableName}",
              "type": "String"
            },
            "NewWatermarkValue": {
              "value": "@{activity('GetNewWatermark').output.firstRow.NewWatermarkValue}",
              "type": "DateTime"
            },
            "ProcessedRecords": {
              "value": "@{activity('CopyIncrementalData').output.rowsCopied}",
              "type": "Int64"
            }
          }
        },
        "linkedServiceName": {
          "referenceName": "AzureSqlDatabase",
          "type": "LinkedServiceReference"
        }
      }
    ]
  }
}
```

### Watermark Update Stored Procedure

```sql
CREATE PROCEDURE UpdateWatermarkValue
    @TableName NVARCHAR(255),
    @NewWatermarkValue DATETIME2,
    @ProcessedRecords BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- Update watermark value
        UPDATE WatermarkTable 
        SET WatermarkValue = @NewWatermarkValue,
            LastUpdateTime = GETDATE(),
            ProcessedRecords = ProcessedRecords + @ProcessedRecords
        WHERE TableName = @TableName;
        
        -- Log the update
        INSERT INTO WatermarkAuditLog (TableName, OldWatermark, NewWatermark, ProcessedRecords, UpdateTime)
        SELECT @TableName, 
               LAG(WatermarkValue) OVER (ORDER BY LastUpdateTime),
               @NewWatermarkValue,
               @ProcessedRecords,
               GETDATE();
        
        COMMIT TRANSACTION;
        
        PRINT 'Watermark updated successfully for table: ' + @TableName;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
```

### Multi-Table Incremental Pipeline

```json
{
  "name": "MultiTableIncrementalPipeline",
  "properties": {
    "activities": [
      {
        "name": "GetTableList",
        "type": "Lookup",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT TableName, WatermarkValue FROM WatermarkTable WHERE Status = 'Active'"
          },
          "dataset": {
            "referenceName": "WatermarkDataset",
            "type": "DatasetReference"
          },
          "firstRowOnly": false
        }
      },
      {
        "name": "ForEachTable",
        "type": "ForEach",
        "dependsOn": [
          {
            "activity": "GetTableList",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "items": {
            "value": "@activity('GetTableList').output.value",
            "type": "Expression"
          },
          "batchCount": 5,
          "activities": [
            {
              "name": "ProcessIncrementalData",
              "type": "ExecutePipeline",
              "typeProperties": {
                "pipeline": {
                  "referenceName": "IncrementalCopyPipeline",
                  "type": "PipelineReference"
                },
                "parameters": {
                  "sourceTableName": "@item().TableName",
                  "watermarkColumnName": "LastModifiedDate"
                },
                "waitOnCompletion": true
              }
            }
          ]
        }
      }
    ]
  }
}
```

### Advanced ADF Pipeline with Error Handling

```json
{
  "name": "RobustIncrementalPipeline",
  "properties": {
    "parameters": {
      "sourceTableName": {"type": "String"},
      "maxRetries": {"type": "Int", "defaultValue": 3},
      "retryIntervalSeconds": {"type": "Int", "defaultValue": 60}
    },
    "activities": [
      {
        "name": "ValidateSource",
        "type": "Validation",
        "typeProperties": {
          "dataset": {
            "referenceName": "SourceDataset",
            "type": "DatasetReference"
          },
          "timeout": "00:05:00"
        }
      },
      {
        "name": "GetWatermarkWithRetry",
        "type": "Until",
        "dependsOn": [{"activity": "ValidateSource", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
          "expression": {
            "value": "@or(equals(variables('retryCount'), pipeline().parameters.maxRetries), equals(variables('watermarkSuccess'), true))",
            "type": "Expression"
          },
          "timeout": "00:10:00",
          "activities": [
            {
              "name": "TryGetWatermark",
              "type": "Lookup",
              "typeProperties": {
                "source": {
                  "type": "AzureSqlSource",
                  "sqlReaderQuery": "SELECT WatermarkValue FROM WatermarkTable WHERE TableName = '@{pipeline().parameters.sourceTableName}'"
                },
                "dataset": {
                  "referenceName": "WatermarkDataset",
                  "type": "DatasetReference"
                }
              }
            },
            {
              "name": "SetWatermarkSuccess",
              "type": "SetVariable",
              "dependsOn": [{"activity": "TryGetWatermark", "dependencyConditions": ["Succeeded"]}],
              "typeProperties": {
                "variableName": "watermarkSuccess",
                "value": true
              }
            },
            {
              "name": "HandleWatermarkError",
              "type": "SetVariable",
              "dependsOn": [{"activity": "TryGetWatermark", "dependencyConditions": ["Failed"]}],
              "typeProperties": {
                "variableName": "retryCount",
                "value": "@add(variables('retryCount'), 1)"
              }
            },
            {
              "name": "WaitBeforeRetry",
              "type": "Wait",
              "dependsOn": [{"activity": "HandleWatermarkError", "dependencyConditions": ["Succeeded"]}],
              "typeProperties": {
                "waitTimeInSeconds": "@pipeline().parameters.retryIntervalSeconds"
              }
            }
          ]
        }
      }
    ],
    "variables": {
      "retryCount": {
        "type": "Integer",
        "defaultValue": 0
      },
      "watermarkSuccess": {
        "type": "Boolean",
        "defaultValue": false
      }
    }
  }
}
```

---

## Azure Databricks Implementation

### Delta Lake Watermark Management

```python
# Databricks notebook for incremental processing with Delta Lake
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IncrementalProcessing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

class WatermarkManager:
    """Manages watermarks for incremental processing in Delta Lake"""
    
    def __init__(self, spark_session, watermark_table_path):
        self.spark = spark_session
        self.watermark_table_path = watermark_table_path
        self._initialize_watermark_table()
    
    def _initialize_watermark_table(self):
        """Initialize watermark table if it doesn't exist"""
        try:
            # Check if watermark table exists
            self.spark.read.format("delta").load(self.watermark_table_path)
            print(f"Watermark table exists at: {self.watermark_table_path}")
        except:
            # Create watermark table schema
            watermark_schema = StructType([
                StructField("table_name", StringType(), False),
                StructField("watermark_value", TimestampType(), True),
                StructField("watermark_id", LongType(), True),
                StructField("last_update_time", TimestampType(), False),
                StructField("processed_records", LongType(), True),
                StructField("status", StringType(), True),
                StructField("watermark_type", StringType(), True)
            ])
            
            # Create empty DataFrame and save as Delta table
            empty_df = self.spark.createDataFrame([], watermark_schema)
            empty_df.write.format("delta").mode("overwrite").save(self.watermark_table_path)
            print(f"Created watermark table at: {self.watermark_table_path}")
    
    def get_watermark(self, table_name, watermark_type="timestamp"):
        """Get current watermark value for a table"""
        watermark_df = self.spark.read.format("delta").load(self.watermark_table_path)
        
        result = watermark_df.filter(
            (col("table_name") == table_name) & 
            (col("watermark_type") == watermark_type)
        ).collect()
        
        if result:
            if watermark_type == "timestamp":
                return result[0]["watermark_value"]
            elif watermark_type == "id":
                return result[0]["watermark_id"]
            else:
                return {
                    "watermark_value": result[0]["watermark_value"],
                    "watermark_id": result[0]["watermark_id"]
                }
        else:
            # Return default watermark for new tables
            if watermark_type == "timestamp":
                return datetime.datetime(1900, 1, 1)
            elif watermark_type == "id":
                return 0
            else:
                return {
                    "watermark_value": datetime.datetime(1900, 1, 1),
                    "watermark_id": 0
                }
    
    def update_watermark(self, table_name, new_watermark_value=None, new_watermark_id=None, 
                        processed_records=0, watermark_type="timestamp"):
        """Update watermark value for a table"""
        
        # Load watermark table as Delta table
        watermark_table = DeltaTable.forPath(self.spark, self.watermark_table_path)
        
        # Prepare update values
        update_values = {
            "last_update_time": current_timestamp(),
            "processed_records": lit(processed_records),
            "status": lit("active")
        }
        
        if watermark_type == "timestamp" and new_watermark_value:
            update_values["watermark_value"] = lit(new_watermark_value)
        elif watermark_type == "id" and new_watermark_id:
            update_values["watermark_id"] = lit(new_watermark_id)
        elif watermark_type == "hybrid":
            if new_watermark_value:
                update_values["watermark_value"] = lit(new_watermark_value)
            if new_watermark_id:
                update_values["watermark_id"] = lit(new_watermark_id)
        
        # Upsert watermark record
        watermark_table.alias("target").merge(
            self.spark.createDataFrame([
                (table_name, new_watermark_value, new_watermark_id, 
                 datetime.datetime.now(), processed_records, "active", watermark_type)
            ], ["table_name", "watermark_value", "watermark_id", "last_update_time", 
                "processed_records", "status", "watermark_type"]).alias("source"),
            "target.table_name = source.table_name AND target.watermark_type = source.watermark_type"
        ).whenMatchedUpdate(set=update_values) \
         .whenNotMatchedInsertAll() \
         .execute()
        
        print(f"Updated watermark for {table_name} ({watermark_type}): {new_watermark_value or new_watermark_id}")

# Initialize watermark manager
watermark_table_path = "/mnt/datalake/watermarks/watermark_table"
wm_manager = WatermarkManager(spark, watermark_table_path)
```

### Incremental Data Processing Functions

```python
class IncrementalProcessor:
    """Handles incremental data processing for various source types"""
    
    def __init__(self, spark_session, watermark_manager):
        self.spark = spark_session
        self.wm_manager = watermark_manager
    
    def process_jdbc_incremental(self, jdbc_config, table_name, watermark_column, 
                                target_path, watermark_type="timestamp"):
        """Process incremental data from JDBC source"""
        
        print(f"Starting incremental processing for {table_name}")
        
        # Get current watermark
        current_watermark = self.wm_manager.get_watermark(table_name, watermark_type)
        print(f"Current watermark: {current_watermark}")
        
        # Build incremental query
        if watermark_type == "timestamp":
            incremental_query = f"""
                SELECT * FROM {table_name} 
                WHERE {watermark_column} > '{current_watermark}'
                ORDER BY {watermark_column}
            """
        elif watermark_type == "id":
            incremental_query = f"""
                SELECT * FROM {table_name} 
                WHERE {watermark_column} > {current_watermark}
                ORDER BY {watermark_column}
            """
        
        # Read incremental data
        incremental_df = self.spark.read.format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("driver", jdbc_config["driver"]) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .option("query", incremental_query) \
            .load()
        
        record_count = incremental_df.count()
        print(f"Found {record_count} new/updated records")
        
        if record_count > 0:
            # Add processing metadata
            processed_df = incremental_df \
                .withColumn("processing_timestamp", current_timestamp()) \
                .withColumn("batch_id", lit(f"{table_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"))
            
            # Write to target (Delta Lake)
            processed_df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(target_path)
            
            # Get new watermark value
            if watermark_type == "timestamp":
                new_watermark = incremental_df.agg(max(col(watermark_column))).collect()[0][0]
                self.wm_manager.update_watermark(table_name, new_watermark_value=new_watermark, 
                                               processed_records=record_count, watermark_type=watermark_type)
            elif watermark_type == "id":
                new_watermark = incremental_df.agg(max(col(watermark_column))).collect()[0][0]
                self.wm_manager.update_watermark(table_name, new_watermark_id=new_watermark, 
                                               processed_records=record_count, watermark_type=watermark_type)
            
            print(f"Processing completed. New watermark: {new_watermark}")
        else:
            print("No new records to process")
        
        return record_count
    
    def process_delta_merge_incremental(self, source_config, target_path, merge_keys, 
                                      table_name, watermark_column):
        """Process incremental data with Delta merge (SCD Type 1/2)"""
        
        print(f"Starting Delta merge processing for {table_name}")
        
        # Get current watermark
        current_watermark = self.wm_manager.get_watermark(table_name)
        
        # Read incremental source data
        source_df = self.spark.read.format("jdbc") \
            .option("url", source_config["url"]) \
            .option("driver", source_config["driver"]) \
            .option("user", source_config["user"]) \
            .option("password", source_config["password"]) \
            .option("query", f"""
                SELECT * FROM {table_name} 
                WHERE {watermark_column} > '{current_watermark}'
            """) \
            .load()
        
        record_count = source_df.count()
        
        if record_count > 0:
            # Load target Delta table
            target_table = DeltaTable.forPath(self.spark, target_path)
            
            # Add metadata columns
            source_with_metadata = source_df \
                .withColumn("effective_date", current_timestamp()) \
                .withColumn("is_current", lit(True)) \
                .withColumn("batch_id", lit(f"batch_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"))
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            # Perform merge operation (SCD Type 2)
            target_table.alias("target").merge(
                source_with_metadata.alias("source"),
                merge_condition
            ).whenMatchedUpdate(
                condition="target.is_current = true",
                set={
                    "is_current": lit(False),
                    "end_date": current_timestamp()
                }
            ).whenNotMatchedInsertAll().execute()
            
            # Update watermark
            new_watermark = source_df.agg(max(col(watermark_column))).collect()[0][0]
            self.wm_manager.update_watermark(table_name, new_watermark_value=new_watermark, 
                                           processed_records=record_count)
            
            print(f"Merge completed. Processed {record_count} records. New watermark: {new_watermark}")
        
        return record_count
    
    def process_file_incremental(self, source_path, target_path, table_name, 
                                file_pattern="*.csv", watermark_type="file_timestamp"):
        """Process incremental files based on file timestamps"""
        
        print(f"Starting file-based incremental processing for {table_name}")
        
        # Get current watermark (last processed file timestamp)
        current_watermark = self.wm_manager.get_watermark(table_name, "timestamp")
        
        # List files newer than watermark
        file_list = dbutils.fs.ls(source_path)
        new_files = [
            f for f in file_list 
            if f.modificationTime > int(current_watermark.timestamp() * 1000) and 
               f.name.endswith(file_pattern.replace("*", ""))
        ]
        
        print(f"Found {len(new_files)} new files to process")
        
        if new_files:
            # Process each file
            total_records = 0
            latest_timestamp = current_watermark
            
            for file_info in new_files:
                file_path = file_info.path
                print(f"Processing file: {file_path}")
                
                # Read file
                file_df = self.spark.read.format("csv") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .load(file_path)
                
                # Add file metadata
                processed_df = file_df \
                    .withColumn("source_file", lit(file_info.name)) \
                    .withColumn("file_timestamp", lit(datetime.datetime.fromtimestamp(file_info.modificationTime / 1000))) \
                    .withColumn("processing_timestamp", current_timestamp())
                
                # Write to target
                processed_df.write.format("delta") \
                    .mode("append") \
                    .save(target_path)
                
                file_record_count = file_df.count()
                total_records += file_record_count
                
                # Track latest file timestamp
                file_timestamp = datetime.datetime.fromtimestamp(file_info.modificationTime / 1000)
                if file_timestamp > latest_timestamp:
                    latest_timestamp = file_timestamp
                
                print(f"Processed {file_record_count} records from {file_info.name}")
            
            # Update watermark with latest file timestamp
            self.wm_manager.update_watermark(table_name, new_watermark_value=latest_timestamp, 
                                           processed_records=total_records)
            
            print(f"File processing completed. Total records: {total_records}")
            return total_records
        else:
            print("No new files to process")
            return 0

# Usage examples
jdbc_config = {
    "url": "jdbc:sqlserver://server:1433;databaseName=AdventureWorks",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "user": dbutils.secrets.get("db-scope", "username"),
    "password": dbutils.secrets.get("db-scope", "password")
}

# Initialize processor
processor = IncrementalProcessor(spark, wm_manager)

# Process different types of incremental data
sales_records = processor.process_jdbc_incremental(
    jdbc_config=jdbc_config,
    table_name="Sales.Orders",
    watermark_column="LastModifiedDate",
    target_path="/mnt/datalake/sales/orders",
    watermark_type="timestamp"
)

customer_records = processor.process_delta_merge_incremental(
    source_config=jdbc_config,
    target_path="/mnt/datalake/customers/customer_history",
    merge_keys=["CustomerID"],
    table_name="Sales.Customer",
    watermark_column="LastModifiedDate"
)

file_records = processor.process_file_incremental(
    source_path="/mnt/datalake/incoming/logs/",
    target_path="/mnt/datalake/processed/logs/",
    table_name="ApplicationLogs",
    file_pattern="*.json"
)

print(f"Processing summary:")
print(f"Sales records: {sales_records}")
print(f"Customer records: {customer_records}")
print(f"File records: {file_records}")
```

### Advanced Databricks Patterns

```python
# Advanced incremental processing patterns
class AdvancedIncrementalProcessor:
    """Advanced patterns for incremental processing"""
    
    def __init__(self, spark_session, watermark_manager):
        self.spark = spark_session
        self.wm_manager = watermark_manager
    
    def process_with_change_data_capture(self, source_config, cdc_table, target_path, 
                                       primary_keys, table_name):
        """Process incremental data using Change Data Capture (CDC)"""
        
        print(f"Processing CDC data for {table_name}")
        
        # Get last processed LSN (Log Sequence Number)
        last_lsn = self.wm_manager.get_watermark(table_name, "id")
        
        # Read CDC changes
        cdc_query = f"""
            SELECT *, __$operation, __$start_lsn
            FROM {cdc_table}
            WHERE __$start_lsn > {last_lsn}
            ORDER BY __$start_lsn
        """
        
        cdc_df = self.spark.read.format("jdbc") \
            .option("url", source_config["url"]) \
            .option("driver", source_config["driver"]) \
            .option("user", source_config["user"]) \
            .option("password", source_config["password"]) \
            .option("query", cdc_query) \
            .load()
        
        if cdc_df.count() > 0:
            # Process different operation types
            inserts = cdc_df.filter(col("__$operation") == 2)  # Insert
            updates = cdc_df.filter(col("__$operation") == 4)  # Update
            deletes = cdc_df.filter(col("__$operation") == 1)  # Delete
            
            # Load target table
            target_table = DeltaTable.forPath(self.spark, target_path)
            
            # Process inserts
            if inserts.count() > 0:
                inserts_clean = inserts.drop("__$operation", "__$start_lsn") \
                    .withColumn("cdc_operation", lit("INSERT")) \
                    .withColumn("cdc_timestamp", current_timestamp())
                
                target_table.alias("target").merge(
                    inserts_clean.alias("source"),
                    " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
                ).whenNotMatchedInsertAll().execute()
            
            # Process updates
            if updates.count() > 0:
                updates_clean = updates.drop("__$operation", "__$start_lsn") \
                    .withColumn("cdc_operation", lit("UPDATE")) \
                    .withColumn("cdc_timestamp", current_timestamp())
                
                # Get column names excluding primary keys and metadata
                update_columns = [col for col in updates_clean.columns 
                                if col not in primary_keys + ["cdc_operation", "cdc_timestamp"]]
                
                update_set = {col: f"source.{col}" for col in update_columns}
                update_set["cdc_operation"] = lit("UPDATE")
                update_set["cdc_timestamp"] = current_timestamp()
                
                target_table.alias("target").merge(
                    updates_clean.alias("source"),
                    " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
                ).whenMatchedUpdate(set=update_set).execute()
            
            # Process deletes (soft delete)
            if deletes.count() > 0:
                delete_keys = deletes.select(*primary_keys).distinct()
                
                target_table.alias("target").merge(
                    delete_keys.alias("source"),
                    " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
                ).whenMatchedUpdate(set={
                    "is_deleted": lit(True),
                    "delete_timestamp": current_timestamp(),
                    "cdc_operation": lit("DELETE")
                }).execute()
            
            # Update watermark with highest LSN
            max_lsn = cdc_df.agg(max(col("__$start_lsn"))).collect()[0][0]
            self.wm_manager.update_watermark(table_name, new_watermark_id=max_lsn, 
                                           processed_records=cdc_df.count(), watermark_type="id")
            
            print(f"CDC processing completed. Processed LSN up to: {max_lsn}")
            return cdc_df.count()
        
        return 0
    
    def process_with_partition_pruning(self, source_config, table_name, partition_column, 
                                     target_path, watermark_column, partition_format="%Y-%m-%d"):
        """Process incremental data with intelligent partition pruning"""
        
        print(f"Processing with partition pruning for {table_name}")
        
        # Get current watermark
        current_watermark = self.wm_manager.get_watermark(table_name)
        
        # Calculate partition range to process
        start_date = current_watermark.date()
        end_date = datetime.datetime.now().date()
        
        # Generate partition list
        date_range = []
        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date.strftime(partition_format))
            current_date += datetime.timedelta(days=1)
        
        print(f"Processing partitions: {date_range}")
        
        # Build partition filter query
        partition_filter = " OR ".join([
            f"DATE({partition_column}) = '{date_str}'" for date_str in date_range
        ])
        
        incremental_query = f"""
            SELECT * FROM {table_name} 
            WHERE ({partition_filter}) 
            AND {watermark_column} > '{current_watermark}'
            ORDER BY {watermark_column}
        """
        
        # Read with partition pruning
        incremental_df = self.spark.read.format("jdbc") \
            .option("url", source_config["url"]) \
            .option("driver", source_config["driver"]) \
            .option("user", source_config["user"]) \
            .option("password", source_config["password"]) \
            .option("query", incremental_query) \
            .option("partitionColumn", partition_column) \
            .option("numPartitions", min(len(date_range), 10)) \
            .load()
        
        record_count = incremental_df.count()
        
        if record_count > 0:
            # Write with partitioning
            incremental_df.write.format("delta") \
                .mode("append") \
                .partitionBy("date_partition") \
                .option("mergeSchema", "true") \
                .save(target_path)
            
            # Update watermark
            new_watermark = incremental_df.agg(max(col(watermark_column))).collect()[0][0]
            self.wm_manager.update_watermark(table_name, new_watermark_value=new_watermark, 
                                           processed_records=record_count)
            
            print(f"Partition processing completed. Records: {record_count}")
        
        return record_count
    
    def process_with_data_quality_checks(self, source_config, table_name, target_path, 
                                       watermark_column, quality_rules):
        """Process incremental data with data quality validation"""
        
        print(f"Processing with data quality checks for {table_name}")
        
        # Get current watermark
        current_watermark = self.wm_manager.get_watermark(table_name)
        
        # Read incremental data
        incremental_query = f"""
            SELECT * FROM {table_name} 
            WHERE {watermark_column} > '{current_watermark}'
        """
        
        raw_df = self.spark.read.format("jdbc") \
            .option("url", source_config["url"]) \
            .option("driver", source_config["driver"]) \
            .option("user", source_config["user"]) \
            .option("password", source_config["password"]) \
            .option("query", incremental_query) \
            .load()
        
        if raw_df.count() > 0:
            # Apply data quality rules
            quality_df = raw_df
            quality_issues = []
            
            for rule_name, rule_condition in quality_rules.items():
                print(f"Applying quality rule: {rule_name}")
                
                # Add quality flag column
                quality_df = quality_df.withColumn(
                    f"quality_{rule_name}",
                    when(expr(rule_condition), lit(True)).otherwise(lit(False))
                )
                
                # Count quality issues
                issue_count = quality_df.filter(col(f"quality_{rule_name}") == False).count()
                quality_issues.append({
                    "rule": rule_name,
                    "failed_records": issue_count,
                    "total_records": raw_df.count()
                })
            
            # Add overall quality score
            quality_columns = [col(f"quality_{rule}") for rule in quality_rules.keys()]
            quality_df = quality_df.withColumn(
                "quality_score",
                (sum([col(f"quality_{rule}").cast("int") for rule in quality_rules.keys()]) / 
                 lit(len(quality_rules))).cast("decimal(5,2)")
            )
            
            # Separate good and bad records
            good_records = quality_df.filter(col("quality_score") >= 0.8)
            quarantine_records = quality_df.filter(col("quality_score") < 0.8)
            
            # Write good records to main target
            good_count = good_records.count()
            if good_count > 0:
                good_records.write.format("delta") \
                    .mode("append") \
                    .save(target_path)
            
            # Write quarantine records to separate location
            quarantine_count = quarantine_records.count()
            if quarantine_count > 0:
                quarantine_path = target_path.replace("/processed/", "/quarantine/")
                quarantine_records.write.format("delta") \
                    .mode("append") \
                    .save(quarantine_path)
            
            # Log quality metrics
            quality_summary = {
                "table_name": table_name,
                "total_records": raw_df.count(),
                "good_records": good_count,
                "quarantine_records": quarantine_count,
                "quality_issues": quality_issues,
                "processing_timestamp": datetime.datetime.now()
            }
            
            print(f"Data quality summary: {quality_summary}")
            
            # Update watermark only for good records
            if good_count > 0:
                new_watermark = good_records.agg(max(col(watermark_column))).collect()[0][0]
                self.wm_manager.update_watermark(table_name, new_watermark_value=new_watermark, 
                                               processed_records=good_count)
            
            return quality_summary
        
        return {"message": "No new records to process"}

# Usage example with quality rules
quality_rules = {
    "not_null_customer_id": "customer_id IS NOT NULL",
    "valid_email_format": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
    "positive_amount": "amount > 0",
    "valid_date_range": "order_date >= '2020-01-01' AND order_date <= current_date()"
}

advanced_processor = AdvancedIncrementalProcessor(spark, wm_manager)

# Process with data quality checks
quality_result = advanced_processor.process_with_data_quality_checks(
    source_config=jdbc_config,
    table_name="Sales.Orders",
    target_path="/mnt/datalake/sales/orders_processed",
    watermark_column="LastModifiedDate",
    quality_rules=quality_rules
)

print(f"Quality processing result: {quality_result}")
```

---

## Performance Optimization

### Optimization Strategies

```json
{
  "performance_optimization": {
    "adf_optimizations": [
      {
        "technique": "Parallel Copy",
        "description": "Use parallelCopies setting for large tables",
        "configuration": {
          "parallelCopies": 8,
          "dataIntegrationUnits": 16
        }
      },
      {
        "technique": "Staging",
        "description": "Enable staging for better performance",
        "configuration": {
          "enableStaging": true,
          "stagingSettings": {
            "linkedServiceName": "AzureBlobStorage",
            "path": "staging/temp"
          }
        }
      },
      {
        "technique": "Query Optimization",
        "description": "Use indexed columns in watermark queries",
        "example": "CREATE INDEX IX_LastModifiedDate ON Sales.Orders (LastModifiedDate)"
      }
    ],
    "databricks_optimizations": [
      {
        "technique": "Adaptive Query Execution",
        "configuration": {
          "spark.sql.adaptive.enabled": "true",
          "spark.sql.adaptive.coalescePartitions.enabled": "true",
          "spark.sql.adaptive.skewJoin.enabled": "true"
        }
      },
      {
        "technique": "Delta Lake Optimization",
        "commands": [
          "OPTIMIZE delta_table ZORDER BY (watermark_column)",
          "VACUUM delta_table RETAIN 168 HOURS"
        ]
      },
      {
        "technique": "Caching Strategy",
        "description": "Cache frequently accessed watermark data",
        "implementation": "watermark_df.cache()"
      }
    ]
  }
}
```

### Performance Monitoring

```python
# Performance monitoring and optimization
class PerformanceMonitor:
    """Monitor and optimize incremental processing performance"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def analyze_watermark_performance(self, watermark_table_path):
        """Analyze watermark table performance"""
        
        watermark_df = self.spark.read.format("delta").load(watermark_table_path)
        
        # Performance metrics
        metrics = {
            "total_tables": watermark_df.count(),
            "active_tables": watermark_df.filter(col("status") == "active").count(),
            "avg_processed_records": watermark_df.agg(avg("processed_records")).collect()[0][0],
            "tables_by_type": watermark_df.groupBy("watermark_type").count().collect(),
            "last_update_distribution": watermark_df.select(
                datediff(current_date(), col("last_update_time")).alias("days_since_update")
            ).groupBy("days_since_update").count().orderBy("days_since_update").collect()
        }
        
        return metrics
    
    def optimize_watermark_table(self, watermark_table_path):
        """Optimize watermark table for better performance"""
        
        print("Optimizing watermark table...")
        
        # Z-order optimization
        self.spark.sql(f"""
            OPTIMIZE delta.`{watermark_table_path}` 
            ZORDER BY (table_name, watermark_type)
        """)
        
        # Vacuum old versions
        self.spark.sql(f"""
            VACUUM delta.`{watermark_table_path}` RETAIN 168 HOURS
        """)
        
        print("Watermark table optimization completed")
    
    def generate_performance_report(self, processing_results):
        """Generate performance report for incremental processing"""
        
        report = {
            "execution_summary": {
                "total_tables_processed": len(processing_results),
                "total_records_processed": sum(r.get("records_processed", 0) for r in processing_results),
                "average_processing_time": sum(r.get("processing_time", 0) for r in processing_results) / len(processing_results),
                "success_rate": len([r for r in processing_results if r.get("status") == "success"]) / len(processing_results) * 100
            },
            "performance_by_table": processing_results,
            "recommendations": self._generate_recommendations(processing_results)
        }
        
        return report
    
    def _generate_recommendations(self, processing_results):
        """Generate performance recommendations"""
        
        recommendations = []
        
        # Identify slow processing tables
        slow_tables = [r for r in processing_results if r.get("processing_time", 0) > 300]  # > 5 minutes
        if slow_tables:
            recommendations.append({
                "type": "performance",
                "priority": "high",
                "message": f"Consider optimizing {len(slow_tables)} slow-processing tables",
                "tables": [t["table_name"] for t in slow_tables]
            })
        
        # Identify tables with large record counts
        large_batch_tables = [r for r in processing_results if r.get("records_processed", 0) > 1000000]
        if large_batch_tables:
            recommendations.append({
                "type": "scalability",
                "priority": "medium",
                "message": f"Consider implementing micro-batching for {len(large_batch_tables)} tables with large batches",
                "tables": [t["table_name"] for t in large_batch_tables]
            })
        
        # Check for failed processing
        failed_tables = [r for r in processing_results if r.get("status") == "failed"]
        if failed_tables:
            recommendations.append({
                "type": "reliability",
                "priority": "critical",
                "message": f"Address failures in {len(failed_tables)} tables",
                "tables": [t["table_name"] for t in failed_tables]
            })
        
        return recommendations

# Usage example
perf_monitor = PerformanceMonitor(spark)

# Analyze watermark performance
watermark_metrics = perf_monitor.analyze_watermark_performance(watermark_table_path)
print(f"Watermark performance metrics: {watermark_metrics}")

# Optimize watermark table
perf_monitor.optimize_watermark_table(watermark_table_path)

# Generate performance report
processing_results = [
    {"table_name": "Sales.Orders", "records_processed": 15000, "processing_time": 120, "status": "success"},
    {"table_name": "HR.Employees", "records_processed": 500, "processing_time": 45, "status": "success"},
    {"table_name": "Inventory.Products", "records_processed": 2000000, "processing_time": 450, "status": "success"}
]

performance_report = perf_monitor.generate_performance_report(processing_results)
print(f"Performance report: {performance_report}")
```

---

## Best Practices

### Implementation Guidelines

```json
{
  "incremental_processing_best_practices": {
    "watermark_design": [
      "Use indexed columns for watermark queries",
      "Implement atomic watermark updates",
      "Store watermarks in reliable, transactional storage",
      "Use appropriate data types (DATETIME2 for timestamps)",
      "Implement watermark versioning for rollback scenarios"
    ],
    "error_handling": [
      "Implement retry logic with exponential backoff",
      "Use dead letter queues for failed records",
      "Log detailed error information for troubleshooting",
      "Implement circuit breaker patterns for external dependencies",
      "Create alerting for watermark update failures"
    ],
    "performance": [
      "Use parallel processing where possible",
      "Implement micro-batching for large datasets",
      "Optimize source queries with appropriate indexes",
      "Use column pruning and predicate pushdown",
      "Monitor and tune resource allocation"
    ],
    "data_quality": [
      "Implement data validation rules",
      "Use quarantine areas for invalid data",
      "Track data lineage and processing metadata",
      "Implement schema evolution handling",
      "Monitor data freshness and completeness"
    ],
    "monitoring": [
      "Track processing metrics and SLAs",
      "Implement health checks for all components",
      "Monitor watermark lag and processing delays",
      "Set up alerting for processing failures",
      "Create dashboards for operational visibility"
    ]
  }
}
```

This comprehensive guide provides everything needed to implement robust incremental data processing with watermarks in Azure Data Factory and Azure Databricks, covering all aspects from basic concepts to advanced patterns, performance optimization, and best practices.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*