# Azure Slowly Changing Dimensions (SCD) - Comprehensive Guide

## Table of Contents
1. [Overview](#overview)
2. [SCD Fundamentals](#scd-fundamentals)
3. [SCD Types (0-7)](#scd-types-0-7)
4. [Azure Data Factory Implementation](#azure-data-factory-implementation)
5. [Azure Databricks Implementation](#azure-databricks-implementation)
6. [Hybrid ADF + Databricks Solutions](#hybrid-adf--databricks-solutions)
7. [Performance Optimization](#performance-optimization)
8. [Real-World Examples](#real-world-examples)
9. [Monitoring & Best Practices](#monitoring--best-practices)
10. [Troubleshooting](#troubleshooting)

---

## Overview

Slowly Changing Dimensions (SCD) are a fundamental concept in data warehousing that handles changes to dimension data over time. In Azure, we can implement SCD using Azure Data Factory (ADF) and Azure Databricks with Delta Lake for robust, scalable solutions.

### Key Benefits in Azure

| Benefit | ADF | Databricks | Combined |
|---------|-----|------------|----------|
| **Scalability** | Pipeline orchestration | Spark processing | End-to-end scale |
| **Performance** | Parallel execution | In-memory compute | Optimized workflows |
| **Reliability** | Built-in retry logic | Delta Lake ACID | Complete data integrity |
| **Cost Efficiency** | Pay-per-use | Auto-scaling | Resource optimization |
| **Integration** | 90+ connectors | Multi-format support | Comprehensive ecosystem |

### Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │  Azure Data     │    │  Azure          │
│  - SQL Server   │───▶│  Factory        │───▶│  Databricks     │
│  - Oracle       │    │  - Pipelines    │    │  - Delta Lake   │
│  - Flat Files   │    │  - Data Flows   │    │  - SCD Logic    │
│  - APIs         │    │  - Triggers     │    │  - Optimization │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                ▼
                       ┌─────────────────┐
                       │  Data Warehouse │
                       │  - Synapse      │
                       │  - SQL Database │
                       │  - Data Lake    │
                       └─────────────────┘
```

---

## SCD Fundamentals

### What are Slowly Changing Dimensions?

Slowly Changing Dimensions are dimension tables that change slowly over time, rather than on a regular schedule. They require special handling to maintain historical accuracy while providing current data.

### Core Concepts

```sql
-- Example: Customer dimension changes over time
-- Original Record (2024-01-01)
CustomerID | Name      | Address        | Phone      | City     | State | EffectiveDate | EndDate
1          | John Doe  | 123 Main St    | 555-1234   | Seattle  | WA    | 2024-01-01    | 9999-12-31

-- Customer moves (2024-06-15)  
-- How do we handle this change?
-- - Keep only current? (Lose history)
-- - Keep both records? (Track changes)
-- - Update existing? (Overwrite)
```

### Change Detection Methods

| Method | Description | Use Case | Performance |
|--------|-------------|----------|-------------|
| **Full Load** | Compare entire source vs target | Small datasets | Low |
| **Timestamp-based** | Use LastModified column | Real-time systems | High |
| **Change Data Capture (CDC)** | Track database changes | Transactional systems | Very High |
| **Hash Comparison** | Compare row hashes | Large datasets | Medium |
| **Trigger-based** | Database triggers | Legacy systems | Medium |

---

## SCD Types (0-7)

### SCD Type 0: Retain Original
**Never change the data - keep original values**

```sql
-- Example: Birth Date (never changes)
CREATE TABLE DimCustomer_Type0 (
    CustomerID INT PRIMARY KEY,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    BirthDate DATE,  -- Type 0: Never changes
    CreatedDate DATETIME2
);

-- No updates allowed on Type 0 fields
-- INSERT only for new records
INSERT INTO DimCustomer_Type0 VALUES (1, 'John', 'Doe', '1985-05-15', GETDATE());
```

### SCD Type 1: Overwrite
**Overwrite old data with new data - no history**

```sql
-- Example: Customer phone number (current only)
CREATE TABLE DimCustomer_Type1 (
    CustomerID INT PRIMARY KEY,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    PhoneNumber NVARCHAR(20),  -- Type 1: Overwrite
    Email NVARCHAR(100),       -- Type 1: Overwrite
    LastUpdated DATETIME2
);

-- Update operation for Type 1
UPDATE DimCustomer_Type1 
SET PhoneNumber = '555-9999', 
    Email = 'john.new@email.com',
    LastUpdated = GETDATE()
WHERE CustomerID = 1;
```

### SCD Type 2: Add New Record
**Add new record for each change - full history**

```sql
-- Example: Customer address (track history)
CREATE TABLE DimCustomer_Type2 (
    CustomerSK INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate Key
    CustomerID INT,                            -- Natural Key
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    Address NVARCHAR(200),     -- Type 2: Track changes
    City NVARCHAR(50),         -- Type 2: Track changes
    State NVARCHAR(10),        -- Type 2: Track changes
    EffectiveDate DATE,        -- When record became active
    EndDate DATE,              -- When record became inactive
    IsCurrent BIT,             -- Current record indicator
    CreatedDate DATETIME2
);

-- Original record
INSERT INTO DimCustomer_Type2 VALUES 
(1, 'John', 'Doe', '123 Main St', 'Seattle', 'WA', '2024-01-01', '9999-12-31', 1, GETDATE());

-- Customer moves - close old record and add new
UPDATE DimCustomer_Type2 
SET EndDate = '2024-06-14', IsCurrent = 0 
WHERE CustomerID = 1 AND IsCurrent = 1;

INSERT INTO DimCustomer_Type2 VALUES 
(1, 'John', 'Doe', '456 Oak Ave', 'Portland', 'OR', '2024-06-15', '9999-12-31', 1, GETDATE());
```

### SCD Type 3: Add New Attribute
**Add columns to track previous values**

```sql
-- Example: Customer status (current + previous)
CREATE TABLE DimCustomer_Type3 (
    CustomerID INT PRIMARY KEY,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    CurrentStatus NVARCHAR(20),      -- Type 3: Current value
    PreviousStatus NVARCHAR(20),     -- Type 3: Previous value
    StatusChangeDate DATE,           -- Type 3: When changed
    LastUpdated DATETIME2
);

-- Update operation for Type 3
UPDATE DimCustomer_Type3 
SET PreviousStatus = CurrentStatus,
    CurrentStatus = 'Premium',
    StatusChangeDate = GETDATE(),
    LastUpdated = GETDATE()
WHERE CustomerID = 1;
```

### SCD Type 4: History Table
**Separate current and historical data**

```sql
-- Current table
CREATE TABLE DimCustomer_Current (
    CustomerID INT PRIMARY KEY,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    Address NVARCHAR(200),
    City NVARCHAR(50),
    State NVARCHAR(10),
    LastUpdated DATETIME2
);

-- History table
CREATE TABLE DimCustomer_History (
    HistoryID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    Address NVARCHAR(200),
    City NVARCHAR(50),
    State NVARCHAR(10),
    EffectiveDate DATE,
    EndDate DATE,
    ChangeType NVARCHAR(10),  -- INSERT, UPDATE, DELETE
    CreatedDate DATETIME2
);

-- Move old record to history before updating current
INSERT INTO DimCustomer_History 
SELECT CustomerID, FirstName, LastName, Address, City, State, 
       CAST(LastUpdated AS DATE), CAST(GETDATE() AS DATE), 'UPDATE', GETDATE()
FROM DimCustomer_Current 
WHERE CustomerID = 1;

-- Update current record
UPDATE DimCustomer_Current 
SET Address = '456 Oak Ave', City = 'Portland', State = 'OR', LastUpdated = GETDATE()
WHERE CustomerID = 1;
```

### SCD Type 6: Hybrid (1+2+3)
**Combination of Type 1, 2, and 3 approaches**

```sql
-- Example: Comprehensive customer tracking
CREATE TABLE DimCustomer_Type6 (
    CustomerSK INT IDENTITY(1,1) PRIMARY KEY,  -- Type 2: Surrogate Key
    CustomerID INT,                            -- Natural Key
    FirstName NVARCHAR(50),                    -- Type 1: Overwrite
    LastName NVARCHAR(50),                     -- Type 1: Overwrite
    CurrentAddress NVARCHAR(200),              -- Type 3: Current value
    OriginalAddress NVARCHAR(200),             -- Type 3: Original value
    Address NVARCHAR(200),                     -- Type 2: Historical value
    City NVARCHAR(50),                         -- Type 2: Historical value
    State NVARCHAR(10),                        -- Type 2: Historical value
    EffectiveDate DATE,                        -- Type 2: Effective date
    EndDate DATE,                              -- Type 2: End date
    IsCurrent BIT,                             -- Type 2: Current flag
    CreatedDate DATETIME2
);

-- Complex update logic combining all three types
-- 1. Update Type 1 fields in all records
UPDATE DimCustomer_Type6 
SET FirstName = 'Jonathan'  -- Type 1: Update everywhere
WHERE CustomerID = 1;

-- 2. Close current Type 2 record
UPDATE DimCustomer_Type6 
SET EndDate = '2024-06-14', IsCurrent = 0 
WHERE CustomerID = 1 AND IsCurrent = 1;

-- 3. Insert new Type 2 record with Type 3 current address
INSERT INTO DimCustomer_Type6 VALUES 
(1, 'Jonathan', 'Doe', '456 Oak Ave', '123 Main St', '456 Oak Ave', 'Portland', 'OR', 
 '2024-06-15', '9999-12-31', 1, GETDATE());
```

---

## Azure Data Factory Implementation

### ADF Data Flow for SCD Type 2

```json
{
    "name": "SCD_Type2_DataFlow",
    "properties": {
        "type": "MappingDataFlow",
        "typeProperties": {
            "sources": [
                {
                    "name": "SourceCustomers",
                    "dataset": {
                        "referenceName": "SourceCustomerDataset",
                        "type": "DatasetReference"
                    }
                },
                {
                    "name": "ExistingDimension",
                    "dataset": {
                        "referenceName": "DimCustomerDataset", 
                        "type": "DatasetReference"
                    }
                }
            ],
            "sinks": [
                {
                    "name": "UpdatedDimension",
                    "dataset": {
                        "referenceName": "DimCustomerDataset",
                        "type": "DatasetReference"
                    }
                }
            ],
            "transformations": [
                {
                    "name": "CreateHashKey",
                    "type": "DerivedColumn",
                    "settings": {
                        "columns": [
                            {
                                "name": "SourceHashKey",
                                "expression": "sha1(concat(toString(CustomerID),Address,City,State))"
                            }
                        ]
                    }
                },
                {
                    "name": "ExistingHashKey", 
                    "type": "DerivedColumn",
                    "settings": {
                        "columns": [
                            {
                                "name": "ExistingHashKey",
                                "expression": "sha1(concat(toString(CustomerID),Address,City,State))"
                            }
                        ]
                    }
                },
                {
                    "name": "JoinOnNaturalKey",
                    "type": "Join",
                    "settings": {
                        "joinType": "outer",
                        "leftSource": "SourceCustomers",
                        "rightSource": "ExistingDimension",
                        "joinCondition": "SourceCustomers@CustomerID == ExistingDimension@CustomerID && ExistingDimension@IsCurrent == true()"
                    }
                },
                {
                    "name": "IdentifyChanges",
                    "type": "DerivedColumn", 
                    "settings": {
                        "columns": [
                            {
                                "name": "ChangeType",
                                "expression": "case(\n    isNull(ExistingDimension@CustomerID), 'INSERT',\n    isNull(SourceCustomers@CustomerID), 'DELETE', \n    SourceHashKey != ExistingHashKey, 'UPDATE',\n    'NO_CHANGE'\n)"
                            }
                        ]
                    }
                },
                {
                    "name": "FilterChanges",
                    "type": "Filter",
                    "settings": {
                        "condition": "ChangeType != 'NO_CHANGE'"
                    }
                },
                {
                    "name": "PrepareInserts",
                    "type": "DerivedColumn",
                    "settings": {
                        "columns": [
                            {
                                "name": "EffectiveDate",
                                "expression": "currentDate()"
                            },
                            {
                                "name": "EndDate", 
                                "expression": "toDate('9999-12-31')"
                            },
                            {
                                "name": "IsCurrent",
                                "expression": "true()"
                            }
                        ]
                    }
                }
            ]
        }
    }
}
```

### ADF Pipeline for SCD Processing

```json
{
    "name": "SCD_Processing_Pipeline",
    "properties": {
        "activities": [
            {
                "name": "Get_Watermark",
                "type": "Lookup",
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": "SELECT MAX(LastModified) as WatermarkValue FROM WatermarkTable WHERE TableName = 'Customers'"
                    },
                    "dataset": {
                        "referenceName": "WatermarkDataset",
                        "type": "DatasetReference"
                    }
                }
            },
            {
                "name": "Get_Source_Data",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "Get_Watermark",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "source": {
                        "type": "SqlServerSource",
                        "sqlReaderQuery": {
                            "value": "SELECT * FROM Customers WHERE LastModified > '@{activity('Get_Watermark').output.firstRow.WatermarkValue}'",
                            "type": "Expression"
                        }
                    },
                    "sink": {
                        "type": "AzureSqlSink",
                        "tableOption": "autoCreate"
                    }
                },
                "inputs": [
                    {
                        "referenceName": "SourceCustomerDataset",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "StagingCustomerDataset", 
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "Process_SCD_Type2",
                "type": "ExecuteDataFlow",
                "dependsOn": [
                    {
                        "activity": "Get_Source_Data",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "dataflow": {
                        "referenceName": "SCD_Type2_DataFlow",
                        "type": "DataFlowReference"
                    },
                    "compute": {
                        "coreCount": 8,
                        "computeType": "General"
                    }
                }
            },
            {
                "name": "Update_Watermark",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [
                    {
                        "activity": "Process_SCD_Type2",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "storedProcedureName": "sp_UpdateWatermark",
                    "storedProcedureParameters": {
                        "TableName": {
                            "value": "Customers",
                            "type": "String"
                        },
                        "WatermarkValue": {
                            "value": {
                                "value": "@utcnow()",
                                "type": "Expression"
                            },
                            "type": "DateTime"
                        }
                    }
                }
            }
        ]
    }
}
```

### ADF Stored Procedures for SCD

```sql
-- Stored Procedure for SCD Type 2 Processing
CREATE PROCEDURE sp_ProcessSCDType2
    @SourceTable NVARCHAR(128),
    @TargetTable NVARCHAR(128),
    @NaturalKey NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    
    -- Step 1: Close existing records for updated customers
    SET @SQL = '
    UPDATE t
    SET EndDate = CAST(GETDATE() AS DATE),
        IsCurrent = 0
    FROM ' + @TargetTable + ' t
    INNER JOIN ' + @SourceTable + ' s ON t.' + @NaturalKey + ' = s.' + @NaturalKey + '
    WHERE t.IsCurrent = 1
      AND (t.Address != s.Address OR t.City != s.City OR t.State != s.State)';
    
    EXEC sp_executesql @SQL;
    
    -- Step 2: Insert new records (new customers and updated customers)
    SET @SQL = '
    INSERT INTO ' + @TargetTable + ' (CustomerID, FirstName, LastName, Address, City, State, EffectiveDate, EndDate, IsCurrent, CreatedDate)
    SELECT s.CustomerID, s.FirstName, s.LastName, s.Address, s.City, s.State,
           CAST(GETDATE() AS DATE) as EffectiveDate,
           CAST(''9999-12-31'' AS DATE) as EndDate,
           1 as IsCurrent,
           GETDATE() as CreatedDate
    FROM ' + @SourceTable + ' s
    LEFT JOIN ' + @TargetTable + ' t ON s.' + @NaturalKey + ' = t.' + @NaturalKey + ' AND t.IsCurrent = 1
    WHERE t.' + @NaturalKey + ' IS NULL  -- New customers
       OR (t.Address != s.Address OR t.City != s.City OR t.State != s.State)  -- Changed customers';
    
    EXEC sp_executesql @SQL;
    
END;
```

---

## Azure Databricks Implementation

### Delta Lake SCD Framework

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import hashlib
from datetime import datetime, date

class DeltaSCDProcessor:
    """
    Comprehensive SCD processor using Delta Lake
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.current_date = date.today()
        self.end_date = date(9999, 12, 31)
    
    def create_hash_key(self, df, columns):
        """
        Create hash key for change detection
        """
        hash_expr = sha1(concat_ws("|", *[coalesce(col(c).cast("string"), lit("NULL")) for c in columns]))
        return df.withColumn("hash_key", hash_expr)
    
    def scd_type_0(self, source_df, target_path, natural_key, type_0_columns):
        """
        SCD Type 0: Retain Original - Never update specified columns
        """
        print("🔒 Processing SCD Type 0 (Retain Original)")
        
        try:
            # Read existing target table
            target_df = self.spark.read.format("delta").load(target_path)
            
            # Only insert new records, never update Type 0 columns
            new_records = source_df.join(target_df, natural_key, "left_anti")
            
            if new_records.count() > 0:
                new_records.write.format("delta").mode("append").save(target_path)
                print(f"✅ Inserted {new_records.count()} new records (Type 0)")
            else:
                print("ℹ️ No new records to insert (Type 0)")
                
        except Exception as e:
            # First load - create table
            source_df.write.format("delta").mode("overwrite").save(target_path)
            print(f"✅ Created new Type 0 table with {source_df.count()} records")
    
    def scd_type_1(self, source_df, target_path, natural_key, type_1_columns):
        """
        SCD Type 1: Overwrite - Update with latest values
        """
        print("🔄 Processing SCD Type 1 (Overwrite)")
        
        try:
            # Create or get Delta table
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
            else:
                # Create initial table
                source_df.write.format("delta").mode("overwrite").save(target_path)
                print(f"✅ Created new Type 1 table with {source_df.count()} records")
                return
            
            # Merge for Type 1 (upsert)
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in natural_key])
            
            update_dict = {col: f"source.{col}" for col in type_1_columns}
            update_dict["last_updated"] = "current_timestamp()"
            
            insert_dict = {col: f"source.{col}" for col in source_df.columns}
            insert_dict["last_updated"] = "current_timestamp()"
            
            (delta_table.alias("target")
             .merge(source_df.alias("source"), merge_condition)
             .whenMatchedUpdate(set=update_dict)
             .whenNotMatchedInsert(values=insert_dict)
             .execute())
            
            print("✅ SCD Type 1 processing completed")
            
        except Exception as e:
            print(f"❌ Error in SCD Type 1 processing: {str(e)}")
            raise
    
    def scd_type_2(self, source_df, target_path, natural_key, scd_columns, 
                   effective_date_col="effective_date", end_date_col="end_date", 
                   is_current_col="is_current"):
        """
        SCD Type 2: Add New Record - Track full history
        """
        print("📈 Processing SCD Type 2 (Add New Record)")
        
        try:
            # Add hash key for change detection
            source_with_hash = self.create_hash_key(source_df, scd_columns)
            
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                target_df = delta_table.toDF()
                
                # Add hash key to existing data
                target_with_hash = self.create_hash_key(target_df, scd_columns)
                
                # Find changes
                current_records = target_with_hash.filter(col(is_current_col) == True)
                
                changes = (source_with_hash.alias("source")
                          .join(current_records.alias("target"), 
                               [col(f"source.{key}") == col(f"target.{key}") for key in natural_key], 
                               "inner")
                          .where(col("source.hash_key") != col("target.hash_key"))
                          .select("source.*"))
                
                new_records = (source_with_hash.alias("source")
                              .join(current_records.alias("target"), 
                                   [col(f"source.{key}") == col(f"target.{key}") for key in natural_key], 
                                   "left_anti"))
                
                # Close changed records
                if changes.count() > 0:
                    change_keys = changes.select(*natural_key).distinct()
                    
                    (delta_table.alias("target")
                     .merge(change_keys.alias("changes"), 
                           " AND ".join([f"target.{key} = changes.{key}" for key in natural_key]))
                     .whenMatchedUpdate(
                         condition=f"target.{is_current_col} = true",
                         set={
                             end_date_col: "current_date()",
                             is_current_col: "false",
                             "last_updated": "current_timestamp()"
                         })
                     .execute())
                    
                    print(f"🔄 Closed {changes.count()} changed records")
                
                # Insert new versions of changed records
                if changes.count() > 0:
                    new_versions = (changes
                                   .withColumn(effective_date_col, lit(self.current_date))
                                   .withColumn(end_date_col, lit(self.end_date))
                                   .withColumn(is_current_col, lit(True))
                                   .withColumn("created_date", current_timestamp())
                                   .withColumn("last_updated", current_timestamp())
                                   .drop("hash_key"))
                    
                    new_versions.write.format("delta").mode("append").save(target_path)
                    print(f"➕ Inserted {changes.count()} new versions")
                
                # Insert completely new records
                if new_records.count() > 0:
                    new_inserts = (new_records
                                  .withColumn(effective_date_col, lit(self.current_date))
                                  .withColumn(end_date_col, lit(self.end_date))
                                  .withColumn(is_current_col, lit(True))
                                  .withColumn("created_date", current_timestamp())
                                  .withColumn("last_updated", current_timestamp())
                                  .drop("hash_key"))
                    
                    new_inserts.write.format("delta").mode("append").save(target_path)
                    print(f"➕ Inserted {new_records.count()} new records")
                    
            else:
                # Initial load
                initial_data = (source_with_hash
                               .withColumn(effective_date_col, lit(self.current_date))
                               .withColumn(end_date_col, lit(self.end_date))
                               .withColumn(is_current_col, lit(True))
                               .withColumn("created_date", current_timestamp())
                               .withColumn("last_updated", current_timestamp())
                               .drop("hash_key"))
                
                initial_data.write.format("delta").mode("overwrite").save(target_path)
                print(f"✅ Created new Type 2 table with {initial_data.count()} records")
                
        except Exception as e:
            print(f"❌ Error in SCD Type 2 processing: {str(e)}")
            raise
    
    def scd_type_3(self, source_df, target_path, natural_key, type_3_columns):
        """
        SCD Type 3: Add New Attribute - Track previous value
        """
        print("🔀 Processing SCD Type 3 (Add New Attribute)")
        
        try:
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Create update expressions for Type 3
                update_dict = {}
                for col_name in type_3_columns:
                    update_dict[f"previous_{col_name}"] = f"target.{col_name}"
                    update_dict[col_name] = f"source.{col_name}"
                    update_dict[f"{col_name}_change_date"] = "current_date()"
                
                update_dict["last_updated"] = "current_timestamp()"
                
                # Merge condition
                merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in natural_key])
                
                # Insert dictionary
                insert_dict = {col: f"source.{col}" for col in source_df.columns}
                for col_name in type_3_columns:
                    insert_dict[f"previous_{col_name}"] = "null"
                    insert_dict[f"{col_name}_change_date"] = "null"
                insert_dict["last_updated"] = "current_timestamp()"
                
                (delta_table.alias("target")
                 .merge(source_df.alias("source"), merge_condition)
                 .whenMatchedUpdate(set=update_dict)
                 .whenNotMatchedInsert(values=insert_dict)
                 .execute())
                
                print("✅ SCD Type 3 processing completed")
                
            else:
                # Initial load with Type 3 structure
                initial_df = source_df
                for col_name in type_3_columns:
                    initial_df = (initial_df
                                 .withColumn(f"previous_{col_name}", lit(None).cast(StringType()))
                                 .withColumn(f"{col_name}_change_date", lit(None).cast(DateType())))
                
                initial_df = initial_df.withColumn("last_updated", current_timestamp())
                initial_df.write.format("delta").mode("overwrite").save(target_path)
                print(f"✅ Created new Type 3 table with {initial_df.count()} records")
                
        except Exception as e:
            print(f"❌ Error in SCD Type 3 processing: {str(e)}")
            raise
    
    def scd_type_6_hybrid(self, source_df, target_path, natural_key, 
                          type_1_columns, type_2_columns, type_3_columns):
        """
        SCD Type 6: Hybrid approach combining Type 1, 2, and 3
        """
        print("🔄 Processing SCD Type 6 (Hybrid: Type 1 + 2 + 3)")
        
        try:
            # Create hash key for Type 2 change detection
            source_with_hash = self.create_hash_key(source_df, type_2_columns)
            
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                target_df = delta_table.toDF()
                
                # Type 1 updates: Update all records for the natural key
                type1_updates = {}
                for col_name in type_1_columns:
                    type1_updates[col_name] = f"source.{col_name}"
                
                # Type 3 updates: Track previous values
                type3_updates = {}
                for col_name in type_3_columns:
                    type3_updates[f"current_{col_name}"] = f"source.{col_name}"
                
                merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in natural_key])
                
                # Update Type 1 fields across all records
                (delta_table.alias("target")
                 .merge(source_with_hash.alias("source"), merge_condition)
                 .whenMatchedUpdate(set={**type1_updates, "last_updated": "current_timestamp()"})
                 .execute())
                
                # Process Type 2 changes (similar to pure Type 2)
                target_with_hash = self.create_hash_key(target_df, type_2_columns)
                current_records = target_with_hash.filter(col("is_current") == True)
                
                changes = (source_with_hash.alias("source")
                          .join(current_records.alias("target"), 
                               [col(f"source.{key}") == col(f"target.{key}") for key in natural_key], 
                               "inner")
                          .where(col("source.hash_key") != col("target.hash_key"))
                          .select("source.*"))
                
                # Close changed records and insert new versions (Type 2 logic)
                if changes.count() > 0:
                    # Close old records
                    change_keys = changes.select(*natural_key).distinct()
                    
                    (delta_table.alias("target")
                     .merge(change_keys.alias("changes"), 
                           " AND ".join([f"target.{key} = changes.{key}" for key in natural_key]))
                     .whenMatchedUpdate(
                         condition="target.is_current = true",
                         set={
                             "end_date": "current_date()",
                             "is_current": "false",
                             "last_updated": "current_timestamp()"
                         })
                     .execute())
                    
                    # Insert new versions
                    new_versions = (changes
                                   .withColumn("effective_date", lit(self.current_date))
                                   .withColumn("end_date", lit(self.end_date))
                                   .withColumn("is_current", lit(True))
                                   .withColumn("created_date", current_timestamp())
                                   .withColumn("last_updated", current_timestamp())
                                   .drop("hash_key"))
                    
                    new_versions.write.format("delta").mode("append").save(target_path)
                    print(f"✅ Processed {changes.count()} Type 6 changes")
                
                print("✅ SCD Type 6 processing completed")
                
            else:
                # Initial load for Type 6
                initial_data = (source_with_hash
                               .withColumn("effective_date", lit(self.current_date))
                               .withColumn("end_date", lit(self.end_date))
                               .withColumn("is_current", lit(True))
                               .withColumn("created_date", current_timestamp())
                               .withColumn("last_updated", current_timestamp()))
                
                # Add Type 3 columns
                for col_name in type_3_columns:
                    initial_data = (initial_data
                                   .withColumn(f"current_{col_name}", col(col_name))
                                   .withColumn(f"original_{col_name}", col(col_name)))
                
                initial_data = initial_data.drop("hash_key")
                initial_data.write.format("delta").mode("overwrite").save(target_path)
                print(f"✅ Created new Type 6 table with {initial_data.count()} records")
                
        except Exception as e:
            print(f"❌ Error in SCD Type 6 processing: {str(e)}")
            raise

# Example usage
spark = SparkSession.builder \
    .appName("DeltaSCDProcessor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Initialize SCD processor
scd_processor = DeltaSCDProcessor(spark)

# Create sample source data
source_data = [
    (1, "John", "Doe", "456 Oak Ave", "Portland", "OR", "Premium"),
    (2, "Jane", "Smith", "789 Pine St", "Seattle", "WA", "Standard"),
    (3, "Bob", "Johnson", "321 Elm St", "Denver", "CO", "Premium"),
    (4, "Alice", "Brown", "654 Maple Ave", "Phoenix", "AZ", "Standard")
]

source_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("status", StringType(), False)
])

source_df = spark.createDataFrame(source_data, source_schema)

print("📊 Source Data:")
source_df.show()

# Process different SCD types
target_path = "/tmp/delta/customers"

# Example: SCD Type 2
scd_processor.scd_type_2(
    source_df=source_df,
    target_path=target_path + "_type2",
    natural_key=["customer_id"],
    scd_columns=["address", "city", "state", "status"]
)

# Read and display results
result_df = spark.read.format("delta").load(target_path + "_type2")
print("\n📈 SCD Type 2 Results:")
result_df.orderBy("customer_id", "effective_date").show(truncate=False)
```

### Advanced Delta Lake SCD Features

```python
class AdvancedSCDProcessor:
    """
    Advanced SCD processing with Delta Lake features
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def scd_with_merge_schema(self, source_df, target_path, natural_key):
        """
        SCD processing with automatic schema evolution
        """
        print("🔄 Processing SCD with Schema Evolution")
        
        # Enable schema evolution
        source_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(target_path)
    
    def scd_with_time_travel(self, target_path, version=None, timestamp=None):
        """
        Query historical versions using Delta Lake time travel
        """
        print("⏰ Querying Historical Data with Time Travel")
        
        if version is not None:
            df = spark.read.format("delta").option("versionAsOf", version).load(target_path)
            print(f"📊 Data at Version {version}:")
        elif timestamp is not None:
            df = spark.read.format("delta").option("timestampAsOf", timestamp).load(target_path)
            print(f"📊 Data at Timestamp {timestamp}:")
        else:
            df = spark.read.format("delta").load(target_path)
            print("📊 Current Data:")
        
        df.show()
        return df
    
    def scd_with_cdf(self, target_path, starting_version=0):
        """
        Use Change Data Feed to track all changes
        """
        print("📝 Reading Change Data Feed")
        
        changes_df = spark.read \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", starting_version) \
            .load(target_path)
        
        print("📊 Change Data Feed:")
        changes_df.show()
        
        return changes_df
    
    def optimize_scd_table(self, target_path, z_order_columns=None):
        """
        Optimize Delta table for better SCD query performance
        """
        print("⚡ Optimizing Delta Table")
        
        # Compact small files
        spark.sql(f"OPTIMIZE delta.`{target_path}`")
        
        # Z-order for better query performance
        if z_order_columns:
            z_order_cols = ", ".join(z_order_columns)
            spark.sql(f"OPTIMIZE delta.`{target_path}` ZORDER BY ({z_order_cols})")
        
        # Vacuum old files (be careful in production)
        spark.sql(f"VACUUM delta.`{target_path}` RETAIN 168 HOURS")  # 7 days
        
        print("✅ Table optimization completed")

# Example usage of advanced features
advanced_processor = AdvancedSCDProcessor(spark)

# Enable Change Data Feed on table creation
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta.`/tmp/delta/customers_advanced` (
        customer_id INT,
        first_name STRING,
        last_name STRING,
        address STRING,
        city STRING,
        state STRING,
        effective_date DATE,
        end_date DATE,
        is_current BOOLEAN
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# Optimize table for SCD queries
advanced_processor.optimize_scd_table(
    "/tmp/delta/customers_advanced",
    z_order_columns=["customer_id", "effective_date"]
)
```

---

## Hybrid ADF + Databricks Solutions

### End-to-End SCD Pipeline

```json
{
    "name": "Hybrid_SCD_Pipeline",
    "properties": {
        "activities": [
            {
                "name": "Extract_Source_Data",
                "type": "Copy",
                "typeProperties": {
                    "source": {
                        "type": "SqlServerSource",
                        "sqlReaderQuery": "SELECT * FROM Customers WHERE LastModified >= '@{pipeline().parameters.watermark}'"
                    },
                    "sink": {
                        "type": "AzureBlobFSSink",
                        "copyBehavior": "PreserveHierarchy"
                    }
                },
                "inputs": [
                    {
                        "referenceName": "OnPremSqlServerDataset",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "ADLSRawDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "filePath": "raw/customers/@{utcnow('yyyy/MM/dd')}/customers.parquet"
                        }
                    }
                ]
            },
            {
                "name": "Process_SCD_in_Databricks",
                "type": "DatabricksNotebook",
                "dependsOn": [
                    {
                        "activity": "Extract_Source_Data",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "notebookPath": "/Shared/SCD/ProcessSCDType2",
                    "baseParameters": {
                        "source_path": "@{activity('Extract_Source_Data').output.effectiveIntegrationRuntime}",
                        "target_path": "processed/dim_customers",
                        "scd_type": "2",
                        "natural_key": "customer_id",
                        "scd_columns": "address,city,state,status"
                    }
                },
                "linkedServiceName": {
                    "referenceName": "DatabricksLinkedService",
                    "type": "LinkedServiceReference"
                }
            },
            {
                "name": "Data_Quality_Checks",
                "type": "DatabricksNotebook",
                "dependsOn": [
                    {
                        "activity": "Process_SCD_in_Databricks",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "notebookPath": "/Shared/DataQuality/ValidateSCDResults",
                    "baseParameters": {
                        "table_path": "processed/dim_customers",
                        "validation_rules": "no_duplicates,current_flag_check,date_consistency"
                    }
                },
                "linkedServiceName": {
                    "referenceName": "DatabricksLinkedService",
                    "type": "LinkedServiceReference"
                }
            },
            {
                "name": "Load_to_Synapse",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "Data_Quality_Checks",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "source": {
                        "type": "ParquetSource"
                    },
                    "sink": {
                        "type": "SqlDWSink",
                        "allowPolyBase": true,
                        "preCopyScript": "TRUNCATE TABLE staging.dim_customers"
                    }
                },
                "inputs": [
                    {
                        "referenceName": "ProcessedCustomersDataset",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "SynapseCustomersDataset",
                        "type": "DatasetReference"
                    }
                ]
            }
        ],
        "parameters": {
            "watermark": {
                "type": "string",
                "defaultValue": "1900-01-01"
            }
        }
    }
}
```

### Databricks Notebook for Hybrid Processing

```python
# Databricks Notebook: ProcessSCDType2
# Parameters: source_path, target_path, scd_type, natural_key, scd_columns

# Get parameters
dbutils.widgets.text("source_path", "", "Source Path")
dbutils.widgets.text("target_path", "", "Target Path") 
dbutils.widgets.text("scd_type", "2", "SCD Type")
dbutils.widgets.text("natural_key", "", "Natural Key")
dbutils.widgets.text("scd_columns", "", "SCD Columns")

source_path = dbutils.widgets.get("source_path")
target_path = dbutils.widgets.get("target_path")
scd_type = dbutils.widgets.get("scd_type")
natural_key = dbutils.widgets.get("natural_key").split(",")
scd_columns = dbutils.widgets.get("scd_columns").split(",")

print(f"Processing SCD Type {scd_type}")
print(f"Source: {source_path}")
print(f"Target: {target_path}")
print(f"Natural Key: {natural_key}")
print(f"SCD Columns: {scd_columns}")

# Read source data
source_df = spark.read.format("parquet").load(source_path)
print(f"Source records: {source_df.count()}")

# Initialize SCD processor
scd_processor = DeltaSCDProcessor(spark)

# Process based on SCD type
if scd_type == "1":
    scd_processor.scd_type_1(source_df, target_path, natural_key, scd_columns)
elif scd_type == "2":
    scd_processor.scd_type_2(source_df, target_path, natural_key, scd_columns)
elif scd_type == "3":
    scd_processor.scd_type_3(source_df, target_path, natural_key, scd_columns)
elif scd_type == "6":
    # For Type 6, we need to specify which columns are Type 1, 2, 3
    type_1_cols = ["first_name", "last_name"]  # Always update
    type_2_cols = ["address", "city", "state"]  # Track history
    type_3_cols = ["status"]  # Track current + previous
    
    scd_processor.scd_type_6_hybrid(source_df, target_path, natural_key, 
                                   type_1_cols, type_2_cols, type_3_cols)

# Return processing statistics
result_df = spark.read.format("delta").load(target_path)
total_records = result_df.count()
current_records = result_df.filter(col("is_current") == True).count()

print(f"✅ Processing completed")
print(f"Total records: {total_records}")
print(f"Current records: {current_records}")

# Return results to ADF
dbutils.notebook.exit({
    "status": "success",
    "total_records": total_records,
    "current_records": current_records,
    "processing_time": datetime.now().isoformat()
})
```

### Data Quality Validation Notebook

```python
# Databricks Notebook: ValidateSCDResults
# Parameters: table_path, validation_rules

dbutils.widgets.text("table_path", "", "Table Path")
dbutils.widgets.text("validation_rules", "", "Validation Rules")

table_path = dbutils.widgets.get("table_path")
validation_rules = dbutils.widgets.get("validation_rules").split(",")

print(f"Validating table: {table_path}")
print(f"Validation rules: {validation_rules}")

# Read the processed table
df = spark.read.format("delta").load(table_path)

validation_results = {}
errors = []

# Rule 1: No duplicates for current records
if "no_duplicates" in validation_rules:
    current_df = df.filter(col("is_current") == True)
    total_current = current_df.count()
    distinct_current = current_df.select("customer_id").distinct().count()
    
    if total_current == distinct_current:
        validation_results["no_duplicates"] = "PASS"
        print("✅ No duplicates check: PASSED")
    else:
        validation_results["no_duplicates"] = "FAIL"
        errors.append(f"Duplicate current records found: {total_current - distinct_current}")
        print("❌ No duplicates check: FAILED")

# Rule 2: Current flag consistency
if "current_flag_check" in validation_rules:
    # Each natural key should have exactly one current record
    current_counts = (df.filter(col("is_current") == True)
                     .groupBy("customer_id")
                     .count()
                     .filter(col("count") > 1))
    
    if current_counts.count() == 0:
        validation_results["current_flag_check"] = "PASS"
        print("✅ Current flag check: PASSED")
    else:
        validation_results["current_flag_check"] = "FAIL"
        errors.append("Multiple current records found for some customers")
        print("❌ Current flag check: FAILED")

# Rule 3: Date consistency
if "date_consistency" in validation_rules:
    # Effective date should be <= End date
    # End date should be > Effective date (except for 9999-12-31)
    date_issues = (df.filter(
        (col("effective_date") > col("end_date")) & 
        (col("end_date") != "9999-12-31")
    ))
    
    if date_issues.count() == 0:
        validation_results["date_consistency"] = "PASS"
        print("✅ Date consistency check: PASSED")
    else:
        validation_results["date_consistency"] = "FAIL"
        errors.append("Date consistency issues found")
        print("❌ Date consistency check: FAILED")

# Summary
total_rules = len(validation_rules)
passed_rules = sum(1 for result in validation_results.values() if result == "PASS")

print(f"\n📊 Validation Summary:")
print(f"Total rules: {total_rules}")
print(f"Passed: {passed_rules}")
print(f"Failed: {total_rules - passed_rules}")

if errors:
    print(f"\n❌ Errors found:")
    for error in errors:
        print(f"  - {error}")
    
    # Return failure to ADF
    dbutils.notebook.exit({
        "status": "failed",
        "validation_results": validation_results,
        "errors": errors
    })
else:
    print(f"\n✅ All validations passed!")
    
    # Return success to ADF
    dbutils.notebook.exit({
        "status": "success", 
        "validation_results": validation_results,
        "total_records": df.count(),
        "current_records": df.filter(col("is_current") == True).count()
    })
```

---

## Performance Optimization

### Delta Lake Optimization Strategies

```python
class SCDPerformanceOptimizer:
    """
    Performance optimization strategies for SCD processing
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def optimize_for_scd_queries(self, table_path, partition_columns=None, z_order_columns=None):
        """
        Optimize Delta table for SCD query patterns
        """
        print("⚡ Optimizing table for SCD queries")
        
        # 1. Compact small files
        self.spark.sql(f"OPTIMIZE delta.`{table_path}`")
        
        # 2. Z-order for better query performance
        if z_order_columns:
            z_order_cols = ", ".join(z_order_columns)
            self.spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({z_order_cols})")
            print(f"✅ Z-ordered by: {z_order_cols}")
        
        # 3. Analyze table statistics
        self.spark.sql(f"ANALYZE TABLE delta.`{table_path}` COMPUTE STATISTICS")
        
        print("✅ Table optimization completed")
    
    def create_optimized_scd_table(self, table_name, schema, partition_columns=None):
        """
        Create Delta table with optimal settings for SCD
        """
        partition_clause = ""
        if partition_columns:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_columns)})"
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {self._schema_to_sql(schema)}
        ) USING DELTA
        {partition_clause}
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.tuneFileSizesForRewrites' = 'true',
            'delta.targetFileSize' = '134217728'  -- 128MB
        )
        """
        
        self.spark.sql(create_sql)
        print(f"✅ Created optimized table: {table_name}")
    
    def _schema_to_sql(self, schema):
        """Convert StructType schema to SQL DDL"""
        columns = []
        for field in schema.fields:
            col_type = self._spark_type_to_sql(field.dataType)
            nullable = "" if field.nullable else " NOT NULL"
            columns.append(f"{field.name} {col_type}{nullable}")
        return ",\n    ".join(columns)
    
    def _spark_type_to_sql(self, spark_type):
        """Convert Spark data type to SQL type"""
        type_mapping = {
            "IntegerType": "INT",
            "StringType": "STRING", 
            "DateType": "DATE",
            "TimestampType": "TIMESTAMP",
            "BooleanType": "BOOLEAN",
            "DoubleType": "DOUBLE",
            "LongType": "BIGINT"
        }
        return type_mapping.get(type(spark_type).__name__, "STRING")
    
    def monitor_scd_performance(self, table_path, operation_type="SCD_TYPE_2"):
        """
        Monitor SCD operation performance
        """
        print(f"📊 Monitoring {operation_type} performance")
        
        # Get table history
        history_df = self.spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`")
        
        # Analyze recent operations
        recent_operations = history_df.filter(
            col("operation").contains("MERGE") | 
            col("operation").contains("WRITE") |
            col("operation").contains("UPDATE")
        ).orderBy(desc("timestamp")).limit(10)
        
        print("📈 Recent Operations:")
        recent_operations.select("timestamp", "operation", "operationMetrics").show(truncate=False)
        
        # Performance metrics
        metrics = recent_operations.select("operationMetrics").collect()
        for metric in metrics[:3]:  # Show last 3 operations
            if metric.operationMetrics:
                print(f"Operation Metrics: {metric.operationMetrics}")
        
        return recent_operations

# Example usage
optimizer = SCDPerformanceOptimizer(spark)

# Create optimized schema
optimized_schema = StructType([
    StructField("customer_sk", LongType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("effective_date", DateType(), False),
    StructField("end_date", DateType(), False),
    StructField("is_current", BooleanType(), False),
    StructField("created_date", TimestampType(), False)
])

# Create optimized table
optimizer.create_optimized_scd_table(
    "delta.customers_optimized",
    optimized_schema,
    partition_columns=["state"]  # Partition by state for better query performance
)

# Optimize existing table
optimizer.optimize_for_scd_queries(
    "/tmp/delta/customers_type2",
    z_order_columns=["customer_id", "effective_date", "is_current"]
)
```

### ADF Performance Optimization

```json
{
    "name": "Optimized_SCD_DataFlow",
    "properties": {
        "type": "MappingDataFlow",
        "typeProperties": {
            "sources": [
                {
                    "name": "SourceData",
                    "dataset": {
                        "referenceName": "SourceDataset",
                        "type": "DatasetReference"
                    },
                    "options": {
                        "enableStaging": true,
                        "enableSkipIncompatibleRow": false,
                        "samplingRatio": 100
                    }
                }
            ],
            "transformations": [
                {
                    "name": "OptimizedHash",
                    "type": "DerivedColumn",
                    "settings": {
                        "columns": [
                            {
                                "name": "row_hash",
                                "expression": "sha1(concat(coalesce(address,''),coalesce(city,''),coalesce(state,'')))"
                            }
                        ]
                    }
                },
                {
                    "name": "CachedLookup",
                    "type": "Lookup",
                    "settings": {
                        "right": "ExistingDimension",
                        "rightKey": ["customer_id"],
                        "leftKey": ["customer_id"],
                        "multiple": false,
                        "pickup": ["customer_sk", "row_hash", "is_current"],
                        "broadcast": "auto"
                    }
                }
            ],
            "script": "parameters{\n\tbatchSize as integer (1000)\n}\nsource(allowSchemaDrift: true,\n\tvalidateSchema: false,\n\tisolationLevel: 'READ_UNCOMMITTED',\n\tformat: 'table',\n\tbatchSize: $batchSize) ~> SourceData"
        },
        "compute": {
            "coreCount": 16,
            "computeType": "MemoryOptimized"
        },
        "traceLevel": "Fine"
    }
}
```

---

## Real-World Examples

### Example 1: Retail Customer Dimension

```python
def retail_customer_scd_example():
    """
    Real-world example: Retail customer dimension with mixed SCD types
    """
    print("🛍️ Retail Customer SCD Example")
    
    # Create sample retail customer data
    customers_data = [
        (1, "John", "Doe", "john.doe@email.com", "555-1234", "123 Main St", "Seattle", "WA", "98101", "Gold", "2024-01-01"),
        (2, "Jane", "Smith", "jane.smith@email.com", "555-5678", "456 Oak Ave", "Portland", "OR", "97201", "Silver", "2024-01-01"),
        (3, "Bob", "Johnson", "bob.j@email.com", "555-9012", "789 Pine St", "Denver", "CO", "80202", "Bronze", "2024-01-01")
    ]
    
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), True),           # Type 1: Always current
        StructField("phone", StringType(), True),           # Type 1: Always current  
        StructField("address", StringType(), True),         # Type 2: Track history
        StructField("city", StringType(), True),           # Type 2: Track history
        StructField("state", StringType(), True),          # Type 2: Track history
        StructField("zip_code", StringType(), True),       # Type 2: Track history
        StructField("loyalty_tier", StringType(), True),   # Type 3: Track current + previous
        StructField("last_modified", StringType(), True)
    ])
    
    customers_df = spark.createDataFrame(customers_data, schema)
    customers_df = customers_df.withColumn("last_modified", to_timestamp(col("last_modified")))
    
    print("📊 Initial Customer Data:")
    customers_df.show(truncate=False)
    
    # Process initial load
    target_path = "/tmp/delta/retail_customers"
    
    # Create comprehensive SCD processor for retail scenario
    class RetailSCDProcessor:
        def __init__(self, spark_session):
            self.spark = spark_session
            self.scd_processor = DeltaSCDProcessor(spark_session)
        
        def process_retail_customers(self, source_df, target_path):
            """
            Process retail customers with mixed SCD types
            """
            # Define column categories
            type_0_columns = ["customer_id"]  # Never change
            type_1_columns = ["first_name", "last_name", "email", "phone"]  # Always update
            type_2_columns = ["address", "city", "state", "zip_code"]  # Track history
            type_3_columns = ["loyalty_tier"]  # Track current + previous
            
            # Process Type 6 (hybrid approach)
            self.scd_processor.scd_type_6_hybrid(
                source_df=source_df,
                target_path=target_path,
                natural_key=["customer_id"],
                type_1_columns=type_1_columns,
                type_2_columns=type_2_columns,
                type_3_columns=type_3_columns
            )
    
    # Initialize retail processor
    retail_processor = RetailSCDProcessor(spark)
    
    # Initial load
    retail_processor.process_retail_customers(customers_df, target_path)
    
    # Show initial results
    result_df = spark.read.format("delta").load(target_path)
    print("\n📈 Initial Load Results:")
    result_df.show(truncate=False)
    
    # Simulate changes
    print("\n🔄 Simulating Customer Changes...")
    
    # Customer 1: Moves (Type 2 change) and email update (Type 1)
    # Customer 2: Loyalty tier change (Type 3)
    # Customer 3: Phone update (Type 1)
    # Customer 4: New customer (Insert)
    
    changed_customers_data = [
        (1, "John", "Doe", "john.doe.new@email.com", "555-1234", "999 New St", "Portland", "OR", "97205", "Platinum", "2024-02-15"),
        (2, "Jane", "Smith", "jane.smith@email.com", "555-5678", "456 Oak Ave", "Portland", "OR", "97201", "Gold", "2024-02-15"),
        (3, "Bob", "Johnson", "bob.j@email.com", "555-0000", "789 Pine St", "Denver", "CO", "80202", "Bronze", "2024-02-15"),
        (4, "Alice", "Brown", "alice.b@email.com", "555-7777", "111 Elm St", "Phoenix", "AZ", "85001", "Silver", "2024-02-15")
    ]
    
    changed_df = spark.createDataFrame(changed_customers_data, schema)
    changed_df = changed_df.withColumn("last_modified", to_timestamp(col("last_modified")))
    
    print("📊 Changed Customer Data:")
    changed_df.show(truncate=False)
    
    # Process changes
    retail_processor.process_retail_customers(changed_df, target_path)
    
    # Show final results
    final_df = spark.read.format("delta").load(target_path)
    print("\n📈 Final Results (All History):")
    final_df.orderBy("customer_id", "effective_date").show(truncate=False)
    
    # Show only current records
    print("\n📈 Current Records Only:")
    current_df = final_df.filter(col("is_current") == True)
    current_df.orderBy("customer_id").show(truncate=False)
    
    # Show customer 1 history (moved)
    print("\n📈 Customer 1 History (Address Change):")
    customer1_history = final_df.filter(col("customer_id") == 1).orderBy("effective_date")
    customer1_history.show(truncate=False)

# Run retail example
retail_customer_scd_example()
```

### Example 2: Employee Dimension with Organizational Changes

```python
def employee_dimension_scd_example():
    """
    Real-world example: Employee dimension tracking organizational changes
    """
    print("👥 Employee Dimension SCD Example")
    
    # Create employee data with organizational hierarchy
    employee_data = [
        (101, "Alice", "Johnson", "Manager", "Sales", "Seattle", "WA", 75000, 201, "2024-01-01"),
        (102, "Bob", "Smith", "Analyst", "Sales", "Seattle", "WA", 55000, 101, "2024-01-01"),
        (103, "Carol", "Davis", "Director", "Marketing", "Portland", "OR", 95000, None, "2024-01-01"),
        (104, "David", "Wilson", "Specialist", "Marketing", "Portland", "OR", 60000, 103, "2024-01-01")
    ]
    
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("job_title", StringType(), True),      # Type 2: Track promotions
        StructField("department", StringType(), True),     # Type 2: Track transfers
        StructField("office_city", StringType(), True),    # Type 2: Track relocations
        StructField("office_state", StringType(), True),   # Type 2: Track relocations
        StructField("salary", DoubleType(), True),         # Type 4: Separate history table
        StructField("manager_id", IntegerType(), True),    # Type 2: Track reporting changes
        StructField("last_modified", StringType(), True)
    ])
    
    employees_df = spark.createDataFrame(employee_data, employee_schema)
    employees_df = employees_df.withColumn("last_modified", to_timestamp(col("last_modified")))
    
    print("📊 Initial Employee Data:")
    employees_df.show(truncate=False)
    
    # Process with Type 2 SCD for organizational tracking
    target_path = "/tmp/delta/dim_employees"
    
    scd_processor.scd_type_2(
        source_df=employees_df,
        target_path=target_path,
        natural_key=["employee_id"],
        scd_columns=["job_title", "department", "office_city", "office_state", "manager_id"]
    )
    
    # Show initial load
    result_df = spark.read.format("delta").load(target_path)
    print("\n📈 Initial Employee Dimension:")
    result_df.show(truncate=False)
    
    # Simulate organizational changes
    print("\n🔄 Simulating Organizational Changes...")
    
    # Changes:
    # - Alice promoted to Director, moved to Portland
    # - Bob promoted to Senior Analyst, new manager (Carol)
    # - Carol moved to Sales department
    # - New employee added
    
    changed_employee_data = [
        (101, "Alice", "Johnson", "Director", "Sales", "Portland", "OR", 85000, None, "2024-06-01"),
        (102, "Bob", "Smith", "Senior Analyst", "Sales", "Seattle", "WA", 65000, 101, "2024-06-01"),
        (103, "Carol", "Davis", "Director", "Sales", "Seattle", "WA", 100000, None, "2024-06-01"),
        (104, "David", "Wilson", "Specialist", "Marketing", "Portland", "OR", 60000, 105, "2024-06-01"),
        (105, "Eve", "Brown", "Manager", "Marketing", "Portland", "OR", 80000, None, "2024-06-01")
    ]
    
    changed_employees_df = spark.createDataFrame(changed_employee_data, employee_schema)
    changed_employees_df = changed_employees_df.withColumn("last_modified", to_timestamp(col("last_modified")))
    
    # Process changes
    scd_processor.scd_type_2(
        source_df=changed_employees_df,
        target_path=target_path,
        natural_key=["employee_id"],
        scd_columns=["job_title", "department", "office_city", "office_state", "manager_id"]
    )
    
    # Show complete history
    final_df = spark.read.format("delta").load(target_path)
    print("\n📈 Complete Employee History:")
    final_df.orderBy("employee_id", "effective_date").show(truncate=False)
    
    # Analyze organizational changes
    print("\n📊 Organizational Change Analysis:")
    
    # Promotions (job title changes)
    promotions = final_df.filter(col("is_current") == False) \
        .join(final_df.filter(col("is_current") == True), "employee_id") \
        .where(col("job_title") != col("right.job_title")) \
        .select(
            col("employee_id"),
            col("first_name"), 
            col("last_name"),
            col("job_title").alias("old_title"),
            col("right.job_title").alias("new_title"),
            col("effective_date").alias("promotion_date")
        )
    
    print("🎯 Promotions:")
    promotions.show(truncate=False)
    
    # Department transfers
    transfers = final_df.filter(col("is_current") == False) \
        .join(final_df.filter(col("is_current") == True), "employee_id") \
        .where(col("department") != col("right.department")) \
        .select(
            col("employee_id"),
            col("first_name"),
            col("last_name"), 
            col("department").alias("old_department"),
            col("right.department").alias("new_department"),
            col("effective_date").alias("transfer_date")
        )
    
    print("🔄 Department Transfers:")
    transfers.show(truncate=False)

# Run employee example
employee_dimension_scd_example()
```

---

## Monitoring & Best Practices

### Comprehensive Monitoring Framework

```python
class SCDMonitoringFramework:
    """
    Comprehensive monitoring and alerting for SCD processes
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def create_scd_metrics_table(self, metrics_path):
        """
        Create table to store SCD processing metrics
        """
        metrics_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("scd_type", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("source_records", LongType(), True),
            StructField("inserted_records", LongType(), True),
            StructField("updated_records", LongType(), True),
            StructField("deleted_records", LongType(), True),
            StructField("error_message", StringType(), True),
            StructField("processing_time_seconds", DoubleType(), True)
        ])
        
        # Create empty DataFrame and save as Delta table
        empty_df = self.spark.createDataFrame([], metrics_schema)
        empty_df.write.format("delta").mode("overwrite").save(metrics_path)
        
        print(f"✅ Created SCD metrics table at {metrics_path}")
    
    def log_scd_metrics(self, metrics_path, run_id, table_name, scd_type, 
                       start_time, end_time, status, source_records=0,
                       inserted_records=0, updated_records=0, deleted_records=0,
                       error_message=None):
        """
        Log SCD processing metrics
        """
        processing_time = (end_time - start_time).total_seconds() if end_time else None
        
        metrics_data = [(
            run_id, table_name, scd_type, start_time, end_time, status,
            source_records, inserted_records, updated_records, deleted_records,
            error_message, processing_time
        )]
        
        metrics_df = self.spark.createDataFrame(metrics_data, [
            "run_id", "table_name", "scd_type", "start_time", "end_time", "status",
            "source_records", "inserted_records", "updated_records", "deleted_records",
            "error_message", "processing_time_seconds"
        ])
        
        metrics_df.write.format("delta").mode("append").save(metrics_path)
        print(f"✅ Logged metrics for run {run_id}")
    
    def generate_scd_health_report(self, metrics_path, days_back=7):
        """
        Generate SCD health report
        """
        print(f"📊 SCD Health Report (Last {days_back} days)")
        
        metrics_df = self.spark.read.format("delta").load(metrics_path)
        
        # Filter recent runs
        cutoff_date = datetime.now() - timedelta(days=days_back)
        recent_runs = metrics_df.filter(col("start_time") >= lit(cutoff_date))
        
        # Success rate by table
        success_rate = recent_runs.groupBy("table_name", "scd_type").agg(
            count("*").alias("total_runs"),
            sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            avg("processing_time_seconds").alias("avg_processing_time")
        ).withColumn(
            "success_rate_pct",
            round((col("successful_runs") / col("total_runs")) * 100, 2)
        )
        
        print("\n📈 Success Rate by Table:")
        success_rate.show()
        
        # Recent failures
        failures = recent_runs.filter(col("status") != "SUCCESS") \
                             .select("run_id", "table_name", "start_time", "error_message") \
                             .orderBy(desc("start_time"))
        
        print(f"\n❌ Recent Failures:")
        failures.show(truncate=False)
        
        # Processing volume trends
        volume_trends = recent_runs.groupBy(
            date_format(col("start_time"), "yyyy-MM-dd").alias("date"),
            "table_name"
        ).agg(
            sum("source_records").alias("total_source_records"),
            sum("inserted_records").alias("total_inserted"),
            sum("updated_records").alias("total_updated"),
            avg("processing_time_seconds").alias("avg_processing_time")
        ).orderBy("date", "table_name")
        
        print(f"\n📊 Daily Processing Volume:")
        volume_trends.show()
        
        return {
            "success_rate": success_rate,
            "failures": failures,
            "volume_trends": volume_trends
        }
    
    def validate_scd_data_quality(self, table_path, table_name):
        """
        Comprehensive data quality validation for SCD tables
        """
        print(f"🔍 Validating data quality for {table_name}")
        
        df = self.spark.read.format("delta").load(table_path)
        issues = []
        
        # Check 1: Duplicate current records
        current_df = df.filter(col("is_current") == True)
        duplicate_current = current_df.groupBy("customer_id").count().filter(col("count") > 1)
        
        if duplicate_current.count() > 0:
            issues.append(f"Found {duplicate_current.count()} customers with multiple current records")
        
        # Check 2: Orphaned records (no current record)
        all_customers = df.select("customer_id").distinct()
        customers_with_current = current_df.select("customer_id").distinct()
        orphaned = all_customers.subtract(customers_with_current)
        
        if orphaned.count() > 0:
            issues.append(f"Found {orphaned.count()} customers with no current record")
        
        # Check 3: Date consistency
        date_issues = df.filter(
            (col("effective_date") > col("end_date")) & 
            (col("end_date") != "9999-12-31")
        )
        
        if date_issues.count() > 0:
            issues.append(f"Found {date_issues.count()} records with date inconsistencies")
        
        # Check 4: Gap in history
        # This is more complex - checking for gaps in date ranges
        window_spec = Window.partitionBy("customer_id").orderBy("effective_date")
        
        gaps = df.withColumn("prev_end_date", lag("end_date").over(window_spec)) \
                 .filter(
                     (col("prev_end_date").isNotNull()) & 
                     (col("effective_date") != date_add(col("prev_end_date"), 1))
                 )
        
        if gaps.count() > 0:
            issues.append(f"Found {gaps.count()} records with date gaps in history")
        
        # Summary
        if issues:
            print("❌ Data Quality Issues Found:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("✅ All data quality checks passed")
        
        return issues

# Example usage
monitoring = SCDMonitoringFramework(spark)

# Create metrics table
metrics_path = "/tmp/delta/scd_metrics"
monitoring.create_scd_metrics_table(metrics_path)

# Example of logging metrics (would be integrated into SCD processor)
from datetime import datetime
import uuid

run_id = str(uuid.uuid4())
start_time = datetime.now()

# Simulate processing...
import time
time.sleep(2)

end_time = datetime.now()

monitoring.log_scd_metrics(
    metrics_path=metrics_path,
    run_id=run_id,
    table_name="dim_customers",
    scd_type="Type_2",
    start_time=start_time,
    end_time=end_time,
    status="SUCCESS",
    source_records=1000,
    inserted_records=50,
    updated_records=25,
    deleted_records=0
)

# Generate health report
health_report = monitoring.generate_scd_health_report(metrics_path)

# Validate data quality
if spark.catalog.tableExists("delta.`/tmp/delta/customers_type2`"):
    quality_issues = monitoring.validate_scd_data_quality("/tmp/delta/customers_type2", "dim_customers")
```

### Best Practices Checklist

```python
class SCDBestPractices:
    """
    Best practices and recommendations for SCD implementation
    """
    
    @staticmethod
    def get_design_best_practices():
        return {
            "table_design": [
                "Always use surrogate keys for Type 2 dimensions",
                "Include effective_date and end_date columns",
                "Add is_current flag for easy current record identification", 
                "Use consistent naming conventions across all dimensions",
                "Consider partitioning large dimensions by date or region"
            ],
            "data_types": [
                "Use appropriate data types (DATE for dates, not VARCHAR)",
                "Ensure consistent data types between source and target",
                "Handle NULL values explicitly in SCD logic",
                "Use DECIMAL for monetary values, not FLOAT"
            ],
            "indexing": [
                "Create indexes on natural keys for lookup performance",
                "Index effective_date and end_date for temporal queries",
                "Consider composite indexes for multi-column lookups",
                "Z-order Delta tables on frequently queried columns"
            ]
        }
    
    @staticmethod
    def get_performance_best_practices():
        return {
            "processing": [
                "Use incremental processing with watermarks",
                "Process changes in batches, not record-by-record",
                "Leverage change data capture (CDC) when available",
                "Consider parallel processing for independent dimensions"
            ],
            "optimization": [
                "Optimize Delta tables regularly (OPTIMIZE, ZORDER)",
                "Use appropriate cluster sizes in Databricks",
                "Enable auto-optimization features",
                "Monitor and tune Spark configurations"
            ],
            "storage": [
                "Use columnar formats (Parquet, Delta) for analytics",
                "Implement lifecycle management for old data",
                "Consider compression options",
                "Partition large tables appropriately"
            ]
        }
    
    @staticmethod
    def get_operational_best_practices():
        return {
            "monitoring": [
                "Implement comprehensive logging and metrics",
                "Set up alerts for SCD processing failures",
                "Monitor data quality continuously",
                "Track processing times and volumes"
            ],
            "testing": [
                "Test SCD logic with various data scenarios",
                "Validate results after each processing run",
                "Test rollback procedures",
                "Performance test with production-like volumes"
            ],
            "documentation": [
                "Document SCD type decisions for each dimension",
                "Maintain data lineage documentation",
                "Document business rules and exceptions",
                "Keep runbooks for troubleshooting"
            ]
        }
    
    @staticmethod
    def get_security_best_practices():
        return {
            "access_control": [
                "Implement row-level security where needed",
                "Use service principals for automated processes",
                "Follow principle of least privilege",
                "Audit access to sensitive dimensions"
            ],
            "data_privacy": [
                "Implement data masking for sensitive attributes",
                "Consider right-to-be-forgotten requirements",
                "Encrypt sensitive data at rest and in transit",
                "Follow data retention policies"
            ]
        }

# Display best practices
practices = SCDBestPractices()

print("🏆 SCD Implementation Best Practices")
print("="*50)

for category, items in practices.get_design_best_practices().items():
    print(f"\n📋 {category.upper()}:")
    for item in items:
        print(f"  ✅ {item}")

for category, items in practices.get_performance_best_practices().items():
    print(f"\n⚡ {category.upper()}:")
    for item in items:
        print(f"  ✅ {item}")

for category, items in practices.get_operational_best_practices().items():
    print(f"\n🔧 {category.upper()}:")
    for item in items:
        print(f"  ✅ {item}")

for category, items in practices.get_security_best_practices().items():
    print(f"\n🔒 {category.upper()}:")
    for item in items:
        print(f"  ✅ {item}")
```

---

## Conclusion

This comprehensive guide covers all aspects of implementing Slowly Changing Dimensions (SCD) in Azure using Data Factory and Databricks:

### Key Takeaways:

1. **SCD Type Selection**: Choose the right SCD type based on business requirements and query patterns
2. **Azure Integration**: Leverage both ADF and Databricks strengths for optimal solutions
3. **Delta Lake Benefits**: Use Delta Lake features for ACID transactions and time travel
4. **Performance Optimization**: Implement proper indexing, partitioning, and optimization strategies
5. **Monitoring & Quality**: Establish comprehensive monitoring and data quality frameworks

### Implementation Strategy:

1. **Start with Requirements**: Understand business needs for historical data
2. **Choose Architecture**: Design optimal ADF + Databricks integration
3. **Implement Incrementally**: Begin with simple SCD types and add complexity
4. **Monitor Continuously**: Establish metrics and alerting from day one
5. **Optimize Regularly**: Continuously tune performance based on usage patterns

This guide provides production-ready patterns for implementing robust, scalable SCD solutions in Azure.

---

*Generated on: $(date)*
*Azure Slowly Changing Dimensions - Comprehensive Guide*

