# Azure Permissive Types - Comprehensive Guide

## Table of Contents
1. [Overview](#overview)
2. [Databricks Permissive Mode](#databricks-permissive-mode)
3. [Azure Synapse Permissive Parsing](#azure-synapse-permissive-parsing)
4. [Data Factory Permissive Copy](#data-factory-permissive-copy)
5. [Cosmos DB Flexible Schema](#cosmos-db-flexible-schema)
6. [Error Handling Strategies](#error-handling-strategies)
7. [Best Practices](#best-practices)
8. [Real-World Examples](#real-world-examples)
9. [Performance Considerations](#performance-considerations)
10. [Troubleshooting](#troubleshooting)

---

## Overview

Permissive types in Azure services provide flexible data handling capabilities that allow for graceful handling of schema mismatches, data quality issues, and evolving data structures. This approach is crucial for:

- **Data Ingestion**: Handling messy or inconsistent source data
- **Schema Evolution**: Managing changing data structures over time
- **Error Recovery**: Continuing processing despite individual record failures
- **Data Quality**: Capturing and analyzing problematic data

### Key Azure Services Supporting Permissive Types

| Service | Permissive Feature | Use Case |
|---------|-------------------|----------|
| **Azure Databricks** | PERMISSIVE mode, corrupt_record column | Schema inference, malformed data handling |
| **Azure Synapse** | Permissive parsing, error handling | Data warehouse loading with fault tolerance |
| **Data Factory** | Fault tolerance, skip incompatible rows | ETL pipelines with data quality issues |
| **Cosmos DB** | Schema-less design, flexible indexing | Document storage with varying structures |
| **Stream Analytics** | Error policies, late arrival handling | Real-time processing with data anomalies |

---

## Databricks Permissive Mode

### PERMISSIVE Mode Overview

In Azure Databricks, PERMISSIVE mode allows reading data files even when some records don't conform to the expected schema. Malformed records are captured in a special `_corrupt_record` column.

#### Basic PERMISSIVE Mode Usage
```python
# Reading CSV with PERMISSIVE mode
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PermissiveDataProcessing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Define expected schema
expected_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("total_orders", IntegerType(), True),
    StructField("total_spent", DoubleType(), True)
])

# Read CSV with PERMISSIVE mode
df_permissive = spark.read \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(expected_schema) \
    .csv("/mnt/raw_data/customer_data.csv")

# Display schema including corrupt record column
df_permissive.printSchema()
```

### Advanced Permissive Data Processing Class
```python
from typing import Dict, List, Tuple, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime

class PermissiveDataProcessor:
    """
    Advanced class for handling permissive data processing in Azure Databricks
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.processing_stats = {
            "total_records": 0,
            "valid_records": 0,
            "corrupt_records": 0,
            "processing_errors": []
        }
    
    def read_with_permissive_mode(self, 
                                 file_path: str,
                                 expected_schema: StructType,
                                 file_format: str = "csv",
                                 options: Dict[str, str] = None) -> DataFrame:
        """
        Read data with permissive mode and comprehensive error handling
        """
        default_options = {
            "header": "true",
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
            "timestampFormat": "yyyy-MM-dd HH:mm:ss",
            "dateFormat": "yyyy-MM-dd"
        }
        
        if options:
            default_options.update(options)
        
        print(f"📂 Reading {file_format.upper()} file: {file_path}")
        print(f"🔧 Options: {json.dumps(default_options, indent=2)}")
        
        try:
            # Read data based on format
            if file_format.lower() == "csv":
                df = self.spark.read \
                    .options(**default_options) \
                    .schema(expected_schema) \
                    .csv(file_path)
            elif file_format.lower() == "json":
                df = self.spark.read \
                    .option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .schema(expected_schema) \
                    .json(file_path)
            elif file_format.lower() == "parquet":
                # Parquet has built-in schema, but we can still handle evolution
                df = self.spark.read \
                    .option("mergeSchema", "true") \
                    .parquet(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Add processing metadata
            df_with_metadata = df.withColumn("_processing_timestamp", current_timestamp()) \
                                .withColumn("_source_file", lit(file_path))
            
            # Update stats
            total_count = df_with_metadata.count()
            corrupt_count = df_with_metadata.filter(col("_corrupt_record").isNotNull()).count()
            
            self.processing_stats.update({
                "total_records": total_count,
                "valid_records": total_count - corrupt_count,
                "corrupt_records": corrupt_count
            })
            
            print(f"📊 Processing Stats:")
            print(f"  Total Records: {total_count:,}")
            print(f"  Valid Records: {total_count - corrupt_count:,}")
            print(f"  Corrupt Records: {corrupt_count:,}")
            print(f"  Success Rate: {((total_count - corrupt_count) / total_count * 100):.2f}%")
            
            return df_with_metadata
            
        except Exception as e:
            error_msg = f"Failed to read file {file_path}: {str(e)}"
            self.processing_stats["processing_errors"].append(error_msg)
            print(f"❌ {error_msg}")
            raise
    
    def analyze_corrupt_records(self, df: DataFrame) -> DataFrame:
        """
        Analyze corrupt records to understand data quality issues
        """
        print("🔍 Analyzing corrupt records...")
        
        corrupt_df = df.filter(col("_corrupt_record").isNotNull()) \
                      .select("_corrupt_record", "_source_file", "_processing_timestamp")
        
        if corrupt_df.count() == 0:
            print("✅ No corrupt records found!")
            return corrupt_df
        
        # Analyze corrupt record patterns
        corrupt_analysis = corrupt_df.groupBy("_source_file") \
            .agg(
                count("*").alias("corrupt_count"),
                collect_list("_corrupt_record").alias("sample_corrupt_records")
            ) \
            .withColumn("sample_records", slice(col("sample_corrupt_records"), 1, 5))
        
        print("📋 Corrupt Records Analysis:")
        corrupt_analysis.select("_source_file", "corrupt_count", "sample_records").show(truncate=False)
        
        return corrupt_df
    
    def clean_and_validate_data(self, df: DataFrame, validation_rules: Dict[str, Any] = None) -> Tuple[DataFrame, DataFrame]:
        """
        Clean data and separate valid from invalid records based on business rules
        """
        print("🧹 Cleaning and validating data...")
        
        # Default validation rules
        default_rules = {
            "remove_corrupt_records": True,
            "null_checks": [],
            "range_checks": {},
            "format_checks": {},
            "custom_validations": []
        }
        
        if validation_rules:
            default_rules.update(validation_rules)
        
        # Start with base dataframe
        clean_df = df
        invalid_records = []
        
        # Remove corrupt records if specified
        if default_rules.get("remove_corrupt_records", True):
            invalid_corrupt = clean_df.filter(col("_corrupt_record").isNotNull())
            clean_df = clean_df.filter(col("_corrupt_record").isNull())
            
            if invalid_corrupt.count() > 0:
                invalid_records.append(
                    invalid_corrupt.withColumn("validation_error", lit("corrupt_record"))
                )
        
        # Apply null checks
        for column_name in default_rules.get("null_checks", []):
            if column_name in clean_df.columns:
                invalid_nulls = clean_df.filter(col(column_name).isNull())
                clean_df = clean_df.filter(col(column_name).isNotNull())
                
                if invalid_nulls.count() > 0:
                    invalid_records.append(
                        invalid_nulls.withColumn("validation_error", lit(f"null_value_in_{column_name}"))
                    )
        
        # Apply range checks
        for column_name, range_config in default_rules.get("range_checks", {}).items():
            if column_name in clean_df.columns:
                min_val = range_config.get("min")
                max_val = range_config.get("max")
                
                range_condition = lit(True)
                if min_val is not None:
                    range_condition = range_condition & (col(column_name) >= min_val)
                if max_val is not None:
                    range_condition = range_condition & (col(column_name) <= max_val)
                
                invalid_range = clean_df.filter(~range_condition)
                clean_df = clean_df.filter(range_condition)
                
                if invalid_range.count() > 0:
                    invalid_records.append(
                        invalid_range.withColumn("validation_error", lit(f"range_violation_{column_name}"))
                    )
        
        # Apply format checks (regex patterns)
        for column_name, pattern in default_rules.get("format_checks", {}).items():
            if column_name in clean_df.columns:
                format_condition = col(column_name).rlike(pattern)
                invalid_format = clean_df.filter(~format_condition)
                clean_df = clean_df.filter(format_condition)
                
                if invalid_format.count() > 0:
                    invalid_records.append(
                        invalid_format.withColumn("validation_error", lit(f"format_violation_{column_name}"))
                    )
        
        # Combine all invalid records
        if invalid_records:
            invalid_df = invalid_records[0]
            for invalid_batch in invalid_records[1:]:
                invalid_df = invalid_df.union(invalid_batch)
        else:
            # Create empty DataFrame with same schema plus validation_error column
            invalid_schema = df.schema.add(StructField("validation_error", StringType(), True))
            invalid_df = self.spark.createDataFrame([], invalid_schema)
        
        clean_count = clean_df.count()
        invalid_count = invalid_df.count()
        total_count = clean_count + invalid_count
        
        print(f"📊 Validation Results:")
        print(f"  Clean Records: {clean_count:,} ({(clean_count/total_count*100):.2f}%)")
        print(f"  Invalid Records: {invalid_count:,} ({(invalid_count/total_count*100):.2f}%)")
        
        return clean_df, invalid_df
    
    def handle_schema_evolution(self, df: DataFrame, target_schema: StructType) -> DataFrame:
        """
        Handle schema evolution by adding missing columns and casting types
        """
        print("🔄 Handling schema evolution...")
        
        current_columns = set(df.columns)
        target_columns = set([field.name for field in target_schema.fields])
        
        # Add missing columns with null values
        missing_columns = target_columns - current_columns
        for col_name in missing_columns:
            target_field = next(field for field in target_schema.fields if field.name == col_name)
            df = df.withColumn(col_name, lit(None).cast(target_field.dataType))
            print(f"➕ Added missing column: {col_name} ({target_field.dataType})")
        
        # Remove extra columns
        extra_columns = current_columns - target_columns - {"_corrupt_record", "_processing_timestamp", "_source_file"}
        for col_name in extra_columns:
            df = df.drop(col_name)
            print(f"➖ Removed extra column: {col_name}")
        
        # Cast columns to target types
        for field in target_schema.fields:
            if field.name in df.columns:
                current_type = dict(df.dtypes)[field.name]
                if current_type != str(field.dataType):
                    df = df.withColumn(field.name, col(field.name).cast(field.dataType))
                    print(f"🔄 Cast {field.name}: {current_type} → {field.dataType}")
        
        return df
    
    def save_with_quality_metrics(self, 
                                 clean_df: DataFrame, 
                                 invalid_df: DataFrame,
                                 output_path: str,
                                 quality_path: str) -> Dict[str, Any]:
        """
        Save clean data and generate quality metrics
        """
        print(f"💾 Saving clean data to: {output_path}")
        print(f"📊 Saving quality metrics to: {quality_path}")
        
        # Save clean data
        clean_df.write \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .parquet(output_path)
        
        # Generate and save quality metrics
        quality_metrics = {
            "processing_timestamp": datetime.now().isoformat(),
            "total_records_processed": self.processing_stats["total_records"],
            "clean_records": clean_df.count(),
            "invalid_records": invalid_df.count(),
            "data_quality_score": (clean_df.count() / self.processing_stats["total_records"]) * 100,
            "validation_errors": {}
        }
        
        # Analyze validation errors
        if invalid_df.count() > 0:
            error_summary = invalid_df.groupBy("validation_error") \
                                    .count() \
                                    .collect()
            
            for row in error_summary:
                quality_metrics["validation_errors"][row["validation_error"]] = row["count"]
        
        # Save quality metrics
        quality_df = self.spark.createDataFrame([quality_metrics])
        quality_df.coalesce(1).write \
            .mode("overwrite") \
            .json(quality_path)
        
        print(f"✅ Data quality score: {quality_metrics['data_quality_score']:.2f}%")
        
        return quality_metrics

# Example usage
processor = PermissiveDataProcessor(spark)

# Define expected schema
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("total_orders", IntegerType(), True),
    StructField("total_spent", DoubleType(), True),
    StructField("status", StringType(), True)
])

# Read data with permissive mode
df = processor.read_with_permissive_mode(
    file_path="/mnt/raw_data/customer_data.csv",
    expected_schema=customer_schema,
    options={"delimiter": ",", "quote": '"'}
)

# Analyze corrupt records
corrupt_analysis = processor.analyze_corrupt_records(df)

# Define validation rules
validation_rules = {
    "remove_corrupt_records": True,
    "null_checks": ["customer_id", "customer_name"],
    "range_checks": {
        "total_orders": {"min": 0, "max": 10000},
        "total_spent": {"min": 0.0, "max": 1000000.0}
    },
    "format_checks": {
        "email": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        "phone": r'^\+?[\d\s\-\(\)]{10,}$'
    }
}

# Clean and validate data
clean_df, invalid_df = processor.clean_and_validate_data(df, validation_rules)

# Handle schema evolution
target_schema = customer_schema.add(StructField("customer_segment", StringType(), True))
evolved_df = processor.handle_schema_evolution(clean_df, target_schema)

# Save results with quality metrics
quality_metrics = processor.save_with_quality_metrics(
    clean_df=evolved_df,
    invalid_df=invalid_df,
    output_path="/mnt/processed_data/customers_clean",
    quality_path="/mnt/quality_metrics/customers_quality"
)
```

### JSON Data Permissive Processing
```python
# Handling JSON data with varying schemas
def process_json_with_permissive_mode():
    """
    Process JSON data with permissive mode for schema flexibility
    """
    
    # Base schema for JSON data
    json_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True),
        StructField("metadata", StructType([
            StructField("source", StringType(), True),
            StructField("version", StringType(), True)
        ]), True)
    ])
    
    # Read JSON with permissive mode
    json_df = spark.read \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(json_schema) \
        .json("/mnt/events/user_events/*.json")
    
    print("📋 JSON Schema Analysis:")
    json_df.printSchema()
    
    # Handle nested structure variations
    processed_df = json_df.withColumn(
        "event_properties",
        when(col("properties").isNotNull(), col("properties"))
        .otherwise(create_map())
    ).withColumn(
        "source_system",
        when(col("metadata.source").isNotNull(), col("metadata.source"))
        .otherwise(lit("unknown"))
    ).withColumn(
        "schema_version",
        when(col("metadata.version").isNotNull(), col("metadata.version"))
        .otherwise(lit("1.0"))
    )
    
    # Separate valid events from corrupt records
    valid_events = processed_df.filter(col("_corrupt_record").isNull())
    corrupt_events = processed_df.filter(col("_corrupt_record").isNotNull())
    
    print(f"📊 JSON Processing Results:")
    print(f"  Valid Events: {valid_events.count():,}")
    print(f"  Corrupt Events: {corrupt_events.count():,}")
    
    # Analyze corrupt JSON patterns
    if corrupt_events.count() > 0:
        print("🔍 Sample Corrupt JSON Records:")
        corrupt_events.select("_corrupt_record").show(5, truncate=False)
    
    return valid_events, corrupt_events

# Execute JSON processing
valid_json, corrupt_json = process_json_with_permissive_mode()
```

---

## Azure Synapse Permissive Parsing

### Synapse SQL Pool Permissive Loading

#### COPY Command with Error Handling
```sql
-- Create table with permissive structure
CREATE TABLE staging.customer_data_permissive (
    customer_id INT,
    customer_name NVARCHAR(255),
    email NVARCHAR(255),
    phone NVARCHAR(50),
    registration_date DATE,
    total_orders INT,
    total_spent DECIMAL(10,2),
    status NVARCHAR(50),
    _error_message NVARCHAR(4000),
    _source_file NVARCHAR(500),
    _load_timestamp DATETIME2 DEFAULT GETDATE()
);

-- COPY command with error handling
COPY INTO staging.customer_data_permissive
FROM 'https://yourstorage.blob.core.windows.net/data/customer_data.csv'
WITH (
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = 'your-key'),
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    ERRORFILE = 'https://yourstorage.blob.core.windows.net/errors/',
    MAXERRORS = 1000,
    REJECT_TYPE = 'percentage',
    REJECT_VALUE = 10.0,
    REJECT_SAMPLE_VALUE = 1000
);

-- Query to check load results
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN _error_message IS NULL THEN 1 END) as successful_rows,
    COUNT(CASE WHEN _error_message IS NOT NULL THEN 1 END) as error_rows,
    (COUNT(CASE WHEN _error_message IS NULL THEN 1 END) * 100.0 / COUNT(*)) as success_rate
FROM staging.customer_data_permissive;
```

#### Advanced Synapse Permissive Processing
```sql
-- Stored procedure for permissive data processing
CREATE PROCEDURE staging.ProcessPermissiveData
    @SourcePath NVARCHAR(500),
    @ErrorThreshold DECIMAL(5,2) = 5.0,
    @MaxErrors INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorCount INT = 0;
    DECLARE @TotalCount INT = 0;
    DECLARE @ErrorRate DECIMAL(5,2);
    DECLARE @LoadId UNIQUEIDENTIFIER = NEWID();
    
    -- Create load tracking entry
    INSERT INTO staging.load_tracking (
        load_id, 
        source_path, 
        start_time, 
        status
    ) VALUES (
        @LoadId, 
        @SourcePath, 
        GETDATE(), 
        'IN_PROGRESS'
    );
    
    BEGIN TRY
        -- Truncate staging table
        TRUNCATE TABLE staging.customer_data_permissive;
        
        -- Dynamic COPY command
        DECLARE @SQL NVARCHAR(MAX) = N'
        COPY INTO staging.customer_data_permissive
        FROM ''' + @SourcePath + '''
        WITH (
            FILE_TYPE = ''CSV'',
            CREDENTIAL = (IDENTITY = ''Storage Account Key'', SECRET = ''your-key''),
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            FIRSTROW = 2,
            ERRORFILE = ''https://yourstorage.blob.core.windows.net/errors/'',
            MAXERRORS = ' + CAST(@MaxErrors AS NVARCHAR(10)) + ',
            REJECT_TYPE = ''percentage'',
            REJECT_VALUE = ' + CAST(@ErrorThreshold AS NVARCHAR(10)) + ',
            REJECT_SAMPLE_VALUE = 1000
        );';
        
        EXEC sp_executesql @SQL;
        
        -- Calculate error statistics
        SELECT 
            @TotalCount = COUNT(*),
            @ErrorCount = COUNT(CASE WHEN _error_message IS NOT NULL THEN 1 END)
        FROM staging.customer_data_permissive;
        
        SET @ErrorRate = CASE 
            WHEN @TotalCount > 0 THEN (@ErrorCount * 100.0 / @TotalCount)
            ELSE 0.0
        END;
        
        -- Update load tracking
        UPDATE staging.load_tracking 
        SET 
            end_time = GETDATE(),
            total_records = @TotalCount,
            error_records = @ErrorCount,
            error_rate = @ErrorRate,
            status = CASE 
                WHEN @ErrorRate <= @ErrorThreshold THEN 'COMPLETED'
                ELSE 'COMPLETED_WITH_ERRORS'
            END
        WHERE load_id = @LoadId;
        
        -- Log results
        PRINT 'Load completed successfully';
        PRINT 'Total Records: ' + CAST(@TotalCount AS NVARCHAR(10));
        PRINT 'Error Records: ' + CAST(@ErrorCount AS NVARCHAR(10));
        PRINT 'Error Rate: ' + CAST(@ErrorRate AS NVARCHAR(10)) + '%';
        
        -- Process valid records
        EXEC staging.ProcessValidRecords @LoadId;
        
    END TRY
    BEGIN CATCH
        -- Handle errors
        UPDATE staging.load_tracking 
        SET 
            end_time = GETDATE(),
            status = 'FAILED',
            error_message = ERROR_MESSAGE()
        WHERE load_id = @LoadId;
        
        THROW;
    END CATCH
END;

-- Create supporting tables
CREATE TABLE staging.load_tracking (
    load_id UNIQUEIDENTIFIER PRIMARY KEY,
    source_path NVARCHAR(500),
    start_time DATETIME2,
    end_time DATETIME2,
    total_records INT,
    error_records INT,
    error_rate DECIMAL(5,2),
    status NVARCHAR(50),
    error_message NVARCHAR(4000)
);

-- Procedure to process valid records
CREATE PROCEDURE staging.ProcessValidRecords
    @LoadId UNIQUEIDENTIFIER
AS
BEGIN
    -- Data cleansing and validation
    WITH CleanedData AS (
        SELECT 
            customer_id,
            LTRIM(RTRIM(customer_name)) as customer_name,
            LOWER(LTRIM(RTRIM(email))) as email,
            LTRIM(RTRIM(phone)) as phone,
            registration_date,
            COALESCE(total_orders, 0) as total_orders,
            COALESCE(total_spent, 0.0) as total_spent,
            UPPER(LTRIM(RTRIM(status))) as status,
            _load_timestamp
        FROM staging.customer_data_permissive
        WHERE _error_message IS NULL
    ),
    ValidatedData AS (
        SELECT *,
            CASE 
                WHEN customer_id IS NULL THEN 'Missing customer ID'
                WHEN customer_name IS NULL OR LEN(customer_name) = 0 THEN 'Missing customer name'
                WHEN email IS NULL OR email NOT LIKE '%@%.%' THEN 'Invalid email format'
                WHEN total_orders < 0 THEN 'Negative order count'
                WHEN total_spent < 0 THEN 'Negative spent amount'
                ELSE NULL
            END as validation_error
        FROM CleanedData
    )
    -- Insert valid records into target table
    INSERT INTO dbo.customers (
        customer_id, customer_name, email, phone, 
        registration_date, total_orders, total_spent, status,
        load_id, created_date
    )
    SELECT 
        customer_id, customer_name, email, phone,
        registration_date, total_orders, total_spent, status,
        @LoadId, GETDATE()
    FROM ValidatedData
    WHERE validation_error IS NULL;
    
    -- Log validation results
    DECLARE @ValidCount INT, @InvalidCount INT;
    
    SELECT @ValidCount = COUNT(*) FROM ValidatedData WHERE validation_error IS NULL;
    SELECT @InvalidCount = COUNT(*) FROM ValidatedData WHERE validation_error IS NOT NULL;
    
    PRINT 'Valid records processed: ' + CAST(@ValidCount AS NVARCHAR(10));
    PRINT 'Invalid records found: ' + CAST(@InvalidCount AS NVARCHAR(10));
    
    -- Store invalid records for analysis
    INSERT INTO staging.validation_errors (
        load_id, customer_id, customer_name, email, 
        validation_error, created_date
    )
    SELECT 
        @LoadId, customer_id, customer_name, email,
        validation_error, GETDATE()
    FROM ValidatedData
    WHERE validation_error IS NOT NULL;
END;
```

### Synapse Serverless SQL Pool with Permissive Parsing
```sql
-- External table with permissive parsing
CREATE EXTERNAL TABLE staging.customer_data_external (
    customer_id INT,
    customer_name NVARCHAR(255),
    email NVARCHAR(255),
    phone NVARCHAR(50),
    registration_date DATE,
    total_orders INT,
    total_spent DECIMAL(10,2),
    status NVARCHAR(50)
)
WITH (
    LOCATION = 'customer_data/',
    DATA_SOURCE = external_data_source,
    FILE_FORMAT = csv_file_format,
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 1000
);

-- Query with error handling using TRY_PARSE and TRY_CONVERT
SELECT 
    TRY_PARSE(customer_id_raw AS INT) as customer_id,
    LTRIM(RTRIM(customer_name_raw)) as customer_name,
    CASE 
        WHEN customer_name_raw LIKE '%@%.%' 
        THEN LOWER(LTRIM(RTRIM(email_raw)))
        ELSE NULL 
    END as email,
    TRY_PARSE(registration_date_raw AS DATE) as registration_date,
    TRY_PARSE(total_orders_raw AS INT) as total_orders,
    TRY_PARSE(total_spent_raw AS DECIMAL(10,2)) as total_spent,
    UPPER(LTRIM(RTRIM(status_raw))) as status,
    -- Add data quality indicators
    CASE 
        WHEN TRY_PARSE(customer_id_raw AS INT) IS NULL THEN 'Invalid customer_id'
        WHEN LTRIM(RTRIM(customer_name_raw)) = '' THEN 'Empty customer_name'
        WHEN email_raw NOT LIKE '%@%.%' THEN 'Invalid email format'
        WHEN TRY_PARSE(registration_date_raw AS DATE) IS NULL THEN 'Invalid date format'
        ELSE 'Valid'
    END as data_quality_status
FROM OPENROWSET(
    BULK 'customer_data.csv',
    DATA_SOURCE = 'external_data_source',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
) WITH (
    customer_id_raw NVARCHAR(50),
    customer_name_raw NVARCHAR(255),
    email_raw NVARCHAR(255),
    phone_raw NVARCHAR(50),
    registration_date_raw NVARCHAR(50),
    total_orders_raw NVARCHAR(50),
    total_spent_raw NVARCHAR(50),
    status_raw NVARCHAR(50)
) as raw_data;
```

---

## Data Factory Permissive Copy

### ADF Copy Activity with Fault Tolerance

#### Fault Tolerant Copy Activity JSON
```json
{
    "name": "PermissiveCopyActivity",
    "type": "Copy",
    "typeProperties": {
        "source": {
            "type": "DelimitedTextSource",
            "storeSettings": {
                "type": "AzureBlobFSReadSettings",
                "recursive": true,
                "wildcardFileName": "*.csv"
            },
            "formatSettings": {
                "type": "DelimitedTextReadSettings",
                "skipLineCount": 0,
                "compressionProperties": {
                    "type": "ZipDeflateReadSettings"
                }
            }
        },
        "sink": {
            "type": "AzureSqlSink",
            "preCopyScript": "TRUNCATE TABLE staging.customer_data_permissive",
            "writeBehavior": "insert",
            "sqlWriterUseTableLock": false,
            "tableOption": "autoCreate",
            "disableMetricsCollection": false
        },
        "enableStaging": false,
        "enableSkipIncompatibleRow": true,
        "redirectIncompatibleRowSettings": {
            "linkedServiceName": {
                "referenceName": "AzureBlobStorage_ErrorLogs",
                "type": "LinkedServiceReference"
            },
            "path": "error-logs/copy-errors"
        },
        "logSettings": {
            "enableCopyActivityLog": true,
            "copyActivityLogSettings": {
                "logLevel": "Warning",
                "enableReliableLogging": false
            },
            "logLocationSettings": {
                "linkedServiceName": {
                    "referenceName": "AzureBlobStorage_Logs",
                    "type": "LinkedServiceReference"
                },
                "path": "activity-logs/copy-logs"
            }
        },
        "translator": {
            "type": "TabularTranslator",
            "mappings": [
                {
                    "source": {
                        "name": "customer_id",
                        "type": "String",
                        "physicalType": "String"
                    },
                    "sink": {
                        "name": "customer_id",
                        "type": "Int32",
                        "physicalType": "int"
                    }
                },
                {
                    "source": {
                        "name": "customer_name",
                        "type": "String",
                        "physicalType": "String"
                    },
                    "sink": {
                        "name": "customer_name",
                        "type": "String",
                        "physicalType": "nvarchar"
                    }
                },
                {
                    "source": {
                        "name": "email",
                        "type": "String",
                        "physicalType": "String"
                    },
                    "sink": {
                        "name": "email",
                        "type": "String",
                        "physicalType": "nvarchar"
                    }
                },
                {
                    "source": {
                        "name": "registration_date",
                        "type": "String",
                        "physicalType": "String"
                    },
                    "sink": {
                        "name": "registration_date",
                        "type": "DateTime",
                        "physicalType": "date"
                    }
                }
            ],
            "typeConversion": true,
            "typeConversionSettings": {
                "allowDataTruncation": true,
                "treatBooleanAsNumber": false,
                "dateTimeFormat": "",
                "dateTimeOffsetFormat": "",
                "timeSpanFormat": "",
                "culture": ""
            }
        }
    },
    "inputs": [
        {
            "referenceName": "SourceCSVDataset",
            "type": "DatasetReference"
        }
    ],
    "outputs": [
        {
            "referenceName": "SinkSQLDataset",
            "type": "DatasetReference"
        }
    ]
}
```

### Advanced ADF Permissive Pipeline
```json
{
    "name": "AdvancedPermissiveDataPipeline",
    "properties": {
        "parameters": {
            "sourceContainer": {
                "type": "string",
                "defaultValue": "raw-data"
            },
            "errorThreshold": {
                "type": "int",
                "defaultValue": 100
            },
            "enableDataProfiling": {
                "type": "bool",
                "defaultValue": true
            }
        },
        "variables": {
            "errorCount": {
                "type": "Integer",
                "defaultValue": 0
            },
            "totalRecords": {
                "type": "Integer",
                "defaultValue": 0
            },
            "processingStatus": {
                "type": "String",
                "defaultValue": "STARTED"
            }
        },
        "activities": [
            {
                "name": "GetSourceFileMetadata",
                "type": "GetMetadata",
                "typeProperties": {
                    "dataset": {
                        "referenceName": "SourceDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "containerName": {
                                "value": "@pipeline().parameters.sourceContainer",
                                "type": "Expression"
                            }
                        }
                    },
                    "fieldList": ["childItems", "size", "lastModified"]
                }
            },
            {
                "name": "ProcessEachFile",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "GetSourceFileMetadata",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "items": {
                        "value": "@activity('GetSourceFileMetadata').output.childItems",
                        "type": "Expression"
                    },
                    "isSequential": false,
                    "batchCount": 5,
                    "activities": [
                        {
                            "name": "CopyWithFaultTolerance",
                            "type": "Copy",
                            "typeProperties": {
                                "source": {
                                    "type": "DelimitedTextSource",
                                    "storeSettings": {
                                        "type": "AzureBlobFSReadSettings",
                                        "recursive": false
                                    }
                                },
                                "sink": {
                                    "type": "AzureSqlSink",
                                    "writeBehavior": "insert",
                                    "sqlWriterUseTableLock": false
                                },
                                "enableSkipIncompatibleRow": true,
                                "redirectIncompatibleRowSettings": {
                                    "linkedServiceName": {
                                        "referenceName": "ErrorLogStorage",
                                        "type": "LinkedServiceReference"
                                    },
                                    "path": {
                                        "value": "@concat('errors/', formatDateTime(utcnow(), 'yyyy-MM-dd'), '/', item().name)",
                                        "type": "Expression"
                                    }
                                },
                                "logSettings": {
                                    "enableCopyActivityLog": true,
                                    "copyActivityLogSettings": {
                                        "logLevel": "Warning"
                                    },
                                    "logLocationSettings": {
                                        "linkedServiceName": {
                                            "referenceName": "ActivityLogStorage",
                                            "type": "LinkedServiceReference"
                                        },
                                        "path": {
                                            "value": "@concat('logs/', formatDateTime(utcnow(), 'yyyy-MM-dd'), '/', item().name)",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            },
                            "inputs": [
                                {
                                    "referenceName": "IndividualFileDataset",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "fileName": {
                                            "value": "@item().name",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            ],
                            "outputs": [
                                {
                                    "referenceName": "StagingTableDataset",
                                    "type": "DatasetReference"
                                }
                            ]
                        },
                        {
                            "name": "LogCopyResults",
                            "type": "WebActivity",
                            "dependsOn": [
                                {
                                    "activity": "CopyWithFaultTolerance",
                                    "dependencyConditions": ["Succeeded", "Failed"]
                                }
                            ],
                            "typeProperties": {
                                "url": "https://logging-api.azurewebsites.net/api/log",
                                "method": "POST",
                                "headers": {
                                    "Content-Type": "application/json"
                                },
                                "body": {
                                    "value": "@concat('{\"fileName\":\"', item().name, '\",\"copyDuration\":', activity('CopyWithFaultTolerance').output.copyDuration, ',\"rowsCopied\":', activity('CopyWithFaultTolerance').output.rowsCopied, ',\"rowsSkipped\":', activity('CopyWithFaultTolerance').output.rowsSkipped, ',\"status\":\"', activity('CopyWithFaultTolerance').output.executionDetails[0].status, '\"}')",
                                    "type": "Expression"
                                }
                            }
                        }
                    ]
                }
            },
            {
                "name": "ValidateDataQuality",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [
                    {
                        "activity": "ProcessEachFile",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "storedProcedureName": "staging.ValidateDataQuality",
                    "storedProcedureParameters": {
                        "ErrorThreshold": {
                            "value": {
                                "value": "@pipeline().parameters.errorThreshold",
                                "type": "Expression"
                            },
                            "type": "Int32"
                        },
                        "PipelineRunId": {
                            "value": {
                                "value": "@pipeline().RunId",
                                "type": "Expression"
                            },
                            "type": "String"
                        }
                    }
                }
            },
            {
                "name": "CheckQualityResults",
                "type": "IfCondition",
                "dependsOn": [
                    {
                        "activity": "ValidateDataQuality",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "expression": {
                        "value": "@greater(activity('ValidateDataQuality').output.firstRow.ErrorCount, pipeline().parameters.errorThreshold)",
                        "type": "Expression"
                    },
                    "ifTrueActivities": [
                        {
                            "name": "SendQualityAlert",
                            "type": "WebActivity",
                            "typeProperties": {
                                "url": "https://alert-service.azurewebsites.net/api/alert",
                                "method": "POST",
                                "body": {
                                    "value": "@concat('{\"alertType\":\"DATA_QUALITY\",\"pipelineRunId\":\"', pipeline().RunId, '\",\"errorCount\":', activity('ValidateDataQuality').output.firstRow.ErrorCount, ',\"threshold\":', pipeline().parameters.errorThreshold, '}')",
                                    "type": "Expression"
                                }
                            }
                        }
                    ],
                    "ifFalseActivities": [
                        {
                            "name": "PromoteToProduction",
                            "type": "SqlServerStoredProcedure",
                            "typeProperties": {
                                "storedProcedureName": "staging.PromoteToProduction",
                                "storedProcedureParameters": {
                                    "PipelineRunId": {
                                        "value": {
                                            "value": "@pipeline().RunId",
                                            "type": "Expression"
                                        },
                                        "type": "String"
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        ]
    }
}
```

---

## Cosmos DB Flexible Schema

### Cosmos DB Permissive Document Handling

#### Python SDK for Permissive Operations
```python
import json
from azure.cosmos import CosmosClient, exceptions
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

class CosmosPermissiveProcessor:
    """
    Handle permissive document operations in Azure Cosmos DB
    """
    
    def __init__(self, endpoint: str, key: str, database_name: str, container_name: str):
        self.client = CosmosClient(endpoint, key)
        self.database = self.client.get_database_client(database_name)
        self.container = self.database.get_container_client(container_name)
        self.processing_stats = {
            "total_processed": 0,
            "successful_inserts": 0,
            "failed_inserts": 0,
            "schema_variations": {}
        }
    
    def insert_with_schema_flexibility(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Insert documents with flexible schema handling
        """
        results = {
            "successful": [],
            "failed": [],
            "schema_analysis": {}
        }
        
        for doc in documents:
            try:
                # Add metadata for tracking
                doc["_inserted_at"] = datetime.utcnow().isoformat()
                doc["_schema_version"] = self._analyze_schema(doc)
                
                # Validate and clean document
                cleaned_doc = self._clean_document(doc)
                
                # Insert document
                response = self.container.create_item(body=cleaned_doc)
                results["successful"].append({
                    "id": response["id"],
                    "schema_version": doc["_schema_version"]
                })
                self.processing_stats["successful_inserts"] += 1
                
            except exceptions.CosmosHttpResponseError as e:
                error_info = {
                    "document_id": doc.get("id", "unknown"),
                    "error_code": e.status_code,
                    "error_message": str(e),
                    "document_schema": self._get_document_schema(doc)
                }
                results["failed"].append(error_info)
                self.processing_stats["failed_inserts"] += 1
                logging.error(f"Failed to insert document: {error_info}")
            
            except Exception as e:
                error_info = {
                    "document_id": doc.get("id", "unknown"),
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                }
                results["failed"].append(error_info)
                self.processing_stats["failed_inserts"] += 1
                logging.error(f"Unexpected error: {error_info}")
        
        self.processing_stats["total_processed"] += len(documents)
        
        # Analyze schema variations
        results["schema_analysis"] = self._analyze_schema_variations(documents)
        
        return results
    
    def _analyze_schema(self, document: Dict[str, Any]) -> str:
        """
        Analyze document schema and assign version
        """
        # Create schema fingerprint based on field names and types
        schema_info = {}
        for key, value in document.items():
            if not key.startswith("_"):  # Skip metadata fields
                schema_info[key] = type(value).__name__
        
        # Create schema hash
        schema_str = json.dumps(schema_info, sort_keys=True)
        schema_hash = str(hash(schema_str))
        
        # Track schema variations
        if schema_hash not in self.processing_stats["schema_variations"]:
            self.processing_stats["schema_variations"][schema_hash] = {
                "count": 0,
                "fields": schema_info,
                "first_seen": datetime.utcnow().isoformat()
            }
        
        self.processing_stats["schema_variations"][schema_hash]["count"] += 1
        
        return schema_hash
    
    def _clean_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and normalize document for consistent storage
        """
        cleaned = {}
        
        for key, value in document.items():
            # Handle null values
            if value is None:
                continue  # Skip null values to save space
            
            # Handle empty strings
            if isinstance(value, str) and value.strip() == "":
                continue  # Skip empty strings
            
            # Handle nested objects
            if isinstance(value, dict):
                cleaned_nested = self._clean_document(value)
                if cleaned_nested:  # Only add if not empty
                    cleaned[key] = cleaned_nested
            
            # Handle arrays
            elif isinstance(value, list):
                cleaned_array = [
                    self._clean_document(item) if isinstance(item, dict) else item
                    for item in value if item is not None
                ]
                if cleaned_array:  # Only add if not empty
                    cleaned[key] = cleaned_array
            
            # Handle primitive values
            else:
                cleaned[key] = value
        
        return cleaned
    
    def _get_document_schema(self, document: Dict[str, Any]) -> Dict[str, str]:
        """
        Get simplified schema representation
        """
        schema = {}
        for key, value in document.items():
            if isinstance(value, dict):
                schema[key] = "object"
            elif isinstance(value, list):
                schema[key] = "array"
            else:
                schema[key] = type(value).__name__
        
        return schema
    
    def _analyze_schema_variations(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze schema variations across documents
        """
        all_fields = set()
        field_types = {}
        field_frequency = {}
        
        for doc in documents:
            for key, value in doc.items():
                if not key.startswith("_"):
                    all_fields.add(key)
                    
                    # Track field frequency
                    field_frequency[key] = field_frequency.get(key, 0) + 1
                    
                    # Track field types
                    field_type = type(value).__name__
                    if key not in field_types:
                        field_types[key] = set()
                    field_types[key].add(field_type)
        
        return {
            "total_unique_fields": len(all_fields),
            "field_frequency": field_frequency,
            "field_types": {k: list(v) for k, v in field_types.items()},
            "schema_consistency": {
                field: (freq / len(documents)) * 100
                for field, freq in field_frequency.items()
            }
        }
    
    def query_with_flexible_schema(self, base_query: str, optional_fields: List[str] = None) -> List[Dict[str, Any]]:
        """
        Query documents with flexible schema handling
        """
        try:
            # Build flexible query
            if optional_fields:
                # Create conditional field selection
                select_clause = "c.id, c._ts"
                for field in optional_fields:
                    select_clause += f", (IS_DEFINED(c.{field}) ? c.{field} : null) as {field}"
                
                flexible_query = f"SELECT {select_clause} FROM c WHERE {base_query}"
            else:
                flexible_query = f"SELECT * FROM c WHERE {base_query}"
            
            # Execute query
            items = list(self.container.query_items(
                query=flexible_query,
                enable_cross_partition_query=True
            ))
            
            return items
            
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"Query failed: {e}")
            return []
    
    def migrate_schema(self, migration_rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Migrate documents to new schema format
        """
        migration_results = {
            "processed": 0,
            "migrated": 0,
            "failed": 0,
            "errors": []
        }
        
        # Query all documents
        all_docs = list(self.container.read_all_items())
        
        for doc in all_docs:
            migration_results["processed"] += 1
            
            try:
                # Apply migration rules
                migrated_doc = self._apply_migration_rules(doc, migration_rules)
                
                if migrated_doc != doc:  # Only update if changed
                    self.container.replace_item(item=doc["id"], body=migrated_doc)
                    migration_results["migrated"] += 1
                
            except Exception as e:
                migration_results["failed"] += 1
                migration_results["errors"].append({
                    "document_id": doc.get("id", "unknown"),
                    "error": str(e)
                })
        
        return migration_results
    
    def _apply_migration_rules(self, document: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply migration rules to document
        """
        migrated = document.copy()
        
        # Field renaming
        for old_name, new_name in rules.get("rename_fields", {}).items():
            if old_name in migrated:
                migrated[new_name] = migrated.pop(old_name)
        
        # Field type conversion
        for field_name, target_type in rules.get("convert_types", {}).items():
            if field_name in migrated:
                try:
                    if target_type == "int":
                        migrated[field_name] = int(migrated[field_name])
                    elif target_type == "float":
                        migrated[field_name] = float(migrated[field_name])
                    elif target_type == "str":
                        migrated[field_name] = str(migrated[field_name])
                    elif target_type == "bool":
                        migrated[field_name] = bool(migrated[field_name])
                except (ValueError, TypeError):
                    pass  # Keep original value if conversion fails
        
        # Add default fields
        for field_name, default_value in rules.get("add_defaults", {}).items():
            if field_name not in migrated:
                migrated[field_name] = default_value
        
        # Remove deprecated fields
        for field_name in rules.get("remove_fields", []):
            migrated.pop(field_name, None)
        
        # Add migration metadata
        migrated["_migrated_at"] = datetime.utcnow().isoformat()
        migrated["_migration_version"] = rules.get("version", "1.0")
        
        return migrated
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive processing statistics
        """
        return {
            **self.processing_stats,
            "success_rate": (
                (self.processing_stats["successful_inserts"] / 
                 max(self.processing_stats["total_processed"], 1)) * 100
            ),
            "schema_diversity": len(self.processing_stats["schema_variations"])
        }

# Example usage
cosmos_processor = CosmosPermissiveProcessor(
    endpoint="https://your-cosmos-account.documents.azure.com:443/",
    key="your-primary-key",
    database_name="flexible_db",
    container_name="documents"
)

# Sample documents with varying schemas
sample_documents = [
    {
        "id": "customer_001",
        "type": "customer",
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30,
        "preferences": {
            "newsletter": True,
            "marketing": False
        }
    },
    {
        "id": "customer_002",
        "type": "customer",
        "name": "Jane Smith",
        "email": "jane@example.com",
        "phone": "+1-555-0123",
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "zip": "12345"
        }
    },
    {
        "id": "order_001",
        "type": "order",
        "customer_id": "customer_001",
        "total": 99.99,
        "items": [
            {"product": "Widget", "quantity": 2, "price": 49.99}
        ],
        "order_date": "2024-01-15"
    }
]

# Insert documents with schema flexibility
results = cosmos_processor.insert_with_schema_flexibility(sample_documents)

print("📊 Insertion Results:")
print(f"  Successful: {len(results['successful'])}")
print(f"  Failed: {len(results['failed'])}")
print(f"  Schema Variations: {len(results['schema_analysis']['field_types'])}")

# Query with flexible schema
flexible_results = cosmos_processor.query_with_flexible_schema(
    base_query="c.type = 'customer'",
    optional_fields=["name", "email", "phone", "age", "address"]
)

print(f"\n🔍 Flexible Query Results: {len(flexible_results)} documents")

# Schema migration example
migration_rules = {
    "version": "2.0",
    "rename_fields": {
        "customer_id": "customerId"
    },
    "convert_types": {
        "age": "int",
        "total": "float"
    },
    "add_defaults": {
        "status": "active",
        "created_at": datetime.utcnow().isoformat()
    },
    "remove_fields": ["deprecated_field"]
}

migration_results = cosmos_processor.migrate_schema(migration_rules)
print(f"\n🔄 Migration Results:")
print(f"  Processed: {migration_results['processed']}")
print(f"  Migrated: {migration_results['migrated']}")
print(f"  Failed: {migration_results['failed']}")

# Get processing statistics
stats = cosmos_processor.get_processing_stats()
print(f"\n📈 Processing Statistics:")
print(f"  Success Rate: {stats['success_rate']:.2f}%")
print(f"  Schema Diversity: {stats['schema_diversity']} variations")
```

---

## Error Handling Strategies

### Comprehensive Error Handling Framework
```python
from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Callable
import logging
from datetime import datetime

class ErrorSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class ErrorCategory(Enum):
    SCHEMA_MISMATCH = "schema_mismatch"
    DATA_TYPE_ERROR = "data_type_error"
    VALIDATION_ERROR = "validation_error"
    CONSTRAINT_VIOLATION = "constraint_violation"
    CORRUPT_DATA = "corrupt_data"
    SYSTEM_ERROR = "system_error"

@dataclass
class PermissiveError:
    error_id: str
    category: ErrorCategory
    severity: ErrorSeverity
    message: str
    source_system: str
    record_id: Optional[str] = None
    field_name: Optional[str] = None
    original_value: Optional[Any] = None
    suggested_fix: Optional[str] = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()

class PermissiveErrorHandler:
    """
    Comprehensive error handling for permissive data processing
    """
    
    def __init__(self):
        self.errors: List[PermissiveError] = []
        self.error_handlers: Dict[ErrorCategory, Callable] = {
            ErrorCategory.SCHEMA_MISMATCH: self._handle_schema_mismatch,
            ErrorCategory.DATA_TYPE_ERROR: self._handle_data_type_error,
            ErrorCategory.VALIDATION_ERROR: self._handle_validation_error,
            ErrorCategory.CONSTRAINT_VIOLATION: self._handle_constraint_violation,
            ErrorCategory.CORRUPT_DATA: self._handle_corrupt_data,
            ErrorCategory.SYSTEM_ERROR: self._handle_system_error
        }
        self.recovery_strategies = {}
    
    def register_error(self, error: PermissiveError) -> str:
        """
        Register an error and return error ID
        """
        if not error.error_id:
            error.error_id = f"ERR_{len(self.errors):06d}_{int(datetime.utcnow().timestamp())}"
        
        self.errors.append(error)
        
        # Log error based on severity
        log_message = f"[{error.category.value}] {error.message}"
        if error.severity == ErrorSeverity.CRITICAL:
            logging.critical(log_message)
        elif error.severity == ErrorSeverity.HIGH:
            logging.error(log_message)
        elif error.severity == ErrorSeverity.MEDIUM:
            logging.warning(log_message)
        else:
            logging.info(log_message)
        
        return error.error_id
    
    def handle_error(self, error: PermissiveError) -> Dict[str, Any]:
        """
        Handle error based on category and return recovery action
        """
        handler = self.error_handlers.get(error.category)
        if handler:
            return handler(error)
        else:
            return {"action": "log_only", "continue_processing": True}
    
    def _handle_schema_mismatch(self, error: PermissiveError) -> Dict[str, Any]:
        """
        Handle schema mismatch errors
        """
        recovery_actions = {
            "action": "schema_adaptation",
            "continue_processing": True,
            "recovery_steps": [
                "Add missing field with default value",
                "Cast incompatible types where possible",
                "Store original value in metadata field"
            ],
            "suggested_fix": "Update schema definition or implement field mapping"
        }
        
        if error.severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            recovery_actions["notify_admin"] = True
        
        return recovery_actions
    
    def _handle_data_type_error(self, error: PermissiveError) -> Dict[str, Any]:
        """
        Handle data type conversion errors
        """
        return {
            "action": "type_coercion",
            "continue_processing": True,
            "recovery_steps": [
                "Attempt type conversion with fallback",
                "Store original value as string",
                "Flag record for manual review"
            ],
            "fallback_value": self._get_fallback_value(error.field_name)
        }
    
    def _handle_validation_error(self, error: PermissiveError) -> Dict[str, Any]:
        """
        Handle business rule validation errors
        """
        return {
            "action": "validation_bypass",
            "continue_processing": True,
            "recovery_steps": [
                "Apply default value if available",
                "Mark record as requiring validation",
                "Continue with warning flag"
            ],
            "requires_review": True
        }
    
    def _handle_constraint_violation(self, error: PermissiveError) -> Dict[str, Any]:
        """
        Handle database constraint violations
        """
        if error.severity == ErrorSeverity.CRITICAL:
            return {
                "action": "reject_record",
                "continue_processing": True,
                "recovery_steps": [
                    "Log constraint violation",
                    "Store record in error table",
                    "Continue with next record"
                ]
            }
        else:
            return {
                "action": "constraint_relaxation",
                "continue_processing": True,
                "recovery_steps": [
                    "Modify value to meet constraint",
                    "Add validation flag",
                    "Continue processing"
                ]
            }
    
    def _handle_corrupt_data(self, error: PermissiveError) -> Dict[str, Any]:
        """
        Handle corrupt or malformed data
        """
        return {
            "action": "data_quarantine",
            "continue_processing": True,
            "recovery_steps": [
                "Move record to quarantine table",
                "Attempt data reconstruction",
                "Flag for manual intervention"
            ],
            "quarantine_reason": error.message
        }
    
    def _handle_system_error(self, error: PermissiveError) -> Dict[str, Any]:
        """
        Handle system-level errors
        """
        if error.severity == ErrorSeverity.CRITICAL:
            return {
                "action": "halt_processing",
                "continue_processing": False,
                "recovery_steps": [
                    "Log system error",
                    "Notify administrators",
                    "Initiate error recovery procedure"
                ],
                "requires_immediate_attention": True
            }
        else:
            return {
                "action": "retry_with_backoff",
                "continue_processing": True,
                "recovery_steps": [
                    "Wait and retry operation",
                    "Log retry attempt",
                    "Continue if retry succeeds"
                ],
                "max_retries": 3
            }
    
    def _get_fallback_value(self, field_name: str) -> Any:
        """
        Get appropriate fallback value based on field name
        """
        fallback_map = {
            "id": "UNKNOWN",
            "name": "N/A",
            "email": "",
            "phone": "",
            "age": 0,
            "date": None,
            "amount": 0.0,
            "count": 0,
            "status": "PENDING"
        }
        
        # Try exact match first
        if field_name in fallback_map:
            return fallback_map[field_name]
        
        # Try pattern matching
        field_lower = field_name.lower()
        if "id" in field_lower:
            return "UNKNOWN"
        elif "name" in field_lower:
            return "N/A"
        elif "email" in field_lower:
            return ""
        elif "date" in field_lower or "time" in field_lower:
            return None
        elif "amount" in field_lower or "price" in field_lower:
            return 0.0
        elif "count" in field_lower or "quantity" in field_lower:
            return 0
        else:
            return None
    
    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive error summary
        """
        if not self.errors:
            return {"total_errors": 0}
        
        summary = {
            "total_errors": len(self.errors),
            "by_category": {},
            "by_severity": {},
            "by_source": {},
            "recent_errors": [],
            "error_rate_by_hour": {}
        }
        
        for error in self.errors:
            # Count by category
            category = error.category.value
            summary["by_category"][category] = summary["by_category"].get(category, 0) + 1
            
            # Count by severity
            severity = error.severity.value
            summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + 1
            
            # Count by source
            source = error.source_system
            summary["by_source"][source] = summary["by_source"].get(source, 0) + 1
        
        # Get recent errors (last 10)
        summary["recent_errors"] = [
            {
                "error_id": error.error_id,
                "category": error.category.value,
                "severity": error.severity.value,
                "message": error.message,
                "timestamp": error.timestamp
            }
            for error in sorted(self.errors, key=lambda x: x.timestamp, reverse=True)[:10]
        ]
        
        return summary
    
    def generate_error_report(self, output_format: str = "json") -> str:
        """
        Generate comprehensive error report
        """
        summary = self.get_error_summary()
        
        if output_format.lower() == "json":
            return json.dumps(summary, indent=2, default=str)
        
        elif output_format.lower() == "html":
            html_report = f"""
            <html>
            <head><title>Permissive Processing Error Report</title></head>
            <body>
                <h1>Error Report - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}</h1>
                <h2>Summary</h2>
                <p>Total Errors: {summary['total_errors']}</p>
                
                <h3>By Category</h3>
                <ul>
                {''.join([f'<li>{cat}: {count}</li>' for cat, count in summary['by_category'].items()])}
                </ul>
                
                <h3>By Severity</h3>
                <ul>
                {''.join([f'<li>{sev}: {count}</li>' for sev, count in summary['by_severity'].items()])}
                </ul>
                
                <h3>Recent Errors</h3>
                <table border="1">
                    <tr><th>Error ID</th><th>Category</th><th>Severity</th><th>Message</th><th>Timestamp</th></tr>
                    {''.join([
                        f'<tr><td>{err["error_id"]}</td><td>{err["category"]}</td><td>{err["severity"]}</td><td>{err["message"]}</td><td>{err["timestamp"]}</td></tr>'
                        for err in summary['recent_errors']
                    ])}
                </table>
            </body>
            </html>
            """
            return html_report
        
        else:  # Plain text
            text_report = f"""
PERMISSIVE PROCESSING ERROR REPORT
Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY
=======
Total Errors: {summary['total_errors']}

BY CATEGORY
===========
{chr(10).join([f'{cat}: {count}' for cat, count in summary['by_category'].items()])}

BY SEVERITY
===========
{chr(10).join([f'{sev}: {count}' for sev, count in summary['by_severity'].items()])}

RECENT ERRORS
=============
{chr(10).join([f'[{err["timestamp"]}] {err["severity"].upper()} - {err["category"]}: {err["message"]}' for err in summary['recent_errors']])}
            """
            return text_report

# Example usage of error handling framework
error_handler = PermissiveErrorHandler()

# Register various types of errors
schema_error = PermissiveError(
    error_id="",
    category=ErrorCategory.SCHEMA_MISMATCH,
    severity=ErrorSeverity.MEDIUM,
    message="Column 'new_field' not found in target schema",
    source_system="customer_data_csv",
    field_name="new_field",
    suggested_fix="Add column mapping or update target schema"
)

data_type_error = PermissiveError(
    error_id="",
    category=ErrorCategory.DATA_TYPE_ERROR,
    severity=ErrorSeverity.LOW,
    message="Cannot convert 'abc' to integer",
    source_system="sales_data_json",
    record_id="record_123",
    field_name="quantity",
    original_value="abc"
)

validation_error = PermissiveError(
    error_id="",
    category=ErrorCategory.VALIDATION_ERROR,
    severity=ErrorSeverity.HIGH,
    message="Email format validation failed",
    source_system="user_registration",
    record_id="user_456",
    field_name="email",
    original_value="invalid-email"
)

# Register errors
error_handler.register_error(schema_error)
error_handler.register_error(data_type_error)
error_handler.register_error(validation_error)

# Handle errors and get recovery actions
for error in error_handler.errors:
    recovery_action = error_handler.handle_error(error)
    print(f"Error {error.error_id}: {recovery_action['action']}")

# Generate error report
error_report = error_handler.generate_error_report("json")
print("\n📊 Error Report:")
print(error_report)
```

---

## Best Practices

### Permissive Processing Guidelines

#### 1. Schema Design Principles
```python
class PermissiveSchemaDesign:
    """
    Best practices for designing permissive schemas
    """
    
    @staticmethod
    def design_flexible_schema() -> Dict[str, Any]:
        """
        Guidelines for flexible schema design
        """
        return {
            "core_principles": {
                "nullable_fields": "Make most fields nullable to handle missing data",
                "default_values": "Provide sensible defaults for required fields",
                "type_flexibility": "Use string types for fields with varying formats",
                "metadata_fields": "Include fields for data quality tracking"
            },
            "recommended_patterns": {
                "version_field": {
                    "field_name": "_schema_version",
                    "type": "string",
                    "purpose": "Track schema evolution"
                },
                "source_tracking": {
                    "field_name": "_source_system",
                    "type": "string", 
                    "purpose": "Identify data origin"
                },
                "quality_indicators": {
                    "field_name": "_data_quality_score",
                    "type": "float",
                    "purpose": "Track record quality"
                },
                "processing_metadata": {
                    "field_name": "_processed_at",
                    "type": "timestamp",
                    "purpose": "Track processing time"
                }
            },
            "anti_patterns": {
                "strict_constraints": "Avoid overly strict constraints that reject valid variations",
                "hardcoded_enums": "Don't use fixed enums for evolving categorical data",
                "required_fields": "Minimize required fields to handle incomplete data",
                "fixed_schemas": "Avoid schemas that can't accommodate new fields"
            }
        }
    
    @staticmethod
    def schema_evolution_strategy() -> Dict[str, Any]:
        """
        Strategy for managing schema evolution
        """
        return {
            "versioning_approach": {
                "semantic_versioning": "Use major.minor.patch for schema versions",
                "backward_compatibility": "Maintain compatibility for at least 2 major versions",
                "deprecation_policy": "Provide 6-month notice for field deprecation"
            },
            "migration_strategies": {
                "additive_changes": {
                    "description": "Add new optional fields",
                    "impact": "Low - existing data remains valid",
                    "implementation": "Default values for new fields"
                },
                "field_renaming": {
                    "description": "Rename existing fields",
                    "impact": "Medium - requires mapping logic",
                    "implementation": "Maintain both old and new names temporarily"
                },
                "type_changes": {
                    "description": "Change field data types",
                    "impact": "High - may require data conversion",
                    "implementation": "Gradual migration with validation"
                },
                "field_removal": {
                    "description": "Remove deprecated fields",
                    "impact": "High - breaking change",
                    "implementation": "Multi-phase deprecation process"
                }
            }
        }

# Display best practices
schema_design = PermissiveSchemaDesign()
flexible_schema = schema_design.design_flexible_schema()
evolution_strategy = schema_design.schema_evolution_strategy()

print("📋 Flexible Schema Design Principles:")
for principle, description in flexible_schema["core_principles"].items():
    print(f"  • {principle}: {description}")

print("\n🔄 Schema Evolution Strategy:")
for strategy, details in evolution_strategy["migration_strategies"].items():
    print(f"  • {strategy}: {details['description']} (Impact: {details['impact']})")
```

#### 2. Data Quality Monitoring
```python
class PermissiveDataQualityMonitor:
    """
    Monitor data quality in permissive processing environments
    """
    
    def __init__(self):
        self.quality_metrics = {}
        self.quality_rules = {}
        self.thresholds = {
            "completeness": 0.95,
            "validity": 0.98,
            "consistency": 0.90,
            "accuracy": 0.95
        }
    
    def define_quality_rules(self) -> Dict[str, Any]:
        """
        Define comprehensive data quality rules
        """
        return {
            "completeness_rules": {
                "required_fields": ["id", "created_at"],
                "critical_fields": ["customer_id", "transaction_id"],
                "optional_fields": ["notes", "comments"],
                "completeness_threshold": 0.95
            },
            "validity_rules": {
                "email_format": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                "phone_format": r'^\+?[\d\s\-\(\)]{10,}$',
                "date_format": "%Y-%m-%d",
                "numeric_ranges": {
                    "age": {"min": 0, "max": 150},
                    "amount": {"min": 0, "max": 1000000}
                }
            },
            "consistency_rules": {
                "cross_field_validation": {
                    "start_date_before_end_date": "start_date < end_date",
                    "total_equals_sum": "total_amount == sum(line_items.amount)"
                },
                "reference_integrity": {
                    "customer_exists": "customer_id in customer_table",
                    "product_exists": "product_id in product_table"
                }
            },
            "accuracy_rules": {
                "business_logic": {
                    "discount_percentage": "0 <= discount_percentage <= 100",
                    "order_status_flow": "valid status transitions"
                },
                "duplicate_detection": {
                    "customer_duplicates": ["email", "phone"],
                    "transaction_duplicates": ["transaction_id", "timestamp"]
                }
            }
        }
    
    def calculate_quality_score(self, data_sample: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate comprehensive data quality score
        """
        quality_scores = {}
        
        # Completeness score
        total_fields = len(data_sample)
        non_null_fields = len([v for v in data_sample.values() if v is not None and v != ""])
        quality_scores["completeness"] = (non_null_fields / total_fields) * 100 if total_fields > 0 else 0
        
        # Validity score (simplified)
        valid_fields = 0
        validated_fields = 0
        
        for field, value in data_sample.items():
            if value is not None:
                validated_fields += 1
                # Simple validation examples
                if field == "email" and "@" in str(value):
                    valid_fields += 1
                elif field == "age" and isinstance(value, (int, float)) and 0 <= value <= 150:
                    valid_fields += 1
                elif field not in ["email", "age"]:  # Assume other fields are valid
                    valid_fields += 1
        
        quality_scores["validity"] = (valid_fields / validated_fields * 100) if validated_fields > 0 else 0
        
        # Overall quality score
        quality_scores["overall"] = (
            quality_scores["completeness"] * 0.4 +
            quality_scores["validity"] * 0.6
        )
        
        return quality_scores
    
    def generate_quality_report(self, processing_results: Dict[str, Any]) -> str:
        """
        Generate comprehensive quality report
        """
        report = f"""
DATA QUALITY REPORT
==================
Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}

PROCESSING SUMMARY
-----------------
Total Records Processed: {processing_results.get('total_records', 0):,}
Successfully Processed: {processing_results.get('successful_records', 0):,}
Failed Records: {processing_results.get('failed_records', 0):,}
Success Rate: {processing_results.get('success_rate', 0):.2f}%

QUALITY METRICS
--------------
Completeness Score: {processing_results.get('completeness_score', 0):.2f}%
Validity Score: {processing_results.get('validity_score', 0):.2f}%
Overall Quality Score: {processing_results.get('overall_quality_score', 0):.2f}%

QUALITY THRESHOLDS
-----------------
{'✅ PASSED' if processing_results.get('completeness_score', 0) >= self.thresholds['completeness'] * 100 else '❌ FAILED'} Completeness: {processing_results.get('completeness_score', 0):.2f}% (Threshold: {self.thresholds['completeness'] * 100}%)
{'✅ PASSED' if processing_results.get('validity_score', 0) >= self.thresholds['validity'] * 100 else '❌ FAILED'} Validity: {processing_results.get('validity_score', 0):.2f}% (Threshold: {self.thresholds['validity'] * 100}%)

ERROR ANALYSIS
--------------
Schema Mismatches: {processing_results.get('schema_errors', 0)}
Type Conversion Errors: {processing_results.get('type_errors', 0)}
Validation Failures: {processing_results.get('validation_errors', 0)}
Corrupt Records: {processing_results.get('corrupt_records', 0)}

RECOMMENDATIONS
--------------
"""
        
        # Add recommendations based on quality scores
        if processing_results.get('completeness_score', 0) < self.thresholds['completeness'] * 100:
            report += "• Improve data completeness by addressing missing required fields\n"
        
        if processing_results.get('validity_score', 0) < self.thresholds['validity'] * 100:
            report += "• Enhance data validation at source systems\n"
        
        if processing_results.get('schema_errors', 0) > 0:
            report += "• Review and update schema definitions\n"
        
        if processing_results.get('corrupt_records', 0) > 0:
            report += "• Investigate and fix data corruption issues\n"
        
        return report

# Example usage
quality_monitor = PermissiveDataQualityMonitor()

# Sample processing results
sample_results = {
    "total_records": 10000,
    "successful_records": 9500,
    "failed_records": 500,
    "success_rate": 95.0,
    "completeness_score": 92.5,
    "validity_score": 96.8,
    "overall_quality_score": 95.2,
    "schema_errors": 50,
    "type_errors": 200,
    "validation_errors": 150,
    "corrupt_records": 100
}

# Generate quality report
quality_report = quality_monitor.generate_quality_report(sample_results)
print(quality_report)
```

---

## Conclusion

This comprehensive guide covers all aspects of permissive types in Azure services:

### Key Takeaways:

1. **Databricks PERMISSIVE Mode**: Handle schema mismatches and corrupt records gracefully
2. **Azure Synapse Permissive Parsing**: Load data with fault tolerance and error handling
3. **Data Factory Permissive Copy**: Skip incompatible rows and redirect errors
4. **Cosmos DB Flexible Schema**: Handle varying document structures dynamically
5. **Error Handling Strategies**: Implement comprehensive error recovery mechanisms

### Recommended Implementation Approach:

1. **Start with Schema Flexibility**: Design schemas that can accommodate variations
2. **Implement Error Handling**: Use comprehensive error handling and recovery strategies
3. **Monitor Data Quality**: Track quality metrics and identify improvement areas
4. **Plan for Evolution**: Design systems that can adapt to changing data structures
5. **Document Patterns**: Maintain clear documentation of permissive processing patterns

This guide provides production-ready patterns and code examples for implementing permissive data processing across Azure services, ensuring robust and flexible data pipelines.

---

*Generated on: $(date)*
*Azure Permissive Types - Comprehensive Guide*

