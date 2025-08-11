# Azure Audit Log Tables in Delta Lake - Comprehensive Guide
## Complete Implementation of Audit Logging with Delta Lake in Azure

---

### Table of Contents

1. [Introduction to Azure Audit Logging](#introduction-to-azure-audit-logging)
2. [Delta Lake for Audit Logs](#delta-lake-for-audit-logs)
3. [Audit Log Table Design](#audit-log-table-design)
4. [Creating Audit Log Tables](#creating-audit-log-tables)
5. [Data Ingestion Patterns](#data-ingestion-patterns)
6. [Query Patterns and Analytics](#query-patterns-and-analytics)
7. [Compliance and Retention](#compliance-and-retention)
8. [Performance Optimization](#performance-optimization)
9. [Security and Access Control](#security-and-access-control)
10. [Monitoring and Alerting](#monitoring-and-alerting)
11. [Integration with Azure Services](#integration-with-azure-services)
12. [Best Practices](#best-practices)
13. [Troubleshooting](#troubleshooting)
14. [Advanced Patterns](#advanced-patterns)
15. [Conclusion](#conclusion)

---

## Introduction to Azure Audit Logging

Azure audit logging is crucial for compliance, security monitoring, and operational insights. Delta Lake provides an ideal foundation for audit log storage due to its ACID transactions, time travel capabilities, and efficient querying features.

### Key Benefits of Delta Lake for Audit Logs

```
Delta Lake Audit Log Benefits:
├── ACID Transactions
│   ├── Guaranteed data integrity
│   ├── Atomic writes for batch operations
│   └── Consistent reads during concurrent operations
├── Time Travel
│   ├── Historical audit trail access
│   ├── Point-in-time compliance reporting
│   └── Data recovery capabilities
├── Schema Evolution
│   ├── Adapt to changing audit requirements
│   ├── Add new fields without breaking existing queries
│   └── Maintain backward compatibility
├── Efficient Querying
│   ├── Columnar storage for analytics
│   ├── Data skipping for fast filtering
│   └── Z-ordering for optimal performance
└── Retention Management
    ├── Automated data lifecycle management
    ├── Compliance-driven retention policies
    └── Efficient storage optimization
```

### Audit Log Categories

```
Azure Audit Log Types:
├── Activity Logs
│   ├── Resource management operations
│   ├── Administrative actions
│   └── Service health events
├── Security Logs
│   ├── Authentication events
│   ├── Authorization changes
│   └── Security policy modifications
├── Application Logs
│   ├── Custom application events
│   ├── Business process auditing
│   └── User interaction tracking
├── Data Access Logs
│   ├── Data read/write operations
│   ├── Schema modifications
│   └── Data export/import events
└── System Logs
    ├── Infrastructure changes
    ├── Configuration updates
    └── Performance metrics
```

---

## Delta Lake for Audit Logs

### Setting Up Delta Lake Environment

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
from datetime import datetime, timedelta
import uuid

# Initialize Spark with Delta Lake
spark = SparkSession.builder \
    .appName("AzureAuditLogDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true") \
    .getOrCreate()

# Azure Data Lake Storage configuration
storage_account = "yourstorageaccount"
container = "audit-logs"
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

print(f"Delta Lake environment initialized")
print(f"Base path: {base_path}")
```

### Audit Log Data Model

```python
# Comprehensive audit log schema
audit_log_schema = StructType([
    # Core identification fields
    StructField("audit_id", StringType(), False),
    StructField("event_id", StringType(), False),
    StructField("correlation_id", StringType(), True),
    StructField("session_id", StringType(), True),
    
    # Temporal fields
    StructField("event_timestamp", TimestampType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("event_date", DateType(), False),  # Partition key
    StructField("event_hour", IntegerType(), False),  # Sub-partition
    
    # Event classification
    StructField("event_type", StringType(), False),
    StructField("event_category", StringType(), False),
    StructField("event_subcategory", StringType(), True),
    StructField("severity_level", StringType(), False),
    StructField("risk_score", IntegerType(), True),
    
    # Actor information
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_email", StringType(), True),
    StructField("user_roles", ArrayType(StringType()), True),
    StructField("service_principal_id", StringType(), True),
    StructField("application_id", StringType(), True),
    
    # Resource information
    StructField("resource_id", StringType(), True),
    StructField("resource_name", StringType(), True),
    StructField("resource_type", StringType(), True),
    StructField("resource_group", StringType(), True),
    StructField("subscription_id", StringType(), True),
    StructField("tenant_id", StringType(), True),
    
    # Location and network
    StructField("location", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("geo_location", StructType([
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]), True),
    
    # Operation details
    StructField("operation_name", StringType(), False),
    StructField("operation_type", StringType(), False),
    StructField("operation_status", StringType(), False),
    StructField("status_code", IntegerType(), True),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    
    # Data and payload
    StructField("request_payload", StringType(), True),
    StructField("response_payload", StringType(), True),
    StructField("data_classification", StringType(), True),
    StructField("sensitive_data_detected", BooleanType(), True),
    
    # Additional metadata
    StructField("source_system", StringType(), False),
    StructField("log_version", StringType(), False),
    StructField("custom_properties", MapType(StringType(), StringType()), True),
    StructField("tags", ArrayType(StringType()), True),
    
    # Compliance fields
    StructField("retention_category", StringType(), False),
    StructField("retention_period_days", IntegerType(), False),
    StructField("compliance_flags", ArrayType(StringType()), True),
    StructField("data_subject_id", StringType(), True),  # GDPR compliance
])

print("Audit log schema defined with", len(audit_log_schema.fields), "fields")
```

---

## Audit Log Table Design

### Creating the Main Audit Log Table

```python
# Create main audit log Delta table
def create_audit_log_table(table_path, schema):
    """
    Create partitioned Delta table for audit logs
    """
    
    print(f"Creating audit log table at: {table_path}")
    
    # Create empty DataFrame with schema
    empty_df = spark.createDataFrame([], schema)
    
    # Write as Delta table with partitioning
    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("event_date", "event_hour") \
        .save(table_path)
    
    # Optimize table structure
    spark.sql(f"""
        ALTER TABLE delta.`{table_path}` 
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.logRetentionDuration' = '30 days',
            'delta.deletedFileRetentionDuration' = '7 days',
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    
    print("Audit log table created successfully")
    return table_path

# Create the main audit table
main_audit_table_path = f"{base_path}/delta/audit_logs"
audit_table_path = create_audit_log_table(main_audit_table_path, audit_log_schema)

# Create table reference
audit_table = DeltaTable.forPath(spark, audit_table_path)
```

### Specialized Audit Tables

```python
# Create specialized audit tables for different log types

def create_security_audit_table():
    """
    Create specialized table for security audit events
    """
    security_schema = StructType([
        StructField("audit_id", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("security_event_type", StringType(), False),
        StructField("threat_level", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("authentication_method", StringType(), True),
        StructField("login_status", StringType(), True),
        StructField("failed_attempts", IntegerType(), True),
        StructField("risk_factors", ArrayType(StringType()), True),
        StructField("security_policies_applied", ArrayType(StringType()), True),
        StructField("detection_rules_triggered", ArrayType(StringType()), True),
        StructField("remediation_actions", ArrayType(StringType()), True),
        StructField("investigation_notes", StringType(), True)
    ])
    
    security_table_path = f"{base_path}/delta/security_audit_logs"
    
    spark.createDataFrame([], security_schema) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .save(security_table_path)
    
    return security_table_path

def create_data_access_audit_table():
    """
    Create specialized table for data access audit events
    """
    data_access_schema = StructType([
        StructField("audit_id", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("access_type", StringType(), False),  # READ, WRITE, DELETE, EXPORT
        StructField("data_source", StringType(), False),
        StructField("database_name", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("column_names", ArrayType(StringType()), True),
        StructField("row_count", LongType(), True),
        StructField("data_volume_bytes", LongType(), True),
        StructField("query_text", StringType(), True),
        StructField("execution_time_ms", LongType(), True),
        StructField("data_classification_level", StringType(), True),
        StructField("pii_detected", BooleanType(), True),
        StructField("encryption_status", StringType(), True),
        StructField("access_granted_by", StringType(), True),
        StructField("purpose_of_use", StringType(), True)
    ])
    
    data_access_table_path = f"{base_path}/delta/data_access_audit_logs"
    
    spark.createDataFrame([], data_access_schema) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .save(data_access_table_path)
    
    return data_access_table_path

# Create specialized tables
security_audit_path = create_security_audit_table()
data_access_audit_path = create_data_access_audit_table()

print(f"Security audit table: {security_audit_path}")
print(f"Data access audit table: {data_access_audit_path}")
```

---

## Creating Audit Log Tables

### Sample Data Generation

```python
# Generate realistic audit log sample data
def generate_audit_log_data(num_records=1000, days_back=7):
    """
    Generate sample audit log data for testing
    """
    
    import random
    from datetime import datetime, timedelta
    
    # Reference data
    event_types = [
        "USER_LOGIN", "USER_LOGOUT", "RESOURCE_CREATE", "RESOURCE_UPDATE", 
        "RESOURCE_DELETE", "DATA_ACCESS", "PERMISSION_CHANGE", "POLICY_UPDATE",
        "EXPORT_DATA", "IMPORT_DATA", "BACKUP_CREATED", "SYSTEM_CONFIG_CHANGE"
    ]
    
    event_categories = [
        "AUTHENTICATION", "AUTHORIZATION", "DATA_ACCESS", "RESOURCE_MANAGEMENT",
        "SECURITY", "COMPLIANCE", "SYSTEM", "APPLICATION"
    ]
    
    severity_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    
    operation_statuses = ["SUCCESS", "FAILURE", "PARTIAL", "TIMEOUT"]
    
    resource_types = [
        "STORAGE_ACCOUNT", "SQL_DATABASE", "KEY_VAULT", "VIRTUAL_MACHINE",
        "APP_SERVICE", "FUNCTION_APP", "COSMOS_DB", "DATA_FACTORY"
    ]
    
    locations = ["East US", "West US", "North Europe", "Southeast Asia"]
    
    users = [
        ("user1@company.com", "John Doe", "john.doe"),
        ("user2@company.com", "Jane Smith", "jane.smith"),
        ("user3@company.com", "Bob Johnson", "bob.johnson"),
        ("admin@company.com", "Admin User", "admin"),
        ("service@company.com", "Service Account", "service")
    ]
    
    data = []
    base_time = datetime.now() - timedelta(days=days_back)
    
    for i in range(num_records):
        # Generate timestamp
        random_minutes = random.randint(0, days_back * 24 * 60)
        event_time = base_time + timedelta(minutes=random_minutes)
        
        # Select random user
        user_email, user_name, user_id = random.choice(users)
        
        # Generate event data
        event_type = random.choice(event_types)
        event_category = random.choice(event_categories)
        severity = random.choice(severity_levels)
        status = random.choice(operation_statuses)
        
        # Create audit record
        record = {
            "audit_id": str(uuid.uuid4()),
            "event_id": f"EVT-{i+1:06d}",
            "correlation_id": str(uuid.uuid4()) if random.random() > 0.3 else None,
            "session_id": f"SES-{random.randint(1000, 9999)}",
            
            "event_timestamp": event_time,
            "ingestion_timestamp": datetime.now(),
            "event_date": event_time.date(),
            "event_hour": event_time.hour,
            
            "event_type": event_type,
            "event_category": event_category,
            "event_subcategory": f"{event_category}_SUB",
            "severity_level": severity,
            "risk_score": random.randint(1, 100),
            
            "user_id": user_id,
            "user_name": user_name,
            "user_email": user_email,
            "user_roles": [random.choice(["Reader", "Contributor", "Owner"])],
            "service_principal_id": str(uuid.uuid4()) if random.random() > 0.7 else None,
            "application_id": f"APP-{random.randint(1000, 9999)}",
            
            "resource_id": f"/subscriptions/{uuid.uuid4()}/resourceGroups/rg-{random.randint(1,10)}/providers/Microsoft.Storage/storageAccounts/storage{random.randint(1,100)}",
            "resource_name": f"resource-{random.randint(1, 100)}",
            "resource_type": random.choice(resource_types),
            "resource_group": f"rg-{random.randint(1, 10)}",
            "subscription_id": str(uuid.uuid4()),
            "tenant_id": str(uuid.uuid4()),
            
            "location": random.choice(locations),
            "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "geo_location": {
                "country": random.choice(["US", "UK", "DE", "FR", "JP"]),
                "region": random.choice(["North", "South", "East", "West"]),
                "city": random.choice(["Seattle", "London", "Berlin", "Paris", "Tokyo"]),
                "latitude": random.uniform(-90, 90),
                "longitude": random.uniform(-180, 180)
            },
            
            "operation_name": f"{event_type}_OPERATION",
            "operation_type": random.choice(["CREATE", "READ", "UPDATE", "DELETE"]),
            "operation_status": status,
            "status_code": 200 if status == "SUCCESS" else random.choice([400, 401, 403, 404, 500]),
            "error_code": None if status == "SUCCESS" else f"ERR-{random.randint(1000, 9999)}",
            "error_message": None if status == "SUCCESS" else f"Operation failed: {event_type}",
            
            "request_payload": json.dumps({"action": event_type, "parameters": {"param1": "value1"}}),
            "response_payload": json.dumps({"result": status, "data": "response_data"}),
            "data_classification": random.choice(["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"]),
            "sensitive_data_detected": random.choice([True, False]),
            
            "source_system": random.choice(["AZURE_AD", "AZURE_MONITOR", "APPLICATION", "CUSTOM"]),
            "log_version": "1.0",
            "custom_properties": {
                "custom_field_1": f"value_{random.randint(1, 100)}",
                "custom_field_2": f"data_{random.randint(1, 100)}"
            },
            "tags": [f"tag_{random.randint(1, 10)}", f"category_{random.randint(1, 5)}"],
            
            "retention_category": random.choice(["STANDARD", "EXTENDED", "PERMANENT"]),
            "retention_period_days": random.choice([365, 2555, 3650]),  # 1, 7, 10 years
            "compliance_flags": [random.choice(["SOX", "GDPR", "HIPAA", "PCI-DSS"])],
            "data_subject_id": f"DS-{random.randint(10000, 99999)}" if random.random() > 0.8 else None
        }
        
        data.append(record)
    
    return data

# Generate sample data
print("Generating sample audit log data...")
sample_data = generate_audit_log_data(5000, 30)  # 5000 records over 30 days

# Create DataFrame
audit_df = spark.createDataFrame(sample_data, audit_log_schema)

print(f"Generated {audit_df.count()} audit log records")
audit_df.show(5, truncate=False)
```

### Loading Data into Delta Table

```python
# Load data into Delta table with optimization
def load_audit_data(df, table_path, mode="append"):
    """
    Load audit data into Delta table with optimizations
    """
    
    print(f"Loading {df.count()} records into audit table...")
    
    # Add ingestion metadata
    enriched_df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                   .withColumn("data_quality_score", lit(95.0)) \
                   .withColumn("ingestion_batch_id", lit(str(uuid.uuid4())))
    
    # Write to Delta table
    enriched_df.write \
        .format("delta") \
        .mode(mode) \
        .option("mergeSchema", "true") \
        .save(table_path)
    
    print("Data loaded successfully")
    
    # Get table statistics
    delta_table = DeltaTable.forPath(spark, table_path)
    
    # Show recent history
    print("\nRecent table operations:")
    delta_table.history(5).select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(truncate=False)
    
    return delta_table

# Load sample data
audit_delta_table = load_audit_data(audit_df, audit_table_path)

# Verify data loading
print(f"\nTotal records in audit table: {spark.read.format('delta').load(audit_table_path).count()}")
```

---

## Data Ingestion Patterns

### Real-time Streaming Ingestion

```python
# Streaming ingestion pattern for real-time audit logs
def setup_streaming_audit_ingestion():
    """
    Set up streaming ingestion for real-time audit logs
    """
    
    # Simulated streaming source (replace with actual Event Hub, Kafka, etc.)
    def generate_streaming_audit_data():
        """Generate continuous audit data stream"""
        while True:
            # Generate single audit record
            record = generate_audit_log_data(1, 0)[0]  # 1 record, current time
            yield record
            time.sleep(1)  # 1 record per second
    
    # Create streaming DataFrame (conceptual - replace with actual streaming source)
    streaming_schema = audit_log_schema
    
    # Example with Event Hub (Azure-specific)
    event_hub_config = {
        "eventhubs.connectionString": "Endpoint=sb://...",
        "eventhubs.consumerGroup": "audit-consumer",
        "eventhubs.startingPosition": "latest"
    }
    
    # Streaming read (conceptual)
    streaming_df = spark.readStream \
        .format("eventhubs") \
        .options(**event_hub_config) \
        .load()
    
    # Parse and transform streaming data
    parsed_streaming_df = streaming_df.select(
        from_json(col("body").cast("string"), streaming_schema).alias("audit_data")
    ).select("audit_data.*")
    
    # Write to Delta table with streaming
    streaming_query = parsed_streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{base_path}/checkpoints/audit_streaming") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="30 seconds") \
        .start(audit_table_path)
    
    print("Streaming ingestion started")
    return streaming_query

# Batch ingestion with deduplication
def batch_ingest_with_deduplication(source_df, target_table_path):
    """
    Batch ingestion with deduplication using merge
    """
    
    print("Starting batch ingestion with deduplication...")
    
    # Load target table
    target_table = DeltaTable.forPath(spark, target_table_path)
    
    # Perform upsert (insert new, update existing)
    merge_result = target_table.alias("target").merge(
        source_df.alias("source"),
        "target.audit_id = source.audit_id"
    ).whenMatchedUpdate(set={
        "ingestion_timestamp": col("source.ingestion_timestamp"),
        "event_timestamp": col("source.event_timestamp"),
        "operation_status": col("source.operation_status"),
        "custom_properties": col("source.custom_properties")
    }).whenNotMatchedInsert(values={
        col_name: col(f"source.{col_name}") for col_name in source_df.columns
    }).execute()
    
    print("Batch ingestion completed with deduplication")
    
    # Show merge statistics
    print("Merge operation metrics:")
    recent_history = target_table.history(1)
    recent_history.select("operationMetrics").show(truncate=False)
    
    return merge_result

# Example batch ingestion
new_batch_data = generate_audit_log_data(1000, 1)  # 1000 records from last day
new_batch_df = spark.createDataFrame(new_batch_data, audit_log_schema)

# Perform batch ingestion
batch_ingest_with_deduplication(new_batch_df, audit_table_path)
```

### Data Quality and Validation

```python
# Data quality validation for audit logs
class AuditLogDataQuality:
    """
    Data quality validation and monitoring for audit logs
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.quality_rules = self._define_quality_rules()
    
    def _define_quality_rules(self):
        """Define data quality rules for audit logs"""
        return {
            "required_fields": [
                "audit_id", "event_timestamp", "event_type", 
                "operation_name", "source_system"
            ],
            "valid_event_types": [
                "USER_LOGIN", "USER_LOGOUT", "RESOURCE_CREATE", 
                "RESOURCE_UPDATE", "RESOURCE_DELETE", "DATA_ACCESS"
            ],
            "valid_severity_levels": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
            "valid_operation_statuses": ["SUCCESS", "FAILURE", "PARTIAL", "TIMEOUT"],
            "timestamp_range_days": 365,  # Max age for audit logs
            "max_payload_size_kb": 1024   # Max payload size
        }
    
    def validate_audit_data(self, df):
        """
        Comprehensive data quality validation
        """
        
        print("=== AUDIT LOG DATA QUALITY VALIDATION ===")
        
        validation_results = {}
        total_records = df.count()
        
        # 1. Check required fields
        print("1. Validating required fields...")
        for field in self.quality_rules["required_fields"]:
            null_count = df.filter(col(field).isNull()).count()
            validation_results[f"null_{field}"] = null_count
            
            if null_count > 0:
                print(f"   WARNING: {null_count} records have null {field}")
        
        # 2. Validate event types
        print("2. Validating event types...")
        invalid_event_types = df.filter(
            ~col("event_type").isin(self.quality_rules["valid_event_types"])
        ).count()
        validation_results["invalid_event_types"] = invalid_event_types
        
        if invalid_event_types > 0:
            print(f"   WARNING: {invalid_event_types} records have invalid event types")
        
        # 3. Validate severity levels
        print("3. Validating severity levels...")
        invalid_severity = df.filter(
            ~col("severity_level").isin(self.quality_rules["valid_severity_levels"])
        ).count()
        validation_results["invalid_severity"] = invalid_severity
        
        # 4. Validate timestamps
        print("4. Validating timestamps...")
        current_time = datetime.now()
        max_age = timedelta(days=self.quality_rules["timestamp_range_days"])
        min_valid_time = current_time - max_age
        
        future_timestamps = df.filter(col("event_timestamp") > current_time).count()
        old_timestamps = df.filter(col("event_timestamp") < min_valid_time).count()
        
        validation_results["future_timestamps"] = future_timestamps
        validation_results["old_timestamps"] = old_timestamps
        
        # 5. Check payload sizes
        print("5. Validating payload sizes...")
        max_payload_bytes = self.quality_rules["max_payload_size_kb"] * 1024
        
        large_request_payloads = df.filter(
            length(col("request_payload")) > max_payload_bytes
        ).count()
        large_response_payloads = df.filter(
            length(col("response_payload")) > max_payload_bytes
        ).count()
        
        validation_results["large_request_payloads"] = large_request_payloads
        validation_results["large_response_payloads"] = large_response_payloads
        
        # 6. Data consistency checks
        print("6. Performing consistency checks...")
        
        # Check for duplicate audit IDs
        duplicate_audit_ids = df.groupBy("audit_id").count().filter(col("count") > 1).count()
        validation_results["duplicate_audit_ids"] = duplicate_audit_ids
        
        # Check event_date consistency with event_timestamp
        inconsistent_dates = df.filter(
            col("event_date") != to_date(col("event_timestamp"))
        ).count()
        validation_results["inconsistent_dates"] = inconsistent_dates
        
        # Generate quality report
        print("\n=== DATA QUALITY REPORT ===")
        print(f"Total records validated: {total_records}")
        
        quality_score = 100.0
        for metric, value in validation_results.items():
            if value > 0:
                percentage = (value / total_records) * 100
                quality_score -= min(percentage, 10)  # Deduct up to 10% per issue
                print(f"{metric}: {value} ({percentage:.2f}%)")
        
        print(f"\nOverall Data Quality Score: {quality_score:.2f}%")
        
        return {
            "total_records": total_records,
            "validation_results": validation_results,
            "quality_score": quality_score,
            "passed_validation": quality_score >= 95.0
        }
    
    def create_quality_dashboard_data(self, df):
        """
        Create aggregated data for quality dashboard
        """
        
        # Daily quality metrics
        daily_quality = df.groupBy("event_date").agg(
            count("*").alias("total_records"),
            sum(when(col("audit_id").isNull(), 1).otherwise(0)).alias("missing_audit_id"),
            sum(when(col("event_timestamp").isNull(), 1).otherwise(0)).alias("missing_timestamp"),
            sum(when(col("operation_status") == "FAILURE", 1).otherwise(0)).alias("failed_operations"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("resource_id").alias("unique_resources")
        ).orderBy("event_date")
        
        return daily_quality

# Initialize data quality validator
dq_validator = AuditLogDataQuality(spark)

# Validate current audit data
current_audit_data = spark.read.format("delta").load(audit_table_path)
quality_report = dq_validator.validate_audit_data(current_audit_data)

print(f"Data quality validation completed. Score: {quality_report['quality_score']:.2f}%")
```

---

## Query Patterns and Analytics

### Common Audit Query Patterns

```python
# Load audit data for analysis
audit_df = spark.read.format("delta").load(audit_table_path)

print(f"Total audit records: {audit_df.count()}")
print("Sample data:")
audit_df.show(5, truncate=False)

# 1. Security Analysis Queries
def security_analysis_queries(df):
    """
    Common security analysis patterns
    """
    
    print("=== SECURITY ANALYSIS QUERIES ===")
    
    # Failed login attempts by user
    print("1. Failed login attempts by user (last 7 days):")
    failed_logins = df.filter(
        (col("event_type") == "USER_LOGIN") & 
        (col("operation_status") == "FAILURE") &
        (col("event_date") >= date_sub(current_date(), 7))
    ).groupBy("user_email", "user_name") \
     .agg(
         count("*").alias("failed_attempts"),
         countDistinct("ip_address").alias("unique_ips"),
         collect_set("ip_address").alias("ip_addresses"),
         min("event_timestamp").alias("first_failure"),
         max("event_timestamp").alias("last_failure")
     ) \
     .orderBy(desc("failed_attempts"))
    
    failed_logins.show(10, truncate=False)
    
    # Suspicious IP addresses (multiple users, multiple failures)
    print("\n2. Suspicious IP addresses:")
    suspicious_ips = df.filter(
        (col("operation_status") == "FAILURE") &
        (col("event_date") >= date_sub(current_date(), 7))
    ).groupBy("ip_address") \
     .agg(
         countDistinct("user_id").alias("unique_users"),
         count("*").alias("total_failures"),
         collect_set("event_type").alias("event_types"),
         collect_set("user_email").alias("affected_users")
     ) \
     .filter((col("unique_users") >= 3) | (col("total_failures") >= 10)) \
     .orderBy(desc("total_failures"))
    
    suspicious_ips.show(truncate=False)
    
    # High-risk operations by severity
    print("\n3. High-risk operations:")
    high_risk_ops = df.filter(
        (col("severity_level").isin(["HIGH", "CRITICAL"])) &
        (col("event_date") >= date_sub(current_date(), 30))
    ).groupBy("operation_name", "severity_level") \
     .agg(
         count("*").alias("occurrence_count"),
         countDistinct("user_id").alias("unique_users"),
         countDistinct("resource_id").alias("unique_resources"),
         avg("risk_score").alias("avg_risk_score")
     ) \
     .orderBy(desc("avg_risk_score"), desc("occurrence_count"))
    
    high_risk_ops.show(truncate=False)
    
    return {
        "failed_logins": failed_logins,
        "suspicious_ips": suspicious_ips,
        "high_risk_ops": high_risk_ops
    }

# 2. Compliance Reporting Queries
def compliance_reporting_queries(df):
    """
    Compliance-focused audit queries
    """
    
    print("=== COMPLIANCE REPORTING QUERIES ===")
    
    # Data access audit for GDPR compliance
    print("1. Data access audit (GDPR compliance):")
    data_access_audit = df.filter(
        (col("event_category") == "DATA_ACCESS") &
        (col("data_subject_id").isNotNull()) &
        (col("event_date") >= date_sub(current_date(), 90))
    ).select(
        "event_timestamp",
        "user_email",
        "operation_name",
        "resource_name",
        "data_subject_id",
        "data_classification",
        "purpose_of_use"
    ).orderBy(desc("event_timestamp"))
    
    data_access_audit.show(20, truncate=False)
    
    # Administrative changes audit
    print("\n2. Administrative changes audit:")
    admin_changes = df.filter(
        (col("event_category") == "AUTHORIZATION") &
        (col("operation_type").isin(["CREATE", "UPDATE", "DELETE"])) &
        (col("event_date") >= date_sub(current_date(), 30))
    ).select(
        "event_timestamp",
        "user_email", 
        "operation_name",
        "resource_type",
        "resource_name",
        "operation_status"
    ).orderBy(desc("event_timestamp"))
    
    admin_changes.show(20, truncate=False)
    
    # Retention compliance report
    print("\n3. Retention compliance report:")
    retention_report = df.groupBy("retention_category", "retention_period_days") \
        .agg(
            count("*").alias("record_count"),
            min("event_timestamp").alias("oldest_record"),
            max("event_timestamp").alias("newest_record"),
            countDistinct("data_subject_id").alias("unique_data_subjects")
        ) \
        .orderBy("retention_period_days")
    
    retention_report.show(truncate=False)
    
    return {
        "data_access_audit": data_access_audit,
        "admin_changes": admin_changes,
        "retention_report": retention_report
    }

# 3. Operational Analytics
def operational_analytics_queries(df):
    """
    Operational insights and analytics
    """
    
    print("=== OPERATIONAL ANALYTICS ===")
    
    # System usage patterns
    print("1. System usage patterns by hour:")
    hourly_usage = df.groupBy("event_hour") \
        .agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("active_users"),
            countDistinct("resource_id").alias("resources_accessed"),
            avg("risk_score").alias("avg_risk_score")
        ) \
        .orderBy("event_hour")
    
    hourly_usage.show(24)
    
    # Resource access patterns
    print("\n2. Most accessed resources:")
    resource_access = df.filter(
        col("event_date") >= date_sub(current_date(), 7)
    ).groupBy("resource_type", "resource_name") \
     .agg(
         count("*").alias("access_count"),
         countDistinct("user_id").alias("unique_users"),
         countDistinct("operation_name").alias("unique_operations"),
         sum(when(col("operation_status") == "FAILURE", 1).otherwise(0)).alias("failed_access")
     ) \
     .orderBy(desc("access_count"))
    
    resource_access.show(20, truncate=False)
    
    # User activity summary
    print("\n3. User activity summary:")
    user_activity = df.filter(
        col("event_date") >= date_sub(current_date(), 30)
    ).groupBy("user_email", "user_name") \
     .agg(
         count("*").alias("total_activities"),
         countDistinct("event_type").alias("unique_event_types"),
         countDistinct("resource_id").alias("unique_resources"),
         countDistinct("event_date").alias("active_days"),
         max("event_timestamp").alias("last_activity"),
         avg("risk_score").alias("avg_risk_score")
     ) \
     .orderBy(desc("total_activities"))
    
    user_activity.show(20, truncate=False)
    
    return {
        "hourly_usage": hourly_usage,
        "resource_access": resource_access,
        "user_activity": user_activity
    }

# Execute analysis queries
security_results = security_analysis_queries(audit_df)
compliance_results = compliance_reporting_queries(audit_df)
operational_results = operational_analytics_queries(audit_df)
```

### Advanced Analytics with Delta Lake Time Travel

```python
# Time travel analytics for audit logs
def time_travel_audit_analysis(table_path):
    """
    Use Delta Lake time travel for historical audit analysis
    """
    
    print("=== TIME TRAVEL AUDIT ANALYSIS ===")
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    # Get table history
    history = delta_table.history()
    print("Delta table history:")
    history.select("version", "timestamp", "operation", "operationMetrics").show(10, truncate=False)
    
    # Compare current vs 1 day ago
    current_df = spark.read.format("delta").load(table_path)
    
    # Get timestamp from 1 day ago
    one_day_ago = datetime.now() - timedelta(days=1)
    
    try:
        historical_df = spark.read.format("delta") \
            .option("timestampAsOf", one_day_ago.strftime("%Y-%m-%d %H:%M:%S")) \
            .load(table_path)
        
        print(f"Current records: {current_df.count()}")
        print(f"Records 1 day ago: {historical_df.count()}")
        
        # Analyze growth
        growth = current_df.count() - historical_df.count()
        print(f"Record growth in last 24 hours: {growth}")
        
        # Compare user activity
        current_users = current_df.groupBy("user_email").count().withColumnRenamed("count", "current_count")
        historical_users = historical_df.groupBy("user_email").count().withColumnRenamed("count", "historical_count")
        
        user_comparison = current_users.join(historical_users, "user_email", "outer") \
            .fillna(0) \
            .withColumn("activity_change", col("current_count") - col("historical_count")) \
            .orderBy(desc("activity_change"))
        
        print("\nUser activity changes:")
        user_comparison.show(10)
        
    except Exception as e:
        print(f"Historical data not available: {str(e)}")
    
    # Version-based comparison
    latest_version = history.select("version").collect()[0]["version"]
    
    if latest_version > 0:
        previous_version_df = spark.read.format("delta") \
            .option("versionAsOf", latest_version - 1) \
            .load(table_path)
        
        print(f"\nVersion comparison:")
        print(f"Latest version ({latest_version}): {current_df.count()} records")
        print(f"Previous version ({latest_version - 1}): {previous_version_df.count()} records")

# Execute time travel analysis
time_travel_audit_analysis(audit_table_path)
```

---

## Compliance and Retention

### Automated Retention Management

```python
# Automated retention management for audit logs
class AuditLogRetentionManager:
    """
    Manage audit log retention policies and cleanup
    """
    
    def __init__(self, spark_session, base_path):
        self.spark = spark_session
        self.base_path = base_path
        self.retention_policies = self._define_retention_policies()
    
    def _define_retention_policies(self):
        """Define retention policies by category"""
        return {
            "STANDARD": {
                "retention_days": 2555,  # 7 years
                "archive_after_days": 365,  # 1 year
                "description": "Standard business records"
            },
            "EXTENDED": {
                "retention_days": 3650,  # 10 years
                "archive_after_days": 1095,  # 3 years
                "description": "Financial and legal records"
            },
            "PERMANENT": {
                "retention_days": -1,  # Never delete
                "archive_after_days": 1825,  # 5 years
                "description": "Permanent regulatory records"
            },
            "SHORT_TERM": {
                "retention_days": 90,  # 3 months
                "archive_after_days": 30,  # 1 month
                "description": "Temporary operational logs"
            }
        }
    
    def analyze_retention_status(self, table_path):
        """
        Analyze current retention status of audit logs
        """
        
        print("=== RETENTION STATUS ANALYSIS ===")
        
        df = self.spark.read.format("delta").load(table_path)
        current_date = datetime.now().date()
        
        # Calculate age of records
        retention_analysis = df.withColumn("record_age_days", 
            datediff(lit(current_date), col("event_date"))
        ).groupBy("retention_category") \
         .agg(
             count("*").alias("total_records"),
             min("record_age_days").alias("min_age_days"),
             max("record_age_days").alias("max_age_days"),
             avg("record_age_days").alias("avg_age_days"),
             sum(when(col("record_age_days") > 365, 1).otherwise(0)).alias("records_over_1_year"),
             sum(when(col("record_age_days") > 2555, 1).otherwise(0)).alias("records_over_7_years")
         )
        
        retention_analysis.show(truncate=False)
        
        # Identify records eligible for archival/deletion
        for category, policy in self.retention_policies.items():
            print(f"\n--- {category} RETENTION POLICY ---")
            print(f"Description: {policy['description']}")
            print(f"Retention period: {policy['retention_days']} days")
            print(f"Archive after: {policy['archive_after_days']} days")
            
            category_df = df.filter(col("retention_category") == category)
            
            if policy['retention_days'] > 0:
                # Records eligible for deletion
                eligible_for_deletion = category_df.filter(
                    datediff(lit(current_date), col("event_date")) > policy['retention_days']
                )
                deletion_count = eligible_for_deletion.count()
                
                if deletion_count > 0:
                    print(f"Records eligible for deletion: {deletion_count}")
                    
                    # Show sample of records to be deleted
                    print("Sample records for deletion:")
                    eligible_for_deletion.select(
                        "audit_id", "event_date", "event_type", "user_email"
                    ).show(5)
            
            # Records eligible for archival
            eligible_for_archive = category_df.filter(
                (datediff(lit(current_date), col("event_date")) > policy['archive_after_days']) &
                (datediff(lit(current_date), col("event_date")) <= policy['retention_days'] if policy['retention_days'] > 0 else lit(True))
            )
            archive_count = eligible_for_archive.count()
            
            if archive_count > 0:
                print(f"Records eligible for archival: {archive_count}")
        
        return retention_analysis
    
    def execute_retention_policy(self, table_path, dry_run=True):
        """
        Execute retention policy (delete old records)
        """
        
        print(f"=== EXECUTING RETENTION POLICY (DRY RUN: {dry_run}) ===")
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        current_date = datetime.now().date()
        
        deletion_summary = {}
        
        for category, policy in self.retention_policies.items():
            if policy['retention_days'] > 0:  # Skip permanent retention
                
                cutoff_date = current_date - timedelta(days=policy['retention_days'])
                
                print(f"\nProcessing {category} category (cutoff date: {cutoff_date})")
                
                # Count records to be deleted
                records_to_delete = self.spark.read.format("delta").load(table_path) \
                    .filter(
                        (col("retention_category") == category) &
                        (col("event_date") < cutoff_date)
                    )
                
                delete_count = records_to_delete.count()
                deletion_summary[category] = delete_count
                
                print(f"Records to delete: {delete_count}")
                
                if delete_count > 0 and not dry_run:
                    # Execute deletion
                    print(f"Deleting {delete_count} records...")
                    
                    delta_table.delete(
                        (col("retention_category") == category) &
                        (col("event_date") < cutoff_date)
                    )
                    
                    print("Deletion completed")
        
        print(f"\n=== RETENTION POLICY SUMMARY ===")
        for category, count in deletion_summary.items():
            print(f"{category}: {count} records {'would be' if dry_run else 'were'} deleted")
        
        return deletion_summary
    
    def archive_old_records(self, table_path, archive_path):
        """
        Archive old records to separate storage
        """
        
        print("=== ARCHIVING OLD RECORDS ===")
        
        df = self.spark.read.format("delta").load(table_path)
        current_date = datetime.now().date()
        
        # Identify records for archival
        archive_records = df.filter(
            datediff(lit(current_date), col("event_date")) > 365  # Archive after 1 year
        )
        
        archive_count = archive_records.count()
        print(f"Records to archive: {archive_count}")
        
        if archive_count > 0:
            # Write to archive location
            archive_records.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("event_date") \
                .save(archive_path)
            
            print(f"Archived {archive_count} records to {archive_path}")
            
            # Remove archived records from main table
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.delete(
                datediff(lit(current_date), col("event_date")) > 365
            )
            
            print("Archived records removed from main table")
        
        return archive_count

# Initialize retention manager
retention_manager = AuditLogRetentionManager(spark, base_path)

# Analyze current retention status
retention_status = retention_manager.analyze_retention_status(audit_table_path)

# Execute retention policy (dry run)
deletion_summary = retention_manager.execute_retention_policy(audit_table_path, dry_run=True)
```

### GDPR Compliance Features

```python
# GDPR compliance features for audit logs
class GDPRComplianceManager:
    """
    GDPR compliance management for audit logs
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def right_to_be_forgotten(self, table_path, data_subject_id):
        """
        Implement GDPR right to be forgotten (data erasure)
        """
        
        print(f"=== GDPR RIGHT TO BE FORGOTTEN ===")
        print(f"Data Subject ID: {data_subject_id}")
        
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        # Find records for the data subject
        subject_records = self.spark.read.format("delta").load(table_path) \
            .filter(col("data_subject_id") == data_subject_id)
        
        record_count = subject_records.count()
        print(f"Found {record_count} records for data subject")
        
        if record_count > 0:
            # Show records to be deleted
            print("Records to be deleted:")
            subject_records.select(
                "audit_id", "event_timestamp", "event_type", 
                "user_email", "operation_name"
            ).show(10, truncate=False)
            
            # Delete records
            delta_table.delete(col("data_subject_id") == data_subject_id)
            
            print(f"Deleted {record_count} records for data subject {data_subject_id}")
            
            # Log the deletion action
            deletion_audit = [{
                "audit_id": str(uuid.uuid4()),
                "event_id": f"GDPR-DELETION-{int(datetime.now().timestamp())}",
                "event_timestamp": datetime.now(),
                "event_date": datetime.now().date(),
                "event_hour": datetime.now().hour,
                "event_type": "DATA_DELETION",
                "event_category": "COMPLIANCE",
                "severity_level": "HIGH",
                "operation_name": "GDPR_RIGHT_TO_BE_FORGOTTEN",
                "operation_status": "SUCCESS",
                "data_subject_id": data_subject_id,
                "custom_properties": {
                    "records_deleted": str(record_count),
                    "deletion_reason": "GDPR_ARTICLE_17",
                    "authorized_by": "compliance_officer"
                },
                "source_system": "GDPR_COMPLIANCE_SYSTEM",
                "retention_category": "PERMANENT",
                "retention_period_days": -1
            }]
            
            # Add deletion audit record
            deletion_df = self.spark.createDataFrame(deletion_audit, audit_log_schema)
            deletion_df.write.format("delta").mode("append").save(table_path)
            
            print("GDPR deletion audit record created")
        
        return record_count
    
    def data_portability_export(self, table_path, data_subject_id, export_path):
        """
        GDPR right to data portability - export user's data
        """
        
        print(f"=== GDPR DATA PORTABILITY EXPORT ===")
        print(f"Data Subject ID: {data_subject_id}")
        
        # Extract all data for the subject
        subject_data = self.spark.read.format("delta").load(table_path) \
            .filter(col("data_subject_id") == data_subject_id)
        
        record_count = subject_data.count()
        print(f"Found {record_count} records for export")
        
        if record_count > 0:
            # Create portable export format
            export_data = subject_data.select(
                "event_timestamp",
                "event_type", 
                "operation_name",
                "resource_name",
                "data_classification",
                "request_payload",
                "response_payload"
            ).orderBy("event_timestamp")
            
            # Export to JSON for portability
            export_data.coalesce(1).write \
                .format("json") \
                .mode("overwrite") \
                .save(export_path)
            
            print(f"Exported {record_count} records to {export_path}")
            
            # Create export audit record
            export_audit = [{
                "audit_id": str(uuid.uuid4()),
                "event_id": f"GDPR-EXPORT-{int(datetime.now().timestamp())}",
                "event_timestamp": datetime.now(),
                "event_date": datetime.now().date(),
                "event_hour": datetime.now().hour,
                "event_type": "DATA_EXPORT",
                "event_category": "COMPLIANCE",
                "severity_level": "MEDIUM",
                "operation_name": "GDPR_DATA_PORTABILITY",
                "operation_status": "SUCCESS",
                "data_subject_id": data_subject_id,
                "custom_properties": {
                    "records_exported": str(record_count),
                    "export_path": export_path,
                    "export_format": "JSON"
                },
                "source_system": "GDPR_COMPLIANCE_SYSTEM"
            }]
            
            # Add export audit record
            export_df = self.spark.createDataFrame(export_audit, audit_log_schema)
            export_df.write.format("delta").mode("append").save(table_path)
            
        return record_count
    
    def generate_gdpr_compliance_report(self, table_path):
        """
        Generate GDPR compliance report
        """
        
        print("=== GDPR COMPLIANCE REPORT ===")
        
        df = self.spark.read.format("delta").load(table_path)
        
        # Data subject statistics
        data_subject_stats = df.filter(col("data_subject_id").isNotNull()) \
            .groupBy("data_subject_id") \
            .agg(
                count("*").alias("total_records"),
                min("event_timestamp").alias("first_record"),
                max("event_timestamp").alias("last_record"),
                collect_set("event_type").alias("event_types"),
                collect_set("data_classification").alias("data_classifications")
            )
        
        print(f"Total data subjects with records: {data_subject_stats.count()}")
        
        # GDPR-related operations
        gdpr_operations = df.filter(
            col("operation_name").rlike("GDPR_.*")
        ).groupBy("operation_name") \
         .agg(
             count("*").alias("operation_count"),
             countDistinct("data_subject_id").alias("unique_subjects"),
             min("event_timestamp").alias("first_operation"),
             max("event_timestamp").alias("last_operation")
         )
        
        print("\nGDPR Operations Summary:")
        gdpr_operations.show(truncate=False)
        
        # Data retention compliance
        retention_compliance = df.groupBy("retention_category") \
            .agg(
                count("*").alias("total_records"),
                countDistinct("data_subject_id").alias("data_subjects_affected"),
                min("event_date").alias("oldest_record_date"),
                max("event_date").alias("newest_record_date")
            )
        
        print("\nData Retention Compliance:")
        retention_compliance.show(truncate=False)
        
        return {
            "data_subject_stats": data_subject_stats,
            "gdpr_operations": gdpr_operations,
            "retention_compliance": retention_compliance
        }

# Initialize GDPR compliance manager
gdpr_manager = GDPRComplianceManager(spark)

# Generate GDPR compliance report
gdpr_report = gdpr_manager.generate_gdpr_compliance_report(audit_table_path)

# Example: Execute right to be forgotten (uncomment to test)
# test_subject_id = "DS-12345"
# deleted_records = gdpr_manager.right_to_be_forgotten(audit_table_path, test_subject_id)
```

---

## Best Practices

### Audit Log Best Practices Implementation

```python
class AuditLogBestPractices:
    """
    Implementation of audit log best practices
    """
    
    def __init__(self, spark_session, table_path):
        self.spark = spark_session
        self.table_path = table_path
        self.best_practices = self._define_best_practices()
    
    def _define_best_practices(self):
        """Define audit log best practices"""
        return {
            "data_integrity": {
                "use_checksums": True,
                "validate_schema": True,
                "prevent_tampering": True
            },
            "performance": {
                "partition_by_date": True,
                "z_order_optimization": True,
                "auto_optimize": True,
                "liquid_clustering": False  # For newer Delta versions
            },
            "security": {
                "encrypt_sensitive_data": True,
                "mask_pii": True,
                "access_control": True,
                "audit_the_auditors": True
            },
            "compliance": {
                "retention_policies": True,
                "gdpr_compliance": True,
                "immutable_logs": True,
                "chain_of_custody": True
            }
        }
    
    def implement_data_integrity_checks(self):
        """
        Implement data integrity checks
        """
        
        print("=== IMPLEMENTING DATA INTEGRITY CHECKS ===")
        
        # Add checksums to audit records
        def add_integrity_checksums(df):
            """Add integrity checksums to audit records"""
            
            # Create record hash for integrity verification
            df_with_hash = df.withColumn(
                "record_hash",
                sha2(
                    concat_ws("|",
                        col("audit_id"),
                        col("event_timestamp").cast("string"),
                        col("event_type"),
                        col("user_id"),
                        col("operation_name"),
                        col("resource_id")
                    ), 256
                )
            )
            
            # Add batch hash for batch integrity
            df_with_batch_hash = df_with_hash.withColumn(
                "batch_id", lit(str(uuid.uuid4()))
            ).withColumn(
                "batch_timestamp", current_timestamp()
            )
            
            return df_with_batch_hash
        
        # Validate record integrity
        def validate_record_integrity(df):
            """Validate record integrity using checksums"""
            
            # Recalculate hashes and compare
            df_with_new_hash = df.withColumn(
                "calculated_hash",
                sha2(
                    concat_ws("|",
                        col("audit_id"),
                        col("event_timestamp").cast("string"),
                        col("event_type"),
                        col("user_id"),
                        col("operation_name"),
                        col("resource_id")
                    ), 256
                )
            )
            
            # Find records with hash mismatches
            integrity_violations = df_with_new_hash.filter(
                col("record_hash") != col("calculated_hash")
            )
            
            violation_count = integrity_violations.count()
            
            if violation_count > 0:
                print(f"WARNING: {violation_count} records failed integrity check!")
                integrity_violations.select(
                    "audit_id", "event_timestamp", "record_hash", "calculated_hash"
                ).show(10, truncate=False)
            else:
                print("All records passed integrity validation")
            
            return violation_count == 0
        
        return {
            "add_checksums": add_integrity_checksums,
            "validate_integrity": validate_record_integrity
        }
    
    def implement_performance_optimizations(self):
        """
        Implement performance optimization best practices
        """
        
        print("=== IMPLEMENTING PERFORMANCE OPTIMIZATIONS ===")
        
        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        
        # 1. Optimize table structure
        print("1. Optimizing table structure...")
        self.spark.sql(f"""
            OPTIMIZE delta.`{self.table_path}`
            ZORDER BY (user_id, event_type, event_timestamp)
        """)
        
        # 2. Set table properties for auto-optimization
        print("2. Setting auto-optimization properties...")
        self.spark.sql(f"""
            ALTER TABLE delta.`{self.table_path}` 
            SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.tuneFileSizesForRewrites' = 'true'
            )
        """)
        
        # 3. Analyze table statistics
        print("3. Analyzing table statistics...")
        table_stats = self.spark.sql(f"""
            DESCRIBE DETAIL delta.`{self.table_path}`
        """)
        
        table_stats.show(truncate=False)
        
        # 4. Vacuum old files
        print("4. Cleaning up old files...")
        vacuum_result = delta_table.vacuum(168)  # 7 days retention
        
        print("Performance optimization completed")
        
        return table_stats
    
    def implement_security_measures(self):
        """
        Implement security best practices
        """
        
        print("=== IMPLEMENTING SECURITY MEASURES ===")
        
        # 1. Data masking for sensitive fields
        def mask_sensitive_data(df):
            """Mask sensitive data in audit logs"""
            
            masked_df = df.withColumn(
                "user_email_masked",
                regexp_replace(col("user_email"), "(?<=.{2}).(?=.*@)", "*")
            ).withColumn(
                "ip_address_masked", 
                regexp_replace(col("ip_address"), r"(\d+\.\d+\.\d+\.)\d+", "$1***")
            ).withColumn(
                "request_payload_masked",
                when(col("sensitive_data_detected") == True, lit("***MASKED***"))
                .otherwise(col("request_payload"))
            )
            
            return masked_df
        
        # 2. Access control validation
        def validate_access_control():
            """Validate access control for audit logs"""
            
            # Check table permissions (conceptual - depends on environment)
            print("Validating access control...")
            
            # In practice, integrate with Azure RBAC, ACLs, etc.
            access_control_report = {
                "table_permissions": "RESTRICTED",
                "read_access": "AUDIT_READERS",
                "write_access": "AUDIT_WRITERS", 
                "admin_access": "AUDIT_ADMINS",
                "encryption_status": "ENABLED"
            }
            
            return access_control_report
        
        # 3. Audit trail for audit system
        def audit_the_auditors(operation, user, details):
            """Create audit trail for audit system operations"""
            
            auditor_audit = [{
                "audit_id": str(uuid.uuid4()),
                "event_timestamp": datetime.now(),
                "event_date": datetime.now().date(),
                "event_hour": datetime.now().hour,
                "event_type": "AUDIT_SYSTEM_OPERATION",
                "event_category": "SYSTEM",
                "operation_name": operation,
                "user_id": user,
                "custom_properties": details,
                "source_system": "AUDIT_SYSTEM_INTERNAL",
                "severity_level": "HIGH"
            }]
            
            # Write to separate auditor audit table
            auditor_df = self.spark.createDataFrame(auditor_audit, audit_log_schema)
            auditor_audit_path = f"{self.table_path}_auditor_audit"
            
            auditor_df.write.format("delta").mode("append").save(auditor_audit_path)
            
            return auditor_audit_path
        
        return {
            "mask_data": mask_sensitive_data,
            "validate_access": validate_access_control,
            "audit_auditors": audit_the_auditors
        }
    
    def generate_best_practices_report(self):
        """
        Generate comprehensive best practices compliance report
        """
        
        print("=== BEST PRACTICES COMPLIANCE REPORT ===")
        
        df = self.spark.read.format("delta").load(self.table_path)
        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        
        # Table structure analysis
        table_detail = self.spark.sql(f"DESCRIBE DETAIL delta.`{self.table_path}`")
        table_properties = self.spark.sql(f"SHOW TBLPROPERTIES delta.`{self.table_path}`")
        
        print("1. Table Structure Compliance:")
        table_detail.select("format", "partitionColumns", "numFiles", "sizeInBytes").show(truncate=False)
        
        print("\n2. Table Properties:")
        table_properties.show(truncate=False)
        
        # Data quality metrics
        print("\n3. Data Quality Metrics:")
        quality_metrics = df.agg(
            count("*").alias("total_records"),
            sum(when(col("audit_id").isNull(), 1).otherwise(0)).alias("missing_audit_id"),
            sum(when(col("event_timestamp").isNull(), 1).otherwise(0)).alias("missing_timestamp"),
            sum(when(col("user_id").isNull(), 1).otherwise(0)).alias("missing_user_id"),
            countDistinct("audit_id").alias("unique_audit_ids"),
            (count("*") - countDistinct("audit_id")).alias("duplicate_audit_ids")
        )
        
        quality_metrics.show(truncate=False)
        
        # Security compliance
        print("\n4. Security Compliance:")
        security_metrics = df.agg(
            sum(when(col("sensitive_data_detected") == True, 1).otherwise(0)).alias("sensitive_records"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("ip_address").alias("unique_ip_addresses"),
            sum(when(col("operation_status") == "FAILURE", 1).otherwise(0)).alias("failed_operations")
        )
        
        security_metrics.show(truncate=False)
        
        # Retention compliance
        print("\n5. Retention Compliance:")
        current_date = datetime.now().date()
        retention_metrics = df.withColumn("record_age_days", 
            datediff(lit(current_date), col("event_date"))
        ).groupBy("retention_category") \
         .agg(
             count("*").alias("record_count"),
             avg("record_age_days").alias("avg_age_days"),
             max("record_age_days").alias("max_age_days")
         )
        
        retention_metrics.show(truncate=False)
        
        return {
            "table_detail": table_detail,
            "quality_metrics": quality_metrics,
            "security_metrics": security_metrics,
            "retention_metrics": retention_metrics
        }

# Initialize best practices implementation
bp_manager = AuditLogBestPractices(spark, audit_table_path)

# Implement data integrity checks
integrity_tools = bp_manager.implement_data_integrity_checks()

# Implement performance optimizations
perf_stats = bp_manager.implement_performance_optimizations()

# Implement security measures
security_tools = bp_manager.implement_security_measures()

# Generate compliance report
compliance_report = bp_manager.generate_best_practices_report()
```

### Conclusion

This comprehensive guide demonstrates how to implement enterprise-grade audit logging using Delta Lake in Azure. Key benefits include:

1. **ACID Transactions**: Guaranteed data integrity for audit logs
2. **Time Travel**: Historical analysis and compliance reporting
3. **Schema Evolution**: Adapt to changing audit requirements
4. **Performance Optimization**: Efficient querying and storage
5. **Compliance Features**: GDPR, retention policies, and regulatory support
6. **Security**: Data masking, access control, and audit trail protection

The Delta Lake format provides an ideal foundation for audit logging systems, offering the reliability, performance, and compliance features required for enterprise environments.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*