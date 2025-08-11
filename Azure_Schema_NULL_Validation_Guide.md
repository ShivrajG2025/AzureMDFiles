# Azure Schema Definition and NULL Value Validation Guide
## Comprehensive Implementation Guide for NULL Value Checks

---

### Table of Contents

1. [Overview](#overview)
2. [Azure SQL Database NULL Constraints](#azure-sql-database-null-constraints)
3. [Azure Databricks Schema Validation](#azure-databricks-schema-validation)
4. [Azure Data Factory Data Validation](#azure-data-factory-data-validation)
5. [Azure Synapse Analytics Validation](#azure-synapse-analytics-validation)
6. [Azure Stream Analytics Schema Validation](#azure-stream-analytics-schema-validation)
7. [Cosmos DB Schema Validation](#cosmos-db-schema-validation)
8. [Automated Validation Framework](#automated-validation-framework)
9. [Monitoring and Alerting](#monitoring-and-alerting)
10. [Best Practices](#best-practices)

---

## Overview

NULL value validation is critical for data quality and integrity in Azure data platforms. This guide provides comprehensive approaches to define schemas with NULL constraints and implement validation checks across various Azure services.

### Architecture Overview

```json
{
  "null_validation_architecture": {
    "validation_layers": {
      "database_layer": {
        "services": ["Azure SQL Database", "Azure SQL Managed Instance"],
        "methods": ["NOT NULL constraints", "CHECK constraints", "Triggers", "Stored procedures"]
      },
      "processing_layer": {
        "services": ["Azure Databricks", "Azure Synapse Analytics"],
        "methods": ["Schema enforcement", "Data validation functions", "Custom validators"]
      },
      "integration_layer": {
        "services": ["Azure Data Factory", "Azure Stream Analytics"],
        "methods": ["Data flow validations", "Pipeline checks", "Real-time validation"]
      },
      "application_layer": {
        "services": ["Azure Functions", "Logic Apps"],
        "methods": ["Custom validation logic", "API validations", "Event-driven checks"]
      }
    },
    "validation_types": [
      "Schema-level constraints",
      "Application-level validation",
      "ETL pipeline validation",
      "Real-time stream validation",
      "Batch validation"
    ]
  }
}
```

### Benefits of NULL Validation

```json
{
  "benefits": {
    "data_quality": {
      "completeness": "Ensures required fields are populated",
      "consistency": "Maintains uniform data standards",
      "reliability": "Prevents downstream processing errors"
    },
    "business_impact": {
      "decision_making": "Reliable data for analytics and reporting",
      "compliance": "Meets regulatory data quality requirements",
      "cost_reduction": "Reduces data cleanup and correction costs"
    },
    "technical_advantages": {
      "performance": "Optimized query execution with proper constraints",
      "maintenance": "Easier debugging and troubleshooting",
      "automation": "Automated data quality monitoring"
    }
  }
}
```

---

## Azure SQL Database NULL Constraints

### Basic NOT NULL Constraints

```sql
-- Create table with NOT NULL constraints
CREATE TABLE dbo.Customer (
    CustomerID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(255) NOT NULL,
    Phone NVARCHAR(20) NULL, -- Optional field
    DateOfBirth DATE NULL, -- Optional field
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    IsActive BIT NOT NULL DEFAULT 1
);

-- Add NOT NULL constraint to existing column
ALTER TABLE dbo.Customer
ALTER COLUMN Phone NVARCHAR(20) NOT NULL;

-- Add CHECK constraint for NULL validation with conditions
ALTER TABLE dbo.Customer
ADD CONSTRAINT CK_Customer_Email_NotEmpty 
CHECK (Email IS NOT NULL AND LEN(TRIM(Email)) > 0);
```

### Advanced NULL Validation with CHECK Constraints

```sql
-- Complex NULL validation scenarios
CREATE TABLE dbo.Order (
    OrderID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ShippingAddress NVARCHAR(500) NULL,
    BillingAddress NVARCHAR(500) NULL,
    OrderStatus NVARCHAR(50) NOT NULL,
    TotalAmount DECIMAL(10,2) NOT NULL,
    ShippingDate DATETIME2 NULL,
    DeliveryDate DATETIME2 NULL,
    
    -- Conditional NULL validation: If order is shipped, shipping date cannot be NULL
    CONSTRAINT CK_Order_ShippingDate 
    CHECK (
        (OrderStatus != 'Shipped' AND OrderStatus != 'Delivered') 
        OR 
        (OrderStatus IN ('Shipped', 'Delivered') AND ShippingDate IS NOT NULL)
    ),
    
    -- If delivery date is provided, shipping date must also be provided
    CONSTRAINT CK_Order_DeliveryDate 
    CHECK (DeliveryDate IS NULL OR ShippingDate IS NOT NULL),
    
    -- Either shipping or billing address must be provided
    CONSTRAINT CK_Order_Address 
    CHECK (ShippingAddress IS NOT NULL OR BillingAddress IS NOT NULL),
    
    -- Total amount must be positive and not null
    CONSTRAINT CK_Order_TotalAmount 
    CHECK (TotalAmount IS NOT NULL AND TotalAmount > 0),
    
    FOREIGN KEY (CustomerID) REFERENCES dbo.Customer(CustomerID)
);
```

### NULL Validation Stored Procedures

```sql
-- Stored procedure for comprehensive NULL validation
CREATE PROCEDURE ValidateTableNullConstraints
    @TableName NVARCHAR(255),
    @ValidationResults NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @ValidationReport TABLE (
        ColumnName NVARCHAR(255),
        NullCount INT,
        TotalRows INT,
        NullPercentage DECIMAL(5,2),
        ValidationStatus NVARCHAR(50)
    );
    
    -- Dynamic SQL to check NULL values in all columns
    DECLARE column_cursor CURSOR FOR
    SELECT 
        COLUMN_NAME,
        IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = @TableName;
    
    DECLARE @ColumnName NVARCHAR(255);
    DECLARE @IsNullable NVARCHAR(3);
    DECLARE @NullCount INT;
    DECLARE @TotalRows INT;
    
    -- Get total row count
    SET @SQL = N'SELECT @TotalRows = COUNT(*) FROM ' + QUOTENAME(@TableName);
    EXEC sp_executesql @SQL, N'@TotalRows INT OUTPUT', @TotalRows OUTPUT;
    
    OPEN column_cursor;
    FETCH NEXT FROM column_cursor INTO @ColumnName, @IsNullable;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Count NULL values for each column
        SET @SQL = N'SELECT @NullCount = COUNT(*) FROM ' + QUOTENAME(@TableName) + 
                   N' WHERE ' + QUOTENAME(@ColumnName) + N' IS NULL';
        
        EXEC sp_executesql @SQL, N'@NullCount INT OUTPUT', @NullCount OUTPUT;
        
        -- Insert validation results
        INSERT INTO @ValidationReport (ColumnName, NullCount, TotalRows, NullPercentage, ValidationStatus)
        VALUES (
            @ColumnName,
            @NullCount,
            @TotalRows,
            CASE WHEN @TotalRows > 0 THEN (CAST(@NullCount AS DECIMAL(10,2)) / @TotalRows) * 100 ELSE 0 END,
            CASE 
                WHEN @IsNullable = 'NO' AND @NullCount > 0 THEN 'CONSTRAINT_VIOLATION'
                WHEN @IsNullable = 'YES' AND @NullCount = @TotalRows THEN 'ALL_NULL_WARNING'
                WHEN @NullCount = 0 THEN 'VALID'
                ELSE 'PARTIAL_NULL'
            END
        );
        
        FETCH NEXT FROM column_cursor INTO @ColumnName, @IsNullable;
    END;
    
    CLOSE column_cursor;
    DEALLOCATE column_cursor;
    
    -- Generate validation report
    SELECT 
        ColumnName,
        NullCount,
        TotalRows,
        NullPercentage,
        ValidationStatus,
        CASE ValidationStatus
            WHEN 'CONSTRAINT_VIOLATION' THEN 'ERROR: NOT NULL constraint violated'
            WHEN 'ALL_NULL_WARNING' THEN 'WARNING: All values are NULL'
            WHEN 'VALID' THEN 'OK: No NULL values found'
            WHEN 'PARTIAL_NULL' THEN 'INFO: Some NULL values present (allowed)'
        END AS ValidationMessage
    FROM @ValidationReport
    ORDER BY ValidationStatus DESC, NullPercentage DESC;
    
    -- Create summary output
    SET @ValidationResults = (
        SELECT 
            'Table: ' + @TableName + CHAR(13) + CHAR(10) +
            'Total Columns: ' + CAST(COUNT(*) AS NVARCHAR(10)) + CHAR(13) + CHAR(10) +
            'Columns with Violations: ' + CAST(SUM(CASE WHEN ValidationStatus = 'CONSTRAINT_VIOLATION' THEN 1 ELSE 0 END) AS NVARCHAR(10)) + CHAR(13) + CHAR(10) +
            'Columns with Warnings: ' + CAST(SUM(CASE WHEN ValidationStatus = 'ALL_NULL_WARNING' THEN 1 ELSE 0 END) AS NVARCHAR(10))
        FROM @ValidationReport
    );
END;
```

### NULL Validation Triggers

```sql
-- Trigger for comprehensive NULL validation on INSERT/UPDATE
CREATE TRIGGER TR_Customer_ValidateNulls
ON dbo.Customer
FOR INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Validate required fields are not NULL or empty
    IF EXISTS (
        SELECT 1 FROM inserted 
        WHERE FirstName IS NULL OR LEN(TRIM(FirstName)) = 0
           OR LastName IS NULL OR LEN(TRIM(LastName)) = 0
           OR Email IS NULL OR LEN(TRIM(Email)) = 0
    )
    BEGIN
        RAISERROR('Required fields (FirstName, LastName, Email) cannot be NULL or empty', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END;
    
    -- Validate email format (not just NULL check)
    IF EXISTS (
        SELECT 1 FROM inserted 
        WHERE Email NOT LIKE '%_@_%._%'
    )
    BEGIN
        RAISERROR('Email format is invalid', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END;
    
    -- Business rule: If phone is provided, it must be valid format
    IF EXISTS (
        SELECT 1 FROM inserted 
        WHERE Phone IS NOT NULL 
        AND Phone NOT LIKE '[0-9][0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
        AND Phone NOT LIKE '([0-9][0-9][0-9]) [0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
    )
    BEGIN
        RAISERROR('Phone format is invalid. Use XXX-XXX-XXXX or (XXX) XXX-XXXX format', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END;
    
    -- Log validation success
    INSERT INTO dbo.ValidationLog (TableName, ValidationDate, ValidationResult, RecordCount)
    SELECT 
        'Customer',
        GETDATE(),
        'NULL_VALIDATION_PASSED',
        COUNT(*)
    FROM inserted;
END;
```

### Dynamic NULL Validation Framework

```sql
-- Create a comprehensive NULL validation framework
CREATE TABLE dbo.ValidationRules (
    RuleID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(255) NOT NULL,
    ColumnName NVARCHAR(255) NOT NULL,
    ValidationRule NVARCHAR(MAX) NOT NULL,
    ErrorMessage NVARCHAR(500) NOT NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE()
);

-- Insert validation rules
INSERT INTO dbo.ValidationRules (TableName, ColumnName, ValidationRule, ErrorMessage) VALUES
('Customer', 'FirstName', 'FirstName IS NOT NULL AND LEN(TRIM(FirstName)) > 0', 'First name is required'),
('Customer', 'LastName', 'LastName IS NOT NULL AND LEN(TRIM(LastName)) > 0', 'Last name is required'),
('Customer', 'Email', 'Email IS NOT NULL AND Email LIKE ''%_@_%._%''', 'Valid email address is required'),
('Order', 'CustomerID', 'CustomerID IS NOT NULL AND CustomerID > 0', 'Customer ID is required'),
('Order', 'TotalAmount', 'TotalAmount IS NOT NULL AND TotalAmount > 0', 'Total amount must be positive');

-- Dynamic validation procedure
CREATE PROCEDURE ExecuteValidationRules
    @TableName NVARCHAR(255),
    @ValidationResults TABLE (
        ColumnName NVARCHAR(255),
        ValidationRule NVARCHAR(MAX),
        ErrorMessage NVARCHAR(500),
        ViolationCount INT,
        SampleViolations NVARCHAR(MAX)
    ) READONLY
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @ColumnName NVARCHAR(255);
    DECLARE @ValidationRule NVARCHAR(MAX);
    DECLARE @ErrorMessage NVARCHAR(500);
    DECLARE @ViolationCount INT;
    
    DECLARE validation_cursor CURSOR FOR
    SELECT ColumnName, ValidationRule, ErrorMessage
    FROM dbo.ValidationRules
    WHERE TableName = @TableName AND IsActive = 1;
    
    CREATE TABLE #ValidationResults (
        ColumnName NVARCHAR(255),
        ValidationRule NVARCHAR(MAX),
        ErrorMessage NVARCHAR(500),
        ViolationCount INT,
        SampleViolations NVARCHAR(MAX)
    );
    
    OPEN validation_cursor;
    FETCH NEXT FROM validation_cursor INTO @ColumnName, @ValidationRule, @ErrorMessage;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Count violations
        SET @SQL = N'SELECT @ViolationCount = COUNT(*) FROM ' + QUOTENAME(@TableName) + 
                   N' WHERE NOT (' + @ValidationRule + N')';
        
        EXEC sp_executesql @SQL, N'@ViolationCount INT OUTPUT', @ViolationCount OUTPUT;
        
        -- Get sample violations (first 5)
        DECLARE @SampleViolations NVARCHAR(MAX) = '';
        IF @ViolationCount > 0
        BEGIN
            SET @SQL = N'SELECT @SampleViolations = STRING_AGG(CAST(' + QUOTENAME(@ColumnName) + N' AS NVARCHAR(MAX)), '', '') ' +
                       N'FROM (SELECT TOP 5 ' + QUOTENAME(@ColumnName) + N' FROM ' + QUOTENAME(@TableName) + 
                       N' WHERE NOT (' + @ValidationRule + N')) AS Violations';
            
            EXEC sp_executesql @SQL, N'@SampleViolations NVARCHAR(MAX) OUTPUT', @SampleViolations OUTPUT;
        END;
        
        INSERT INTO #ValidationResults
        VALUES (@ColumnName, @ValidationRule, @ErrorMessage, @ViolationCount, @SampleViolations);
        
        FETCH NEXT FROM validation_cursor INTO @ColumnName, @ValidationRule, @ErrorMessage;
    END;
    
    CLOSE validation_cursor;
    DEALLOCATE validation_cursor;
    
    -- Return results
    SELECT * FROM #ValidationResults WHERE ViolationCount > 0;
    
    DROP TABLE #ValidationResults;
END;
```

---

## Azure Databricks Schema Validation

### Spark Schema Definition with NULL Constraints

```python
# Databricks notebook for schema validation with NULL checks
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
import datetime
from typing import List, Dict, Tuple

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NullValidation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

class SchemaValidator:
    """Comprehensive schema validation class for NULL value checks"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.validation_results = []
    
    def define_schema_with_null_constraints(self, table_name: str) -> StructType:
        """Define schema with explicit NULL/NOT NULL constraints"""
        
        schemas = {
            "customer": StructType([
                StructField("customer_id", IntegerType(), False),  # NOT NULL
                StructField("first_name", StringType(), False),    # NOT NULL
                StructField("last_name", StringType(), False),     # NOT NULL
                StructField("email", StringType(), False),         # NOT NULL
                StructField("phone", StringType(), True),          # NULLABLE
                StructField("date_of_birth", DateType(), True),    # NULLABLE
                StructField("created_date", TimestampType(), False), # NOT NULL
                StructField("is_active", BooleanType(), False)     # NOT NULL
            ]),
            
            "order": StructType([
                StructField("order_id", IntegerType(), False),     # NOT NULL
                StructField("customer_id", IntegerType(), False),  # NOT NULL
                StructField("order_date", TimestampType(), False), # NOT NULL
                StructField("total_amount", DecimalType(10, 2), False), # NOT NULL
                StructField("shipping_address", StringType(), True), # NULLABLE
                StructField("billing_address", StringType(), True),  # NULLABLE
                StructField("order_status", StringType(), False),    # NOT NULL
                StructField("shipping_date", TimestampType(), True), # NULLABLE
                StructField("delivery_date", TimestampType(), True)  # NULLABLE
            ]),
            
            "product": StructType([
                StructField("product_id", IntegerType(), False),   # NOT NULL
                StructField("product_name", StringType(), False),  # NOT NULL
                StructField("category", StringType(), False),      # NOT NULL
                StructField("price", DecimalType(10, 2), False),   # NOT NULL
                StructField("description", StringType(), True),    # NULLABLE
                StructField("weight", DoubleType(), True),         # NULLABLE
                StructField("dimensions", StringType(), True),     # NULLABLE
                StructField("is_available", BooleanType(), False)  # NOT NULL
            ])
        }
        
        return schemas.get(table_name.lower())
    
    def validate_dataframe_nulls(self, df: DataFrame, schema: StructType, table_name: str) -> Dict:
        """Validate DataFrame against schema NULL constraints"""
        
        print(f"Validating NULL constraints for table: {table_name}")
        
        validation_results = {
            "table_name": table_name,
            "total_rows": df.count(),
            "validation_timestamp": datetime.datetime.now(),
            "column_validations": [],
            "overall_status": "PASSED"
        }
        
        for field in schema.fields:
            column_name = field.name
            is_nullable = field.nullable
            
            # Count NULL values
            null_count = df.filter(col(column_name).isNull()).count()
            null_percentage = (null_count / validation_results["total_rows"]) * 100 if validation_results["total_rows"] > 0 else 0
            
            # Validate NOT NULL constraints
            constraint_violated = not is_nullable and null_count > 0
            
            column_validation = {
                "column_name": column_name,
                "is_nullable": is_nullable,
                "null_count": null_count,
                "null_percentage": round(null_percentage, 2),
                "constraint_violated": constraint_violated,
                "status": "FAILED" if constraint_violated else "PASSED"
            }
            
            if constraint_violated:
                validation_results["overall_status"] = "FAILED"
                print(f"❌ CONSTRAINT VIOLATION: Column '{column_name}' has {null_count} NULL values but is defined as NOT NULL")
            else:
                print(f"✅ Column '{column_name}': {null_count} NULL values ({'allowed' if is_nullable else 'none expected'})")
            
            validation_results["column_validations"].append(column_validation)
        
        return validation_results
    
    def create_validation_report(self, validation_results: Dict) -> DataFrame:
        """Create a detailed validation report as DataFrame"""
        
        report_data = []
        for col_val in validation_results["column_validations"]:
            report_data.append((
                validation_results["table_name"],
                col_val["column_name"],
                col_val["is_nullable"],
                col_val["null_count"],
                col_val["null_percentage"],
                col_val["constraint_violated"],
                col_val["status"],
                validation_results["validation_timestamp"]
            ))
        
        report_schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("column_name", StringType(), False),
            StructField("is_nullable", BooleanType(), False),
            StructField("null_count", IntegerType(), False),
            StructField("null_percentage", DoubleType(), False),
            StructField("constraint_violated", BooleanType(), False),
            StructField("validation_status", StringType(), False),
            StructField("validation_timestamp", TimestampType(), False)
        ])
        
        return self.spark.createDataFrame(report_data, report_schema)
    
    def apply_null_validation_rules(self, df: DataFrame, validation_rules: Dict) -> DataFrame:
        """Apply custom NULL validation rules to DataFrame"""
        
        validated_df = df
        
        for rule_name, rule_config in validation_rules.items():
            condition = rule_config["condition"]
            error_message = rule_config["error_message"]
            action = rule_config.get("action", "flag")  # flag, drop, or error
            
            print(f"Applying validation rule: {rule_name}")
            
            if action == "flag":
                # Add validation flag column
                validated_df = validated_df.withColumn(
                    f"validation_{rule_name}",
                    when(expr(condition), lit(True)).otherwise(lit(False))
                )
                
                # Count violations
                violation_count = validated_df.filter(col(f"validation_{rule_name}") == False).count()
                print(f"  Rule '{rule_name}': {violation_count} violations found")
                
            elif action == "drop":
                # Drop rows that violate the rule
                before_count = validated_df.count()
                validated_df = validated_df.filter(expr(condition))
                after_count = validated_df.count()
                dropped_count = before_count - after_count
                print(f"  Rule '{rule_name}': Dropped {dropped_count} rows")
                
            elif action == "error":
                # Check if any violations exist and raise error
                violation_count = validated_df.filter(~expr(condition)).count()
                if violation_count > 0:
                    raise ValueError(f"Validation rule '{rule_name}' failed: {error_message}. {violation_count} violations found.")
        
        return validated_df
    
    def generate_null_statistics(self, df: DataFrame) -> DataFrame:
        """Generate comprehensive NULL statistics for all columns"""
        
        total_rows = df.count()
        stats_data = []
        
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            non_null_count = total_rows - null_count
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
            
            # Get data type
            data_type = dict(df.dtypes)[column]
            
            # Calculate additional statistics for string columns
            if data_type in ['string']:
                empty_count = df.filter((col(column).isNull()) | (trim(col(column)) == "")).count()
                empty_percentage = (empty_count / total_rows) * 100 if total_rows > 0 else 0
            else:
                empty_count = null_count
                empty_percentage = null_percentage
            
            stats_data.append((
                column,
                data_type,
                total_rows,
                null_count,
                non_null_count,
                round(null_percentage, 2),
                empty_count,
                round(empty_percentage, 2)
            ))
        
        stats_schema = StructType([
            StructField("column_name", StringType(), False),
            StructField("data_type", StringType(), False),
            StructField("total_rows", IntegerType(), False),
            StructField("null_count", IntegerType(), False),
            StructField("non_null_count", IntegerType(), False),
            StructField("null_percentage", DoubleType(), False),
            StructField("empty_count", IntegerType(), False),
            StructField("empty_percentage", DoubleType(), False)
        ])
        
        return self.spark.createDataFrame(stats_data, stats_schema)

# Usage examples
validator = SchemaValidator(spark)

# Create sample data with NULL values for testing
sample_customer_data = [
    (1, "John", "Doe", "john.doe@email.com", "123-456-7890", datetime.date(1990, 1, 1), datetime.datetime.now(), True),
    (2, "Jane", None, "jane.smith@email.com", None, None, datetime.datetime.now(), True),  # NULL last_name
    (3, None, "Johnson", "bob.johnson@email.com", "987-654-3210", datetime.date(1985, 5, 15), datetime.datetime.now(), True),  # NULL first_name
    (4, "Alice", "Williams", None, "555-123-4567", datetime.date(1992, 8, 20), datetime.datetime.now(), True),  # NULL email
    (5, "Bob", "Brown", "bob.brown@email.com", None, datetime.date(1988, 3, 10), datetime.datetime.now(), True)  # NULL phone (allowed)
]

# Define schema
customer_schema = validator.define_schema_with_null_constraints("customer")

# Create DataFrame
customer_df = spark.createDataFrame(sample_customer_data, customer_schema)

# Validate NULL constraints
validation_results = validator.validate_dataframe_nulls(customer_df, customer_schema, "customer")

# Create validation report
report_df = validator.create_validation_report(validation_results)
report_df.show()

# Generate NULL statistics
stats_df = validator.generate_null_statistics(customer_df)
stats_df.show()
```

### Advanced NULL Validation Patterns

```python
# Advanced validation patterns for complex business rules
class AdvancedNullValidator:
    """Advanced NULL validation with business rules"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def validate_conditional_nulls(self, df: DataFrame, conditional_rules: List[Dict]) -> DataFrame:
        """Validate NULL values based on conditional business rules"""
        
        validated_df = df
        
        for rule in conditional_rules:
            rule_name = rule["name"]
            condition = rule["condition"]
            null_check = rule["null_check"]
            error_message = rule["error_message"]
            
            print(f"Applying conditional NULL rule: {rule_name}")
            
            # Create validation column
            validation_expr = f"""
                CASE 
                    WHEN {condition} THEN {null_check}
                    ELSE true
                END
            """
            
            validated_df = validated_df.withColumn(
                f"validation_{rule_name}",
                expr(validation_expr)
            )
            
            # Count violations
            violations = validated_df.filter(col(f"validation_{rule_name}") == False)
            violation_count = violations.count()
            
            if violation_count > 0:
                print(f"❌ Rule '{rule_name}': {violation_count} violations found")
                print(f"   Error: {error_message}")
                
                # Show sample violations
                print("   Sample violations:")
                violations.select("*").show(5)
            else:
                print(f"✅ Rule '{rule_name}': No violations found")
        
        return validated_df
    
    def validate_cross_column_nulls(self, df: DataFrame, cross_column_rules: List[Dict]) -> DataFrame:
        """Validate NULL values across multiple columns"""
        
        validated_df = df
        
        for rule in cross_column_rules:
            rule_name = rule["name"]
            columns = rule["columns"]
            validation_logic = rule["validation_logic"]
            error_message = rule["error_message"]
            
            print(f"Applying cross-column NULL rule: {rule_name}")
            
            # Apply validation logic
            if validation_logic == "at_least_one_not_null":
                # At least one of the specified columns must not be NULL
                null_conditions = [col(column).isNull() for column in columns]
                validation_condition = ~reduce(lambda a, b: a & b, null_conditions)
                
            elif validation_logic == "all_or_none":
                # Either all columns are NULL or none are NULL
                null_conditions = [col(column).isNull() for column in columns]
                all_null = reduce(lambda a, b: a & b, null_conditions)
                none_null = reduce(lambda a, b: a & b, [~condition for condition in null_conditions])
                validation_condition = all_null | none_null
                
            elif validation_logic == "mutually_exclusive":
                # Only one of the columns can be non-NULL
                non_null_count = sum([when(col(column).isNotNull(), 1).otherwise(0) for column in columns])
                validation_condition = non_null_count <= 1
                
            else:
                # Custom validation logic
                validation_condition = expr(validation_logic)
            
            validated_df = validated_df.withColumn(
                f"validation_{rule_name}",
                validation_condition
            )
            
            # Count violations
            violation_count = validated_df.filter(col(f"validation_{rule_name}") == False).count()
            
            if violation_count > 0:
                print(f"❌ Rule '{rule_name}': {violation_count} violations found")
                print(f"   Error: {error_message}")
            else:
                print(f"✅ Rule '{rule_name}': No violations found")
        
        return validated_df
    
    def create_data_quality_dashboard(self, validation_results: List[Dict]) -> None:
        """Create a comprehensive data quality dashboard"""
        
        # Aggregate validation results
        dashboard_data = []
        
        for result in validation_results:
            table_name = result["table_name"]
            total_rows = result["total_rows"]
            overall_status = result["overall_status"]
            
            failed_columns = [col for col in result["column_validations"] if col["status"] == "FAILED"]
            passed_columns = [col for col in result["column_validations"] if col["status"] == "PASSED"]
            
            dashboard_data.append({
                "table_name": table_name,
                "total_rows": total_rows,
                "total_columns": len(result["column_validations"]),
                "failed_columns": len(failed_columns),
                "passed_columns": len(passed_columns),
                "success_rate": (len(passed_columns) / len(result["column_validations"])) * 100,
                "overall_status": overall_status,
                "validation_timestamp": result["validation_timestamp"]
            })
        
        # Create dashboard DataFrame
        dashboard_df = self.spark.createDataFrame(dashboard_data)
        
        # Display dashboard
        print("=" * 80)
        print("DATA QUALITY DASHBOARD - NULL VALIDATION SUMMARY")
        print("=" * 80)
        
        dashboard_df.select(
            "table_name",
            "total_rows",
            "total_columns",
            "failed_columns",
            "passed_columns",
            round("success_rate", 2).alias("success_rate_%"),
            "overall_status"
        ).show(truncate=False)
        
        # Summary statistics
        total_tables = len(validation_results)
        passed_tables = len([r for r in validation_results if r["overall_status"] == "PASSED"])
        failed_tables = total_tables - passed_tables
        
        print(f"\nSUMMARY:")
        print(f"Total Tables Validated: {total_tables}")
        print(f"Tables Passed: {passed_tables}")
        print(f"Tables Failed: {failed_tables}")
        print(f"Overall Success Rate: {(passed_tables / total_tables) * 100:.2f}%")

# Usage examples for advanced validation
advanced_validator = AdvancedNullValidator(spark)

# Define conditional NULL validation rules
conditional_rules = [
    {
        "name": "shipped_order_must_have_shipping_date",
        "condition": "order_status IN ('Shipped', 'Delivered')",
        "null_check": "shipping_date IS NOT NULL",
        "error_message": "Orders with status 'Shipped' or 'Delivered' must have a shipping date"
    },
    {
        "name": "delivery_requires_shipping",
        "condition": "delivery_date IS NOT NULL",
        "null_check": "shipping_date IS NOT NULL",
        "error_message": "Orders with delivery date must have shipping date"
    }
]

# Define cross-column NULL validation rules
cross_column_rules = [
    {
        "name": "address_requirement",
        "columns": ["shipping_address", "billing_address"],
        "validation_logic": "at_least_one_not_null",
        "error_message": "Either shipping address or billing address must be provided"
    },
    {
        "name": "contact_info_consistency",
        "columns": ["phone", "email"],
        "validation_logic": "at_least_one_not_null",
        "error_message": "At least one contact method (phone or email) must be provided"
    }
]

# Apply advanced validations (assuming we have an order DataFrame)
# validated_order_df = advanced_validator.validate_conditional_nulls(order_df, conditional_rules)
# validated_order_df = advanced_validator.validate_cross_column_nulls(validated_order_df, cross_column_rules)
```

### Delta Lake Schema Evolution with NULL Constraints

```python
# Delta Lake schema evolution with NULL constraint management
from delta.tables import *

class DeltaSchemaManager:
    """Manage Delta Lake schemas with NULL constraints"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def create_delta_table_with_constraints(self, table_path: str, schema: StructType, 
                                          constraints: Dict[str, str]) -> None:
        """Create Delta table with NULL constraints"""
        
        # Create empty DataFrame with schema
        empty_df = self.spark.createDataFrame([], schema)
        
        # Write initial Delta table
        empty_df.write.format("delta").mode("overwrite").save(table_path)
        
        # Add constraints
        delta_table = DeltaTable.forPath(self.spark, table_path)
        
        for constraint_name, constraint_expression in constraints.items():
            try:
                # Add CHECK constraint (Delta Lake 2.0+)
                self.spark.sql(f"""
                    ALTER TABLE delta.`{table_path}` 
                    ADD CONSTRAINT {constraint_name} 
                    CHECK ({constraint_expression})
                """)
                print(f"✅ Added constraint '{constraint_name}': {constraint_expression}")
            except Exception as e:
                print(f"❌ Failed to add constraint '{constraint_name}': {str(e)}")
    
    def validate_before_merge(self, source_df: DataFrame, target_path: str, 
                            validation_rules: List[Dict]) -> DataFrame:
        """Validate source data before merging into Delta table"""
        
        print("Validating source data before merge...")
        
        validated_df = source_df
        validation_summary = {"passed": 0, "failed": 0, "total_rules": len(validation_rules)}
        
        for rule in validation_rules:
            rule_name = rule["name"]
            condition = rule["condition"]
            action = rule.get("action", "flag")
            
            if action == "reject":
                # Reject rows that don't meet the condition
                before_count = validated_df.count()
                validated_df = validated_df.filter(expr(condition))
                after_count = validated_df.count()
                rejected_count = before_count - after_count
                
                if rejected_count > 0:
                    print(f"❌ Rule '{rule_name}': Rejected {rejected_count} rows")
                    validation_summary["failed"] += 1
                else:
                    print(f"✅ Rule '{rule_name}': All rows passed")
                    validation_summary["passed"] += 1
            
            elif action == "flag":
                # Add validation flag
                validated_df = validated_df.withColumn(
                    f"validation_{rule_name}",
                    expr(condition)
                )
                
                violation_count = validated_df.filter(col(f"validation_{rule_name}") == False).count()
                if violation_count > 0:
                    print(f"⚠️  Rule '{rule_name}': {violation_count} violations flagged")
                    validation_summary["failed"] += 1
                else:
                    print(f"✅ Rule '{rule_name}': No violations")
                    validation_summary["passed"] += 1
        
        print(f"\nValidation Summary: {validation_summary['passed']}/{validation_summary['total_rules']} rules passed")
        return validated_df
    
    def safe_merge_with_validation(self, source_df: DataFrame, target_path: str, 
                                 merge_keys: List[str], validation_rules: List[Dict]) -> None:
        """Perform safe merge with pre-validation"""
        
        # Validate source data
        validated_df = self.validate_before_merge(source_df, target_path, validation_rules)
        
        # Load target Delta table
        target_table = DeltaTable.forPath(self.spark, target_path)
        
        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Perform merge with error handling
        try:
            target_table.alias("target").merge(
                validated_df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            print("✅ Merge completed successfully")
            
        except Exception as e:
            print(f"❌ Merge failed: {str(e)}")
            
            # Log failed merge attempt
            self.log_validation_failure(target_path, str(e), validated_df.count())
    
    def log_validation_failure(self, table_path: str, error_message: str, record_count: int) -> None:
        """Log validation failures for monitoring"""
        
        failure_log = self.spark.createDataFrame([
            (table_path, error_message, record_count, datetime.datetime.now())
        ], ["table_path", "error_message", "record_count", "failure_timestamp"])
        
        # Write to failure log table (create if doesn't exist)
        failure_log_path = "/mnt/datalake/logs/validation_failures"
        
        failure_log.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(failure_log_path)

# Usage example
delta_manager = DeltaSchemaManager(spark)

# Define constraints for Delta table
customer_constraints = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "first_name_not_null": "first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 0",
    "last_name_not_null": "last_name IS NOT NULL AND LENGTH(TRIM(last_name)) > 0",
    "email_not_null": "email IS NOT NULL AND email LIKE '%@%.%'",
    "customer_id_positive": "customer_id > 0",
    "is_active_not_null": "is_active IS NOT NULL"
}

# Create Delta table with constraints
customer_table_path = "/mnt/datalake/tables/customers"
customer_schema = validator.define_schema_with_null_constraints("customer")

delta_manager.create_delta_table_with_constraints(
    customer_table_path, 
    customer_schema, 
    customer_constraints
)

# Define validation rules for merging
merge_validation_rules = [
    {
        "name": "required_fields_not_null",
        "condition": "customer_id IS NOT NULL AND first_name IS NOT NULL AND last_name IS NOT NULL AND email IS NOT NULL",
        "action": "reject"
    },
    {
        "name": "valid_email_format",
        "condition": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
        "action": "reject"
    },
    {
        "name": "positive_customer_id",
        "condition": "customer_id > 0",
        "action": "reject"
    }
]

# Perform safe merge with validation
# delta_manager.safe_merge_with_validation(
#     source_df=new_customer_data,
#     target_path=customer_table_path,
#     merge_keys=["customer_id"],
#     validation_rules=merge_validation_rules
# )
```

---

## Azure Data Factory Data Validation

### Data Flow NULL Validation

```json
{
  "name": "NullValidationDataFlow",
  "properties": {
    "type": "MappingDataFlow",
    "typeProperties": {
      "sources": [
        {
          "name": "SourceData",
          "dataset": {
            "referenceName": "SourceDataset",
            "type": "DatasetReference"
          }
        }
      ],
      "sinks": [
        {
          "name": "ValidData",
          "dataset": {
            "referenceName": "ValidDataSink",
            "type": "DatasetReference"
          }
        },
        {
          "name": "InvalidData",
          "dataset": {
            "referenceName": "InvalidDataSink",
            "type": "DatasetReference"
          }
        }
      ],
      "transformations": [
        {
          "name": "AddValidationColumns",
          "type": "DerivedColumn",
          "typeProperties": {
            "columns": [
              {
                "name": "is_first_name_valid",
                "expression": "!isNull(first_name) && length(trim(first_name)) > 0"
              },
              {
                "name": "is_last_name_valid",
                "expression": "!isNull(last_name) && length(trim(last_name)) > 0"
              },
              {
                "name": "is_email_valid",
                "expression": "!isNull(email) && regexMatch(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')"
              },
              {
                "name": "is_customer_id_valid",
                "expression": "!isNull(customer_id) && customer_id > 0"
              },
              {
                "name": "validation_score",
                "expression": "(toInteger(is_first_name_valid) + toInteger(is_last_name_valid) + toInteger(is_email_valid) + toInteger(is_customer_id_valid)) / 4.0"
              },
              {
                "name": "is_record_valid",
                "expression": "validation_score >= 1.0"
              },
              {
                "name": "validation_errors",
                "expression": "case(!is_first_name_valid, 'First name is required; ', '') + case(!is_last_name_valid, 'Last name is required; ', '') + case(!is_email_valid, 'Valid email is required; ', '') + case(!is_customer_id_valid, 'Valid customer ID is required; ', '')"
              }
            ]
          }
        },
        {
          "name": "SplitValidInvalid",
          "type": "ConditionalSplit",
          "typeProperties": {
            "conditions": [
              {
                "name": "ValidRecords",
                "expression": "is_record_valid == true()"
              },
              {
                "name": "InvalidRecords",
                "expression": "is_record_valid == false()"
              }
            ]
          }
        }
      ]
    }
  }
}
```

### Pipeline with Comprehensive NULL Validation

```json
{
  "name": "ComprehensiveNullValidationPipeline",
  "properties": {
    "parameters": {
      "sourceTableName": {
        "type": "String"
      },
      "validationThreshold": {
        "type": "Float",
        "defaultValue": 0.95
      }
    },
    "activities": [
      {
        "name": "GetSourceRowCount",
        "type": "Lookup",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT COUNT(*) as TotalRows FROM @{pipeline().parameters.sourceTableName}"
          },
          "dataset": {
            "referenceName": "SourceDataset",
            "type": "DatasetReference"
          }
        }
      },
      {
        "name": "ValidateNullConstraints",
        "type": "Lookup",
        "dependsOn": [
          {
            "activity": "GetSourceRowCount",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": {
              "value": "EXEC ValidateTableNullConstraints '@{pipeline().parameters.sourceTableName}'",
              "type": "Expression"
            }
          },
          "dataset": {
            "referenceName": "ValidationDataset",
            "type": "DatasetReference"
          },
          "firstRowOnly": false
        }
      },
      {
        "name": "CheckValidationResults",
        "type": "ForEach",
        "dependsOn": [
          {
            "activity": "ValidateNullConstraints",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "items": {
            "value": "@activity('ValidateNullConstraints').output.value",
            "type": "Expression"
          },
          "activities": [
            {
              "name": "ProcessValidationResult",
              "type": "IfCondition",
              "typeProperties": {
                "expression": {
                  "value": "@equals(item().ValidationStatus, 'CONSTRAINT_VIOLATION')",
                  "type": "Expression"
                },
                "ifTrueActivities": [
                  {
                    "name": "LogValidationError",
                    "type": "SqlServerStoredProcedure",
                    "typeProperties": {
                      "storedProcedureName": "LogValidationError",
                      "storedProcedureParameters": {
                        "TableName": {
                          "value": "@pipeline().parameters.sourceTableName",
                          "type": "String"
                        },
                        "ColumnName": {
                          "value": "@item().ColumnName",
                          "type": "String"
                        },
                        "ErrorType": {
                          "value": "NULL_CONSTRAINT_VIOLATION",
                          "type": "String"
                        },
                        "ErrorMessage": {
                          "value": "@item().ValidationMessage",
                          "type": "String"
                        },
                        "NullCount": {
                          "value": "@item().NullCount",
                          "type": "Int32"
                        }
                      }
                    },
                    "linkedServiceName": {
                      "referenceName": "AzureSqlDatabase",
                      "type": "LinkedServiceReference"
                    }
                  },
                  {
                    "name": "SendAlertEmail",
                    "type": "WebActivity",
                    "typeProperties": {
                      "url": "https://logic-app-url.com/trigger",
                      "method": "POST",
                      "body": {
                        "table": "@pipeline().parameters.sourceTableName",
                        "column": "@item().ColumnName",
                        "error": "@item().ValidationMessage",
                        "nullCount": "@item().NullCount",
                        "timestamp": "@utcnow()"
                      }
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      {
        "name": "CalculateOverallValidation",
        "type": "SetVariable",
        "dependsOn": [
          {
            "activity": "CheckValidationResults",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "variableName": "validationScore",
          "value": {
            "value": "@div(sub(length(activity('ValidateNullConstraints').output.value), length(filter(activity('ValidateNullConstraints').output.value, equals(item().ValidationStatus, 'CONSTRAINT_VIOLATION')))), length(activity('ValidateNullConstraints').output.value))",
            "type": "Expression"
          }
        }
      },
      {
        "name": "CheckValidationThreshold",
        "type": "IfCondition",
        "dependsOn": [
          {
            "activity": "CalculateOverallValidation",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "expression": {
            "value": "@greater(variables('validationScore'), pipeline().parameters.validationThreshold)",
            "type": "Expression"
          },
          "ifTrueActivities": [
            {
              "name": "ProceedWithDataProcessing",
              "type": "ExecutePipeline",
              "typeProperties": {
                "pipeline": {
                  "referenceName": "DataProcessingPipeline",
                  "type": "PipelineReference"
                },
                "parameters": {
                  "sourceTable": "@pipeline().parameters.sourceTableName"
                }
              }
            }
          ],
          "ifFalseActivities": [
            {
              "name": "FailPipeline",
              "type": "Fail",
              "typeProperties": {
                "message": "Data validation failed. Validation score: @{variables('validationScore')} is below threshold: @{pipeline().parameters.validationThreshold}",
                "errorCode": "VALIDATION_FAILED"
              }
            }
          ]
        }
      }
    ],
    "variables": {
      "validationScore": {
        "type": "Float",
        "defaultValue": 0.0
      }
    }
  }
}
```

### Custom Validation Activity

```json
{
  "name": "CustomNullValidationActivity",
  "type": "AzureFunction",
  "typeProperties": {
    "functionName": "ValidateNullConstraints",
    "method": "POST",
    "body": {
      "tableName": "@pipeline().parameters.sourceTableName",
      "connectionString": "@linkedService().connectionString",
      "validationRules": [
        {
          "columnName": "customer_id",
          "rule": "NOT_NULL",
          "required": true
        },
        {
          "columnName": "first_name",
          "rule": "NOT_NULL_AND_NOT_EMPTY",
          "required": true
        },
        {
          "columnName": "last_name",
          "rule": "NOT_NULL_AND_NOT_EMPTY",
          "required": true
        },
        {
          "columnName": "email",
          "rule": "NOT_NULL_AND_VALID_FORMAT",
          "required": true,
          "format": "email"
        },
        {
          "columnName": "phone",
          "rule": "NULLABLE",
          "required": false
        }
      ]
    }
  },
  "linkedServiceName": {
    "referenceName": "AzureFunctionService",
    "type": "LinkedServiceReference"
  }
}
```

---

## Best Practices

### Implementation Guidelines

```json
{
  "null_validation_best_practices": {
    "schema_design": [
      "Define NOT NULL constraints at table creation time",
      "Use appropriate default values for required fields",
      "Document business rules for nullable fields",
      "Implement cascading validation rules",
      "Use consistent naming conventions for validation columns"
    ],
    "validation_strategy": [
      "Implement validation at multiple layers (database, application, ETL)",
      "Use schema evolution strategies for existing data",
      "Create comprehensive validation reports",
      "Implement automated validation monitoring",
      "Establish clear error handling procedures"
    ],
    "performance_optimization": [
      "Index columns used in NULL validation queries",
      "Use batch validation for large datasets",
      "Implement parallel validation where possible",
      "Cache validation results for repeated checks",
      "Optimize validation queries for better performance"
    ],
    "monitoring_and_alerting": [
      "Set up real-time validation monitoring",
      "Create dashboards for validation metrics",
      "Implement automated alerting for validation failures",
      "Track validation trends over time",
      "Establish SLAs for data quality"
    ],
    "error_handling": [
      "Implement graceful error handling for validation failures",
      "Create quarantine processes for invalid data",
      "Provide detailed error messages for troubleshooting",
      "Implement retry mechanisms for transient failures",
      "Log all validation activities for audit trails"
    ]
  }
}
```

This comprehensive guide provides everything needed to implement robust NULL value validation across Azure data platforms, ensuring data quality and integrity at every layer of your data architecture.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*