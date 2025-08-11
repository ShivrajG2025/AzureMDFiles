# Azure Partitioning Comprehensive Guide
## Complete Guide to All Partitioning Types in Azure

---

### Table of Contents

1. [Overview](#overview)
2. [Azure SQL Database Partitioning](#azure-sql-database-partitioning)
3. [Azure Cosmos DB Partitioning](#azure-cosmos-db-partitioning)
4. [Azure Databricks/Spark Partitioning](#azure-databricksspark-partitioning)
5. [Azure Data Lake Partitioning](#azure-data-lake-partitioning)
6. [Azure Synapse Analytics Partitioning](#azure-synapse-analytics-partitioning)
7. [Azure Service Bus Partitioning](#azure-service-bus-partitioning)
8. [Azure Event Hubs Partitioning](#azure-event-hubs-partitioning)
9. [Azure Table Storage Partitioning](#azure-table-storage-partitioning)
10. [Performance Optimization](#performance-optimization)
11. [Best Practices](#best-practices)

---

## Overview

Partitioning is a fundamental concept in Azure that enables horizontal scaling, improves performance, and provides better data management across various Azure services. This guide covers all partitioning strategies available in Azure with practical examples.

### Partitioning Benefits

```json
{
  "partitioning_benefits": {
    "performance": {
      "parallel_processing": "Multiple partitions can be processed simultaneously",
      "query_optimization": "Queries can target specific partitions (partition elimination)",
      "reduced_contention": "Distributed workload reduces resource contention",
      "faster_maintenance": "Operations like backup, restore can be parallelized"
    },
    "scalability": {
      "horizontal_scaling": "Scale out by adding more partitions",
      "elastic_capacity": "Dynamically adjust partition count based on load",
      "distributed_storage": "Data distributed across multiple storage units",
      "load_distribution": "Even distribution of read/write operations"
    },
    "availability": {
      "fault_isolation": "Failure in one partition doesn't affect others",
      "maintenance_windows": "Rolling maintenance across partitions",
      "geo_distribution": "Partitions can be distributed across regions",
      "backup_strategies": "Independent backup and recovery per partition"
    },
    "cost_optimization": {
      "storage_tiering": "Different partitions can use different storage tiers",
      "compute_optimization": "Allocate resources based on partition usage",
      "archival_strategies": "Move old partitions to cheaper storage",
      "pay_per_use": "Pay only for active partitions"
    }
  }
}
```

### Partitioning Types Overview

```json
{
  "azure_partitioning_types": {
    "horizontal_partitioning": {
      "description": "Split data rows across multiple tables/containers",
      "also_known_as": "Sharding",
      "use_cases": ["Large datasets", "High throughput applications", "Multi-tenant systems"],
      "azure_services": ["SQL Database", "Cosmos DB", "Table Storage"]
    },
    "vertical_partitioning": {
      "description": "Split table columns across multiple tables",
      "also_known_as": "Column splitting",
      "use_cases": ["Wide tables", "Different access patterns", "Security isolation"],
      "azure_services": ["SQL Database", "Synapse Analytics"]
    },
    "functional_partitioning": {
      "description": "Split data by business function or feature",
      "also_known_as": "Service partitioning",
      "use_cases": ["Microservices architecture", "Domain-driven design", "Service isolation"],
      "azure_services": ["Multiple databases", "Separate Cosmos DB containers"]
    },
    "range_partitioning": {
      "description": "Split data based on value ranges",
      "partition_key": "Date ranges, numeric ranges, alphabetical ranges",
      "use_cases": ["Time-series data", "Sequential data", "Ordered datasets"],
      "azure_services": ["SQL Database", "Data Lake", "Synapse Analytics"]
    },
    "hash_partitioning": {
      "description": "Split data using hash function on partition key",
      "partition_key": "Hash of primary key or specific column",
      "use_cases": ["Even distribution", "Random access patterns", "Load balancing"],
      "azure_services": ["Cosmos DB", "Event Hubs", "Service Bus"]
    },
    "list_partitioning": {
      "description": "Split data based on predefined list of values",
      "partition_key": "Specific values like regions, categories, status",
      "use_cases": ["Categorical data", "Geographic distribution", "Business units"],
      "azure_services": ["SQL Database", "Data Lake", "Spark"]
    }
  }
}
```

---

## Azure SQL Database Partitioning

### Horizontal Partitioning (Sharding)

```sql
-- Example: E-commerce database with customer sharding
-- Create partition function for customer ID ranges
CREATE PARTITION FUNCTION CustomerPartitionFunction (INT)
AS RANGE RIGHT FOR VALUES (100000, 200000, 300000, 400000, 500000);

-- Create partition scheme
CREATE PARTITION SCHEME CustomerPartitionScheme
AS PARTITION CustomerPartitionFunction
TO ([PRIMARY], [FG1], [FG2], [FG3], [FG4], [FG5]);

-- Create partitioned table
CREATE TABLE dbo.Customers (
    CustomerID INT NOT NULL,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(255) NOT NULL,
    Phone NVARCHAR(20),
    Address NVARCHAR(500),
    City NVARCHAR(100),
    State NVARCHAR(50),
    ZipCode NVARCHAR(10),
    Country NVARCHAR(50),
    DateCreated DATETIME2 DEFAULT GETDATE(),
    LastModified DATETIME2 DEFAULT GETDATE(),
    IsActive BIT DEFAULT 1,
    
    CONSTRAINT PK_Customers PRIMARY KEY (CustomerID)
) ON CustomerPartitionScheme(CustomerID);

-- Create partitioned Orders table
CREATE TABLE dbo.Orders (
    OrderID BIGINT IDENTITY(1,1) NOT NULL,
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 DEFAULT GETDATE(),
    TotalAmount DECIMAL(10,2) NOT NULL,
    OrderStatus NVARCHAR(50) DEFAULT 'Pending',
    ShippingAddress NVARCHAR(500),
    BillingAddress NVARCHAR(500),
    PaymentMethod NVARCHAR(50),
    TrackingNumber NVARCHAR(100),
    EstimatedDelivery DATETIME2,
    ActualDelivery DATETIME2,
    CreatedBy NVARCHAR(100),
    ModifiedBy NVARCHAR(100),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT PK_Orders PRIMARY KEY (OrderID, CustomerID),
    CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
) ON CustomerPartitionScheme(CustomerID);

-- Insert sample data to demonstrate partitioning
INSERT INTO dbo.Customers (CustomerID, FirstName, LastName, Email, Phone, City, State, Country)
VALUES 
    -- Partition 1: CustomerID < 100000
    (50000, 'John', 'Doe', 'john.doe@email.com', '123-456-7890', 'New York', 'NY', 'USA'),
    (75000, 'Jane', 'Smith', 'jane.smith@email.com', '234-567-8901', 'Los Angeles', 'CA', 'USA'),
    
    -- Partition 2: 100000 <= CustomerID < 200000
    (150000, 'Bob', 'Johnson', 'bob.johnson@email.com', '345-678-9012', 'Chicago', 'IL', 'USA'),
    (175000, 'Alice', 'Williams', 'alice.williams@email.com', '456-789-0123', 'Houston', 'TX', 'USA'),
    
    -- Partition 3: 200000 <= CustomerID < 300000
    (250000, 'Charlie', 'Brown', 'charlie.brown@email.com', '567-890-1234', 'Phoenix', 'AZ', 'USA'),
    (275000, 'Diana', 'Davis', 'diana.davis@email.com', '678-901-2345', 'Philadelphia', 'PA', 'USA');

-- Insert corresponding orders
INSERT INTO dbo.Orders (CustomerID, TotalAmount, OrderStatus, ShippingAddress)
VALUES 
    (50000, 299.99, 'Completed', '123 Main St, New York, NY 10001'),
    (50000, 149.50, 'Shipped', '123 Main St, New York, NY 10001'),
    (150000, 89.99, 'Pending', '456 Oak Ave, Chicago, IL 60601'),
    (250000, 199.99, 'Processing', '789 Pine St, Phoenix, AZ 85001');
```

### Advanced Partitioning with Date Ranges

```sql
-- Time-based partitioning for large transaction tables
CREATE PARTITION FUNCTION TransactionDatePartitionFunction (DATETIME2)
AS RANGE RIGHT FOR VALUES 
    ('2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', 
     '2023-05-01', '2023-06-01', '2023-07-01', '2023-08-01',
     '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01', '2024-01-01');

CREATE PARTITION SCHEME TransactionDatePartitionScheme
AS PARTITION TransactionDatePartitionFunction
TO ([FG_2022], [FG_2023_Q1_Jan], [FG_2023_Q1_Feb], [FG_2023_Q1_Mar],
    [FG_2023_Q2_Apr], [FG_2023_Q2_May], [FG_2023_Q2_Jun],
    [FG_2023_Q3_Jul], [FG_2023_Q3_Aug], [FG_2023_Q3_Sep],
    [FG_2023_Q4_Oct], [FG_2023_Q4_Nov], [FG_2023_Q4_Dec], [FG_2024]);

-- Create comprehensive transaction table
CREATE TABLE dbo.Transactions (
    TransactionID BIGINT IDENTITY(1,1) NOT NULL,
    CustomerID INT NOT NULL,
    OrderID BIGINT,
    TransactionDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    TransactionType NVARCHAR(50) NOT NULL, -- 'Purchase', 'Refund', 'Transfer'
    Amount DECIMAL(15,2) NOT NULL,
    Currency CHAR(3) DEFAULT 'USD',
    PaymentMethod NVARCHAR(50), -- 'Credit Card', 'PayPal', 'Bank Transfer'
    PaymentProvider NVARCHAR(100),
    TransactionStatus NVARCHAR(50) DEFAULT 'Pending', -- 'Completed', 'Failed', 'Cancelled'
    Description NVARCHAR(500),
    ReferenceNumber NVARCHAR(100),
    ProcessingFee DECIMAL(10,2) DEFAULT 0,
    TaxAmount DECIMAL(10,2) DEFAULT 0,
    DiscountAmount DECIMAL(10,2) DEFAULT 0,
    NetAmount AS (Amount - ProcessingFee - DiscountAmount + TaxAmount) PERSISTED,
    
    -- Audit fields
    CreatedBy NVARCHAR(100) DEFAULT SYSTEM_USER,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedBy NVARCHAR(100),
    ModifiedDate DATETIME2,
    
    -- Partition key must be part of primary key
    CONSTRAINT PK_Transactions PRIMARY KEY (TransactionID, TransactionDate),
    CONSTRAINT FK_Transactions_Customers FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID),
    CONSTRAINT CK_Transactions_Amount CHECK (Amount <> 0),
    CONSTRAINT CK_Transactions_Currency CHECK (Currency IN ('USD', 'EUR', 'GBP', 'CAD', 'AUD'))
) ON TransactionDatePartitionScheme(TransactionDate);

-- Create indexes on partitioned table
CREATE NONCLUSTERED INDEX IX_Transactions_CustomerID_Date
ON dbo.Transactions (CustomerID, TransactionDate DESC)
ON TransactionDatePartitionScheme(TransactionDate);

CREATE NONCLUSTERED INDEX IX_Transactions_Status_Date
ON dbo.Transactions (TransactionStatus, TransactionDate DESC)
ON TransactionDatePartitionScheme(TransactionDate);

-- Insert sample transaction data across different partitions
INSERT INTO dbo.Transactions (CustomerID, TransactionDate, TransactionType, Amount, PaymentMethod, TransactionStatus, Description)
VALUES 
    -- January 2023 partition
    (50000, '2023-01-15 10:30:00', 'Purchase', 299.99, 'Credit Card', 'Completed', 'Online purchase - Electronics'),
    (75000, '2023-01-20 14:45:00', 'Purchase', 149.50, 'PayPal', 'Completed', 'Online purchase - Clothing'),
    
    -- March 2023 partition
    (150000, '2023-03-10 09:15:00', 'Purchase', 89.99, 'Credit Card', 'Completed', 'Online purchase - Books'),
    (150000, '2023-03-12 16:20:00', 'Refund', -25.00, 'Credit Card', 'Completed', 'Partial refund - Damaged item'),
    
    -- June 2023 partition
    (250000, '2023-06-05 11:30:00', 'Purchase', 199.99, 'Bank Transfer', 'Completed', 'Online purchase - Home & Garden'),
    (275000, '2023-06-18 13:45:00', 'Purchase', 75.50, 'Credit Card', 'Completed', 'Online purchase - Sports'),
    
    -- December 2023 partition
    (50000, '2023-12-24 20:15:00', 'Purchase', 449.99, 'Credit Card', 'Completed', 'Holiday purchase - Electronics'),
    (150000, '2023-12-31 23:59:00', 'Purchase', 99.99, 'PayPal', 'Completed', 'Year-end purchase - Subscription');
```

### Partition Management Procedures

```sql
-- Comprehensive partition management framework
CREATE PROCEDURE ManagePartitions
    @TableName NVARCHAR(255),
    @Action NVARCHAR(50), -- 'ADD', 'SPLIT', 'MERGE', 'SWITCH', 'ANALYZE'
    @PartitionValue SQL_VARIANT = NULL,
    @TargetFileGroup NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @PartitionScheme NVARCHAR(255);
    DECLARE @PartitionFunction NVARCHAR(255);
    
    -- Get partition scheme and function for the table
    SELECT 
        @PartitionScheme = ps.name,
        @PartitionFunction = pf.name
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
    INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
    WHERE t.name = @TableName;
    
    IF @PartitionScheme IS NULL
    BEGIN
        RAISERROR('Table %s is not partitioned or not found', 16, 1, @TableName);
        RETURN;
    END;
    
    -- Execute based on action
    IF @Action = 'ADD'
    BEGIN
        -- Add new filegroup if specified
        IF @TargetFileGroup IS NOT NULL
        BEGIN
            SET @SQL = N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + 
                      N' ADD FILEGROUP ' + QUOTENAME(@TargetFileGroup);
            EXEC sp_executesql @SQL;
            
            -- Add file to filegroup (simplified - adjust path as needed)
            SET @SQL = N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + 
                      N' ADD FILE (NAME = ''' + @TargetFileGroup + '_Data'', ' +
                      N'FILENAME = ''C:\Data\' + @TargetFileGroup + '.ndf'', ' +
                      N'SIZE = 100MB, FILEGROWTH = 10MB) TO FILEGROUP ' + QUOTENAME(@TargetFileGroup);
            EXEC sp_executesql @SQL;
        END;
        
        -- Add partition to scheme
        SET @SQL = N'ALTER PARTITION SCHEME ' + QUOTENAME(@PartitionScheme) + 
                  N' NEXT USED ' + ISNULL(QUOTENAME(@TargetFileGroup), '[PRIMARY]');
        EXEC sp_executesql @SQL;
        
        -- Split partition function
        SET @SQL = N'ALTER PARTITION FUNCTION ' + QUOTENAME(@PartitionFunction) + 
                  N'() SPLIT RANGE (@PartitionValue)';
        EXEC sp_executesql @SQL, N'@PartitionValue SQL_VARIANT', @PartitionValue;
        
        PRINT 'Successfully added partition with boundary value: ' + CAST(@PartitionValue AS NVARCHAR(50));
    END
    ELSE IF @Action = 'ANALYZE'
    BEGIN
        -- Analyze partition usage and provide recommendations
        WITH PartitionStats AS (
            SELECT 
                p.partition_number,
                p.rows,
                p.data_compression_desc,
                au.total_pages,
                au.used_pages,
                au.data_pages,
                prv.value AS boundary_value,
                fg.name AS filegroup_name
            FROM sys.partitions p
            INNER JOIN sys.allocation_units au ON p.partition_id = au.container_id
            INNER JOIN sys.partition_schemes ps ON ps.name = @PartitionScheme
            INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id 
                AND p.partition_number = dds.destination_id
            INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
            LEFT JOIN sys.partition_range_values prv ON ps.function_id = prv.function_id 
                AND p.partition_number = prv.boundary_id + 1
            WHERE p.object_id = OBJECT_ID(@TableName)
            AND p.index_id IN (0, 1)
        )
        SELECT 
            partition_number,
            ISNULL(CAST(boundary_value AS NVARCHAR(50)), 'N/A') AS boundary_value,
            rows,
            CAST(total_pages * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb,
            CAST(used_pages * 8.0 / 1024 AS DECIMAL(10,2)) AS used_mb,
            data_compression_desc,
            filegroup_name,
            CASE 
                WHEN rows = 0 THEN 'EMPTY - Consider merging'
                WHEN rows < 1000 THEN 'SMALL - Monitor growth'
                WHEN rows > 10000000 THEN 'LARGE - Consider splitting'
                ELSE 'OPTIMAL'
            END AS recommendation
        FROM PartitionStats
        ORDER BY partition_number;
    END;
END;

-- Usage examples
EXEC ManagePartitions 'Transactions', 'ANALYZE';

-- Add new partition for 2024 data
EXEC ManagePartitions 'Transactions', 'ADD', '2024-02-01', 'FG_2024_Feb';
```

### Vertical Partitioning Example

```sql
-- Vertical partitioning: Split wide customer table
-- Main customer table with frequently accessed columns
CREATE TABLE dbo.CustomerCore (
    CustomerID INT NOT NULL PRIMARY KEY,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(255) NOT NULL UNIQUE,
    Phone NVARCHAR(20),
    DateCreated DATETIME2 DEFAULT GETDATE(),
    LastLoginDate DATETIME2,
    IsActive BIT DEFAULT 1,
    CustomerStatus NVARCHAR(50) DEFAULT 'Active'
);

-- Extended customer information (less frequently accessed)
CREATE TABLE dbo.CustomerExtended (
    CustomerID INT NOT NULL PRIMARY KEY,
    DateOfBirth DATE,
    Gender CHAR(1),
    MaritalStatus NVARCHAR(20),
    Occupation NVARCHAR(100),
    AnnualIncome DECIMAL(12,2),
    Education NVARCHAR(50),
    PreferredLanguage NVARCHAR(20) DEFAULT 'English',
    NewsletterSubscription BIT DEFAULT 0,
    MarketingConsent BIT DEFAULT 0,
    DataProcessingConsent BIT DEFAULT 1,
    LastSurveyDate DATETIME2,
    CustomerNotes NVARCHAR(MAX),
    InternalNotes NVARCHAR(MAX),
    
    CONSTRAINT FK_CustomerExtended_Core 
        FOREIGN KEY (CustomerID) REFERENCES dbo.CustomerCore(CustomerID)
        ON DELETE CASCADE
);

-- Customer address information (separate due to 1:many relationship potential)
CREATE TABLE dbo.CustomerAddresses (
    AddressID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CustomerID INT NOT NULL,
    AddressType NVARCHAR(20) NOT NULL, -- 'Billing', 'Shipping', 'Home', 'Work'
    AddressLine1 NVARCHAR(255) NOT NULL,
    AddressLine2 NVARCHAR(255),
    City NVARCHAR(100) NOT NULL,
    StateProvince NVARCHAR(100),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(100) NOT NULL DEFAULT 'USA',
    IsDefault BIT DEFAULT 0,
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2,
    
    CONSTRAINT FK_CustomerAddresses_Core 
        FOREIGN KEY (CustomerID) REFERENCES dbo.CustomerCore(CustomerID)
        ON DELETE CASCADE,
    CONSTRAINT UQ_CustomerAddresses_Default 
        UNIQUE (CustomerID, AddressType, IsDefault)
);

-- Customer preferences (JSON or key-value storage)
CREATE TABLE dbo.CustomerPreferences (
    CustomerID INT NOT NULL,
    PreferenceCategory NVARCHAR(50) NOT NULL, -- 'Display', 'Communication', 'Privacy'
    PreferenceName NVARCHAR(100) NOT NULL,
    PreferenceValue NVARCHAR(MAX),
    DataType NVARCHAR(20) DEFAULT 'STRING', -- 'STRING', 'BOOLEAN', 'INTEGER', 'JSON'
    LastModified DATETIME2 DEFAULT GETDATE(),
    ModifiedBy NVARCHAR(100) DEFAULT SYSTEM_USER,
    
    CONSTRAINT PK_CustomerPreferences 
        PRIMARY KEY (CustomerID, PreferenceCategory, PreferenceName),
    CONSTRAINT FK_CustomerPreferences_Core 
        FOREIGN KEY (CustomerID) REFERENCES dbo.CustomerCore(CustomerID)
        ON DELETE CASCADE
);

-- View to reconstruct complete customer information when needed
CREATE VIEW dbo.CustomerComplete
AS
SELECT 
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.Email,
    c.Phone,
    c.DateCreated,
    c.LastLoginDate,
    c.IsActive,
    c.CustomerStatus,
    
    -- Extended information
    e.DateOfBirth,
    e.Gender,
    e.MaritalStatus,
    e.Occupation,
    e.AnnualIncome,
    e.Education,
    e.PreferredLanguage,
    e.NewsletterSubscription,
    e.MarketingConsent,
    e.DataProcessingConsent,
    e.CustomerNotes,
    
    -- Default addresses
    ba.AddressLine1 AS BillingAddress1,
    ba.AddressLine2 AS BillingAddress2,
    ba.City AS BillingCity,
    ba.StateProvince AS BillingState,
    ba.PostalCode AS BillingPostalCode,
    ba.Country AS BillingCountry,
    
    sa.AddressLine1 AS ShippingAddress1,
    sa.AddressLine2 AS ShippingAddress2,
    sa.City AS ShippingCity,
    sa.StateProvince AS ShippingState,
    sa.PostalCode AS ShippingPostalCode,
    sa.Country AS ShippingCountry

FROM dbo.CustomerCore c
LEFT JOIN dbo.CustomerExtended e ON c.CustomerID = e.CustomerID
LEFT JOIN dbo.CustomerAddresses ba ON c.CustomerID = ba.CustomerID 
    AND ba.AddressType = 'Billing' AND ba.IsDefault = 1
LEFT JOIN dbo.CustomerAddresses sa ON c.CustomerID = sa.CustomerID 
    AND sa.AddressType = 'Shipping' AND sa.IsDefault = 1;

-- Sample data insertion
INSERT INTO dbo.CustomerCore (CustomerID, FirstName, LastName, Email, Phone)
VALUES 
    (1, 'John', 'Doe', 'john.doe@email.com', '123-456-7890'),
    (2, 'Jane', 'Smith', 'jane.smith@email.com', '234-567-8901');

INSERT INTO dbo.CustomerExtended (CustomerID, DateOfBirth, Gender, Occupation, AnnualIncome)
VALUES 
    (1, '1985-05-15', 'M', 'Software Engineer', 95000.00),
    (2, '1990-08-22', 'F', 'Marketing Manager', 78000.00);

INSERT INTO dbo.CustomerAddresses (CustomerID, AddressType, AddressLine1, City, StateProvince, PostalCode, IsDefault)
VALUES 
    (1, 'Billing', '123 Main St', 'New York', 'NY', '10001', 1),
    (1, 'Shipping', '456 Oak Ave', 'New York', 'NY', '10002', 1),
    (2, 'Billing', '789 Pine St', 'Los Angeles', 'CA', '90001', 1),
    (2, 'Shipping', '789 Pine St', 'Los Angeles', 'CA', '90001', 1);

INSERT INTO dbo.CustomerPreferences (CustomerID, PreferenceCategory, PreferenceName, PreferenceValue, DataType)
VALUES 
    (1, 'Communication', 'EmailNotifications', 'true', 'BOOLEAN'),
    (1, 'Communication', 'SMSNotifications', 'false', 'BOOLEAN'),
    (1, 'Display', 'Theme', 'Dark', 'STRING'),
    (2, 'Communication', 'EmailNotifications', 'true', 'BOOLEAN'),
    (2, 'Display', 'Language', 'Spanish', 'STRING');
```

---

## Azure Cosmos DB Partitioning

### Logical Partitioning Strategy

```python
# Azure Cosmos DB partitioning examples using Python SDK
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceNotFoundError
import json
import datetime
import uuid
import random

class CosmosDBPartitioningDemo:
    """Demonstrate Cosmos DB partitioning strategies"""
    
    def __init__(self, endpoint, key, database_name):
        self.client = CosmosClient(endpoint, key)
        self.database_name = database_name
        self.database = self.client.create_database_if_not_exists(database_name)
    
    def create_containers_with_different_partition_strategies(self):
        """Create containers with different partitioning strategies"""
        
        # Strategy 1: Customer ID based partitioning (for customer-centric queries)
        customer_container = self.database.create_container_if_not_exists(
            id="customers",
            partition_key=PartitionKey(path="/customerId"),
            offer_throughput=400
        )
        
        # Strategy 2: Date-based partitioning (for time-series data)
        orders_container = self.database.create_container_if_not_exists(
            id="orders",
            partition_key=PartitionKey(path="/orderDate"),
            offer_throughput=400
        )
        
        # Strategy 3: Category-based partitioning (for product catalog)
        products_container = self.database.create_container_if_not_exists(
            id="products",
            partition_key=PartitionKey(path="/category"),
            offer_throughput=400
        )
        
        # Strategy 4: Tenant-based partitioning (for multi-tenant applications)
        tenant_data_container = self.database.create_container_if_not_exists(
            id="tenantData",
            partition_key=PartitionKey(path="/tenantId"),
            offer_throughput=400
        )
        
        # Strategy 5: Composite partitioning (using synthetic partition key)
        analytics_container = self.database.create_container_if_not_exists(
            id="analytics",
            partition_key=PartitionKey(path="/partitionKey"),  # Synthetic key
            offer_throughput=400
        )
        
        return {
            "customers": customer_container,
            "orders": orders_container,
            "products": products_container,
            "tenantData": tenant_data_container,
            "analytics": analytics_container
        }
    
    def demonstrate_customer_partitioning(self, container):
        """Demonstrate customer-based partitioning"""
        
        print("=== Customer-Based Partitioning Demo ===")
        
        # Sample customer data with good partition distribution
        customers = [
            {
                "id": str(uuid.uuid4()),
                "customerId": "CUST_001",  # Partition key
                "firstName": "John",
                "lastName": "Doe",
                "email": "john.doe@email.com",
                "phone": "123-456-7890",
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "state": "NY",
                    "zipCode": "10001",
                    "country": "USA"
                },
                "preferences": {
                    "newsletter": True,
                    "notifications": ["email", "sms"],
                    "language": "en-US"
                },
                "metadata": {
                    "createdDate": datetime.datetime.utcnow().isoformat(),
                    "lastModified": datetime.datetime.utcnow().isoformat(),
                    "version": 1,
                    "isActive": True
                }
            },
            {
                "id": str(uuid.uuid4()),
                "customerId": "CUST_002",
                "firstName": "Jane",
                "lastName": "Smith",
                "email": "jane.smith@email.com",
                "phone": "234-567-8901",
                "address": {
                    "street": "456 Oak Ave",
                    "city": "Los Angeles",
                    "state": "CA",
                    "zipCode": "90001",
                    "country": "USA"
                },
                "preferences": {
                    "newsletter": False,
                    "notifications": ["email"],
                    "language": "en-US"
                },
                "metadata": {
                    "createdDate": datetime.datetime.utcnow().isoformat(),
                    "lastModified": datetime.datetime.utcnow().isoformat(),
                    "version": 1,
                    "isActive": True
                }
            }
        ]
        
        # Insert customers
        for customer in customers:
            try:
                container.create_item(customer)
                print(f"Inserted customer: {customer['customerId']}")
            except Exception as e:
                print(f"Error inserting customer {customer['customerId']}: {str(e)}")
        
        # Query customers within same partition (efficient)
        query = "SELECT * FROM c WHERE c.customerId = @customerId"
        parameters = [{"name": "@customerId", "value": "CUST_001"}]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=False  # Single partition query
        ))
        
        print(f"Found {len(items)} customers in partition CUST_001")
        
        return len(customers)
    
    def demonstrate_time_series_partitioning(self, container):
        """Demonstrate time-series partitioning for orders"""
        
        print("\n=== Time-Series Partitioning Demo ===")
        
        # Generate orders across different dates (partitions)
        base_date = datetime.datetime(2024, 1, 1)
        orders = []
        
        for i in range(20):
            order_date = base_date + datetime.timedelta(days=i)
            order_date_str = order_date.strftime("%Y-%m-%d")
            
            order = {
                "id": str(uuid.uuid4()),
                "orderId": f"ORD_{i+1:06d}",
                "orderDate": order_date_str,  # Partition key
                "customerId": f"CUST_{(i % 5) + 1:03d}",
                "items": [
                    {
                        "productId": f"PROD_{random.randint(1, 100):03d}",
                        "productName": f"Product {random.randint(1, 100)}",
                        "quantity": random.randint(1, 5),
                        "unitPrice": round(random.uniform(10.0, 500.0), 2)
                    }
                    for _ in range(random.randint(1, 3))
                ],
                "totalAmount": 0,  # Will be calculated
                "orderStatus": random.choice(["Pending", "Processing", "Shipped", "Delivered"]),
                "shippingAddress": {
                    "street": f"{random.randint(100, 999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm'])} St",
                    "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston"]),
                    "state": random.choice(["NY", "CA", "IL", "TX"]),
                    "zipCode": f"{random.randint(10000, 99999)}"
                },
                "metadata": {
                    "createdDate": order_date.isoformat(),
                    "lastModified": order_date.isoformat(),
                    "version": 1
                }
            }
            
            # Calculate total amount
            order["totalAmount"] = sum(item["quantity"] * item["unitPrice"] for item in order["items"])
            orders.append(order)
        
        # Insert orders
        for order in orders:
            try:
                container.create_item(order)
                print(f"Inserted order: {order['orderId']} for date {order['orderDate']}")
            except Exception as e:
                print(f"Error inserting order {order['orderId']}: {str(e)}")
        
        # Query orders for specific date range (cross-partition query)
        query = """
        SELECT * FROM c 
        WHERE c.orderDate >= @startDate AND c.orderDate <= @endDate
        ORDER BY c.orderDate DESC
        """
        parameters = [
            {"name": "@startDate", "value": "2024-01-01"},
            {"name": "@endDate", "value": "2024-01-07"}
        ]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True  # Cross-partition query required
        ))
        
        print(f"Found {len(items)} orders in date range")
        
        # Query orders for single date (single partition)
        single_date_query = "SELECT * FROM c WHERE c.orderDate = @orderDate"
        single_date_params = [{"name": "@orderDate", "value": "2024-01-05"}]
        
        single_date_items = list(container.query_items(
            query=single_date_query,
            parameters=single_date_params,
            enable_cross_partition_query=False
        ))
        
        print(f"Found {len(single_date_items)} orders for single date (single partition)")
        
        return len(orders)
    
    def demonstrate_synthetic_partition_key(self, container):
        """Demonstrate synthetic partition key for better distribution"""
        
        print("\n=== Synthetic Partition Key Demo ===")
        
        # Analytics data with synthetic partition key for even distribution
        analytics_data = []
        
        for i in range(50):
            # Create synthetic partition key using hash of user ID
            user_id = f"USER_{i+1:05d}"
            partition_key = f"PART_{hash(user_id) % 10:02d}"  # Distribute across 10 partitions
            
            event = {
                "id": str(uuid.uuid4()),
                "partitionKey": partition_key,  # Synthetic partition key
                "userId": user_id,
                "eventType": random.choice(["page_view", "click", "purchase", "search", "login"]),
                "timestamp": (datetime.datetime.utcnow() - datetime.timedelta(
                    minutes=random.randint(0, 1440)
                )).isoformat(),
                "properties": {
                    "page": random.choice(["/home", "/products", "/cart", "/checkout", "/profile"]),
                    "userAgent": "Mozilla/5.0 (compatible; analytics)",
                    "ipAddress": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
                    "sessionId": str(uuid.uuid4()),
                    "referrer": random.choice(["google.com", "facebook.com", "direct", "email"])
                },
                "metrics": {
                    "duration": random.randint(1, 300),
                    "value": round(random.uniform(0, 1000), 2) if random.random() > 0.7 else None
                }
            }
            
            analytics_data.append(event)
        
        # Insert analytics events
        for event in analytics_data:
            try:
                container.create_item(event)
                print(f"Inserted event: {event['eventType']} for user {event['userId']} in partition {event['partitionKey']}")
            except Exception as e:
                print(f"Error inserting event: {str(e)}")
        
        # Query events for specific user (single partition due to synthetic key)
        user_query = "SELECT * FROM c WHERE c.userId = @userId"
        user_params = [{"name": "@userId", "value": "USER_00001"}]
        
        user_events = list(container.query_items(
            query=user_query,
            parameters=user_params,
            enable_cross_partition_query=True  # Might need cross-partition if user spans partitions
        ))
        
        print(f"Found {len(user_events)} events for specific user")
        
        # Analyze partition distribution
        partition_distribution = {}
        for event in analytics_data:
            partition_key = event["partitionKey"]
            partition_distribution[partition_key] = partition_distribution.get(partition_key, 0) + 1
        
        print("Partition distribution:")
        for partition, count in sorted(partition_distribution.items()):
            print(f"  {partition}: {count} items")
        
        return len(analytics_data)
    
    def analyze_partition_metrics(self, container_name):
        """Analyze partition key effectiveness"""
        
        print(f"\n=== Partition Metrics Analysis for {container_name} ===")
        
        try:
            # This would require Azure Monitor integration or custom metrics collection
            # For demonstration, we'll show the structure of what to monitor
            
            metrics_to_monitor = {
                "request_units_consumed": "Monitor RU consumption per partition",
                "storage_usage": "Monitor storage distribution across partitions", 
                "hot_partitions": "Identify partitions receiving disproportionate traffic",
                "query_performance": "Measure cross-partition vs single-partition query performance",
                "throttling_events": "Monitor for 429 (Too Many Requests) errors per partition"
            }
            
            print("Key metrics to monitor for partition effectiveness:")
            for metric, description in metrics_to_monitor.items():
                print(f"  {metric}: {description}")
            
            # Simulated partition health check
            partition_health = {
                "total_partitions": 10,
                "hot_partitions": 1,
                "balanced_distribution": 85.5,  # Percentage
                "cross_partition_queries": 23.2,  # Percentage
                "average_ru_per_partition": 42.5,
                "recommendation": "Consider using composite partition key for better distribution"
            }
            
            print(f"\nPartition Health Summary:")
            for key, value in partition_health.items():
                print(f"  {key}: {value}")
                
        except Exception as e:
            print(f"Error analyzing partition metrics: {str(e)}")

# Usage example
# cosmos_demo = CosmosDBPartitioningDemo(
#     endpoint="https://your-cosmos-account.documents.azure.com:443/",
#     key="your-primary-key",
#     database_name="PartitioningDemo"
# )
# 
# containers = cosmos_demo.create_containers_with_different_partition_strategies()
# 
# # Demonstrate different partitioning strategies
# customer_count = cosmos_demo.demonstrate_customer_partitioning(containers["customers"])
# order_count = cosmos_demo.demonstrate_time_series_partitioning(containers["orders"])
# analytics_count = cosmos_demo.demonstrate_synthetic_partition_key(containers["analytics"])
# 
# # Analyze partition effectiveness
# cosmos_demo.analyze_partition_metrics("customers")
```

### Cosmos DB Partition Key Selection Guide

```json
{
  "cosmos_db_partition_key_selection": {
    "good_partition_keys": {
      "high_cardinality": {
        "description": "Many possible values to distribute data evenly",
        "examples": ["userId", "customerId", "deviceId", "sessionId"],
        "benefits": ["Even distribution", "Parallel processing", "Scalability"]
      },
      "query_aligned": {
        "description": "Frequently used in WHERE clauses",
        "examples": ["tenantId", "categoryId", "regionId"],
        "benefits": ["Single partition queries", "Better performance", "Lower RU cost"]
      },
      "composite_keys": {
        "description": "Combination of fields for better distribution",
        "examples": ["userId + date", "tenantId + category", "region + productType"],
        "benefits": ["Improved distribution", "Query flexibility", "Reduced hot partitions"]
      }
    },
    "poor_partition_keys": {
      "low_cardinality": {
        "description": "Few possible values leading to hot partitions",
        "examples": ["gender", "status", "boolean fields", "country (for global apps)"],
        "problems": ["Uneven distribution", "Hot partitions", "Throttling"]
      },
      "sequential_keys": {
        "description": "Monotonically increasing values",
        "examples": ["timestamp", "auto-increment ID", "sequential order number"],
        "problems": ["All writes go to one partition", "Write hotspots", "Poor scalability"]
      },
      "rarely_queried": {
        "description": "Fields not commonly used in queries",
        "examples": ["description", "notes", "random UUID"],
        "problems": ["Cross-partition queries", "Higher RU cost", "Poor performance"]
      }
    },
    "partition_key_patterns": {
      "time_based_with_prefix": {
        "pattern": "region-2024-01-15",
        "use_case": "Time series data with geographic distribution",
        "benefits": ["Prevents hot partitions", "Enables time-range queries"]
      },
      "hash_suffix": {
        "pattern": "userId-{hash(userId) % 100}",
        "use_case": "Even distribution of user data",
        "benefits": ["Uniform distribution", "Predictable partition location"]
      },
      "hierarchical": {
        "pattern": "tenant/department/team",
        "use_case": "Multi-tenant applications with hierarchy",
        "benefits": ["Logical grouping", "Tenant isolation", "Scalable multi-tenancy"]
      }
    }
  }
}
```

---

## Azure Databricks/Spark Partitioning

### DataFrame Partitioning Strategies

```python
# Comprehensive Spark DataFrame partitioning examples
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import random
from datetime import datetime, timedelta

class SparkPartitioningFramework:
    """Comprehensive Spark partitioning framework with examples"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SparkPartitioningDemo") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
    
    def create_sample_datasets(self):
        """Create comprehensive sample datasets for partitioning demos"""
        
        print("=== Creating Sample Datasets ===")
        
        # E-commerce dataset with multiple partitioning opportunities
        ecommerce_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("category", StringType(), False),
            StructField("subcategory", StringType(), False),
            StructField("brand", StringType(), False),
            StructField("transaction_date", DateType(), False),
            StructField("transaction_timestamp", TimestampType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(10, 2), False),
            StructField("total_amount", DecimalType(12, 2), False),
            StructField("discount_amount", DecimalType(10, 2), True),
            StructField("tax_amount", DecimalType(10, 2), False),
            StructField("payment_method", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("region", StringType(), False),
            StructField("country", StringType(), False),
            StructField("customer_segment", StringType(), False),
            StructField("promotion_code", StringType(), True),
            StructField("channel", StringType(), False)  # online, mobile, store
        ])
        
        # Generate sample data
        categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Beauty", "Automotive"]
        regions = ["North", "South", "East", "West", "Central"]
        countries = ["USA", "Canada", "UK", "Germany", "France", "Japan", "Australia"]
        channels = ["online", "mobile", "store"]
        payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "cash"]
        customer_segments = ["Premium", "Standard", "Basic", "VIP"]
        
        # Generate 100,000 sample transactions
        sample_data = []
        base_date = datetime(2023, 1, 1)
        
        for i in range(100000):
            transaction_date = base_date + timedelta(days=random.randint(0, 365))
            transaction_timestamp = transaction_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            category = random.choice(categories)
            region = random.choice(regions)
            country = random.choice(countries)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(5.0, 500.0), 2)
            total_amount = quantity * unit_price
            discount_amount = round(total_amount * random.uniform(0, 0.2), 2) if random.random() > 0.7 else None
            tax_amount = round(total_amount * 0.08, 2)
            
            sample_data.append((
                f"TXN_{i+1:08d}",  # transaction_id
                f"CUST_{(i % 10000) + 1:06d}",  # customer_id (10K unique customers)
                f"PROD_{random.randint(1, 5000):06d}",  # product_id
                category,
                f"{category}_Sub_{random.randint(1, 5)}",  # subcategory
                f"Brand_{random.randint(1, 50)}",  # brand
                transaction_date.date(),  # transaction_date
                transaction_timestamp,  # transaction_timestamp
                quantity,
                unit_price,
                total_amount,
                discount_amount,
                tax_amount,
                random.choice(payment_methods),
                f"STORE_{random.randint(1, 100):03d}",  # store_id
                region,
                country,
                random.choice(customer_segments),
                f"PROMO_{random.randint(1, 20):03d}" if random.random() > 0.8 else None,
                random.choice(channels)
            ))
        
        # Create DataFrame
        ecommerce_df = self.spark.createDataFrame(sample_data, ecommerce_schema)
        
        print(f"Created e-commerce dataset with {ecommerce_df.count():,} transactions")
        print(f"Default partitions: {ecommerce_df.rdd.getNumPartitions()}")
        
        return ecommerce_df
    
    def demonstrate_range_partitioning(self, df):
        """Demonstrate range-based partitioning strategies"""
        
        print("\n=== Range Partitioning Demonstration ===")
        
        # Date-based range partitioning
        print("1. Date-based Range Partitioning:")
        
        # Create date ranges for partitioning
        date_ranges = [
            "2023-01-01", "2023-04-01", "2023-07-01", "2023-10-01", "2024-01-01"
        ]
        
        # Partition by date ranges (quarters)
        date_partitioned = df.withColumn(
            "date_partition",
            when(col("transaction_date") < "2023-04-01", "Q1_2023")
            .when(col("transaction_date") < "2023-07-01", "Q2_2023")
            .when(col("transaction_date") < "2023-10-01", "Q3_2023")
            .when(col("transaction_date") < "2024-01-01", "Q4_2023")
            .otherwise("Q1_2024")
        ).repartition(col("date_partition"))
        
        # Analyze partition distribution
        partition_distribution = date_partitioned.groupBy("date_partition").count().collect()
        print("Date partition distribution:")
        for row in sorted(partition_distribution, key=lambda x: x["date_partition"]):
            print(f"  {row['date_partition']}: {row['count']:,} records")
        
        # Amount-based range partitioning
        print("\n2. Amount-based Range Partitioning:")
        
        amount_partitioned = df.withColumn(
            "amount_partition",
            when(col("total_amount") < 50, "Small")
            .when(col("total_amount") < 200, "Medium")
            .when(col("total_amount") < 500, "Large")
            .otherwise("XLarge")
        ).repartition(col("amount_partition"))
        
        amount_distribution = amount_partitioned.groupBy("amount_partition").count().collect()
        print("Amount partition distribution:")
        for row in sorted(amount_distribution, key=lambda x: x["amount_partition"]):
            print(f"  {row['amount_partition']}: {row['count']:,} records")
        
        return {
            "date_partitioned": date_partitioned,
            "amount_partitioned": amount_partitioned,
            "date_distribution": {row["date_partition"]: row["count"] for row in partition_distribution},
            "amount_distribution": {row["amount_partition"]: row["count"] for row in amount_distribution}
        }
    
    def demonstrate_hash_partitioning(self, df):
        """Demonstrate hash-based partitioning for even distribution"""
        
        print("\n=== Hash Partitioning Demonstration ===")
        
        # Customer ID hash partitioning
        print("1. Customer ID Hash Partitioning:")
        
        # Create hash-based partitions
        hash_partitioned = df.withColumn(
            "customer_hash_partition",
            abs(hash(col("customer_id")) % 8)
        ).repartition(8, col("customer_hash_partition"))
        
        # Analyze distribution
        hash_distribution = hash_partitioned.groupBy("customer_hash_partition").count().collect()
        print("Customer hash partition distribution:")
        for row in sorted(hash_distribution, key=lambda x: x["customer_hash_partition"]):
            print(f"  Partition {row['customer_hash_partition']}: {row['count']:,} records")
        
        # Product ID hash partitioning
        print("\n2. Product ID Hash Partitioning:")
        
        product_hash_partitioned = df.withColumn(
            "product_hash_partition",
            abs(hash(col("product_id")) % 10)
        ).repartition(10, col("product_hash_partition"))
        
        product_hash_distribution = product_hash_partitioned.groupBy("product_hash_partition").count().collect()
        print("Product hash partition distribution:")
        for row in sorted(product_hash_distribution, key=lambda x: x["product_hash_partition"]):
            print(f"  Partition {row['product_hash_partition']}: {row['count']:,} records")
        
        # Calculate distribution statistics
        hash_counts = [row["count"] for row in hash_distribution]
        hash_std_dev = (sum((x - sum(hash_counts)/len(hash_counts))**2 for x in hash_counts) / len(hash_counts))**0.5
        hash_coefficient_of_variation = hash_std_dev / (sum(hash_counts)/len(hash_counts))
        
        print(f"\nHash Distribution Statistics:")
        print(f"  Standard Deviation: {hash_std_dev:.2f}")
        print(f"  Coefficient of Variation: {hash_coefficient_of_variation:.4f}")
        print(f"  Distribution Quality: {'Excellent' if hash_coefficient_of_variation < 0.1 else 'Good' if hash_coefficient_of_variation < 0.2 else 'Poor'}")
        
        return {
            "customer_hash_partitioned": hash_partitioned,
            "product_hash_partitioned": product_hash_partitioned,
            "hash_distribution": {row["customer_hash_partition"]: row["count"] for row in hash_distribution},
            "distribution_quality": hash_coefficient_of_variation
        }
    
    def demonstrate_list_partitioning(self, df):
        """Demonstrate list-based partitioning for categorical data"""
        
        print("\n=== List Partitioning Demonstration ===")
        
        # Category-based list partitioning
        print("1. Category-based List Partitioning:")
        
        category_partitioned = df.repartition(col("category"))
        
        category_distribution = category_partitioned.groupBy("category").count().collect()
        print("Category partition distribution:")
        for row in sorted(category_distribution, key=lambda x: x["category"]):
            print(f"  {row['category']}: {row['count']:,} records")
        
        # Region-based list partitioning
        print("\n2. Region-based List Partitioning:")
        
        region_partitioned = df.repartition(col("region"))
        
        region_distribution = region_partitioned.groupBy("region").count().collect()
        print("Region partition distribution:")
        for row in sorted(region_distribution, key=lambda x: x["region"]):
            print(f"  {row['region']}: {row['count']:,} records")
        
        # Channel-based list partitioning
        print("\n3. Channel-based List Partitioning:")
        
        channel_partitioned = df.repartition(col("channel"))
        
        channel_distribution = channel_partitioned.groupBy("channel").count().collect()
        print("Channel partition distribution:")
        for row in sorted(channel_distribution, key=lambda x: x["channel"]):
            print(f"  {row['channel']}: {row['count']:,} records")
        
        return {
            "category_partitioned": category_partitioned,
            "region_partitioned": region_partitioned,
            "channel_partitioned": channel_partitioned,
            "distributions": {
                "category": {row["category"]: row["count"] for row in category_distribution},
                "region": {row["region"]: row["count"] for row in region_distribution},
                "channel": {row["channel"]: row["count"] for row in channel_distribution}
            }
        }
    
    def demonstrate_composite_partitioning(self, df):
        """Demonstrate composite partitioning strategies"""
        
        print("\n=== Composite Partitioning Demonstration ===")
        
        # Multi-column partitioning
        print("1. Multi-column Partitioning (Region + Category):")
        
        composite_partitioned = df.repartition(col("region"), col("category"))
        
        composite_distribution = composite_partitioned.groupBy("region", "category").count().collect()
        print("Composite partition distribution (showing top 10):")
        for row in sorted(composite_distribution, key=lambda x: x["count"], reverse=True)[:10]:
            print(f"  {row['region']} + {row['category']}: {row['count']:,} records")
        
        print(f"Total composite partitions: {len(composite_distribution)}")
        
        # Hierarchical partitioning (Country -> Region -> Category)
        print("\n2. Hierarchical Partitioning:")
        
        hierarchical_partitioned = df.withColumn(
            "hierarchy_key",
            concat(col("country"), lit("-"), col("region"), lit("-"), col("category"))
        ).repartition(col("hierarchy_key"))
        
        hierarchical_distribution = hierarchical_partitioned.groupBy("hierarchy_key").count().collect()
        print("Hierarchical partition distribution (showing top 10):")
        for row in sorted(hierarchical_distribution, key=lambda x: x["count"], reverse=True)[:10]:
            print(f"  {row['hierarchy_key']}: {row['count']:,} records")
        
        print(f"Total hierarchical partitions: {len(hierarchical_distribution)}")
        
        # Time + Category composite partitioning
        print("\n3. Time + Category Composite Partitioning:")
        
        time_category_partitioned = df.withColumn(
            "year_month", date_format(col("transaction_date"), "yyyy-MM")
        ).repartition(col("year_month"), col("category"))
        
        time_category_distribution = time_category_partitioned.groupBy("year_month", "category").count().collect()
        print("Time + Category partition distribution (showing sample):")
        for row in sorted(time_category_distribution, key=lambda x: (x["year_month"], x["category"]))[:15]:
            print(f"  {row['year_month']} + {row['category']}: {row['count']:,} records")
        
        return {
            "composite_partitioned": composite_partitioned,
            "hierarchical_partitioned": hierarchical_partitioned,
            "time_category_partitioned": time_category_partitioned,
            "partition_counts": {
                "composite": len(composite_distribution),
                "hierarchical": len(hierarchical_distribution),
                "time_category": len(time_category_distribution)
            }
        }
    
    def analyze_partitioning_performance(self, original_df, partitioned_dfs):
        """Analyze performance impact of different partitioning strategies"""
        
        print("\n=== Partitioning Performance Analysis ===")
        
        # Test queries on different partitioning strategies
        test_queries = {
            "category_filter": {
                "description": "Filter by category (benefits list partitioning)",
                "query": lambda df: df.filter(col("category") == "Electronics").count()
            },
            "date_range_filter": {
                "description": "Filter by date range (benefits range partitioning)",
                "query": lambda df: df.filter(
                    (col("transaction_date") >= "2023-07-01") & 
                    (col("transaction_date") < "2023-10-01")
                ).count()
            },
            "customer_aggregation": {
                "description": "Aggregate by customer (benefits hash partitioning)",
                "query": lambda df: df.groupBy("customer_id").agg(
                    sum("total_amount").alias("total_spent"),
                    count("*").alias("transaction_count")
                ).count()
            },
            "region_category_join": {
                "description": "Complex query with multiple dimensions",
                "query": lambda df: df.groupBy("region", "category").agg(
                    avg("total_amount").alias("avg_amount"),
                    sum("quantity").alias("total_quantity")
                ).count()
            }
        }
        
        performance_results = {}
        
        # Test original DataFrame
        print("Testing original DataFrame (default partitioning):")
        original_results = {}
        for query_name, query_info in test_queries.items():
            start_time = time.time()
            result = query_info["query"](original_df)
            end_time = time.time()
            execution_time = end_time - start_time
            original_results[query_name] = {
                "result": result,
                "execution_time": execution_time,
                "description": query_info["description"]
            }
            print(f"  {query_name}: {execution_time:.3f}s (result: {result:,})")
        
        performance_results["original"] = original_results
        
        # Test partitioned DataFrames
        partition_strategies = [
            ("category_partitioned", "Category List Partitioning"),
            ("region_partitioned", "Region List Partitioning"), 
            ("date_partitioned", "Date Range Partitioning"),
            ("hash_partitioned", "Customer Hash Partitioning")
        ]
        
        for strategy_key, strategy_name in partition_strategies:
            if strategy_key in partitioned_dfs:
                print(f"\nTesting {strategy_name}:")
                strategy_results = {}
                
                for query_name, query_info in test_queries.items():
                    start_time = time.time()
                    result = query_info["query"](partitioned_dfs[strategy_key])
                    end_time = time.time()
                    execution_time = end_time - start_time
                    
                    # Calculate performance improvement
                    original_time = original_results[query_name]["execution_time"]
                    improvement = ((original_time - execution_time) / original_time) * 100
                    
                    strategy_results[query_name] = {
                        "result": result,
                        "execution_time": execution_time,
                        "improvement_percent": improvement,
                        "description": query_info["description"]
                    }
                    
                    improvement_indicator = "↑" if improvement > 0 else "↓" if improvement < 0 else "="
                    print(f"  {query_name}: {execution_time:.3f}s ({improvement:+.1f}% {improvement_indicator})")
                
                performance_results[strategy_key] = strategy_results
        
        # Generate performance summary
        print(f"\n=== Performance Summary ===")
        print("Best partitioning strategy for each query type:")
        
        for query_name in test_queries.keys():
            best_strategy = "original"
            best_time = original_results[query_name]["execution_time"]
            
            for strategy_key in performance_results.keys():
                if strategy_key != "original":
                    strategy_time = performance_results[strategy_key].get(query_name, {}).get("execution_time", float('inf'))
                    if strategy_time < best_time:
                        best_time = strategy_time
                        best_strategy = strategy_key
            
            improvement = ((original_results[query_name]["execution_time"] - best_time) / original_results[query_name]["execution_time"]) * 100
            print(f"  {query_name}: {best_strategy} ({improvement:+.1f}% improvement)")
        
        return performance_results

# Usage example
import time

# Initialize framework
spark_partitioning = SparkPartitioningFramework()

# Create sample dataset
ecommerce_df = spark_partitioning.create_sample_datasets()

# Demonstrate different partitioning strategies
range_results = spark_partitioning.demonstrate_range_partitioning(ecommerce_df)
hash_results = spark_partitioning.demonstrate_hash_partitioning(ecommerce_df)
list_results = spark_partitioning.demonstrate_list_partitioning(ecommerce_df)
composite_results = spark_partitioning.demonstrate_composite_partitioning(ecommerce_df)

# Combine all partitioned DataFrames for performance testing
all_partitioned_dfs = {
    "date_partitioned": range_results["date_partitioned"],
    "hash_partitioned": hash_results["customer_hash_partitioned"],
    "category_partitioned": list_results["category_partitioned"],
    "region_partitioned": list_results["region_partitioned"],
    "composite_partitioned": composite_results["composite_partitioned"]
}

# Analyze performance
performance_analysis = spark_partitioning.analyze_partitioning_performance(
    ecommerce_df, all_partitioned_dfs
)

print("\n=== Partitioning Demonstration Complete ===")
```

---

## Best Practices

### Partitioning Decision Framework

```json
{
  "partitioning_decision_framework": {
    "data_characteristics": {
      "volume": {
        "small": "< 1GB - Consider single partition or minimal partitioning",
        "medium": "1GB - 100GB - Implement moderate partitioning strategy",
        "large": "100GB - 10TB - Use comprehensive partitioning approach",
        "very_large": "> 10TB - Implement advanced partitioning with multiple strategies"
      },
      "growth_rate": {
        "static": "No growth - Simple partitioning sufficient",
        "slow": "< 10% monthly - Plan for gradual scaling",
        "moderate": "10-50% monthly - Implement scalable partitioning",
        "rapid": "> 50% monthly - Use dynamic partitioning strategies"
      },
      "access_patterns": {
        "random": "Random access - Use hash partitioning",
        "sequential": "Sequential access - Use range partitioning",
        "categorical": "Category-based access - Use list partitioning",
        "temporal": "Time-based access - Use date/time partitioning"
      }
    },
    "performance_requirements": {
      "query_performance": {
        "oltp": "High concurrency, low latency - Use hash partitioning",
        "olap": "Complex analytics - Use range or list partitioning",
        "mixed": "Mixed workload - Use composite partitioning"
      },
      "scalability": {
        "horizontal": "Scale out - Use hash or range partitioning",
        "vertical": "Scale up - Consider vertical partitioning",
        "elastic": "Auto-scaling - Use cloud-native partitioning"
      }
    },
    "operational_requirements": {
      "maintenance": {
        "automated": "Minimal maintenance - Use service-managed partitioning",
        "scheduled": "Regular maintenance windows - Use manual partitioning",
        "continuous": "Zero-downtime requirements - Use online partitioning"
      },
      "backup_recovery": {
        "full": "Full backup/restore - Simple partitioning",
        "incremental": "Incremental backup - Time-based partitioning",
        "point_in_time": "Point-in-time recovery - Transaction log partitioning"
      }
    }
  }
}
```

This comprehensive guide provides everything needed to understand and implement all types of partitioning strategies across Azure services, with practical examples and performance optimization techniques.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*