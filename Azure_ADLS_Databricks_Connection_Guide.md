# Azure ADLS to Databricks Connection Guide
## Complete Guide to Connecting Azure Data Lake Storage with Azure Databricks

---

### Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Authentication Methods](#authentication-methods)
4. [Service Principal Authentication](#service-principal-authentication)
5. [Storage Account Key Authentication](#storage-account-key-authentication)
6. [SAS Token Authentication](#sas-token-authentication)
7. [Mounting ADLS in Databricks](#mounting-adls-in-databricks)
8. [Data Operations](#data-operations)
9. [Security Best Practices](#security-best-practices)
10. [Performance Optimization](#performance-optimization)
11. [Troubleshooting](#troubleshooting)
12. [Real-World Examples](#real-world-examples)

---

## Overview

Azure Data Lake Storage (ADLS) Gen2 provides a hierarchical file system with massive scalability and cost-effectiveness for big data analytics. Integrating ADLS with Azure Databricks enables powerful data processing capabilities with seamless access to your data lake.

### Connection Architecture

```json
{
  "adls_databricks_architecture": {
    "components": {
      "azure_data_lake_storage_gen2": {
        "description": "Hierarchical namespace storage service",
        "features": ["POSIX-compliant", "Hadoop-compatible", "Multi-protocol access"],
        "storage_tiers": ["Hot", "Cool", "Archive"]
      },
      "azure_databricks": {
        "description": "Apache Spark-based analytics platform",
        "features": ["Managed Spark clusters", "Collaborative notebooks", "MLflow integration"],
        "compute_types": ["All-purpose clusters", "Job clusters", "SQL warehouses"]
      },
      "authentication_layer": {
        "methods": ["Service Principal", "Storage Account Keys", "SAS Tokens", "Azure AD passthrough"],
        "security_features": ["RBAC", "ACLs", "Network security", "Encryption"]
      }
    },
    "connection_patterns": {
      "direct_access": "Direct file system operations using authentication",
      "mount_points": "Mount ADLS containers as Databricks file system paths",
      "unity_catalog": "Managed access through Unity Catalog governance"
    }
  }
}
```

### Key Benefits

```json
{
  "integration_benefits": {
    "performance": {
      "high_throughput": "Optimized for big data workloads with parallel processing",
      "low_latency": "Direct access to data without intermediate copying",
      "auto_scaling": "Databricks clusters scale based on data processing needs"
    },
    "cost_efficiency": {
      "storage_optimization": "ADLS Gen2 provides cost-effective storage tiers",
      "compute_optimization": "Pay-per-use Databricks clusters reduce idle costs",
      "data_lifecycle": "Automated data archiving and cleanup policies"
    },
    "security": {
      "enterprise_grade": "Azure AD integration with RBAC and ACLs",
      "encryption": "Data encrypted at rest and in transit",
      "compliance": "Supports various compliance standards (GDPR, HIPAA, SOC)"
    },
    "scalability": {
      "petabyte_scale": "Handle petabytes of data seamlessly",
      "elastic_compute": "Scale compute resources up/down based on workload",
      "global_availability": "Multi-region deployment capabilities"
    }
  }
}
```

---

## Prerequisites

### Azure Resources Required

```json
{
  "required_resources": {
    "azure_data_lake_storage_gen2": {
      "account_type": "StorageV2 (general purpose v2)",
      "hierarchical_namespace": "Enabled",
      "replication": "LRS, ZRS, GRS, or GZRS based on requirements",
      "access_tier": "Hot (for frequently accessed data)"
    },
    "azure_databricks_workspace": {
      "pricing_tier": "Standard or Premium",
      "location": "Same region as ADLS for optimal performance",
      "networking": "Default or custom VNet integration"
    },
    "azure_active_directory": {
      "service_principal": "For authentication (recommended)",
      "app_registration": "Required for service principal",
      "rbac_roles": "Storage Blob Data Contributor/Reader"
    },
    "azure_key_vault": {
      "purpose": "Secure credential storage (recommended)",
      "access_policies": "Configure for Databricks service principal",
      "secrets": "Store connection strings and keys"
    }
  }
}
```

### PowerShell Setup Script

```powershell
# Azure ADLS and Databricks Setup Script
# Run this script to set up required Azure resources

# Variables
$resourceGroupName = "rg-adls-databricks-demo"
$location = "East US 2"
$storageAccountName = "adlsdemo$(Get-Random -Minimum 1000 -Maximum 9999)"
$databricksWorkspaceName = "databricks-adls-demo"
$keyVaultName = "kv-adls-demo-$(Get-Random -Minimum 1000 -Maximum 9999)"
$servicePrincipalName = "sp-adls-databricks-demo"

# Login to Azure
Connect-AzAccount

# Create Resource Group
Write-Host "Creating Resource Group..." -ForegroundColor Green
New-AzResourceGroup -Name $resourceGroupName -Location $location

# Create ADLS Gen2 Storage Account
Write-Host "Creating ADLS Gen2 Storage Account..." -ForegroundColor Green
$storageAccount = New-AzStorageAccount `
    -ResourceGroupName $resourceGroupName `
    -Name $storageAccountName `
    -Location $location `
    -SkuName "Standard_LRS" `
    -Kind "StorageV2" `
    -EnableHierarchicalNamespace $true `
    -AccessTier "Hot"

# Create containers
Write-Host "Creating storage containers..." -ForegroundColor Green
$storageContext = $storageAccount.Context
New-AzStorageContainer -Name "raw-data" -Context $storageContext -Permission Off
New-AzStorageContainer -Name "processed-data" -Context $storageContext -Permission Off
New-AzStorageContainer -Name "curated-data" -Context $storageContext -Permission Off

# Create Azure Databricks Workspace
Write-Host "Creating Azure Databricks Workspace..." -ForegroundColor Green
$databricksWorkspace = New-AzDatabricksWorkspace `
    -ResourceGroupName $resourceGroupName `
    -Name $databricksWorkspaceName `
    -Location $location `
    -Sku "standard"

# Create Key Vault
Write-Host "Creating Azure Key Vault..." -ForegroundColor Green
$keyVault = New-AzKeyVault `
    -ResourceGroupName $resourceGroupName `
    -VaultName $keyVaultName `
    -Location $location `
    -Sku "Standard"

# Create Service Principal
Write-Host "Creating Service Principal..." -ForegroundColor Green
$servicePrincipal = New-AzADServicePrincipal -DisplayName $servicePrincipalName

# Assign Storage Blob Data Contributor role to Service Principal
Write-Host "Assigning RBAC roles..." -ForegroundColor Green
$roleAssignment = New-AzRoleAssignment `
    -ObjectId $servicePrincipal.Id `
    -RoleDefinitionName "Storage Blob Data Contributor" `
    -Scope $storageAccount.Id

# Store secrets in Key Vault
Write-Host "Storing secrets in Key Vault..." -ForegroundColor Green

# Grant Key Vault access to current user
$currentUser = Get-AzContext
Set-AzKeyVaultAccessPolicy `
    -VaultName $keyVaultName `
    -UserPrincipalName $currentUser.Account.Id `
    -PermissionsToSecrets Get,Set,Delete,List

# Store storage account key
$storageKey = (Get-AzStorageAccountKey -ResourceGroupName $resourceGroupName -Name $storageAccountName)[0].Value
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "storage-account-key" -SecretValue (ConvertTo-SecureString $storageKey -AsPlainText -Force)

# Store service principal credentials
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "service-principal-client-id" -SecretValue (ConvertTo-SecureString $servicePrincipal.AppId -AsPlainText -Force)
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "service-principal-client-secret" -SecretValue $servicePrincipal.PasswordCredentials.SecretText

# Get tenant ID
$tenantId = (Get-AzContext).Tenant.Id
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "tenant-id" -SecretValue (ConvertTo-SecureString $tenantId -AsPlainText -Force)

# Output configuration details
Write-Host "`n=== Configuration Summary ===" -ForegroundColor Cyan
Write-Host "Resource Group: $resourceGroupName" -ForegroundColor White
Write-Host "Storage Account: $storageAccountName" -ForegroundColor White
Write-Host "Databricks Workspace: $databricksWorkspaceName" -ForegroundColor White
Write-Host "Key Vault: $keyVaultName" -ForegroundColor White
Write-Host "Service Principal: $servicePrincipalName" -ForegroundColor White
Write-Host "Service Principal App ID: $($servicePrincipal.AppId)" -ForegroundColor White
Write-Host "Tenant ID: $tenantId" -ForegroundColor White
Write-Host "`nStorage Account URL: https://$storageAccountName.dfs.core.windows.net" -ForegroundColor Yellow
Write-Host "Databricks Workspace URL: https://$($databricksWorkspace.WorkspaceUrl)" -ForegroundColor Yellow
```

---

## Authentication Methods

### Overview of Authentication Options

```json
{
  "authentication_methods": {
    "service_principal": {
      "description": "Azure AD application with assigned permissions",
      "security_level": "High",
      "use_cases": ["Production environments", "Automated workflows", "CI/CD pipelines"],
      "advantages": ["Fine-grained permissions", "Auditable", "Rotatable credentials"],
      "disadvantages": ["Initial setup complexity", "Requires Azure AD management"]
    },
    "storage_account_keys": {
      "description": "Storage account access keys",
      "security_level": "Medium",
      "use_cases": ["Development environments", "Quick prototypes", "Simple scenarios"],
      "advantages": ["Simple setup", "Full storage account access"],
      "disadvantages": ["Broad permissions", "Key rotation challenges", "Security risks"]
    },
    "sas_tokens": {
      "description": "Shared Access Signature with limited permissions",
      "security_level": "Medium-High",
      "use_cases": ["Time-limited access", "External partner access", "Specific container access"],
      "advantages": ["Time-bound access", "Limited scope", "Easily revocable"],
      "disadvantages": ["Token management overhead", "Expiration handling required"]
    },
    "azure_ad_passthrough": {
      "description": "User's Azure AD identity passed through to storage",
      "security_level": "High",
      "use_cases": ["Interactive notebooks", "User-specific access", "Compliance requirements"],
      "advantages": ["User-level auditing", "No credential management", "Dynamic permissions"],
      "disadvantages": ["Premium Databricks required", "Limited to interactive scenarios"]
    }
  }
}
```

---

## Service Principal Authentication

### Step 1: Create and Configure Service Principal

```python
# Python script to create and configure Service Principal (run in Azure Cloud Shell or local with Azure CLI)

import json
import subprocess
import random
import string

class ServicePrincipalSetup:
    """Setup Service Principal for ADLS-Databricks connection"""
    
    def __init__(self, subscription_id, resource_group, storage_account_name):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.storage_account_name = storage_account_name
        self.sp_name = f"sp-adls-databricks-{''.join(random.choices(string.ascii_lowercase + string.digits, k=6))}"
    
    def create_service_principal(self):
        """Create service principal with required permissions"""
        
        print("=== Creating Service Principal ===")
        
        # Create service principal
        sp_create_cmd = [
            "az", "ad", "sp", "create-for-rbac",
            "--name", self.sp_name,
            "--role", "Storage Blob Data Contributor",
            "--scopes", f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Storage/storageAccounts/{self.storage_account_name}",
            "--output", "json"
        ]
        
        try:
            result = subprocess.run(sp_create_cmd, capture_output=True, text=True, check=True)
            sp_info = json.loads(result.stdout)
            
            self.client_id = sp_info["appId"]
            self.client_secret = sp_info["password"]
            self.tenant_id = sp_info["tenant"]
            
            print(f"Service Principal created successfully:")
            print(f"  Name: {self.sp_name}")
            print(f"  Client ID: {self.client_id}")
            print(f"  Tenant ID: {self.tenant_id}")
            print(f"  Client Secret: [HIDDEN]")
            
            return sp_info
            
        except subprocess.CalledProcessError as e:
            print(f"Error creating service principal: {e}")
            print(f"Error output: {e.stderr}")
            return None
    
    def assign_additional_roles(self):
        """Assign additional roles if needed"""
        
        print("\n=== Assigning Additional Roles ===")
        
        # Get storage account resource ID
        storage_scope = f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Storage/storageAccounts/{self.storage_account_name}"
        
        # Additional roles that might be needed
        additional_roles = [
            "Storage Blob Data Reader",  # For read-only access
            "Storage Queue Data Contributor"  # If using queues
        ]
        
        for role in additional_roles:
            assign_cmd = [
                "az", "role", "assignment", "create",
                "--assignee", self.client_id,
                "--role", role,
                "--scope", storage_scope
            ]
            
            try:
                subprocess.run(assign_cmd, capture_output=True, text=True, check=True)
                print(f"  Assigned role: {role}")
            except subprocess.CalledProcessError as e:
                print(f"  Warning: Could not assign role {role}: {e.stderr}")
    
    def test_service_principal(self):
        """Test service principal access"""
        
        print("\n=== Testing Service Principal Access ===")
        
        # Login with service principal
        login_cmd = [
            "az", "login",
            "--service-principal",
            "--username", self.client_id,
            "--password", self.client_secret,
            "--tenant", self.tenant_id
        ]
        
        try:
            subprocess.run(login_cmd, capture_output=True, text=True, check=True)
            print("  Service principal login successful")
            
            # Test storage access
            list_cmd = [
                "az", "storage", "container", "list",
                "--account-name", self.storage_account_name,
                "--auth-mode", "login"
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True, check=True)
            containers = json.loads(result.stdout)
            print(f"  Successfully accessed storage account, found {len(containers)} containers")
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"  Error testing service principal: {e.stderr}")
            return False
    
    def generate_databricks_config(self):
        """Generate configuration for Databricks"""
        
        config = {
            "service_principal": {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "tenant_id": self.tenant_id
            },
            "storage_account": {
                "name": self.storage_account_name,
                "url": f"https://{self.storage_account_name}.dfs.core.windows.net"
            },
            "databricks_secrets": {
                "scope_name": "adls-secrets",
                "secrets": {
                    "client-id": self.client_id,
                    "client-secret": self.client_secret,
                    "tenant-id": self.tenant_id
                }
            }
        }
        
        print("\n=== Databricks Configuration ===")
        print(json.dumps(config, indent=2))
        
        return config

# Usage example
# sp_setup = ServicePrincipalSetup(
#     subscription_id="your-subscription-id",
#     resource_group="your-resource-group", 
#     storage_account_name="your-storage-account"
# )
# sp_info = sp_setup.create_service_principal()
# if sp_info:
#     sp_setup.assign_additional_roles()
#     sp_setup.test_service_principal()
#     config = sp_setup.generate_databricks_config()
```

### Step 2: Configure Databricks Secrets

```python
# Databricks notebook cell - Configure secrets for Service Principal authentication

# Create secret scope (run this once)
dbutils.secrets.createScope("adls-secrets")

# Add secrets (replace with your actual values)
# Note: In production, use Databricks CLI or REST API to add secrets securely

# Service Principal credentials
client_id = "your-service-principal-client-id"
client_secret = "your-service-principal-client-secret"  
tenant_id = "your-azure-tenant-id"
storage_account_name = "your-storage-account-name"

print("=== Service Principal Configuration ===")
print(f"Client ID: {client_id}")
print(f"Tenant ID: {tenant_id}")
print(f"Storage Account: {storage_account_name}")
print("Client Secret: [CONFIGURED IN SECRETS]")

# Test secret retrieval
try:
    retrieved_client_id = dbutils.secrets.get(scope="adls-secrets", key="client-id")
    print(f"✓ Successfully retrieved client ID from secrets")
except Exception as e:
    print(f"✗ Error retrieving secrets: {e}")
```

### Step 3: Establish Connection with Service Principal

```python
# Databricks notebook - Connect to ADLS using Service Principal

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

class ADLSServicePrincipalConnector:
    """Connect to ADLS using Service Principal authentication"""
    
    def __init__(self, storage_account_name, container_name):
        self.storage_account_name = storage_account_name
        self.container_name = container_name
        self.spark = SparkSession.builder.getOrCreate()
        
        # Get credentials from Databricks secrets
        self.client_id = dbutils.secrets.get(scope="adls-secrets", key="client-id")
        self.client_secret = dbutils.secrets.get(scope="adls-secrets", key="client-secret")
        self.tenant_id = dbutils.secrets.get(scope="adls-secrets", key="tenant-id")
        
        # Configure Spark for ADLS access
        self.configure_spark()
    
    def configure_spark(self):
        """Configure Spark session for ADLS access with Service Principal"""
        
        print("=== Configuring Spark for ADLS Access ===")
        
        # Set Spark configuration for OAuth authentication
        self.spark.conf.set(
            f"fs.azure.account.auth.type.{self.storage_account_name}.dfs.core.windows.net", 
            "OAuth"
        )
        
        self.spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{self.storage_account_name}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )
        
        self.spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{self.storage_account_name}.dfs.core.windows.net",
            self.client_id
        )
        
        self.spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{self.storage_account_name}.dfs.core.windows.net",
            self.client_secret
        )
        
        self.spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{self.storage_account_name}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"
        )
        
        print("✓ Spark configuration completed")
    
    def test_connection(self):
        """Test the connection to ADLS"""
        
        print("\n=== Testing ADLS Connection ===")
        
        try:
            # Test basic connectivity
            adls_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/"
            
            # List files in root directory
            files = dbutils.fs.ls(adls_path)
            print(f"✓ Successfully connected to ADLS")
            print(f"✓ Found {len(files)} items in container root")
            
            # Show first few items
            for i, file_info in enumerate(files[:5]):
                print(f"  {i+1}. {file_info.name} ({file_info.size} bytes)")
            
            return True
            
        except Exception as e:
            print(f"✗ Connection failed: {str(e)}")
            return False
    
    def create_test_data(self):
        """Create test data to verify write operations"""
        
        print("\n=== Creating Test Data ===")
        
        # Create sample DataFrame
        test_data = [
            (1, "Alice", "Engineering", 75000, "2023-01-15"),
            (2, "Bob", "Sales", 65000, "2023-02-20"),
            (3, "Charlie", "Marketing", 70000, "2023-03-10"),
            (4, "Diana", "Engineering", 80000, "2023-04-05"),
            (5, "Eve", "HR", 60000, "2023-05-12")
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("department", StringType(), False),
            StructField("salary", IntegerType(), False),
            StructField("hire_date", StringType(), False)
        ])
        
        df = self.spark.createDataFrame(test_data, schema)
        
        # Add computed columns
        df_enhanced = df.withColumn("hire_date", to_date(col("hire_date"))) \
                       .withColumn("salary_grade", 
                           when(col("salary") >= 75000, "Senior")
                           .when(col("salary") >= 65000, "Mid")
                           .otherwise("Junior")) \
                       .withColumn("years_of_service", 
                           datediff(current_date(), col("hire_date")) / 365.25)
        
        print("✓ Test data created successfully")
        df_enhanced.show()
        
        return df_enhanced
    
    def write_data_to_adls(self, df, path, format_type="parquet"):
        """Write DataFrame to ADLS"""
        
        print(f"\n=== Writing Data to ADLS ({format_type.upper()}) ===")
        
        try:
            adls_write_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/{path}"
            
            start_time = time.time()
            
            if format_type.lower() == "parquet":
                df.write.mode("overwrite").parquet(adls_write_path)
            elif format_type.lower() == "csv":
                df.write.mode("overwrite").option("header", "true").csv(adls_write_path)
            elif format_type.lower() == "json":
                df.write.mode("overwrite").json(adls_write_path)
            elif format_type.lower() == "delta":
                df.write.mode("overwrite").format("delta").save(adls_write_path)
            
            write_time = time.time() - start_time
            
            print(f"✓ Data written successfully to: {adls_write_path}")
            print(f"✓ Write operation completed in {write_time:.2f} seconds")
            
            # Verify write by listing files
            files = dbutils.fs.ls(adls_write_path)
            total_size = sum(f.size for f in files)
            print(f"✓ Verification: {len(files)} files written, total size: {total_size:,} bytes")
            
            return adls_write_path
            
        except Exception as e:
            print(f"✗ Write operation failed: {str(e)}")
            return None
    
    def read_data_from_adls(self, path, format_type="parquet"):
        """Read DataFrame from ADLS"""
        
        print(f"\n=== Reading Data from ADLS ({format_type.upper()}) ===")
        
        try:
            adls_read_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/{path}"
            
            start_time = time.time()
            
            if format_type.lower() == "parquet":
                df = self.spark.read.parquet(adls_read_path)
            elif format_type.lower() == "csv":
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(adls_read_path)
            elif format_type.lower() == "json":
                df = self.spark.read.json(adls_read_path)
            elif format_type.lower() == "delta":
                df = self.spark.read.format("delta").load(adls_read_path)
            
            read_time = time.time() - start_time
            
            print(f"✓ Data read successfully from: {adls_read_path}")
            print(f"✓ Read operation completed in {read_time:.2f} seconds")
            print(f"✓ DataFrame shape: {df.count()} rows, {len(df.columns)} columns")
            
            # Show sample data
            print("\nSample data:")
            df.show(5)
            
            return df
            
        except Exception as e:
            print(f"✗ Read operation failed: {str(e)}")
            return None
    
    def demonstrate_advanced_operations(self):
        """Demonstrate advanced data operations with ADLS"""
        
        print("\n=== Advanced ADLS Operations Demo ===")
        
        # Create larger dataset for demonstration
        print("1. Creating larger dataset...")
        large_data = self.spark.range(100000) \
            .withColumn("customer_id", (col("id") % 10000).cast("string")) \
            .withColumn("product_category", 
                when(col("id") % 4 == 0, "Electronics")
                .when(col("id") % 4 == 1, "Clothing") 
                .when(col("id") % 4 == 2, "Books")
                .otherwise("Home")) \
            .withColumn("purchase_amount", (rand() * 1000).cast("decimal(10,2)")) \
            .withColumn("purchase_date", 
                date_add(lit("2023-01-01"), (col("id") % 365).cast("int"))) \
            .withColumn("region", 
                when(col("id") % 3 == 0, "North")
                .when(col("id") % 3 == 1, "South")
                .otherwise("East"))
        
        print(f"   Created dataset with {large_data.count():,} records")
        
        # Write partitioned data
        print("2. Writing partitioned data to ADLS...")
        partitioned_path = "data/sales/partitioned"
        adls_partitioned_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/{partitioned_path}"
        
        large_data.write.mode("overwrite") \
                 .partitionBy("product_category", "region") \
                 .parquet(adls_partitioned_path)
        
        print("   ✓ Partitioned data written successfully")
        
        # Read and analyze partitioned data
        print("3. Reading and analyzing partitioned data...")
        partitioned_df = self.spark.read.parquet(adls_partitioned_path)
        
        # Perform aggregations
        analysis_result = partitioned_df.groupBy("product_category", "region") \
            .agg(
                count("id").alias("transaction_count"),
                sum("purchase_amount").alias("total_revenue"),
                avg("purchase_amount").alias("avg_purchase"),
                countDistinct("customer_id").alias("unique_customers")
            ) \
            .withColumn("revenue_per_customer", 
                col("total_revenue") / col("unique_customers")) \
            .orderBy(desc("total_revenue"))
        
        print("   Analysis Results:")
        analysis_result.show(20, truncate=False)
        
        # Save analysis results
        print("4. Saving analysis results...")
        analysis_path = "results/sales_analysis"
        adls_analysis_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/{analysis_path}"
        
        analysis_result.write.mode("overwrite") \
                       .option("header", "true") \
                       .csv(adls_analysis_path)
        
        print("   ✓ Analysis results saved successfully")
        
        return {
            "dataset_size": large_data.count(),
            "partitioned_path": adls_partitioned_path,
            "analysis_path": adls_analysis_path,
            "analysis_results": analysis_result.count()
        }

# Initialize and demonstrate Service Principal connection
storage_account_name = "your-storage-account-name"  # Replace with your storage account
container_name = "raw-data"  # Replace with your container name

connector = ADLSServicePrincipalConnector(storage_account_name, container_name)

# Test connection
connection_success = connector.test_connection()

if connection_success:
    # Create and write test data
    test_df = connector.create_test_data()
    
    # Test different formats
    formats_to_test = ["parquet", "csv", "json"]
    
    for fmt in formats_to_test:
        write_path = f"test_data/employees_{fmt}"
        written_path = connector.write_data_to_adls(test_df, write_path, fmt)
        
        if written_path:
            read_df = connector.read_data_from_adls(write_path, fmt)
    
    # Demonstrate advanced operations
    advanced_results = connector.demonstrate_advanced_operations()
    
    print("\n=== Service Principal Connection Demo Complete ===")
    print("✓ All operations completed successfully")
else:
    print("✗ Connection failed - check your configuration")
```

---

## Storage Account Key Authentication

### Configuration and Usage

```python
# Databricks notebook - Connect to ADLS using Storage Account Key

class ADLSStorageKeyConnector:
    """Connect to ADLS using Storage Account Key authentication"""
    
    def __init__(self, storage_account_name, storage_account_key, container_name):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.container_name = container_name
        self.spark = SparkSession.builder.getOrCreate()
        
        # Configure Spark for ADLS access
        self.configure_spark()
    
    def configure_spark(self):
        """Configure Spark session for ADLS access with Storage Account Key"""
        
        print("=== Configuring Spark for ADLS Access (Storage Key) ===")
        
        # Set the storage account key
        self.spark.conf.set(
            f"fs.azure.account.key.{self.storage_account_name}.dfs.core.windows.net",
            self.storage_account_key
        )
        
        print("✓ Spark configuration completed")
    
    def test_connection_and_operations(self):
        """Test connection and perform basic operations"""
        
        print("\n=== Testing Storage Key Connection ===")
        
        try:
            # Test basic connectivity
            adls_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/"
            
            # List files in root directory
            files = dbutils.fs.ls(adls_path)
            print(f"✓ Successfully connected to ADLS")
            print(f"✓ Found {len(files)} items in container root")
            
            # Create test data
            test_data = [
                ("2023-01-01", "Product A", 100, 25.50),
                ("2023-01-02", "Product B", 150, 30.75),
                ("2023-01-03", "Product C", 200, 15.25),
                ("2023-01-04", "Product A", 120, 25.50),
                ("2023-01-05", "Product B", 180, 30.75)
            ]
            
            schema = StructType([
                StructField("date", StringType(), False),
                StructField("product", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("price", DoubleType(), False)
            ])
            
            df = self.spark.createDataFrame(test_data, schema)
            df = df.withColumn("date", to_date(col("date"))) \
                   .withColumn("total_value", col("quantity") * col("price"))
            
            print("\nTest data created:")
            df.show()
            
            # Write data to ADLS
            write_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/test_storage_key/sales_data"
            
            df.write.mode("overwrite").parquet(write_path)
            print(f"✓ Data written to: {write_path}")
            
            # Read data back
            read_df = self.spark.read.parquet(write_path)
            print("✓ Data read back successfully:")
            read_df.show()
            
            return True
            
        except Exception as e:
            print(f"✗ Operation failed: {str(e)}")
            return False

# Usage with Storage Account Key
# WARNING: Store keys securely in production environments

# Get storage account key from Databricks secrets (recommended)
try:
    storage_account_key = dbutils.secrets.get(scope="adls-secrets", key="storage-account-key")
    print("✓ Retrieved storage account key from secrets")
except:
    # Fallback - NOT recommended for production
    storage_account_key = "your-storage-account-key-here"
    print("⚠️  Using hardcoded storage key - not recommended for production")

storage_account_name = "your-storage-account-name"
container_name = "raw-data"

# Initialize connector
key_connector = ADLSStorageKeyConnector(
    storage_account_name=storage_account_name,
    storage_account_key=storage_account_key,
    container_name=container_name
)

# Test connection and operations
success = key_connector.test_connection_and_operations()

if success:
    print("✓ Storage Account Key authentication successful")
else:
    print("✗ Storage Account Key authentication failed")
```

---

## SAS Token Authentication

### SAS Token Generation and Usage

```python
# Python script to generate SAS token (run in Azure Cloud Shell or with Azure SDK)

from azure.storage.blob import BlobServiceClient, generate_container_sas, ContainerSasPermissions
from datetime import datetime, timedelta
import json

class SASTokenGenerator:
    """Generate SAS tokens for ADLS access"""
    
    def __init__(self, storage_account_name, storage_account_key):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=storage_account_key
        )
    
    def generate_container_sas_token(self, container_name, permissions="rwdl", expiry_hours=24):
        """Generate SAS token for container access"""
        
        print(f"=== Generating SAS Token for Container: {container_name} ===")
        
        # Define permissions
        permission_map = {
            'r': 'read',
            'w': 'write', 
            'd': 'delete',
            'l': 'list',
            'a': 'add',
            'c': 'create'
        }
        
        # Set expiry time
        expiry_time = datetime.utcnow() + timedelta(hours=expiry_hours)
        
        # Generate SAS token
        sas_token = generate_container_sas(
            account_name=self.storage_account_name,
            container_name=container_name,
            account_key=self.storage_account_key,
            permission=ContainerSasPermissions(
                read='r' in permissions,
                write='w' in permissions,
                delete='d' in permissions,
                list='l' in permissions,
                add='a' in permissions,
                create='c' in permissions
            ),
            expiry=expiry_time
        )
        
        # Construct full URL
        container_url_with_sas = f"https://{self.storage_account_name}.blob.core.windows.net/{container_name}?{sas_token}"
        
        token_info = {
            "container_name": container_name,
            "sas_token": sas_token,
            "container_url_with_sas": container_url_with_sas,
            "permissions": [permission_map.get(p, p) for p in permissions],
            "expiry_time": expiry_time.isoformat(),
            "expiry_hours": expiry_hours
        }
        
        print(f"✓ SAS Token generated successfully")
        print(f"  Container: {container_name}")
        print(f"  Permissions: {', '.join(token_info['permissions'])}")
        print(f"  Expires: {expiry_time}")
        print(f"  Token: {sas_token[:20]}...")
        
        return token_info
    
    def generate_multiple_tokens(self, containers, permissions="rwdl", expiry_hours=24):
        """Generate SAS tokens for multiple containers"""
        
        print(f"=== Generating SAS Tokens for Multiple Containers ===")
        
        tokens = {}
        for container in containers:
            try:
                token_info = self.generate_container_sas_token(
                    container_name=container,
                    permissions=permissions,
                    expiry_hours=expiry_hours
                )
                tokens[container] = token_info
            except Exception as e:
                print(f"✗ Failed to generate token for {container}: {e}")
        
        return tokens

# Generate SAS tokens
# sas_generator = SASTokenGenerator(
#     storage_account_name="your-storage-account",
#     storage_account_key="your-storage-key"
# )

# containers = ["raw-data", "processed-data", "curated-data"]
# sas_tokens = sas_generator.generate_multiple_tokens(containers, permissions="rwdl", expiry_hours=168)  # 7 days
```

```python
# Databricks notebook - Connect to ADLS using SAS Token

class ADLSSASTokenConnector:
    """Connect to ADLS using SAS Token authentication"""
    
    def __init__(self, storage_account_name, container_name, sas_token):
        self.storage_account_name = storage_account_name
        self.container_name = container_name
        self.sas_token = sas_token
        self.spark = SparkSession.builder.getOrCreate()
        
        # Configure Spark for ADLS access
        self.configure_spark()
    
    def configure_spark(self):
        """Configure Spark session for ADLS access with SAS Token"""
        
        print("=== Configuring Spark for ADLS Access (SAS Token) ===")
        
        # Set the SAS token for the specific container
        self.spark.conf.set(
            f"fs.azure.sas.{self.container_name}.{self.storage_account_name}.dfs.core.windows.net",
            self.sas_token
        )
        
        print("✓ Spark configuration completed")
    
    def test_sas_connection(self):
        """Test SAS token connection and perform operations"""
        
        print("\n=== Testing SAS Token Connection ===")
        
        try:
            # Test basic connectivity
            adls_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/"
            
            # List files in root directory
            files = dbutils.fs.ls(adls_path)
            print(f"✓ Successfully connected to ADLS using SAS token")
            print(f"✓ Found {len(files)} items in container root")
            
            # Create sample data for testing
            sample_data = [
                (1, "2023-01-01", "Customer A", "Electronics", 1500.00),
                (2, "2023-01-02", "Customer B", "Clothing", 750.00),
                (3, "2023-01-03", "Customer C", "Books", 250.00),
                (4, "2023-01-04", "Customer A", "Home", 900.00),
                (5, "2023-01-05", "Customer D", "Electronics", 2100.00)
            ]
            
            schema = StructType([
                StructField("transaction_id", IntegerType(), False),
                StructField("date", StringType(), False),
                StructField("customer", StringType(), False),
                StructField("category", StringType(), False),
                StructField("amount", DoubleType(), False)
            ])
            
            df = self.spark.createDataFrame(sample_data, schema)
            df = df.withColumn("date", to_date(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("amount_tier", 
                       when(col("amount") > 1000, "High")
                       .when(col("amount") > 500, "Medium")
                       .otherwise("Low"))
            
            print("\nSample data created:")
            df.show()
            
            # Test write operation
            write_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/sas_test/transactions"
            
            df.coalesce(1).write.mode("overwrite") \
              .option("header", "true") \
              .csv(write_path)
            
            print(f"✓ Data written to: {write_path}")
            
            # Test read operation
            read_df = self.spark.read.option("header", "true") \
                                   .option("inferSchema", "true") \
                                   .csv(write_path)
            
            print("✓ Data read back successfully:")
            read_df.show()
            
            # Perform aggregation
            summary = read_df.groupBy("category", "amount_tier") \
                           .agg(
                               count("transaction_id").alias("transaction_count"),
                               sum("amount").alias("total_amount"),
                               avg("amount").alias("avg_amount")
                           ) \
                           .orderBy("category", "amount_tier")
            
            print("Transaction summary:")
            summary.show()
            
            return True
            
        except Exception as e:
            print(f"✗ SAS token operation failed: {str(e)}")
            return False
    
    def check_token_expiry(self):
        """Check if SAS token is still valid"""
        
        print("\n=== Checking SAS Token Validity ===")
        
        try:
            # Try a simple operation
            adls_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/"
            files = dbutils.fs.ls(adls_path)
            
            print("✓ SAS token is still valid")
            return True
            
        except Exception as e:
            error_msg = str(e).lower()
            if "forbidden" in error_msg or "unauthorized" in error_msg:
                print("✗ SAS token has expired or insufficient permissions")
            else:
                print(f"✗ Token validation failed: {e}")
            return False

# Usage with SAS Token
# Get SAS token from secrets (recommended) or generate it

try:
    sas_token = dbutils.secrets.get(scope="adls-secrets", key="sas-token")
    print("✓ Retrieved SAS token from secrets")
except:
    # Example SAS token (replace with actual token)
    sas_token = "your-sas-token-here"
    print("⚠️  Using example SAS token - replace with actual token")

storage_account_name = "your-storage-account-name"
container_name = "raw-data"

# Initialize SAS connector
sas_connector = ADLSSASTokenConnector(
    storage_account_name=storage_account_name,
    container_name=container_name,
    sas_token=sas_token
)

# Test SAS connection
sas_success = sas_connector.test_sas_connection()

if sas_success:
    print("✓ SAS Token authentication successful")
    
    # Check token validity
    sas_connector.check_token_expiry()
else:
    print("✗ SAS Token authentication failed")
```

---

## Mounting ADLS in Databricks

### Mount Configuration

```python
# Databricks notebook - Mount ADLS containers

class ADLSMountManager:
    """Manage ADLS mounts in Databricks"""
    
    def __init__(self):
        self.mounted_paths = {}
    
    def mount_with_service_principal(self, storage_account_name, container_name, mount_point, 
                                   client_id, client_secret, tenant_id):
        """Mount ADLS container using Service Principal"""
        
        print(f"=== Mounting {container_name} with Service Principal ===")
        
        try:
            # Check if already mounted
            if self.is_mounted(mount_point):
                print(f"⚠️  Mount point {mount_point} already exists. Unmounting first...")
                self.unmount(mount_point)
            
            # Configure mount
            configs = {
                "fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
            }
            
            # Mount the container
            dbutils.fs.mount(
                source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                mount_point=mount_point,
                extra_configs=configs
            )
            
            # Verify mount
            if self.is_mounted(mount_point):
                print(f"✓ Successfully mounted {container_name} at {mount_point}")
                
                # Test mount by listing contents
                files = dbutils.fs.ls(mount_point)
                print(f"✓ Mount verification: found {len(files)} items")
                
                # Store mount info
                self.mounted_paths[mount_point] = {
                    "storage_account": storage_account_name,
                    "container": container_name,
                    "auth_type": "service_principal",
                    "mount_time": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                return True
            else:
                print(f"✗ Mount verification failed")
                return False
                
        except Exception as e:
            print(f"✗ Mount operation failed: {str(e)}")
            return False
    
    def mount_with_storage_key(self, storage_account_name, container_name, mount_point, storage_key):
        """Mount ADLS container using Storage Account Key"""
        
        print(f"=== Mounting {container_name} with Storage Key ===")
        
        try:
            # Check if already mounted
            if self.is_mounted(mount_point):
                print(f"⚠️  Mount point {mount_point} already exists. Unmounting first...")
                self.unmount(mount_point)
            
            # Configure mount
            configs = {
                f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net": storage_key
            }
            
            # Mount the container
            dbutils.fs.mount(
                source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                mount_point=mount_point,
                extra_configs=configs
            )
            
            # Verify mount
            if self.is_mounted(mount_point):
                print(f"✓ Successfully mounted {container_name} at {mount_point}")
                
                # Store mount info
                self.mounted_paths[mount_point] = {
                    "storage_account": storage_account_name,
                    "container": container_name,
                    "auth_type": "storage_key",
                    "mount_time": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                return True
            else:
                print(f"✗ Mount verification failed")
                return False
                
        except Exception as e:
            print(f"✗ Mount operation failed: {str(e)}")
            return False
    
    def mount_with_sas_token(self, storage_account_name, container_name, mount_point, sas_token):
        """Mount ADLS container using SAS Token"""
        
        print(f"=== Mounting {container_name} with SAS Token ===")
        
        try:
            # Check if already mounted
            if self.is_mounted(mount_point):
                print(f"⚠️  Mount point {mount_point} already exists. Unmounting first...")
                self.unmount(mount_point)
            
            # Configure mount
            configs = {
                f"fs.azure.sas.{container_name}.{storage_account_name}.dfs.core.windows.net": sas_token
            }
            
            # Mount the container
            dbutils.fs.mount(
                source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                mount_point=mount_point,
                extra_configs=configs
            )
            
            # Verify mount
            if self.is_mounted(mount_point):
                print(f"✓ Successfully mounted {container_name} at {mount_point}")
                
                # Store mount info
                self.mounted_paths[mount_point] = {
                    "storage_account": storage_account_name,
                    "container": container_name,
                    "auth_type": "sas_token",
                    "mount_time": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                return True
            else:
                print(f"✗ Mount verification failed")
                return False
                
        except Exception as e:
            print(f"✗ Mount operation failed: {str(e)}")
            return False
    
    def is_mounted(self, mount_point):
        """Check if a mount point exists"""
        try:
            mounts = dbutils.fs.mounts()
            return any(mount.mountPoint == mount_point for mount in mounts)
        except:
            return False
    
    def list_mounts(self):
        """List all current mounts"""
        
        print("=== Current Mounts ===")
        
        try:
            mounts = dbutils.fs.mounts()
            
            if not mounts:
                print("No mounts found")
                return []
            
            print(f"Found {len(mounts)} mount(s):")
            
            for i, mount in enumerate(mounts, 1):
                print(f"{i}. Mount Point: {mount.mountPoint}")
                print(f"   Source: {mount.source}")
                
                # Add additional info if available
                if mount.mountPoint in self.mounted_paths:
                    info = self.mounted_paths[mount.mountPoint]
                    print(f"   Storage Account: {info['storage_account']}")
                    print(f"   Container: {info['container']}")
                    print(f"   Auth Type: {info['auth_type']}")
                    print(f"   Mount Time: {info['mount_time']}")
                print()
            
            return mounts
            
        except Exception as e:
            print(f"✗ Failed to list mounts: {str(e)}")
            return []
    
    def unmount(self, mount_point):
        """Unmount a specific mount point"""
        
        print(f"=== Unmounting {mount_point} ===")
        
        try:
            if self.is_mounted(mount_point):
                dbutils.fs.unmount(mount_point)
                print(f"✓ Successfully unmounted {mount_point}")
                
                # Remove from tracking
                if mount_point in self.mounted_paths:
                    del self.mounted_paths[mount_point]
                
                return True
            else:
                print(f"⚠️  Mount point {mount_point} not found")
                return False
                
        except Exception as e:
            print(f"✗ Unmount operation failed: {str(e)}")
            return False
    
    def unmount_all(self):
        """Unmount all mounts (use with caution)"""
        
        print("=== Unmounting All Mounts ===")
        
        try:
            mounts = dbutils.fs.mounts()
            
            # Skip default mounts (typically /databricks/*)
            custom_mounts = [mount for mount in mounts if not mount.mountPoint.startswith('/databricks')]
            
            if not custom_mounts:
                print("No custom mounts to unmount")
                return True
            
            print(f"Found {len(custom_mounts)} custom mount(s) to unmount")
            
            for mount in custom_mounts:
                print(f"Unmounting: {mount.mountPoint}")
                try:
                    dbutils.fs.unmount(mount.mountPoint)
                    print(f"✓ Unmounted {mount.mountPoint}")
                except Exception as e:
                    print(f"✗ Failed to unmount {mount.mountPoint}: {e}")
            
            # Clear tracking
            self.mounted_paths.clear()
            
            return True
            
        except Exception as e:
            print(f"✗ Unmount all operation failed: {str(e)}")
            return False
    
    def test_mount_operations(self, mount_point):
        """Test operations on a mounted path"""
        
        print(f"=== Testing Mount Operations: {mount_point} ===")
        
        if not self.is_mounted(mount_point):
            print(f"✗ Mount point {mount_point} not found")
            return False
        
        try:
            # Test listing files
            print("1. Testing file listing...")
            files = dbutils.fs.ls(mount_point)
            print(f"   ✓ Found {len(files)} items")
            
            # Test creating a test file
            print("2. Testing file creation...")
            test_file_path = f"{mount_point}/test_mount_file.txt"
            
            # Create test content
            test_content = f"Mount test file created at {time.strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Write test file
            dbutils.fs.put(test_file_path, test_content, overwrite=True)
            print(f"   ✓ Created test file: {test_file_path}")
            
            # Test reading the file
            print("3. Testing file reading...")
            file_content = dbutils.fs.head(test_file_path)
            print(f"   ✓ Read content: {file_content.strip()}")
            
            # Test DataFrame operations
            print("4. Testing DataFrame operations...")
            
            # Create test DataFrame
            test_data = [
                ("mount_test", 1, "2023-01-01"),
                ("mount_test", 2, "2023-01-02"),
                ("mount_test", 3, "2023-01-03")
            ]
            
            schema = StructType([
                StructField("test_type", StringType(), False),
                StructField("id", IntegerType(), False),
                StructField("date", StringType(), False)
            ])
            
            df = spark.createDataFrame(test_data, schema)
            df = df.withColumn("date", to_date(col("date")))
            
            # Write DataFrame to mount
            df_path = f"{mount_point}/test_dataframe"
            df.write.mode("overwrite").parquet(df_path)
            print(f"   ✓ DataFrame written to: {df_path}")
            
            # Read DataFrame back
            read_df = spark.read.parquet(df_path)
            print(f"   ✓ DataFrame read back: {read_df.count()} rows")
            
            # Clean up test files
            print("5. Cleaning up test files...")
            dbutils.fs.rm(test_file_path)
            dbutils.fs.rm(df_path, recurse=True)
            print("   ✓ Test files cleaned up")
            
            return True
            
        except Exception as e:
            print(f"✗ Mount test failed: {str(e)}")
            return False

# Initialize mount manager
mount_manager = ADLSMountManager()

# Configuration
storage_account_name = "your-storage-account-name"
containers = ["raw-data", "processed-data", "curated-data"]

# Get credentials from secrets
client_id = dbutils.secrets.get(scope="adls-secrets", key="client-id")
client_secret = dbutils.secrets.get(scope="adls-secrets", key="client-secret")
tenant_id = dbutils.secrets.get(scope="adls-secrets", key="tenant-id")

# Mount containers using Service Principal
print("=== Mounting ADLS Containers ===")

mount_success = {}

for container in containers:
    mount_point = f"/mnt/{container}"
    
    success = mount_manager.mount_with_service_principal(
        storage_account_name=storage_account_name,
        container_name=container,
        mount_point=mount_point,
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id
    )
    
    mount_success[container] = success
    
    if success:
        # Test mount operations
        mount_manager.test_mount_operations(mount_point)

# List all mounts
mount_manager.list_mounts()

# Summary
print("\n=== Mount Summary ===")
for container, success in mount_success.items():
    status = "✓ Success" if success else "✗ Failed"
    print(f"{container}: {status}")
```

This comprehensive guide provides everything needed to connect Azure Data Lake Storage to Azure Databricks with multiple authentication methods, detailed examples, and best practices for secure and efficient data operations.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*