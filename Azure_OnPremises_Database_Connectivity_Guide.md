# Azure On-Premises Database Connectivity Guide
## Complete Guide for Connecting ADF and Databricks to SQL Server, Oracle, and MySQL

---

### Table of Contents

1. [Overview](#overview)
2. [Self-Hosted Integration Runtime Setup](#self-hosted-integration-runtime-setup)
3. [SQL Server Connectivity](#sql-server-connectivity)
4. [Oracle Database Connectivity](#oracle-database-connectivity)
5. [MySQL Database Connectivity](#mysql-database-connectivity)
6. [Azure Databricks Integration](#azure-databricks-integration)
7. [Security and Authentication](#security-and-authentication)
8. [Performance Optimization](#performance-optimization)
9. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
10. [Best Practices](#best-practices)

---

## Overview

Connecting Azure services to on-premises databases requires careful planning for security, performance, and reliability. This guide covers comprehensive connectivity solutions for Azure Data Factory (ADF) and Azure Databricks to on-premises SQL Server, Oracle, and MySQL databases.

### Architecture Overview

```json
{
  "connectivity_architecture": {
    "components": {
      "azure_services": ["Azure Data Factory", "Azure Databricks", "Azure Key Vault"],
      "on_premises": ["SQL Server", "Oracle Database", "MySQL", "Self-Hosted IR"],
      "networking": ["VPN Gateway", "ExpressRoute", "Private Endpoints"]
    },
    "data_flow": "On-Premises DB -> Self-Hosted IR -> Azure Services",
    "security_layers": ["Network Security", "Authentication", "Encryption", "Access Control"]
  }
}
```

### Prerequisites

```powershell
# Prerequisites checklist
$prerequisites = @{
    "Azure_Resources" = @(
        "Azure Data Factory instance",
        "Azure Databricks workspace", 
        "Azure Key Vault for secrets",
        "Resource Group with appropriate permissions"
    )
    "On_Premises_Requirements" = @(
        "Windows Server (2012 R2 or later) for Self-Hosted IR",
        "Network connectivity to Azure (VPN/ExpressRoute)",
        "Database access credentials",
        "Firewall rules configured"
    )
    "Database_Versions" = @{
        "SQL_Server" = "2008 R2 or later"
        "Oracle" = "10g or later"
        "MySQL" = "5.6 or later"
    }
}
```

---

## Self-Hosted Integration Runtime Setup

### Installation and Configuration

```powershell
# Step 1: Download and install Self-Hosted Integration Runtime
# Download from: https://www.microsoft.com/en-us/download/details.aspx?id=39717

# Step 2: PowerShell script for automated installation
param(
    [Parameter(Mandatory=$true)]
    [string]$AuthKey,
    [string]$NodeName = $env:COMPUTERNAME,
    [string]$InstallPath = "C:\Program Files\Microsoft Integration Runtime\5.0"
)

Write-Host "Installing Self-Hosted Integration Runtime..."

# Create installation directory
if (!(Test-Path $InstallPath)) {
    New-Item -ItemType Directory -Path $InstallPath -Force
}

# Download installer (replace with actual download logic)
$installerUrl = "https://download.microsoft.com/download/..."
$installerPath = "$env:TEMP\IntegrationRuntime.msi"

# Install silently
Start-Process msiexec.exe -Wait -ArgumentList "/i $installerPath /quiet /norestart"

# Register with Azure Data Factory
$configPath = "$InstallPath\Microsoft.DataTransfer.Gateway.Host.Service.exe"
& $configPath -key $AuthKey -nodeName $NodeName

Write-Host "Self-Hosted IR installation completed successfully"
```

### Configuration Script

```json
{
  "self_hosted_ir_config": {
    "name": "OnPremisesSHIR",
    "description": "Self-Hosted Integration Runtime for on-premises databases",
    "properties": {
      "type": "SelfHosted",
      "typeProperties": {
        "linkedInfo": {
          "resourceId": "/subscriptions/{subscription}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{adf}/integrationRuntimes/OnPremisesSHIR",
          "authorizationType": "Key"
        }
      }
    },
    "nodes": [
      {
        "nodeName": "OnPremNode01",
        "status": "Online",
        "version": "5.x.x.x",
        "capabilities": ["SQL Server", "Oracle", "MySQL"]
      }
    ]
  }
}
```

### PowerShell Management Script

```powershell
# Self-Hosted IR Management Functions
function Get-SHIRStatus {
    param([string]$NodeName)
    
    $serviceName = "DIAHostService"
    $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
    
    if ($service) {
        return @{
            "Status" = $service.Status
            "StartType" = $service.StartType
            "NodeName" = $NodeName
            "LastUpdated" = Get-Date
        }
    }
    return $null
}

function Start-SHIRService {
    try {
        Start-Service -Name "DIAHostService"
        Write-Host "Self-Hosted IR service started successfully"
    }
    catch {
        Write-Error "Failed to start Self-Hosted IR service: $_"
    }
}

function Test-DatabaseConnectivity {
    param(
        [string]$ServerName,
        [string]$DatabaseName,
        [string]$ConnectionType,
        [PSCredential]$Credential
    )
    
    switch ($ConnectionType) {
        "SQLServer" {
            $connectionString = "Server=$ServerName;Database=$DatabaseName;Integrated Security=false;User ID=$($Credential.UserName);Password=$($Credential.GetNetworkCredential().Password)"
        }
        "Oracle" {
            $connectionString = "Data Source=$ServerName;User Id=$($Credential.UserName);Password=$($Credential.GetNetworkCredential().Password);"
        }
        "MySQL" {
            $connectionString = "Server=$ServerName;Database=$DatabaseName;Uid=$($Credential.UserName);Pwd=$($Credential.GetNetworkCredential().Password);"
        }
    }
    
    # Test connection logic here
    Write-Host "Testing $ConnectionType connectivity to $ServerName/$DatabaseName"
    return $true
}

# Usage examples
$status = Get-SHIRStatus -NodeName "OnPremNode01"
Write-Host "SHIR Status: $($status.Status)"
```

---

## SQL Server Connectivity

### Linked Service Configuration

```json
{
  "name": "OnPremisesSQLServer",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "SqlServer",
    "connectVia": {
      "referenceName": "OnPremisesSHIR",
      "type": "IntegrationRuntimeReference"
    },
    "typeProperties": {
      "server": "SQLSERVER01.contoso.local",
      "database": "AdventureWorks2019",
      "authenticationType": "SQL",
      "userName": "adf_service_account",
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVault1",
          "type": "LinkedServiceReference"
        },
        "secretName": "sql-server-password"
      },
      "encrypt": "true",
      "trustServerCertificate": "false",
      "connectionTimeout": 30,
      "commandTimeout": 120
    }
  }
}
```

### Multiple Database Configuration

```json
{
  "sql_server_databases": [
    {
      "name": "AdventureWorks_LinkedService",
      "server": "SQLSERVER01.contoso.local",
      "database": "AdventureWorks2019",
      "description": "Production sales and inventory database"
    },
    {
      "name": "HumanResources_LinkedService", 
      "server": "SQLSERVER01.contoso.local",
      "database": "HumanResources",
      "description": "HR and employee management database"
    },
    {
      "name": "Finance_LinkedService",
      "server": "SQLSERVER02.contoso.local",
      "database": "FinanceDB",
      "description": "Financial reporting and analytics database"
    }
  ]
}
```

### Dataset Configuration

```json
{
  "name": "SqlServerTable_AdventureWorks",
  "properties": {
    "type": "SqlServerTable",
    "linkedServiceName": {
      "referenceName": "OnPremisesSQLServer",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "schema": "Sales",
      "table": "SalesOrderHeader"
    },
    "parameters": {
      "schemaName": {
        "type": "String",
        "defaultValue": "Sales"
      },
      "tableName": {
        "type": "String",
        "defaultValue": "SalesOrderHeader"
      }
    }
  }
}
```

### Pipeline Examples

```json
{
  "name": "CopySQLServerData",
  "properties": {
    "activities": [
      {
        "name": "CopyFromSQLServer",
        "type": "Copy",
        "inputs": [
          {
            "referenceName": "SqlServerTable_AdventureWorks",
            "type": "DatasetReference",
            "parameters": {
              "schemaName": "Sales",
              "tableName": "SalesOrderHeader"
            }
          }
        ],
        "outputs": [
          {
            "referenceName": "AzureDataLakeGen2",
            "type": "DatasetReference"
          }
        ],
        "typeProperties": {
          "source": {
            "type": "SqlServerSource",
            "sqlReaderQuery": "SELECT * FROM Sales.SalesOrderHeader WHERE ModifiedDate >= '@{formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd')}'"
          },
          "sink": {
            "type": "DelimitedTextSink",
            "storeSettings": {
              "type": "AzureBlobFSWriteSettings"
            },
            "formatSettings": {
              "type": "DelimitedTextWriteSettings",
              "quoteAllText": true,
              "fileExtension": ".csv"
            }
          },
          "enableStaging": false,
          "parallelCopies": 4
        }
      }
    ]
  }
}
```

---

## Oracle Database Connectivity

### Oracle Client Installation

```powershell
# Oracle Instant Client installation script
param(
    [string]$OracleClientPath = "C:\oracle\instantclient_21_3",
    [string]$DownloadUrl = "https://download.oracle.com/otn_software/nt/instantclient/213000/instantclient-basic-windows.x64-21.3.0.0.0.zip"
)

# Create Oracle directory
if (!(Test-Path $OracleClientPath)) {
    New-Item -ItemType Directory -Path $OracleClientPath -Force
}

# Download and extract Oracle Instant Client
Write-Host "Downloading Oracle Instant Client..."
$zipPath = "$env:TEMP\oracle_client.zip"
# Download logic here

# Extract to installation path
Expand-Archive -Path $zipPath -DestinationPath $OracleClientPath -Force

# Set environment variables
[Environment]::SetEnvironmentVariable("ORACLE_HOME", $OracleClientPath, "Machine")
[Environment]::SetEnvironmentVariable("TNS_ADMIN", "$OracleClientPath\network\admin", "Machine")

# Update PATH
$currentPath = [Environment]::GetEnvironmentVariable("PATH", "Machine")
if ($currentPath -notlike "*$OracleClientPath*") {
    [Environment]::SetEnvironmentVariable("PATH", "$currentPath;$OracleClientPath", "Machine")
}

Write-Host "Oracle Instant Client installed successfully"
```

### Linked Service Configuration

```json
{
  "name": "OnPremisesOracle",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "Oracle",
    "connectVia": {
      "referenceName": "OnPremisesSHIR",
      "type": "IntegrationRuntimeReference"
    },
    "typeProperties": {
      "server": "ORACLEDB01.contoso.local",
      "serviceName": "ORCL",
      "port": 1521,
      "authenticationType": "Basic",
      "username": "oracle_service_account",
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVault1",
          "type": "LinkedServiceReference"
        },
        "secretName": "oracle-password"
      },
      "connectionString": "Data Source=ORACLEDB01.contoso.local:1521/ORCL;User Id=oracle_service_account;Password={password};"
    }
  }
}
```

### Multiple Oracle Database Configuration

```json
{
  "oracle_databases": [
    {
      "name": "OracleHR_LinkedService",
      "server": "ORACLEDB01.contoso.local",
      "serviceName": "HRDB",
      "port": 1521,
      "description": "Oracle HR Database"
    },
    {
      "name": "OracleFinance_LinkedService",
      "server": "ORACLEDB02.contoso.local", 
      "serviceName": "FINDB",
      "port": 1521,
      "description": "Oracle Finance Database"
    },
    {
      "name": "OracleInventory_LinkedService",
      "server": "ORACLEDB01.contoso.local",
      "serviceName": "INVDB",
      "port": 1521,
      "description": "Oracle Inventory Management Database"
    }
  ]
}
```

### TNS Names Configuration

```ini
# tnsnames.ora configuration file
# Location: %ORACLE_HOME%\network\admin\tnsnames.ora

HRDB =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = ORACLEDB01.contoso.local)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = HRDB)
    )
  )

FINDB =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = ORACLEDB02.contoso.local)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = FINDB)
    )
  )

INVDB =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = ORACLEDB01.contoso.local)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = INVDB)
    )
  )
```

### Oracle Dataset Configuration

```json
{
  "name": "OracleTable_HR",
  "properties": {
    "type": "OracleTable",
    "linkedServiceName": {
      "referenceName": "OnPremisesOracle",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "schema": "HR",
      "table": "EMPLOYEES"
    },
    "parameters": {
      "schemaName": {
        "type": "String",
        "defaultValue": "HR"
      },
      "tableName": {
        "type": "String", 
        "defaultValue": "EMPLOYEES"
      }
    }
  }
}
```

---

## MySQL Database Connectivity

### MySQL ODBC Driver Installation

```powershell
# MySQL ODBC Driver installation script
param(
    [string]$DriverVersion = "8.0.33",
    [string]$DownloadUrl = "https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.33-win32.msi"
)

Write-Host "Installing MySQL ODBC Driver..."

# Download MySQL ODBC Driver
$installerPath = "$env:TEMP\mysql-odbc-driver.msi"
# Download logic here

# Install silently
Start-Process msiexec.exe -Wait -ArgumentList "/i $installerPath /quiet /norestart"

# Verify installation
$odbcDrivers = Get-OdbcDriver | Where-Object {$_.Name -like "*MySQL*"}
if ($odbcDrivers) {
    Write-Host "MySQL ODBC Driver installed successfully"
    $odbcDrivers | ForEach-Object { Write-Host "  - $($_.Name)" }
} else {
    Write-Error "MySQL ODBC Driver installation failed"
}
```

### Linked Service Configuration

```json
{
  "name": "OnPremisesMySQL",
  "type": "Microsoft.DataFactory/factories/linkedservices", 
  "properties": {
    "type": "MySql",
    "connectVia": {
      "referenceName": "OnPremisesSHIR",
      "type": "IntegrationRuntimeReference"
    },
    "typeProperties": {
      "server": "MYSQLDB01.contoso.local",
      "port": 3306,
      "database": "ecommerce",
      "username": "mysql_service_account",
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVault1", 
          "type": "LinkedServiceReference"
        },
        "secretName": "mysql-password"
      },
      "sslMode": "Required",
      "useSystemTrustStore": false
    }
  }
}
```

### Multiple MySQL Database Configuration

```json
{
  "mysql_databases": [
    {
      "name": "MySQLEcommerce_LinkedService",
      "server": "MYSQLDB01.contoso.local",
      "database": "ecommerce",
      "port": 3306,
      "description": "E-commerce application database"
    },
    {
      "name": "MySQLAnalytics_LinkedService",
      "server": "MYSQLDB02.contoso.local",
      "database": "analytics",
      "port": 3306,
      "description": "Analytics and reporting database"
    },
    {
      "name": "MySQLUserData_LinkedService",
      "server": "MYSQLDB01.contoso.local",
      "database": "userdata",
      "port": 3306,
      "description": "User profiles and preferences database"
    }
  ]
}
```

### MySQL Dataset Configuration

```json
{
  "name": "MySqlTable_Products",
  "properties": {
    "type": "MySqlTable",
    "linkedServiceName": {
      "referenceName": "OnPremisesMySQL",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "tableName": "products"
    },
    "parameters": {
      "tableName": {
        "type": "String",
        "defaultValue": "products"
      },
      "databaseName": {
        "type": "String",
        "defaultValue": "ecommerce"
      }
    }
  }
}
```

---

## Azure Databricks Integration

### Databricks Cluster Configuration

```python
# Databricks cluster configuration for on-premises connectivity
cluster_config = {
    "cluster_name": "OnPremisesDataCluster",
    "spark_version": "11.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 2,
    "autoscale": {
        "min_workers": 1,
        "max_workers": 8
    },
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    "custom_tags": {
        "Environment": "Production",
        "Project": "OnPremisesIntegration"
    }
}
```

### JDBC Connection Examples

```python
# Databricks notebook for connecting to on-premises databases
from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# SQL Server connection
def connect_to_sql_server():
    """Connect to on-premises SQL Server"""
    
    # Connection properties
    sql_server_config = {
        "url": "jdbc:sqlserver://SQLSERVER01.contoso.local:1433;databaseName=AdventureWorks2019",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": dbutils.secrets.get(scope="onpremises-db", key="sql-server-username"),
        "password": dbutils.secrets.get(scope="onpremises-db", key="sql-server-password")
    }
    
    # Read data from SQL Server
    df_sql_server = spark.read \
        .format("jdbc") \
        .option("url", sql_server_config["url"]) \
        .option("driver", sql_server_config["driver"]) \
        .option("user", sql_server_config["user"]) \
        .option("password", sql_server_config["password"]) \
        .option("dbtable", "Sales.SalesOrderHeader") \
        .load()
    
    print(f"SQL Server data loaded: {df_sql_server.count()} rows")
    return df_sql_server

# Oracle connection
def connect_to_oracle():
    """Connect to on-premises Oracle database"""
    
    oracle_config = {
        "url": "jdbc:oracle:thin:@ORACLEDB01.contoso.local:1521:ORCL",
        "driver": "oracle.jdbc.driver.OracleDriver",
        "user": dbutils.secrets.get(scope="onpremises-db", key="oracle-username"),
        "password": dbutils.secrets.get(scope="onpremises-db", key="oracle-password")
    }
    
    # Read data from Oracle
    df_oracle = spark.read \
        .format("jdbc") \
        .option("url", oracle_config["url"]) \
        .option("driver", oracle_config["driver"]) \
        .option("user", oracle_config["user"]) \
        .option("password", oracle_config["password"]) \
        .option("dbtable", "HR.EMPLOYEES") \
        .load()
    
    print(f"Oracle data loaded: {df_oracle.count()} rows")
    return df_oracle

# MySQL connection
def connect_to_mysql():
    """Connect to on-premises MySQL database"""
    
    mysql_config = {
        "url": "jdbc:mysql://MYSQLDB01.contoso.local:3306/ecommerce",
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": dbutils.secrets.get(scope="onpremises-db", key="mysql-username"),
        "password": dbutils.secrets.get(scope="onpremises-db", key="mysql-password")
    }
    
    # Read data from MySQL
    df_mysql = spark.read \
        .format("jdbc") \
        .option("url", mysql_config["url"]) \
        .option("driver", mysql_config["driver"]) \
        .option("user", mysql_config["user"]) \
        .option("password", mysql_config["password"]) \
        .option("dbtable", "products") \
        .load()
    
    print(f"MySQL data loaded: {df_mysql.count()} rows")
    return df_mysql

# Execute connections
sql_server_df = connect_to_sql_server()
oracle_df = connect_to_oracle()
mysql_df = connect_to_mysql()

# Display sample data
print("=== SQL Server Sample Data ===")
sql_server_df.show(5)

print("=== Oracle Sample Data ===")
oracle_df.show(5)

print("=== MySQL Sample Data ===")
mysql_df.show(5)
```

### Advanced Databricks Integration

```python
# Advanced integration patterns for multiple databases
class OnPremisesDatabaseManager:
    """Manager class for on-premises database connections"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.connections = {}
    
    def register_sql_server_connection(self, name, server, database, schema=None):
        """Register SQL Server connection"""
        config = {
            "type": "sqlserver",
            "url": f"jdbc:sqlserver://{server}:1433;databaseName={database}",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user": dbutils.secrets.get(scope="onpremises-db", key=f"{name}-username"),
            "password": dbutils.secrets.get(scope="onpremises-db", key=f"{name}-password"),
            "schema": schema
        }
        self.connections[name] = config
        print(f"Registered SQL Server connection: {name}")
    
    def register_oracle_connection(self, name, server, service_name, port=1521):
        """Register Oracle connection"""
        config = {
            "type": "oracle",
            "url": f"jdbc:oracle:thin:@{server}:{port}:{service_name}",
            "driver": "oracle.jdbc.driver.OracleDriver",
            "user": dbutils.secrets.get(scope="onpremises-db", key=f"{name}-username"),
            "password": dbutils.secrets.get(scope="onpremises-db", key=f"{name}-password")
        }
        self.connections[name] = config
        print(f"Registered Oracle connection: {name}")
    
    def register_mysql_connection(self, name, server, database, port=3306):
        """Register MySQL connection"""
        config = {
            "type": "mysql",
            "url": f"jdbc:mysql://{server}:{port}/{database}",
            "driver": "com.mysql.cj.jdbc.Driver",
            "user": dbutils.secrets.get(scope="onpremises-db", key=f"{name}-username"),
            "password": dbutils.secrets.get(scope="onpremises-db", key=f"{name}-password")
        }
        self.connections[name] = config
        print(f"Registered MySQL connection: {name}")
    
    def read_table(self, connection_name, table_name, query=None):
        """Read table from registered connection"""
        if connection_name not in self.connections:
            raise ValueError(f"Connection '{connection_name}' not found")
        
        config = self.connections[connection_name]
        
        reader = self.spark.read.format("jdbc") \
            .option("url", config["url"]) \
            .option("driver", config["driver"]) \
            .option("user", config["user"]) \
            .option("password", config["password"])
        
        if query:
            reader = reader.option("query", query)
        else:
            # Handle schema prefix for SQL Server
            if config["type"] == "sqlserver" and config.get("schema"):
                table_name = f"{config['schema']}.{table_name}"
            reader = reader.option("dbtable", table_name)
        
        return reader.load()
    
    def write_table(self, df, connection_name, table_name, mode="overwrite"):
        """Write DataFrame to registered connection"""
        if connection_name not in self.connections:
            raise ValueError(f"Connection '{connection_name}' not found")
        
        config = self.connections[connection_name]
        
        df.write.format("jdbc") \
            .option("url", config["url"]) \
            .option("driver", config["driver"]) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("dbtable", table_name) \
            .mode(mode) \
            .save()
        
        print(f"Data written to {table_name} in {connection_name}")

# Usage example
db_manager = OnPremisesDatabaseManager(spark)

# Register multiple database connections
db_manager.register_sql_server_connection("adventureworks", "SQLSERVER01.contoso.local", "AdventureWorks2019", "Sales")
db_manager.register_sql_server_connection("hr", "SQLSERVER01.contoso.local", "HumanResources", "dbo")
db_manager.register_oracle_connection("oracle-hr", "ORACLEDB01.contoso.local", "HRDB")
db_manager.register_mysql_connection("ecommerce", "MYSQLDB01.contoso.local", "ecommerce")

# Read data from multiple sources
sales_df = db_manager.read_table("adventureworks", "SalesOrderHeader")
employees_df = db_manager.read_table("oracle-hr", "EMPLOYEES")
products_df = db_manager.read_table("ecommerce", "products")

# Perform cross-database analytics
print("=== Cross-Database Analytics ===")
print(f"Total sales orders: {sales_df.count()}")
print(f"Total employees: {employees_df.count()}")
print(f"Total products: {products_df.count()}")
```

---

## Security and Authentication

### Azure Key Vault Integration

```json
{
  "key_vault_secrets": {
    "vault_name": "onpremises-secrets-kv",
    "secrets": [
      {
        "name": "sql-server-username",
        "value": "adf_service_account",
        "description": "SQL Server service account username"
      },
      {
        "name": "sql-server-password", 
        "value": "SecurePassword123!",
        "description": "SQL Server service account password"
      },
      {
        "name": "oracle-username",
        "value": "oracle_service_account",
        "description": "Oracle database service account username"
      },
      {
        "name": "oracle-password",
        "value": "OracleSecure456!",
        "description": "Oracle database service account password"
      },
      {
        "name": "mysql-username",
        "value": "mysql_service_account", 
        "description": "MySQL service account username"
      },
      {
        "name": "mysql-password",
        "value": "MySQLSecure789!",
        "description": "MySQL service account password"
      }
    ]
  }
}
```

### PowerShell Script for Key Vault Management

```powershell
# Azure Key Vault management script
param(
    [Parameter(Mandatory=$true)]
    [string]$KeyVaultName,
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory=$true)]
    [string]$SubscriptionId
)

# Connect to Azure
Connect-AzAccount
Set-AzContext -SubscriptionId $SubscriptionId

# Create Key Vault if it doesn't exist
$keyVault = Get-AzKeyVault -VaultName $KeyVaultName -ResourceGroupName $ResourceGroupName -ErrorAction SilentlyContinue

if (-not $keyVault) {
    Write-Host "Creating Key Vault: $KeyVaultName"
    $keyVault = New-AzKeyVault -VaultName $KeyVaultName -ResourceGroupName $ResourceGroupName -Location "East US"
}

# Function to set secrets
function Set-DatabaseSecrets {
    param([hashtable]$Secrets)
    
    foreach ($secret in $Secrets.GetEnumerator()) {
        $secretValue = ConvertTo-SecureString $secret.Value -AsPlainText -Force
        Set-AzKeyVaultSecret -VaultName $KeyVaultName -Name $secret.Key -SecretValue $secretValue
        Write-Host "Set secret: $($secret.Key)"
    }
}

# Database credentials (replace with actual values)
$databaseSecrets = @{
    "sql-server-username" = "adf_service_account"
    "sql-server-password" = "SecurePassword123!"
    "oracle-username" = "oracle_service_account"
    "oracle-password" = "OracleSecure456!"
    "mysql-username" = "mysql_service_account"
    "mysql-password" = "MySQLSecure789!"
}

# Set all secrets
Set-DatabaseSecrets -Secrets $databaseSecrets

# Grant access to Azure Data Factory
$dataFactoryName = "your-data-factory-name"
$dataFactory = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $dataFactoryName

if ($dataFactory) {
    Set-AzKeyVaultAccessPolicy -VaultName $KeyVaultName -ObjectId $dataFactory.Identity.PrincipalId -PermissionsToSecrets get,list
    Write-Host "Granted Key Vault access to Data Factory: $dataFactoryName"
}

Write-Host "Key Vault configuration completed successfully"
```

### Network Security Configuration

```json
{
  "network_security": {
    "firewall_rules": [
      {
        "name": "AllowAzureDataFactory",
        "source": "Azure Data Factory IP ranges",
        "destination": "On-premises database servers",
        "ports": ["1433", "1521", "3306"],
        "protocol": "TCP"
      },
      {
        "name": "AllowSelfHostedIR",
        "source": "Self-Hosted IR server IP",
        "destination": "Database servers",
        "ports": ["1433", "1521", "3306"],
        "protocol": "TCP"
      }
    ],
    "vpn_configuration": {
      "type": "Site-to-Site VPN",
      "gateway_sku": "VpnGw1",
      "encryption": "AES256",
      "authentication": "PSK"
    },
    "expressroute": {
      "circuit_bandwidth": "100 Mbps",
      "peering_type": "Private",
      "vlan_id": "100"
    }
  }
}
```

---

## Best Practices

### Performance Optimization

```json
{
  "performance_best_practices": {
    "self_hosted_ir": [
      "Install on dedicated server with sufficient resources",
      "Use SSD storage for better I/O performance",
      "Configure multiple nodes for high availability",
      "Monitor memory and CPU usage regularly"
    ],
    "database_connections": [
      "Use connection pooling",
      "Optimize query performance with indexes",
      "Implement incremental data loading",
      "Use parallel copy for large datasets"
    ],
    "network_optimization": [
      "Use ExpressRoute for consistent performance",
      "Configure appropriate bandwidth",
      "Implement data compression",
      "Schedule large transfers during off-peak hours"
    ]
  }
}
```

### Monitoring and Alerting

```powershell
# Monitoring script for on-premises connectivity
function Test-OnPremisesConnectivity {
    $results = @()
    
    # Test SQL Server connectivity
    $sqlTest = Test-NetConnection -ComputerName "SQLSERVER01.contoso.local" -Port 1433
    $results += [PSCustomObject]@{
        Service = "SQL Server"
        Server = "SQLSERVER01.contoso.local"
        Port = 1433
        Status = if($sqlTest.TcpTestSucceeded) {"Connected"} else {"Failed"}
        ResponseTime = $sqlTest.PingReplyDetails.RoundtripTime
    }
    
    # Test Oracle connectivity
    $oracleTest = Test-NetConnection -ComputerName "ORACLEDB01.contoso.local" -Port 1521
    $results += [PSCustomObject]@{
        Service = "Oracle"
        Server = "ORACLEDB01.contoso.local"
        Port = 1521
        Status = if($oracleTest.TcpTestSucceeded) {"Connected"} else {"Failed"}
        ResponseTime = $oracleTest.PingReplyDetails.RoundtripTime
    }
    
    # Test MySQL connectivity
    $mysqlTest = Test-NetConnection -ComputerName "MYSQLDB01.contoso.local" -Port 3306
    $results += [PSCustomObject]@{
        Service = "MySQL"
        Server = "MYSQLDB01.contoso.local"
        Port = 3306
        Status = if($mysqlTest.TcpTestSucceeded) {"Connected"} else {"Failed"}
        ResponseTime = $mysqlTest.PingReplyDetails.RoundtripTime
    }
    
    return $results
}

# Run connectivity tests
$connectivityResults = Test-OnPremisesConnectivity
$connectivityResults | Format-Table -AutoSize

# Send alerts for failed connections
$failedConnections = $connectivityResults | Where-Object {$_.Status -eq "Failed"}
if ($failedConnections) {
    Write-Warning "Failed connections detected:"
    $failedConnections | Format-Table -AutoSize
    
    # Send email alert (implement email logic here)
    # Send-MailMessage -To "admin@contoso.com" -Subject "Database Connectivity Alert" -Body "Connection failures detected"
}
```

This comprehensive guide provides everything needed to successfully connect Azure Data Factory and Azure Databricks to on-premises SQL Server, Oracle, and MySQL databases with multiple database support, security best practices, and monitoring capabilities.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*