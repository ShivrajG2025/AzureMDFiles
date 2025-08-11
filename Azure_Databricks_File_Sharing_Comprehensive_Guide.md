# Azure Databricks File Sharing Comprehensive Guide
## Complete Guide to File Sharing in Azure Databricks (ADB)

---

### Table of Contents

1. [Overview](#overview)
2. [Databricks File System (DBFS)](#databricks-file-system-dbfs)
3. [Azure Storage Integration](#azure-storage-integration)
4. [Unity Catalog File Sharing](#unity-catalog-file-sharing)
5. [Workspace-Level File Sharing](#workspace-level-file-sharing)
6. [External Storage Mounting](#external-storage-mounting)
7. [File Sharing Security](#file-sharing-security)
8. [Collaborative Workflows](#collaborative-workflows)
9. [Data Lake Integration](#data-lake-integration)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

---

## Overview

Azure Databricks provides multiple mechanisms for file sharing and data access, enabling teams to collaborate effectively while maintaining security and governance. This guide covers all aspects of file sharing in Azure Databricks environments.

### File Sharing Architecture

```json
{
  "databricks_file_sharing_architecture": {
    "dbfs_layer": {
      "description": "Databricks File System - cluster-local storage",
      "components": ["Local SSD storage", "Distributed file system", "Temporary storage"],
      "use_cases": ["Intermediate data", "Temporary files", "Cluster-specific data"],
      "characteristics": ["High performance", "Ephemeral", "Cluster-scoped"]
    },
    "external_storage": {
      "description": "Integration with Azure storage services",
      "components": ["ADLS Gen2", "Azure Blob Storage", "Azure Files", "SQL databases"],
      "use_cases": ["Persistent data", "Data lakes", "Shared datasets", "Archive storage"],
      "characteristics": ["Persistent", "Scalable", "Multi-workspace access"]
    },
    "unity_catalog": {
      "description": "Unified governance and sharing platform",
      "components": ["Catalogs", "Schemas", "Tables", "Volumes", "External locations"],
      "use_cases": ["Data governance", "Cross-workspace sharing", "Access control", "Lineage tracking"],
      "characteristics": ["Centralized", "Governed", "Auditable"]
    },
    "workspace_sharing": {
      "description": "Workspace-level file and notebook sharing",
      "components": ["Shared folders", "Notebooks", "Libraries", "Repos"],
      "use_cases": ["Code collaboration", "Notebook sharing", "Library management", "Version control"],
      "characteristics": ["Collaborative", "Version-controlled", "Access-controlled"]
    }
  }
}
```

### File Sharing Patterns

```json
{
  "file_sharing_patterns": {
    "direct_dbfs_access": {
      "description": "Direct access to DBFS for temporary and intermediate data",
      "pattern": "dbfs:/path/to/file",
      "benefits": ["High performance", "Simple access", "No external dependencies"],
      "limitations": ["Ephemeral", "Cluster-scoped", "Limited persistence"],
      "use_cases": ["ETL intermediates", "Model artifacts", "Temporary datasets"]
    },
    "mounted_storage": {
      "description": "Mount external storage as DBFS paths",
      "pattern": "/mnt/storage-name/path/to/file",
      "benefits": ["Persistent storage", "Cross-cluster access", "Azure integration"],
      "limitations": ["Mount management", "Authentication complexity", "Performance overhead"],
      "use_cases": ["Data lakes", "Shared datasets", "Production data"]
    },
    "unity_catalog_volumes": {
      "description": "Governed file access through Unity Catalog",
      "pattern": "/Volumes/catalog/schema/volume/path/to/file",
      "benefits": ["Centralized governance", "Fine-grained access", "Audit trails"],
      "limitations": ["Unity Catalog requirement", "Setup complexity", "Learning curve"],
      "use_cases": ["Enterprise data", "Regulated environments", "Multi-workspace scenarios"]
    },
    "direct_azure_access": {
      "description": "Direct access to Azure storage using credentials",
      "pattern": "abfss://container@account.dfs.core.windows.net/path",
      "benefits": ["No mount required", "Flexible authentication", "Native Azure integration"],
      "limitations": ["Credential management", "URL complexity", "Security considerations"],
      "use_cases": ["Ad-hoc analysis", "External data sources", "Integration scenarios"]
    }
  }
}
```

---

## Databricks File System (DBFS)

### DBFS Overview and Structure

DBFS is a distributed file system installed on Databricks clusters that provides a unified interface for file operations.

```python
# DBFS File Operations in Azure Databricks
import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import shutil

class DBFSFileManager:
    """Comprehensive DBFS file management for Azure Databricks"""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session or SparkSession.builder.getOrCreate()
        self.dbutils = None
        try:
            # Initialize dbutils (available in Databricks environment)
            from pyspark.dbutils import DBUtils
            self.dbutils = DBUtils(self.spark)
        except ImportError:
            print("Warning: dbutils not available (not in Databricks environment)")
    
    def explore_dbfs_structure(self) -> dict:
        """Explore the DBFS file system structure"""
        
        print("=== DBFS File System Exploration ===")
        
        structure = {
            "root_directories": [],
            "system_directories": [],
            "user_directories": [],
            "mounted_storage": []
        }
        
        if not self.dbutils:
            print("dbutils not available - cannot explore DBFS")
            return structure
        
        try:
            # List root directories
            root_dirs = self.dbutils.fs.ls("/")
            for dir_info in root_dirs:
                dir_entry = {
                    "path": dir_info.path,
                    "name": dir_info.name,
                    "size": dir_info.size,
                    "is_directory": dir_info.isDir()
                }
                structure["root_directories"].append(dir_entry)
                
                print(f"📁 {dir_info.name} - {dir_info.path}")
            
            # Explore specific important directories
            important_dirs = [
                "/databricks-datasets/",
                "/FileStore/",
                "/tmp/",
                "/mnt/",
                "/databricks/",
                "/user/"
            ]
            
            for dir_path in important_dirs:
                try:
                    if self._directory_exists(dir_path):
                        contents = self.dbutils.fs.ls(dir_path)
                        print(f"\n📂 Contents of {dir_path}:")
                        for item in contents[:5]:  # Show first 5 items
                            print(f"  {'📁' if item.isDir() else '📄'} {item.name}")
                        if len(contents) > 5:
                            print(f"  ... and {len(contents) - 5} more items")
                except Exception as e:
                    print(f"  ❌ Cannot access {dir_path}: {str(e)}")
            
            # Check for mounted storage
            try:
                mounts = self.dbutils.fs.mounts()
                for mount in mounts:
                    mount_info = {
                        "mount_point": mount.mountPoint,
                        "source": mount.source,
                        "extra_configs": mount.extraConfigs
                    }
                    structure["mounted_storage"].append(mount_info)
                    print(f"🔗 Mount: {mount.mountPoint} -> {mount.source}")
            except Exception as e:
                print(f"Error listing mounts: {str(e)}")
            
            return structure
            
        except Exception as e:
            print(f"Error exploring DBFS: {str(e)}")
            return structure
    
    def _directory_exists(self, path: str) -> bool:
        """Check if a directory exists in DBFS"""
        try:
            self.dbutils.fs.ls(path)
            return True
        except:
            return False
    
    def create_file_sharing_workspace(self, workspace_path: str = "/FileStore/shared_workspace") -> dict:
        """Create a structured workspace for file sharing"""
        
        print(f"=== Creating File Sharing Workspace: {workspace_path} ===")
        
        workspace_structure = {
            "base_path": workspace_path,
            "directories": {
                "data": f"{workspace_path}/data",
                "notebooks": f"{workspace_path}/notebooks", 
                "models": f"{workspace_path}/models",
                "reports": f"{workspace_path}/reports",
                "temp": f"{workspace_path}/temp",
                "shared": f"{workspace_path}/shared",
                "user_folders": f"{workspace_path}/users"
            },
            "subdirectories": {
                "raw_data": f"{workspace_path}/data/raw",
                "processed_data": f"{workspace_path}/data/processed",
                "archived_data": f"{workspace_path}/data/archived",
                "model_artifacts": f"{workspace_path}/models/artifacts",
                "model_experiments": f"{workspace_path}/models/experiments",
                "daily_reports": f"{workspace_path}/reports/daily",
                "monthly_reports": f"{workspace_path}/reports/monthly"
            }
        }
        
        if not self.dbutils:
            print("dbutils not available - cannot create directories")
            return workspace_structure
        
        try:
            # Create main directories
            for dir_name, dir_path in workspace_structure["directories"].items():
                self.dbutils.fs.mkdirs(dir_path)
                print(f"✓ Created directory: {dir_name} -> {dir_path}")
            
            # Create subdirectories
            for subdir_name, subdir_path in workspace_structure["subdirectories"].items():
                self.dbutils.fs.mkdirs(subdir_path)
                print(f"✓ Created subdirectory: {subdir_name} -> {subdir_path}")
            
            # Create README files for each main directory
            readme_contents = {
                "data": "# Data Directory\\n\\nThis directory contains all data files:\\n- raw/: Raw, unprocessed data\\n- processed/: Cleaned and transformed data\\n- archived/: Historical data archives",
                "notebooks": "# Notebooks Directory\\n\\nShared notebooks and analysis scripts:\\n- Store collaborative notebooks here\\n- Use descriptive naming conventions",
                "models": "# Models Directory\\n\\nMachine learning models and artifacts:\\n- artifacts/: Trained model files\\n- experiments/: Experiment tracking data",
                "reports": "# Reports Directory\\n\\nGenerated reports and visualizations:\\n- daily/: Daily automated reports\\n- monthly/: Monthly summary reports",
                "temp": "# Temporary Directory\\n\\nTemporary files and intermediate results:\\n- Files here may be cleaned up periodically",
                "shared": "# Shared Directory\\n\\nFiles shared across teams:\\n- Common datasets and utilities"
            }
            
            for dir_name, content in readme_contents.items():
                readme_path = f"{workspace_structure['directories'][dir_name]}/README.md"
                self._write_text_file(readme_path, content)
                print(f"✓ Created README: {readme_path}")
            
            # Create a workspace manifest
            manifest = {
                "workspace_name": "Databricks File Sharing Workspace",
                "created_date": datetime.now().isoformat(),
                "structure": workspace_structure,
                "usage_guidelines": [
                    "Use descriptive file names with timestamps",
                    "Store raw data in data/raw directory",
                    "Keep temporary files in temp directory",
                    "Use shared directory for cross-team collaboration",
                    "Document data sources and transformations"
                ],
                "access_patterns": [
                    "Read data: spark.read.format('...').load('dbfs:/FileStore/...')",
                    "Write data: df.write.format('...').save('dbfs:/FileStore/...')",
                    "List files: dbutils.fs.ls('/FileStore/...')",
                    "Copy files: dbutils.fs.cp('source', 'destination')"
                ]
            }
            
            manifest_path = f"{workspace_path}/workspace_manifest.json"
            self._write_json_file(manifest_path, manifest)
            print(f"✓ Created workspace manifest: {manifest_path}")
            
            return workspace_structure
            
        except Exception as e:
            print(f"Error creating workspace: {str(e)}")
            return workspace_structure
    
    def _write_text_file(self, path: str, content: str) -> None:
        """Write text content to DBFS file"""
        if self.dbutils:
            # Convert content to bytes and write
            content_bytes = content.encode('utf-8')
            self.dbutils.fs.put(path, content, overwrite=True)
    
    def _write_json_file(self, path: str, data: dict) -> None:
        """Write JSON data to DBFS file"""
        if self.dbutils:
            json_content = json.dumps(data, indent=2)
            self.dbutils.fs.put(path, json_content, overwrite=True)
    
    def demonstrate_file_operations(self, base_path: str = "/FileStore/demo") -> dict:
        """Demonstrate various DBFS file operations"""
        
        print(f"=== DBFS File Operations Demo ===")
        
        operations_log = {
            "base_path": base_path,
            "operations": [],
            "files_created": [],
            "errors": []
        }
        
        if not self.dbutils:
            print("dbutils not available - cannot demonstrate file operations")
            return operations_log
        
        try:
            # Create demo directory
            self.dbutils.fs.mkdirs(base_path)
            operations_log["operations"].append(f"Created directory: {base_path}")
            print(f"✓ Created demo directory: {base_path}")
            
            # 1. Create and write different file types
            
            # Text file
            text_file_path = f"{base_path}/sample_text.txt"
            text_content = """This is a sample text file in DBFS.
It can contain multiple lines of text.
Created for demonstration purposes.
Timestamp: """ + datetime.now().isoformat()
            
            self.dbutils.fs.put(text_file_path, text_content, overwrite=True)
            operations_log["files_created"].append(text_file_path)
            operations_log["operations"].append(f"Created text file: {text_file_path}")
            print(f"✓ Created text file: {text_file_path}")
            
            # JSON file
            json_file_path = f"{base_path}/sample_data.json"
            sample_data = {
                "users": [
                    {"id": 1, "name": "Alice", "department": "Engineering"},
                    {"id": 2, "name": "Bob", "department": "Data Science"},
                    {"id": 3, "name": "Carol", "department": "Analytics"}
                ],
                "metadata": {
                    "created_date": datetime.now().isoformat(),
                    "source": "DBFS Demo",
                    "record_count": 3
                }
            }
            
            json_content = json.dumps(sample_data, indent=2)
            self.dbutils.fs.put(json_file_path, json_content, overwrite=True)
            operations_log["files_created"].append(json_file_path)
            operations_log["operations"].append(f"Created JSON file: {json_file_path}")
            print(f"✓ Created JSON file: {json_file_path}")
            
            # CSV data using Spark DataFrame
            csv_file_path = f"{base_path}/sample_data.csv"
            sample_df = self.spark.createDataFrame([
                (1, "Product A", 100.0, "Electronics"),
                (2, "Product B", 250.0, "Books"),
                (3, "Product C", 75.0, "Clothing"),
                (4, "Product D", 500.0, "Electronics"),
                (5, "Product E", 25.0, "Books")
            ], ["id", "name", "price", "category"])
            
            # Write as CSV
            sample_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"dbfs:{csv_file_path}")
            operations_log["files_created"].append(csv_file_path)
            operations_log["operations"].append(f"Created CSV file: {csv_file_path}")
            print(f"✓ Created CSV file: {csv_file_path}")
            
            # Parquet file
            parquet_file_path = f"{base_path}/sample_data.parquet"
            sample_df.write.mode("overwrite").parquet(f"dbfs:{parquet_file_path}")
            operations_log["files_created"].append(parquet_file_path)
            operations_log["operations"].append(f"Created Parquet file: {parquet_file_path}")
            print(f"✓ Created Parquet file: {parquet_file_path}")
            
            # 2. File listing and inspection
            print(f"\n📂 Contents of {base_path}:")
            files = self.dbutils.fs.ls(base_path)
            for file_info in files:
                file_type = "📁 Directory" if file_info.isDir() else "📄 File"
                size_mb = file_info.size / (1024 * 1024) if file_info.size > 0 else 0
                print(f"  {file_type}: {file_info.name} ({size_mb:.2f} MB)")
                
                operations_log["operations"].append(f"Listed file: {file_info.path}")
            
            # 3. File reading operations
            print(f"\n📖 Reading files:")
            
            # Read text file
            text_content_read = self.dbutils.fs.head(text_file_path, max_bytes=500)
            print(f"Text file content (first 500 bytes):")
            print(text_content_read[:200] + "..." if len(text_content_read) > 200 else text_content_read)
            operations_log["operations"].append(f"Read text file: {text_file_path}")
            
            # Read JSON file
            json_content_read = self.dbutils.fs.head(json_file_path)
            json_data_read = json.loads(json_content_read)
            print(f"JSON file data - Users count: {len(json_data_read['users'])}")
            operations_log["operations"].append(f"Read JSON file: {json_file_path}")
            
            # Read CSV with Spark
            csv_df = self.spark.read.option("header", "true").csv(f"dbfs:{csv_file_path}")
            print(f"CSV file - Rows: {csv_df.count()}, Columns: {len(csv_df.columns)}")
            operations_log["operations"].append(f"Read CSV file: {csv_file_path}")
            
            # Read Parquet with Spark
            parquet_df = self.spark.read.parquet(f"dbfs:{parquet_file_path}")
            print(f"Parquet file - Rows: {parquet_df.count()}, Columns: {len(parquet_df.columns)}")
            operations_log["operations"].append(f"Read Parquet file: {parquet_file_path}")
            
            # 4. File operations (copy, move, delete)
            
            # Copy file
            backup_path = f"{base_path}/backup"
            self.dbutils.fs.mkdirs(backup_path)
            
            backup_text_path = f"{backup_path}/sample_text_backup.txt"
            self.dbutils.fs.cp(text_file_path, backup_text_path)
            operations_log["operations"].append(f"Copied file: {text_file_path} -> {backup_text_path}")
            print(f"✓ Copied file to backup: {backup_text_path}")
            
            # File information
            file_info = self.dbutils.fs.ls(text_file_path)[0]
            print(f"📊 File info - Size: {file_info.size} bytes, Modified: {file_info.modificationTime}")
            
            return operations_log
            
        except Exception as e:
            error_msg = f"Error in file operations: {str(e)}"
            operations_log["errors"].append(error_msg)
            print(f"❌ {error_msg}")
            return operations_log
    
    def create_shared_data_pipeline(self, pipeline_path: str = "/FileStore/shared_pipeline") -> dict:
        """Create a shared data pipeline structure for team collaboration"""
        
        print(f"=== Creating Shared Data Pipeline ===")
        
        pipeline_config = {
            "pipeline_path": pipeline_path,
            "stages": {
                "ingestion": f"{pipeline_path}/01_ingestion",
                "validation": f"{pipeline_path}/02_validation", 
                "transformation": f"{pipeline_path}/03_transformation",
                "enrichment": f"{pipeline_path}/04_enrichment",
                "output": f"{pipeline_path}/05_output"
            },
            "shared_resources": {
                "config": f"{pipeline_path}/config",
                "utilities": f"{pipeline_path}/utilities",
                "schemas": f"{pipeline_path}/schemas",
                "logs": f"{pipeline_path}/logs"
            }
        }
        
        if not self.dbutils:
            print("dbutils not available - cannot create pipeline structure")
            return pipeline_config
        
        try:
            # Create pipeline directories
            for stage_name, stage_path in pipeline_config["stages"].items():
                self.dbutils.fs.mkdirs(stage_path)
                print(f"✓ Created stage directory: {stage_name} -> {stage_path}")
            
            for resource_name, resource_path in pipeline_config["shared_resources"].items():
                self.dbutils.fs.mkdirs(resource_path)
                print(f"✓ Created resource directory: {resource_name} -> {resource_path}")
            
            # Create configuration files
            pipeline_manifest = {
                "pipeline_name": "Shared Data Pipeline",
                "version": "1.0",
                "created_date": datetime.now().isoformat(),
                "stages": [
                    {
                        "name": "ingestion",
                        "description": "Data ingestion from various sources",
                        "inputs": ["External APIs", "Files", "Databases"],
                        "outputs": ["Raw data in standardized format"]
                    },
                    {
                        "name": "validation",
                        "description": "Data quality validation and cleansing",
                        "inputs": ["Raw data"],
                        "outputs": ["Validated data", "Quality reports"]
                    },
                    {
                        "name": "transformation",
                        "description": "Core business logic transformations",
                        "inputs": ["Validated data"],
                        "outputs": ["Transformed data"]
                    },
                    {
                        "name": "enrichment",
                        "description": "Data enrichment with external sources",
                        "inputs": ["Transformed data", "Reference data"],
                        "outputs": ["Enriched data"]
                    },
                    {
                        "name": "output",
                        "description": "Final output generation and distribution",
                        "inputs": ["Enriched data"],
                        "outputs": ["Final datasets", "Reports", "APIs"]
                    }
                ],
                "shared_resources": {
                    "config": "Configuration files and parameters",
                    "utilities": "Shared utility functions and libraries",
                    "schemas": "Data schemas and validation rules",
                    "logs": "Pipeline execution logs and monitoring"
                },
                "collaboration_guidelines": [
                    "Each stage should have clear input/output contracts",
                    "Use consistent naming conventions for files",
                    "Document all transformations and business logic",
                    "Store reusable code in utilities directory",
                    "Log all operations for debugging and monitoring"
                ]
            }
            
            manifest_path = f"{pipeline_path}/pipeline_manifest.json"
            self._write_json_file(manifest_path, pipeline_manifest)
            print(f"✓ Created pipeline manifest: {manifest_path}")
            
            # Create sample configuration
            sample_config = {
                "data_sources": {
                    "primary_db": {
                        "type": "azure_sql",
                        "connection_string": "connection_string_from_key_vault",
                        "tables": ["customers", "orders", "products"]
                    },
                    "external_api": {
                        "type": "rest_api",
                        "base_url": "https://api.example.com",
                        "auth_type": "bearer_token"
                    }
                },
                "processing_config": {
                    "batch_size": 10000,
                    "max_retries": 3,
                    "timeout_seconds": 300,
                    "output_format": "parquet",
                    "compression": "snappy"
                },
                "quality_rules": {
                    "required_fields": ["id", "timestamp", "source"],
                    "data_types": {
                        "id": "string",
                        "timestamp": "timestamp",
                        "amount": "decimal"
                    },
                    "value_ranges": {
                        "amount": {"min": 0, "max": 1000000}
                    }
                }
            }
            
            config_path = f"{pipeline_config['shared_resources']['config']}/pipeline_config.json"
            self._write_json_file(config_path, sample_config)
            print(f"✓ Created sample configuration: {config_path}")
            
            # Create utility functions template
            utility_code = '''# Shared Utility Functions for Data Pipeline
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class DataPipelineUtils:
    """Shared utilities for the data pipeline"""
    
    @staticmethod
    def validate_required_columns(df: DataFrame, required_columns: list) -> bool:
        """Validate that DataFrame contains all required columns"""
        df_columns = set(df.columns)
        required_set = set(required_columns)
        missing_columns = required_set - df_columns
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        return True
    
    @staticmethod
    def add_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
        """Add audit columns to DataFrame"""
        return df.withColumn("audit_source", lit(source_name)) \\
                 .withColumn("audit_timestamp", current_timestamp()) \\
                 .withColumn("audit_date", current_date())
    
    @staticmethod
    def clean_column_names(df: DataFrame) -> DataFrame:
        """Clean column names by removing spaces and special characters"""
        new_columns = []
        for col_name in df.columns:
            clean_name = col_name.strip().lower().replace(" ", "_").replace("-", "_")
            new_columns.append(clean_name)
        
        return df.toDF(*new_columns)
    
    @staticmethod
    def log_dataframe_info(df: DataFrame, stage_name: str) -> dict:
        """Log DataFrame information for monitoring"""
        info = {
            "stage": stage_name,
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "timestamp": datetime.now().isoformat()
        }
        
        print(f"Stage {stage_name}: {info['row_count']} rows, {info['column_count']} columns")
        return info

# Example usage:
# from utilities.pipeline_utils import DataPipelineUtils
# utils = DataPipelineUtils()
# df_clean = utils.add_audit_columns(df, "source_system")
'''
            
            utility_path = f"{pipeline_config['shared_resources']['utilities']}/pipeline_utils.py"
            self._write_text_file(utility_path, utility_code)
            print(f"✓ Created utility functions: {utility_path}")
            
            return pipeline_config
            
        except Exception as e:
            print(f"Error creating pipeline: {str(e)}")
            return pipeline_config

# Example usage and demonstration
def demonstrate_dbfs_file_sharing():
    """Comprehensive demonstration of DBFS file sharing"""
    
    print("=== Azure Databricks DBFS File Sharing Demonstration ===")
    
    try:
        # Initialize DBFS manager
        dbfs_manager = DBFSFileManager()
        
        # Explore DBFS structure
        structure = dbfs_manager.explore_dbfs_structure()
        
        # Create shared workspace
        workspace = dbfs_manager.create_file_sharing_workspace()
        
        # Demonstrate file operations
        operations = dbfs_manager.demonstrate_file_operations()
        
        # Create shared pipeline
        pipeline = dbfs_manager.create_shared_data_pipeline()
        
        return {
            "status": "success",
            "dbfs_structure": structure,
            "shared_workspace": workspace,
            "file_operations": operations,
            "shared_pipeline": pipeline
        }
        
    except Exception as e:
        print(f"Demonstration failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

# Run demonstration
# demo_results = demonstrate_dbfs_file_sharing()
```

---

## Azure Storage Integration

### Mounting Azure Storage in Databricks

Azure Databricks can mount various Azure storage services to provide persistent, shared access to data.

```python
# Azure Storage Mounting and Integration
from azure.identity import DefaultAzureCredential
import json
from datetime import datetime

class AzureStorageMountManager:
    """Comprehensive Azure Storage mounting for Databricks"""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session or SparkSession.builder.getOrCreate()
        self.dbutils = None
        try:
            from pyspark.dbutils import DBUtils
            self.dbutils = DBUtils(self.spark)
        except ImportError:
            print("Warning: dbutils not available")
    
    def mount_adls_gen2(self, storage_account: str, container: str, mount_point: str,
                       auth_method: str = "service_principal", **auth_config) -> dict:
        """Mount Azure Data Lake Storage Gen2"""
        
        print(f"=== Mounting ADLS Gen2: {storage_account}/{container} ===")
        
        mount_config = {
            "storage_account": storage_account,
            "container": container,
            "mount_point": mount_point,
            "auth_method": auth_method,
            "status": "pending"
        }
        
        if not self.dbutils:
            print("dbutils not available - cannot mount storage")
            mount_config["status"] = "failed"
            mount_config["error"] = "dbutils not available"
            return mount_config
        
        try:
            # Check if already mounted
            existing_mounts = self.dbutils.fs.mounts()
            for mount in existing_mounts:
                if mount.mountPoint == mount_point:
                    print(f"⚠️ Mount point already exists: {mount_point}")
                    mount_config["status"] = "already_mounted"
                    mount_config["existing_source"] = mount.source
                    return mount_config
            
            # Prepare mount configuration based on auth method
            if auth_method == "service_principal":
                mount_config.update(self._mount_with_service_principal(
                    storage_account, container, mount_point, auth_config
                ))
            elif auth_method == "access_key":
                mount_config.update(self._mount_with_access_key(
                    storage_account, container, mount_point, auth_config
                ))
            elif auth_method == "sas_token":
                mount_config.update(self._mount_with_sas_token(
                    storage_account, container, mount_point, auth_config
                ))
            else:
                raise ValueError(f"Unsupported auth method: {auth_method}")
            
            print(f"✓ Successfully mounted: {mount_point}")
            mount_config["status"] = "success"
            
            # Test the mount
            test_result = self._test_mount(mount_point)
            mount_config["test_result"] = test_result
            
            return mount_config
            
        except Exception as e:
            error_msg = f"Failed to mount {storage_account}/{container}: {str(e)}"
            print(f"❌ {error_msg}")
            mount_config["status"] = "failed"
            mount_config["error"] = error_msg
            return mount_config
    
    def _mount_with_service_principal(self, storage_account: str, container: str, 
                                    mount_point: str, auth_config: dict) -> dict:
        """Mount using Service Principal authentication"""
        
        required_keys = ["client_id", "client_secret", "tenant_id"]
        for key in required_keys:
            if key not in auth_config:
                raise ValueError(f"Missing required auth config: {key}")
        
        # Configure OAuth with Service Principal
        configs = {
            f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net": "OAuth",
            f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net": 
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net": 
                auth_config["client_id"],
            f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net": 
                auth_config["client_secret"],
            f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net": 
                f"https://login.microsoftonline.com/{auth_config['tenant_id']}/oauth2/token"
        }
        
        source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        
        self.dbutils.fs.mount(
            source=source,
            mount_point=mount_point,
            extra_configs=configs
        )
        
        return {
            "source": source,
            "configs_used": "Service Principal OAuth",
            "client_id": auth_config["client_id"]
        }
    
    def _mount_with_access_key(self, storage_account: str, container: str,
                              mount_point: str, auth_config: dict) -> dict:
        """Mount using Storage Account Access Key"""
        
        if "access_key" not in auth_config:
            raise ValueError("Missing required auth config: access_key")
        
        configs = {
            f"fs.azure.account.key.{storage_account}.dfs.core.windows.net": auth_config["access_key"]
        }
        
        source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        
        self.dbutils.fs.mount(
            source=source,
            mount_point=mount_point,
            extra_configs=configs
        )
        
        return {
            "source": source,
            "configs_used": "Access Key",
            "security_note": "Access key provides full account access"
        }
    
    def _mount_with_sas_token(self, storage_account: str, container: str,
                             mount_point: str, auth_config: dict) -> dict:
        """Mount using SAS Token"""
        
        if "sas_token" not in auth_config:
            raise ValueError("Missing required auth config: sas_token")
        
        # Remove leading '?' if present
        sas_token = auth_config["sas_token"].lstrip("?")
        
        configs = {
            f"fs.azure.sas.{container}.{storage_account}.dfs.core.windows.net": sas_token
        }
        
        source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        
        self.dbutils.fs.mount(
            source=source,
            mount_point=mount_point,
            extra_configs=configs
        )
        
        return {
            "source": source,
            "configs_used": "SAS Token",
            "security_note": "SAS token provides limited, time-bound access"
        }
    
    def _test_mount(self, mount_point: str) -> dict:
        """Test the mounted storage"""
        
        test_result = {
            "mount_point": mount_point,
            "accessible": False,
            "file_count": 0,
            "test_timestamp": datetime.now().isoformat()
        }
        
        try:
            # List files in mount point
            files = self.dbutils.fs.ls(mount_point)
            test_result["accessible"] = True
            test_result["file_count"] = len(files)
            
            # Show first few files
            if files:
                test_result["sample_files"] = []
                for file_info in files[:5]:
                    test_result["sample_files"].append({
                        "name": file_info.name,
                        "path": file_info.path,
                        "size": file_info.size,
                        "is_directory": file_info.isDir()
                    })
            
            print(f"✓ Mount test successful: {len(files)} items found")
            
        except Exception as e:
            test_result["error"] = str(e)
            print(f"❌ Mount test failed: {str(e)}")
        
        return test_result
    
    def mount_blob_storage(self, storage_account: str, container: str, mount_point: str,
                          access_key: str) -> dict:
        """Mount Azure Blob Storage"""
        
        print(f"=== Mounting Blob Storage: {storage_account}/{container} ===")
        
        mount_config = {
            "storage_account": storage_account,
            "container": container,
            "mount_point": mount_point,
            "storage_type": "blob",
            "status": "pending"
        }
        
        if not self.dbutils:
            print("dbutils not available - cannot mount storage")
            mount_config["status"] = "failed"
            return mount_config
        
        try:
            # Check if already mounted
            existing_mounts = self.dbutils.fs.mounts()
            for mount in existing_mounts:
                if mount.mountPoint == mount_point:
                    print(f"⚠️ Mount point already exists: {mount_point}")
                    mount_config["status"] = "already_mounted"
                    return mount_config
            
            # Configure Blob Storage mount
            configs = {
                f"fs.azure.account.key.{storage_account}.blob.core.windows.net": access_key
            }
            
            source = f"wasbs://{container}@{storage_account}.blob.core.windows.net/"
            
            self.dbutils.fs.mount(
                source=source,
                mount_point=mount_point,
                extra_configs=configs
            )
            
            mount_config["source"] = source
            mount_config["status"] = "success"
            
            # Test the mount
            test_result = self._test_mount(mount_point)
            mount_config["test_result"] = test_result
            
            print(f"✓ Successfully mounted Blob Storage: {mount_point}")
            return mount_config
            
        except Exception as e:
            error_msg = f"Failed to mount blob storage: {str(e)}"
            print(f"❌ {error_msg}")
            mount_config["status"] = "failed"
            mount_config["error"] = error_msg
            return mount_config
    
    def list_mounts(self) -> list:
        """List all current mounts"""
        
        print("=== Current Storage Mounts ===")
        
        if not self.dbutils:
            print("dbutils not available - cannot list mounts")
            return []
        
        try:
            mounts = self.dbutils.fs.mounts()
            mount_list = []
            
            for mount in mounts:
                mount_info = {
                    "mount_point": mount.mountPoint,
                    "source": mount.source,
                    "extra_configs": dict(mount.extraConfigs) if mount.extraConfigs else {}
                }
                mount_list.append(mount_info)
                
                # Determine storage type
                if "dfs.core.windows.net" in mount.source:
                    storage_type = "ADLS Gen2"
                elif "blob.core.windows.net" in mount.source:
                    storage_type = "Blob Storage"
                else:
                    storage_type = "Other"
                
                print(f"🔗 {mount.mountPoint} -> {mount.source} ({storage_type})")
            
            return mount_list
            
        except Exception as e:
            print(f"Error listing mounts: {str(e)}")
            return []
    
    def unmount_storage(self, mount_point: str) -> bool:
        """Unmount storage"""
        
        print(f"=== Unmounting Storage: {mount_point} ===")
        
        if not self.dbutils:
            print("dbutils not available - cannot unmount storage")
            return False
        
        try:
            # Check if mount exists
            existing_mounts = self.dbutils.fs.mounts()
            mount_exists = any(mount.mountPoint == mount_point for mount in existing_mounts)
            
            if not mount_exists:
                print(f"⚠️ Mount point does not exist: {mount_point}")
                return False
            
            # Unmount
            self.dbutils.fs.unmount(mount_point)
            print(f"✓ Successfully unmounted: {mount_point}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to unmount {mount_point}: {str(e)}")
            return False
    
    def create_comprehensive_mount_setup(self) -> dict:
        """Create a comprehensive mount setup for different storage types"""
        
        print("=== Creating Comprehensive Mount Setup ===")
        
        mount_setup = {
            "timestamp": datetime.now().isoformat(),
            "mounts_created": [],
            "mount_configs": {},
            "errors": []
        }
        
        # Example mount configurations (replace with actual values)
        mount_configurations = [
            {
                "name": "data_lake",
                "storage_account": "yourdatalake",
                "container": "raw-data",
                "mount_point": "/mnt/datalake",
                "auth_method": "service_principal",
                "auth_config": {
                    "client_id": "your-client-id",
                    "client_secret": "your-client-secret",
                    "tenant_id": "your-tenant-id"
                }
            },
            {
                "name": "processed_data",
                "storage_account": "yourprocessed",
                "container": "processed",
                "mount_point": "/mnt/processed",
                "auth_method": "service_principal",
                "auth_config": {
                    "client_id": "your-client-id",
                    "client_secret": "your-client-secret",
                    "tenant_id": "your-tenant-id"
                }
            },
            {
                "name": "archive_storage",
                "storage_account": "yourarchive",
                "container": "archive",
                "mount_point": "/mnt/archive",
                "auth_method": "access_key",
                "auth_config": {
                    "access_key": "your-access-key"
                }
            }
        ]
        
        for config in mount_configurations:
            try:
                print(f"\n--- Setting up mount: {config['name']} ---")
                
                mount_result = self.mount_adls_gen2(
                    storage_account=config["storage_account"],
                    container=config["container"],
                    mount_point=config["mount_point"],
                    auth_method=config["auth_method"],
                    **config["auth_config"]
                )
                
                mount_setup["mounts_created"].append(config["name"])
                mount_setup["mount_configs"][config["name"]] = mount_result
                
            except Exception as e:
                error_msg = f"Failed to create mount {config['name']}: {str(e)}"
                mount_setup["errors"].append(error_msg)
                print(f"❌ {error_msg}")
        
        # Create mount documentation
        mount_documentation = {
            "mount_setup_guide": {
                "data_lake": {
                    "purpose": "Raw data ingestion and landing zone",
                    "usage": "Store incoming data from various sources",
                    "access_pattern": "Write-heavy, append-only",
                    "retention": "Long-term storage with lifecycle policies"
                },
                "processed_data": {
                    "purpose": "Cleaned and transformed data",
                    "usage": "Store processed datasets for analytics",
                    "access_pattern": "Read-heavy for analytics workloads",
                    "retention": "Medium-term with regular cleanup"
                },
                "archive_storage": {
                    "purpose": "Long-term archival and compliance",
                    "usage": "Store historical data and backups",
                    "access_pattern": "Infrequent access, compliance-driven",
                    "retention": "Long-term with compliance requirements"
                }
            },
            "best_practices": [
                "Use Service Principal authentication for production",
                "Implement proper RBAC and access controls",
                "Monitor mount usage and performance",
                "Regularly rotate access keys and secrets",
                "Use Azure Key Vault for secret management",
                "Implement data lifecycle policies",
                "Monitor costs and optimize storage tiers"
            ],
            "troubleshooting": {
                "common_issues": [
                    "Authentication failures - check credentials",
                    "Network connectivity - verify firewall rules",
                    "Permission errors - verify RBAC assignments",
                    "Mount conflicts - check existing mounts"
                ],
                "diagnostic_commands": [
                    "dbutils.fs.mounts() - list all mounts",
                    "dbutils.fs.ls('/mnt/mount-name') - test mount access",
                    "spark.conf.get('fs.azure.account.key.account.dfs.core.windows.net') - check config"
                ]
            }
        }
        
        mount_setup["documentation"] = mount_documentation
        
        return mount_setup

# Example usage
def demonstrate_azure_storage_mounting():
    """Demonstrate Azure storage mounting capabilities"""
    
    print("=== Azure Storage Mounting Demonstration ===")
    
    try:
        # Initialize mount manager
        mount_manager = AzureStorageMountManager()
        
        # List current mounts
        current_mounts = mount_manager.list_mounts()
        
        # Create comprehensive mount setup (with example configurations)
        mount_setup = mount_manager.create_comprehensive_mount_setup()
        
        return {
            "status": "success",
            "current_mounts": current_mounts,
            "mount_setup": mount_setup
        }
        
    except Exception as e:
        print(f"Demonstration failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

# Run demonstration
# mount_demo_results = demonstrate_azure_storage_mounting()
```

---

## Unity Catalog File Sharing

### Unity Catalog Volumes and External Locations

Unity Catalog provides centralized governance for file sharing across multiple workspaces.

```python
# Unity Catalog File Sharing Implementation
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
import json
from datetime import datetime

class UnityCatalogFileManager:
    """Unity Catalog file sharing and governance"""
    
    def __init__(self, workspace_url: str = None, token: str = None):
        try:
            # Initialize Databricks SDK client
            if workspace_url and token:
                self.w = WorkspaceClient(host=workspace_url, token=token)
            else:
                self.w = WorkspaceClient()  # Uses default authentication
            
            # Initialize Spark session if available
            try:
                from pyspark.sql import SparkSession
                self.spark = SparkSession.builder.getOrCreate()
            except:
                self.spark = None
                
        except Exception as e:
            print(f"Warning: Could not initialize Unity Catalog client: {e}")
            self.w = None
            self.spark = None
    
    def create_catalog_structure(self, catalog_name: str = "shared_data") -> dict:
        """Create a comprehensive catalog structure for file sharing"""
        
        print(f"=== Creating Unity Catalog Structure: {catalog_name} ===")
        
        catalog_structure = {
            "catalog_name": catalog_name,
            "schemas": {
                "raw_data": f"{catalog_name}.raw_data",
                "processed_data": f"{catalog_name}.processed_data",
                "shared_files": f"{catalog_name}.shared_files",
                "team_workspaces": f"{catalog_name}.team_workspaces"
            },
            "volumes": {},
            "external_locations": {},
            "created_objects": [],
            "errors": []
        }
        
        if not self.w:
            print("Unity Catalog client not available")
            catalog_structure["errors"].append("Unity Catalog client not initialized")
            return catalog_structure
        
        try:
            # Create catalog
            try:
                catalog_info = CatalogInfo(
                    name=catalog_name,
                    comment=f"Shared data catalog for file sharing - Created {datetime.now().isoformat()}",
                    properties={
                        "purpose": "file_sharing",
                        "created_by": "unity_catalog_manager",
                        "created_date": datetime.now().strftime("%Y-%m-%d")
                    }
                )
                
                created_catalog = self.w.catalogs.create(catalog_info)
                catalog_structure["created_objects"].append(f"catalog:{catalog_name}")
                print(f"✓ Created catalog: {catalog_name}")
                
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"⚠️ Catalog already exists: {catalog_name}")
                else:
                    raise e
            
            # Create schemas
            schema_configs = {
                "raw_data": "Raw data files from various sources",
                "processed_data": "Processed and cleaned data files", 
                "shared_files": "Files shared across teams and projects",
                "team_workspaces": "Team-specific file workspaces"
            }
            
            for schema_name, description in schema_configs.items():
                try:
                    schema_info = SchemaInfo(
                        name=schema_name,
                        catalog_name=catalog_name,
                        comment=description,
                        properties={
                            "purpose": schema_name.replace("_", " "),
                            "created_date": datetime.now().strftime("%Y-%m-%d")
                        }
                    )
                    
                    created_schema = self.w.schemas.create(schema_info)
                    catalog_structure["created_objects"].append(f"schema:{catalog_name}.{schema_name}")
                    print(f"✓ Created schema: {catalog_name}.{schema_name}")
                    
                except Exception as e:
                    if "already exists" in str(e).lower():
                        print(f"⚠️ Schema already exists: {catalog_name}.{schema_name}")
                    else:
                        catalog_structure["errors"].append(f"Failed to create schema {schema_name}: {str(e)}")
                        print(f"❌ Failed to create schema {schema_name}: {str(e)}")
            
            # Create external locations (examples - replace with actual storage paths)
            external_location_configs = [
                {
                    "name": f"{catalog_name}_raw_data_location",
                    "url": "abfss://raw-data@yourstorageaccount.dfs.core.windows.net/",
                    "credential_name": "your_storage_credential",
                    "comment": "External location for raw data files"
                },
                {
                    "name": f"{catalog_name}_processed_location", 
                    "url": "abfss://processed@yourstorageaccount.dfs.core.windows.net/",
                    "credential_name": "your_storage_credential",
                    "comment": "External location for processed data files"
                }
            ]
            
            for location_config in external_location_configs:
                try:
                    external_location_info = ExternalLocationInfo(
                        name=location_config["name"],
                        url=location_config["url"],
                        credential_name=location_config["credential_name"],
                        comment=location_config["comment"]
                    )
                    
                    # Note: This requires proper storage credentials to be set up
                    # created_location = self.w.external_locations.create(external_location_info)
                    # catalog_structure["created_objects"].append(f"external_location:{location_config['name']}")
                    # print(f"✓ Created external location: {location_config['name']}")
                    
                    catalog_structure["external_locations"][location_config["name"]] = location_config
                    print(f"📝 External location config prepared: {location_config['name']}")
                    
                except Exception as e:
                    catalog_structure["errors"].append(f"Failed to create external location {location_config['name']}: {str(e)}")
                    print(f"❌ Failed to create external location {location_config['name']}: {str(e)}")
            
            # Create volumes for file sharing
            volume_configs = [
                {
                    "name": "raw_files",
                    "schema": "raw_data",
                    "volume_type": "MANAGED",
                    "comment": "Volume for raw data files"
                },
                {
                    "name": "processed_files",
                    "schema": "processed_data", 
                    "volume_type": "MANAGED",
                    "comment": "Volume for processed data files"
                },
                {
                    "name": "shared_notebooks",
                    "schema": "shared_files",
                    "volume_type": "MANAGED",
                    "comment": "Volume for shared notebooks and scripts"
                },
                {
                    "name": "team_resources",
                    "schema": "team_workspaces",
                    "volume_type": "MANAGED",
                    "comment": "Volume for team-specific resources"
                }
            ]
            
            for volume_config in volume_configs:
                try:
                    volume_info = VolumeInfo(
                        name=volume_config["name"],
                        catalog_name=catalog_name,
                        schema_name=volume_config["schema"],
                        volume_type=VolumeType(volume_config["volume_type"]),
                        comment=volume_config["comment"]
                    )
                    
                    created_volume = self.w.volumes.create(volume_info)
                    volume_path = f"/Volumes/{catalog_name}/{volume_config['schema']}/{volume_config['name']}"
                    catalog_structure["volumes"][volume_config["name"]] = {
                        "path": volume_path,
                        "schema": volume_config["schema"],
                        "type": volume_config["volume_type"],
                        "comment": volume_config["comment"]
                    }
                    catalog_structure["created_objects"].append(f"volume:{volume_path}")
                    print(f"✓ Created volume: {volume_path}")
                    
                except Exception as e:
                    if "already exists" in str(e).lower():
                        print(f"⚠️ Volume already exists: {volume_config['name']}")
                        volume_path = f"/Volumes/{catalog_name}/{volume_config['schema']}/{volume_config['name']}"
                        catalog_structure["volumes"][volume_config["name"]] = {
                            "path": volume_path,
                            "schema": volume_config["schema"],
                            "type": volume_config["volume_type"],
                            "comment": volume_config["comment"]
                        }
                    else:
                        catalog_structure["errors"].append(f"Failed to create volume {volume_config['name']}: {str(e)}")
                        print(f"❌ Failed to create volume {volume_config['name']}: {str(e)}")
            
            return catalog_structure
            
        except Exception as e:
            error_msg = f"Error creating catalog structure: {str(e)}"
            catalog_structure["errors"].append(error_msg)
            print(f"❌ {error_msg}")
            return catalog_structure
    
    def demonstrate_volume_file_operations(self, catalog_name: str = "shared_data") -> dict:
        """Demonstrate file operations with Unity Catalog volumes"""
        
        print(f"=== Unity Catalog Volume File Operations ===")
        
        operations_result = {
            "catalog_name": catalog_name,
            "operations": [],
            "files_created": [],
            "errors": []
        }
        
        if not self.spark:
            print("Spark session not available")
            operations_result["errors"].append("Spark session not available")
            return operations_result
        
        try:
            # Define volume paths
            volume_paths = {
                "raw_files": f"/Volumes/{catalog_name}/raw_data/raw_files",
                "processed_files": f"/Volumes/{catalog_name}/processed_data/processed_files",
                "shared_notebooks": f"/Volumes/{catalog_name}/shared_files/shared_notebooks",
                "team_resources": f"/Volumes/{catalog_name}/team_workspaces/team_resources"
            }
            
            # Create sample data and files in each volume
            for volume_name, volume_path in volume_paths.items():
                try:
                    print(f"\n--- Working with volume: {volume_name} ---")
                    
                    # Create sample DataFrame
                    if volume_name == "raw_files":
                        # Raw transaction data
                        sample_data = [
                            (1, "2024-01-01", "customer_001", 150.00, "online"),
                            (2, "2024-01-01", "customer_002", 75.50, "store"),
                            (3, "2024-01-02", "customer_001", 200.00, "online"),
                            (4, "2024-01-02", "customer_003", 300.25, "store"),
                            (5, "2024-01-03", "customer_002", 125.00, "online")
                        ]
                        columns = ["transaction_id", "date", "customer_id", "amount", "channel"]
                        
                    elif volume_name == "processed_files":
                        # Processed customer summary
                        sample_data = [
                            ("customer_001", 2, 350.00, 175.00, "online"),
                            ("customer_002", 2, 200.50, 100.25, "mixed"),
                            ("customer_003", 1, 300.25, 300.25, "store")
                        ]
                        columns = ["customer_id", "transaction_count", "total_amount", "avg_amount", "primary_channel"]
                        
                    else:
                        # Generic sample data
                        sample_data = [
                            (1, f"{volume_name}_item_1", "active"),
                            (2, f"{volume_name}_item_2", "inactive"),
                            (3, f"{volume_name}_item_3", "active")
                        ]
                        columns = ["id", "name", "status"]
                    
                    df = self.spark.createDataFrame(sample_data, columns)
                    
                    # Write as Parquet
                    parquet_path = f"{volume_path}/sample_data.parquet"
                    df.write.mode("overwrite").parquet(parquet_path)
                    operations_result["files_created"].append(parquet_path)
                    operations_result["operations"].append(f"Created Parquet file: {parquet_path}")
                    print(f"✓ Created Parquet file: {parquet_path}")
                    
                    # Write as CSV
                    csv_path = f"{volume_path}/sample_data.csv"
                    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
                    operations_result["files_created"].append(csv_path)
                    operations_result["operations"].append(f"Created CSV file: {csv_path}")
                    print(f"✓ Created CSV file: {csv_path}")
                    
                    # Create metadata file
                    metadata = {
                        "volume_name": volume_name,
                        "volume_path": volume_path,
                        "data_description": f"Sample data for {volume_name}",
                        "schema": {
                            "columns": columns,
                            "row_count": len(sample_data)
                        },
                        "created_timestamp": datetime.now().isoformat(),
                        "file_formats": ["parquet", "csv"]
                    }
                    
                    # Note: In actual Unity Catalog volumes, you would use:
                    # dbutils.fs.put() or write files directly to the volume path
                    print(f"📝 Metadata prepared for: {volume_name}")
                    operations_result["operations"].append(f"Prepared metadata for: {volume_name}")
                    
                except Exception as e:
                    error_msg = f"Error with volume {volume_name}: {str(e)}"
                    operations_result["errors"].append(error_msg)
                    print(f"❌ {error_msg}")
            
            # Demonstrate cross-volume data sharing
            try:
                print(f"\n--- Cross-Volume Data Sharing Example ---")
                
                # Read from raw volume
                raw_path = f"{volume_paths['raw_files']}/sample_data.parquet"
                # raw_df = self.spark.read.parquet(raw_path)
                
                # Process and write to processed volume
                processed_path = f"{volume_paths['processed_files']}/aggregated_data.parquet"
                # processed_df = raw_df.groupBy("customer_id").agg(
                #     count("transaction_id").alias("transaction_count"),
                #     sum("amount").alias("total_amount"),
                #     avg("amount").alias("avg_amount")
                # )
                # processed_df.write.mode("overwrite").parquet(processed_path)
                
                print(f"📊 Cross-volume processing example prepared")
                operations_result["operations"].append("Cross-volume processing example prepared")
                
            except Exception as e:
                error_msg = f"Error in cross-volume processing: {str(e)}"
                operations_result["errors"].append(error_msg)
                print(f"❌ {error_msg}")
            
            return operations_result
            
        except Exception as e:
            error_msg = f"Error in volume file operations: {str(e)}"
            operations_result["errors"].append(error_msg)
            print(f"❌ {error_msg}")
            return operations_result
    
    def create_file_sharing_governance(self, catalog_name: str = "shared_data") -> dict:
        """Create governance structure for file sharing"""
        
        print(f"=== Creating File Sharing Governance ===")
        
        governance_config = {
            "catalog_name": catalog_name,
            "access_policies": {},
            "data_classification": {},
            "sharing_guidelines": {},
            "audit_configuration": {}
        }
        
        # Define access policies
        governance_config["access_policies"] = {
            "data_engineers": {
                "privileges": ["SELECT", "MODIFY", "CREATE"],
                "scope": ["raw_data.*", "processed_data.*"],
                "description": "Full access to data engineering volumes"
            },
            "data_analysts": {
                "privileges": ["SELECT"],
                "scope": ["processed_data.*", "shared_files.*"],
                "description": "Read access to processed data and shared files"
            },
            "data_scientists": {
                "privileges": ["SELECT", "CREATE"],
                "scope": ["processed_data.*", "team_workspaces.*"],
                "description": "Read processed data, create in team workspaces"
            },
            "external_users": {
                "privileges": ["SELECT"],
                "scope": ["shared_files.public_datasets"],
                "description": "Limited access to public datasets only"
            }
        }
        
        # Define data classification
        governance_config["data_classification"] = {
            "public": {
                "description": "Data that can be shared publicly",
                "locations": ["shared_files.public_datasets"],
                "access_level": "open",
                "retention": "indefinite"
            },
            "internal": {
                "description": "Data for internal use only",
                "locations": ["processed_data.*", "team_workspaces.*"],
                "access_level": "authenticated_users",
                "retention": "7_years"
            },
            "confidential": {
                "description": "Sensitive business data",
                "locations": ["raw_data.customer_data", "raw_data.financial_data"],
                "access_level": "authorized_users_only",
                "retention": "10_years"
            },
            "restricted": {
                "description": "Highly sensitive data with strict access controls",
                "locations": ["raw_data.pii_data"],
                "access_level": "explicit_approval_required",
                "retention": "regulatory_compliance"
            }
        }
        
        # Define sharing guidelines
        governance_config["sharing_guidelines"] = {
            "file_naming_conventions": {
                "format": "{source}_{type}_{date}_{version}",
                "examples": [
                    "salesforce_customers_20240101_v1.parquet",
                    "web_analytics_events_20240101_v2.json",
                    "finance_transactions_20240101_final.csv"
                ],
                "required_elements": ["source", "type", "date"],
                "optional_elements": ["version", "environment"]
            },
            "metadata_requirements": {
                "mandatory_fields": [
                    "data_source",
                    "created_date", 
                    "created_by",
                    "data_classification",
                    "retention_period"
                ],
                "recommended_fields": [
                    "description",
                    "schema_version",
                    "data_quality_score",
                    "last_validated"
                ]
            },
            "approval_workflows": {
                "public_sharing": "data_governance_team_approval",
                "cross_team_sharing": "team_lead_approval",
                "external_sharing": "security_team_approval",
                "sensitive_data": "compliance_team_approval"
            }
        }
        
        # Define audit configuration
        governance_config["audit_configuration"] = {
            "logging_enabled": True,
            "audit_events": [
                "file_access",
                "file_creation",
                "file_modification",
                "file_deletion",
                "permission_changes",
                "sharing_activities"
            ],
            "retention_period": "3_years",
            "alert_conditions": [
                "unusual_access_patterns",
                "bulk_data_downloads",
                "unauthorized_access_attempts",
                "sensitive_data_access"
            ],
            "reporting_schedule": {
                "daily": ["access_summary", "failed_attempts"],
                "weekly": ["usage_statistics", "permission_changes"],
                "monthly": ["compliance_report", "data_lineage_analysis"]
            }
        }
        
        # Create governance documentation
        governance_documentation = f"""
# File Sharing Governance for {catalog_name}

## Overview
This document outlines the governance framework for file sharing in the {catalog_name} Unity Catalog.

## Access Control Matrix

| Role | Raw Data | Processed Data | Shared Files | Team Workspaces |
|------|----------|----------------|--------------|-----------------|
| Data Engineers | Full Access | Full Access | Read/Write | Read Only |
| Data Analysts | No Access | Read Only | Read/Write | Read Only |
| Data Scientists | No Access | Read Only | Read Only | Read/Write |
| External Users | No Access | No Access | Read Only (Public) | No Access |

## Data Classification Levels

### Public
- Openly shareable data
- No access restrictions
- Located in: shared_files.public_datasets

### Internal  
- Company internal use only
- Requires authentication
- Located in: processed_data.*, team_workspaces.*

### Confidential
- Sensitive business data
- Requires authorization
- Located in: raw_data.customer_data, raw_data.financial_data

### Restricted
- Highly sensitive data
- Requires explicit approval
- Located in: raw_data.pii_data

## File Naming Standards

Format: `{{source}}_{{type}}_{{date}}_{{version}}`

Examples:
- `salesforce_customers_20240101_v1.parquet`
- `web_analytics_events_20240101_v2.json`
- `finance_transactions_20240101_final.csv`

## Approval Workflows

- **Public Sharing**: Data Governance Team approval required
- **Cross-Team Sharing**: Team Lead approval required  
- **External Sharing**: Security Team approval required
- **Sensitive Data**: Compliance Team approval required

## Compliance and Auditing

All file access and sharing activities are logged and monitored. Regular compliance reports are generated and reviewed by the Data Governance Team.

For questions or requests, contact: data-governance@company.com
"""
        
        governance_config["documentation"] = governance_documentation
        
        print("✓ File sharing governance framework created")
        print("✓ Access policies defined for all user roles")
        print("✓ Data classification levels established")
        print("✓ Sharing guidelines and approval workflows defined")
        print("✓ Audit and compliance configuration prepared")
        
        return governance_config

# Example usage and demonstration
def demonstrate_unity_catalog_file_sharing():
    """Comprehensive demonstration of Unity Catalog file sharing"""
    
    print("=== Unity Catalog File Sharing Demonstration ===")
    
    try:
        # Initialize Unity Catalog manager
        uc_manager = UnityCatalogFileManager()
        
        # Create catalog structure
        catalog_structure = uc_manager.create_catalog_structure()
        
        # Demonstrate volume file operations
        file_operations = uc_manager.demonstrate_volume_file_operations()
        
        # Create governance framework
        governance = uc_manager.create_file_sharing_governance()
        
        return {
            "status": "success",
            "catalog_structure": catalog_structure,
            "file_operations": file_operations,
            "governance_framework": governance
        }
        
    except Exception as e:
        print(f"Demonstration failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

# Run demonstration
# unity_demo_results = demonstrate_unity_catalog_file_sharing()
```

This comprehensive guide provides everything needed to understand, implement, and manage file sharing in Azure Databricks environments, ensuring effective collaboration while maintaining security and governance.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*