# Azure Storage Types - Comprehensive Guide
## Complete Analysis of All Azure Storage Services with Detailed Examples and Use Cases

---

### Table of Contents

1. [Introduction to Azure Storage](#introduction-to-azure-storage)
2. [Azure Storage Account Types](#azure-storage-account-types)
3. [Blob Storage Services](#blob-storage-services)
4. [File Storage Services](#file-storage-services)
5. [Queue Storage Services](#queue-storage-services)
6. [Table Storage Services](#table-storage-services)
7. [Disk Storage Services](#disk-storage-services)
8. [Data Lake Storage](#data-lake-storage)
9. [Archive and Backup Storage](#archive-and-backup-storage)
10. [Database Storage Services](#database-storage-services)
11. [Specialized Storage Services](#specialized-storage-services)
12. [Storage Performance Tiers](#storage-performance-tiers)
13. [Storage Redundancy Options](#storage-redundancy-options)
14. [Cost Optimization Strategies](#cost-optimization-strategies)
15. [Use Case Decision Matrix](#use-case-decision-matrix)
16. [Implementation Examples](#implementation-examples)
17. [Best Practices](#best-practices)
18. [Conclusion](#conclusion)

---

## Introduction to Azure Storage

Azure provides a comprehensive suite of storage services designed to meet diverse application requirements, from simple file storage to complex data analytics workloads. Understanding the different storage types and their optimal use cases is crucial for building efficient and cost-effective cloud solutions.

### Azure Storage Service Categories

```
Azure Storage Services:
├── Core Storage Services
│   ├── Blob Storage (Object Storage)
│   ├── File Storage (SMB/NFS File Shares)
│   ├── Queue Storage (Message Queuing)
│   └── Table Storage (NoSQL Key-Value)
├── Managed Disk Services
│   ├── Premium SSD (High Performance)
│   ├── Standard SSD (Balanced Performance)
│   ├── Standard HDD (Cost Optimized)
│   └── Ultra Disk (Ultra High Performance)
├── Data Platform Storage
│   ├── Data Lake Storage Gen2
│   ├── SQL Database Storage
│   ├── Cosmos DB Storage
│   └── Synapse Analytics Storage
├── Backup and Archive
│   ├── Azure Backup
│   ├── Azure Site Recovery
│   └── Archive Storage Tiers
└── Specialized Storage
    ├── NetApp Files
    ├── HPC Cache
    ├── Data Box (Offline Transfer)
    └── StorSimple (Hybrid Storage)
```

### Storage Service Comparison Overview

| Storage Type | Use Case | Capacity | Performance | Access Pattern | Cost Level |
|--------------|----------|----------|-------------|----------------|------------|
| **Blob Storage** | Object storage, web content, backup | Up to 5 PB | High throughput | Random/Sequential | Low-Medium |
| **File Storage** | File shares, lift-and-shift | Up to 100 TiB | Medium | Random | Medium |
| **Queue Storage** | Message queuing | Up to 500 TB | High messages/sec | FIFO | Low |
| **Table Storage** | NoSQL key-value | Up to 500 TB | High IOPS | Key-based | Low |
| **Premium SSD** | High-performance VMs | Up to 32 TiB | Very High IOPS | Random | High |
| **Data Lake Gen2** | Big data analytics | Unlimited | High throughput | Sequential/Batch | Medium |

---

## Azure Storage Account Types

Azure Storage Accounts serve as the foundation for all storage services, with different account types optimized for specific scenarios.

### Storage Account Types Comparison

#### 1. General Purpose v2 (GPv2) - Recommended

```json
{
  "storageAccountType": "Standard_LRS",
  "kind": "StorageV2",
  "location": "East US",
  "properties": {
    "accessTier": "Hot",
    "supportsHttpsTrafficOnly": true,
    "allowBlobPublicAccess": false,
    "minimumTlsVersion": "TLS1_2",
    "allowSharedKeyAccess": true,
    "networkAcls": {
      "defaultAction": "Deny",
      "virtualNetworkRules": [],
      "ipRules": []
    },
    "encryption": {
      "services": {
        "blob": {"enabled": true},
        "file": {"enabled": true},
        "table": {"enabled": true},
        "queue": {"enabled": true}
      },
      "keySource": "Microsoft.Storage"
    }
  },
  "supportedServices": [
    "Blob Storage",
    "File Storage", 
    "Queue Storage",
    "Table Storage"
  ],
  "features": [
    "All access tiers (Hot, Cool, Archive)",
    "Data Lake Storage Gen2",
    "Advanced security features",
    "Lifecycle management",
    "Change feed",
    "Point-in-time restore"
  ]
}
```

**Use Cases:**
- Modern applications requiring all storage services
- Data analytics and big data workloads
- Web applications with varied access patterns
- Enterprise applications with compliance requirements

**Example Implementation:**
```bash
# Create GPv2 storage account with Azure CLI
az storage account create \
  --name mystorageaccountgpv2 \
  --resource-group myResourceGroup \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --access-tier Hot \
  --https-only true \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  --tags Environment=Production Application=WebApp
```

#### 2. Blob Storage Accounts

```json
{
  "storageAccountType": "Standard_LRS",
  "kind": "BlobStorage", 
  "location": "West US",
  "properties": {
    "accessTier": "Cool",
    "supportsHttpsTrafficOnly": true
  },
  "supportedServices": ["Blob Storage Only"],
  "features": [
    "Hot and Cool access tiers only",
    "Block and Append blobs",
    "Optimized for blob workloads",
    "Lower cost for blob-only scenarios"
  ],
  "limitations": [
    "No Archive tier support",
    "No File, Queue, or Table storage",
    "Being deprecated in favor of GPv2"
  ]
}
```

**Use Cases:**
- Legacy blob-only applications
- Simple object storage requirements
- Cost-sensitive blob storage scenarios

#### 3. Block Blob Storage Accounts (Premium)

```json
{
  "storageAccountType": "Premium_LRS",
  "kind": "BlockBlobStorage",
  "location": "East US 2",
  "properties": {
    "supportsHttpsTrafficOnly": true,
    "allowBlobPublicAccess": false
  },
  "supportedServices": ["Premium Block Blob Storage"],
  "features": [
    "Premium SSD storage",
    "Low latency (single-digit milliseconds)",
    "High IOPS and throughput",
    "Block blobs and append blobs only"
  ],
  "performance": {
    "iops": "Up to 100,000 IOPS",
    "throughput": "Up to 10 Gbps",
    "latency": "< 10ms"
  }
}
```

**Use Cases:**
- High-performance applications
- Real-time analytics workloads
- Gaming applications
- IoT data ingestion with low latency requirements

**Example Implementation:**
```bash
# Create Premium Block Blob storage account
az storage account create \
  --name premiumblockblobstorage \
  --resource-group myResourceGroup \
  --location eastus2 \
  --sku Premium_LRS \
  --kind BlockBlobStorage \
  --https-only true \
  --tags Performance=Premium Workload=Analytics
```

#### 4. File Storage Accounts (Premium)

```json
{
  "storageAccountType": "Premium_LRS",
  "kind": "FileStorage",
  "location": "West US 2",
  "properties": {
    "supportsHttpsTrafficOnly": true
  },
  "supportedServices": ["Premium File Storage"],
  "features": [
    "Premium SSD-backed file shares",
    "SMB and NFS protocol support",
    "Low latency file operations",
    "High IOPS for file workloads"
  ],
  "performance": {
    "iops": "Up to 100,000 IOPS per share",
    "throughput": "Up to 10 Gbps per share",
    "latency": "< 10ms"
  }
}
```

**Use Cases:**
- High-performance file shares
- Database file storage
- Content repositories requiring low latency
- Enterprise file services

---

## Blob Storage Services

Azure Blob Storage is Microsoft's object storage solution, designed for storing massive amounts of unstructured data.

### Blob Storage Types

#### 1. Block Blobs

```python
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
from datetime import datetime

# Initialize blob service client
connection_string = "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Block blob example - for general-purpose file storage
def upload_block_blob_example():
    """
    Block blobs are optimized for uploading large amounts of data efficiently
    Maximum size: 4.75 TB (50,000 blocks × 100 MB)
    """
    
    container_name = "block-blob-container"
    blob_name = "large-dataset.csv"
    
    # Create container if it doesn't exist
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception as e:
        print(f"Container may already exist: {e}")
    
    # Upload large file in blocks
    local_file_path = "path/to/large-dataset.csv"
    
    with open(local_file_path, "rb") as data:
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        
        # Upload with metadata and properties
        blob_client.upload_blob(
            data,
            blob_type="BlockBlob",
            overwrite=True,
            metadata={
                "upload_date": datetime.now().isoformat(),
                "content_type": "text/csv",
                "data_classification": "internal"
            },
            content_settings={
                "content_type": "text/csv",
                "content_encoding": "utf-8"
            }
        )
        
        print(f"Block blob uploaded: {blob_name}")
        
        # Get blob properties
        blob_properties = blob_client.get_blob_properties()
        print(f"Blob size: {blob_properties.size} bytes")
        print(f"Last modified: {blob_properties.last_modified}")
        print(f"ETag: {blob_properties.etag}")

# Use cases for Block Blobs
block_blob_use_cases = {
    "web_content": {
        "description": "Images, videos, documents for web applications",
        "example": "Storing user-uploaded images in a social media app",
        "access_pattern": "Random read access",
        "typical_size": "1 KB to 100 MB per file"
    },
    "backup_archives": {
        "description": "Database backups, system backups",
        "example": "Daily SQL database backup files",
        "access_pattern": "Write once, read occasionally",
        "typical_size": "100 MB to 4.75 TB per file"
    },
    "data_analytics": {
        "description": "Raw data files for analytics processing",
        "example": "CSV files with sales data for analysis",
        "access_pattern": "Sequential read for processing",
        "typical_size": "10 MB to 1 GB per file"
    },
    "media_storage": {
        "description": "Video files, audio files, images",
        "example": "Video streaming service content storage",
        "access_pattern": "Sequential read for streaming",
        "typical_size": "100 MB to 10 GB per file"
    }
}
```

#### 2. Append Blobs

```python
def append_blob_logging_example():
    """
    Append blobs are optimized for append operations (logging scenarios)
    Maximum size: 195 GB
    """
    
    container_name = "log-container"
    blob_name = f"application-log-{datetime.now().strftime('%Y-%m-%d')}.log"
    
    # Create container
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except:
        pass
    
    # Get append blob client
    append_blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )
    
    # Create append blob if it doesn't exist
    try:
        append_blob_client.create_append_blob()
    except Exception as e:
        if "BlobAlreadyExists" not in str(e):
            raise e
    
    # Append log entries
    log_entries = [
        f"[{datetime.now().isoformat()}] INFO: Application started\n",
        f"[{datetime.now().isoformat()}] INFO: User login successful - UserID: 12345\n",
        f"[{datetime.now().isoformat()}] WARN: High memory usage detected - 85%\n",
        f"[{datetime.now().isoformat()}] ERROR: Database connection timeout\n"
    ]
    
    for entry in log_entries:
        append_blob_client.append_block(entry.encode('utf-8'))
        print(f"Appended log entry: {entry.strip()}")
    
    # Get current blob size
    properties = append_blob_client.get_blob_properties()
    print(f"Current log file size: {properties.size} bytes")

# Append blob use cases
append_blob_use_cases = {
    "application_logging": {
        "description": "Continuous application log files",
        "example": "Web application error and access logs",
        "access_pattern": "Append-only writes, occasional reads",
        "benefits": ["Optimized for append operations", "Atomic append operations"]
    },
    "audit_trails": {
        "description": "Compliance and audit log storage",
        "example": "Financial transaction audit logs",
        "access_pattern": "Write-only with compliance reads",
        "benefits": ["Immutable append operations", "Timestamp ordering"]
    },
    "iot_telemetry": {
        "description": "IoT device telemetry data",
        "example": "Sensor data from manufacturing equipment",
        "access_pattern": "High-frequency append operations",
        "benefits": ["High throughput append", "Time-series data storage"]
    }
}
```

#### 3. Page Blobs

```python
def page_blob_vhd_example():
    """
    Page blobs are optimized for random read/write operations
    Maximum size: 8 TB
    Used primarily for VM disks
    """
    
    container_name = "vhd-container"
    blob_name = "vm-disk.vhd"
    
    # Create container
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except:
        pass
    
    # Get page blob client
    page_blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )
    
    # Create page blob (size must be multiple of 512 bytes)
    blob_size = 1024 * 1024 * 1024  # 1 GB
    page_blob_client.create_page_blob(size=blob_size)
    
    # Write pages (512-byte aligned)
    page_data = b"x" * 512  # 512 bytes of data
    page_blob_client.upload_page(page_data, offset=0, length=512)
    
    # Write another page at different offset
    page_blob_client.upload_page(page_data, offset=1024, length=512)
    
    # Get page ranges (which pages contain data)
    page_ranges = page_blob_client.get_page_ranges()
    print("Page ranges with data:")
    for page_range in page_ranges:
        print(f"  Start: {page_range['start']}, End: {page_range['end']}")

# Page blob use cases
page_blob_use_cases = {
    "vm_disks": {
        "description": "Virtual machine disk storage",
        "example": "Azure VM OS and data disks",
        "access_pattern": "Random read/write operations",
        "benefits": ["Optimized for disk I/O", "Sparse allocation"]
    },
    "database_files": {
        "description": "Database file storage requiring random access",
        "example": "SQL Server database files in Azure VMs",
        "access_pattern": "Random read/write with high IOPS",
        "benefits": ["Low latency random access", "Consistent performance"]
    }
}
```

### Blob Storage Access Tiers

#### Hot, Cool, and Archive Tier Management

```python
def manage_blob_access_tiers():
    """
    Demonstrate blob access tier management for cost optimization
    """
    
    container_name = "tiered-storage-demo"
    
    # Create container
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except:
        pass
    
    # Upload blobs with different access tiers
    blob_configs = [
        {
            "name": "frequently-accessed-data.json",
            "tier": "Hot",
            "description": "Data accessed multiple times per day",
            "cost_profile": "Higher storage cost, lower access cost"
        },
        {
            "name": "monthly-reports.pdf", 
            "tier": "Cool",
            "description": "Data accessed few times per month",
            "cost_profile": "Lower storage cost, higher access cost"
        },
        {
            "name": "archived-logs-2020.zip",
            "tier": "Archive",
            "description": "Data rarely accessed, long-term retention",
            "cost_profile": "Lowest storage cost, highest access cost and retrieval time"
        }
    ]
    
    for config in blob_configs:
        # Create sample data
        sample_data = f"Sample data for {config['description']}"
        
        # Upload blob
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=config["name"]
        )
        
        blob_client.upload_blob(
            sample_data.encode('utf-8'),
            overwrite=True,
            standard_blob_tier=config["tier"],
            metadata={
                "tier_reason": config["description"],
                "cost_profile": config["cost_profile"]
            }
        )
        
        print(f"Uploaded {config['name']} to {config['tier']} tier")
    
    # Demonstrate tier changes
    print("\nChanging blob tiers based on access patterns...")
    
    # Move a blob from Hot to Cool (after 30 days of infrequent access)
    hot_blob = blob_service_client.get_blob_client(
        container=container_name,
        blob="frequently-accessed-data.json"
    )
    
    # Change tier
    hot_blob.set_standard_blob_tier("Cool")
    print("Moved frequently-accessed-data.json from Hot to Cool tier")
    
    # Archive old data
    cool_blob = blob_service_client.get_blob_client(
        container=container_name,
        blob="monthly-reports.pdf"
    )
    
    cool_blob.set_standard_blob_tier("Archive")
    print("Archived monthly-reports.pdf to Archive tier")

# Access tier cost comparison
access_tier_comparison = {
    "Hot": {
        "storage_cost": "$0.0184 per GB/month",
        "access_cost": "$0.0004 per 10,000 operations",
        "use_case": "Data accessed frequently (daily)",
        "availability": "99.9%",
        "retrieval_time": "Immediate"
    },
    "Cool": {
        "storage_cost": "$0.0115 per GB/month", 
        "access_cost": "$0.01 per 10,000 operations",
        "use_case": "Data accessed infrequently (monthly)",
        "availability": "99.9%",
        "retrieval_time": "Immediate"
    },
    "Archive": {
        "storage_cost": "$0.00099 per GB/month",
        "access_cost": "$5.00 per 10,000 operations",
        "use_case": "Data rarely accessed (yearly)",
        "availability": "99.9% (after rehydration)",
        "retrieval_time": "Up to 15 hours (Standard), 1 hour (High priority)"
    }
}
```

---

## File Storage Services

Azure File Storage provides fully managed file shares accessible via SMB and NFS protocols.

### Azure Files Implementation

#### Standard File Shares

```python
from azure.storage.fileshare import ShareServiceClient, ShareClient, ShareFileClient
import os

# Initialize file service client
file_service_client = ShareServiceClient.from_connection_string(connection_string)

def create_standard_file_share():
    """
    Create and manage standard Azure file shares
    """
    
    share_name = "company-shared-files"
    
    # Create file share
    share_client = file_service_client.create_share(
        share=share_name,
        quota=1024,  # 1 TB quota
        metadata={
            "department": "IT",
            "purpose": "Shared company documents",
            "created_by": "admin@company.com"
        }
    )
    
    print(f"Created file share: {share_name}")
    
    # Create directory structure
    directories = [
        "HR/Policies",
        "HR/Templates", 
        "Finance/Reports",
        "Finance/Budgets",
        "IT/Documentation",
        "IT/Scripts"
    ]
    
    for directory in directories:
        # Create nested directories
        dir_client = share_client.get_directory_client(directory)
        try:
            dir_client.create_directory()
            print(f"Created directory: {directory}")
        except Exception as e:
            if "ResourceAlreadyExists" not in str(e):
                print(f"Error creating directory {directory}: {e}")
    
    # Upload files to directories
    sample_files = [
        {
            "local_path": "local/hr-policy.pdf",
            "remote_path": "HR/Policies/employee-handbook.pdf",
            "content": "Sample HR policy content"
        },
        {
            "local_path": "local/budget-template.xlsx", 
            "remote_path": "Finance/Budgets/2024-budget-template.xlsx",
            "content": "Sample budget template content"
        }
    ]
    
    for file_info in sample_files:
        file_client = share_client.get_file_client(file_info["remote_path"])
        
        # Upload file
        file_client.upload_file(
            file_info["content"].encode('utf-8'),
            metadata={
                "uploaded_by": "system",
                "upload_date": datetime.now().isoformat(),
                "file_type": "document"
            }
        )
        
        print(f"Uploaded file: {file_info['remote_path']}")
    
    return share_client

# Standard file share use cases
standard_file_share_use_cases = {
    "lift_and_shift": {
        "description": "Migrate on-premises file shares to Azure",
        "example": "Company network drives accessible via SMB",
        "benefits": ["No application changes needed", "Familiar file system interface"],
        "performance": "Up to 1,000 IOPS, 60 MiB/s throughput"
    },
    "shared_configuration": {
        "description": "Shared configuration files across multiple VMs",
        "example": "Application config files shared by web server cluster",
        "benefits": ["Centralized configuration management", "Consistent across instances"],
        "performance": "Standard performance for configuration access"
    },
    "content_repositories": {
        "description": "Document and media file repositories",
        "example": "Corporate document library with departmental folders",
        "benefits": ["Hierarchical organization", "SMB protocol compatibility"],
        "performance": "Suitable for document access patterns"
    }
}
```

#### Premium File Shares

```python
def create_premium_file_share():
    """
    Create and manage premium Azure file shares for high-performance scenarios
    """
    
    # Note: Premium file shares require a FileStorage account
    premium_share_name = "high-performance-share"
    
    try:
        # Create premium file share
        premium_share_client = file_service_client.create_share(
            share=premium_share_name,
            quota=1024,  # 1 TB provisioned
            metadata={
                "performance_tier": "Premium",
                "workload_type": "Database",
                "created_for": "SQL Server cluster"
            }
        )
        
        print(f"Created premium file share: {premium_share_name}")
        
        # Premium shares support higher IOPS and throughput
        share_properties = premium_share_client.get_share_properties()
        print(f"Share quota: {share_properties.quota} GB")
        
        # Create database directory structure
        db_directories = [
            "MSSQL/Data",
            "MSSQL/Logs", 
            "MSSQL/Backups",
            "MSSQL/TempDB"
        ]
        
        for directory in db_directories:
            dir_client = premium_share_client.get_directory_client(directory)
            try:
                dir_client.create_directory()
                print(f"Created database directory: {directory}")
            except Exception as e:
                if "ResourceAlreadyExists" not in str(e):
                    print(f"Error: {e}")
        
        return premium_share_client
        
    except Exception as e:
        print(f"Premium file share creation failed: {e}")
        print("Note: Premium file shares require a FileStorage account type")

# Premium file share performance characteristics
premium_file_share_specs = {
    "performance": {
        "baseline_iops": "1 IOPS per GB provisioned (minimum 100)",
        "max_iops": "100,000 IOPS per share",
        "baseline_throughput": "0.04 MiB/s per GB provisioned",
        "max_throughput": "10 Gbps per share",
        "latency": "< 10ms for most operations"
    },
    "use_cases": {
        "database_storage": {
            "description": "SQL Server, Oracle database files",
            "example": "SQL Server Always On cluster with shared storage",
            "benefits": ["High IOPS for database operations", "Low latency", "SMB 3.0 features"]
        },
        "hpc_workloads": {
            "description": "High-performance computing file storage",
            "example": "Scientific computing shared datasets",
            "benefits": ["High throughput", "Concurrent access", "POSIX compliance via NFS"]
        },
        "content_repositories": {
            "description": "High-performance content management",
            "example": "Video editing shared storage",
            "benefits": ["High bandwidth", "Multiple concurrent editors", "Version control"]
        }
    }
}
```

### File Share Mounting Examples

#### Windows VM Mounting

```powershell
# PowerShell script to mount Azure file share on Windows VM
$resourceGroupName = "myResourceGroup"
$storageAccountName = "mystorageaccount"
$shareName = "company-shared-files"
$driveMapping = "Z:"

# Get storage account key
$storageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $resourceGroupName -Name $storageAccountName)[0].Value

# Create credential
$credential = New-Object System.Management.Automation.PSCredential -ArgumentList "Azure\$storageAccountName", (ConvertTo-SecureString -String $storageAccountKey -AsPlainText -Force)

# Mount the file share
New-PSDrive -Name $driveMapping.Substring(0,1) -PSProvider FileSystem -Root "\\$storageAccountName.file.core.windows.net\$shareName" -Credential $credential -Persist

Write-Host "Azure file share mounted successfully to $driveMapping"

# Verify mount
Get-PSDrive -Name $driveMapping.Substring(0,1)

# Create test file
"Test file created on $(Get-Date)" | Out-File -FilePath "$driveMapping\test-file.txt"
Write-Host "Test file created at $driveMapping\test-file.txt"
```

#### Linux VM Mounting

```bash
#!/bin/bash
# Bash script to mount Azure file share on Linux VM

STORAGE_ACCOUNT_NAME="mystorageaccount"
SHARE_NAME="company-shared-files"
MOUNT_POINT="/mnt/azure-files"
STORAGE_ACCOUNT_KEY="your-storage-account-key"

# Install cifs-utils if not already installed
sudo apt-get update
sudo apt-get install -y cifs-utils

# Create mount point
sudo mkdir -p $MOUNT_POINT

# Create credentials file
sudo bash -c "cat > /etc/samba/credentials << EOF
username=$STORAGE_ACCOUNT_NAME
password=$STORAGE_ACCOUNT_KEY
EOF"

# Secure credentials file
sudo chmod 600 /etc/samba/credentials

# Mount the file share
sudo mount -t cifs //$STORAGE_ACCOUNT_NAME.file.core.windows.net/$SHARE_NAME $MOUNT_POINT \
    -o credentials=/etc/samba/credentials,dir_mode=0777,file_mode=0777,serverino,nosharesock,actimeo=30

# Verify mount
df -h $MOUNT_POINT

# Add to fstab for persistent mounting
echo "//$STORAGE_ACCOUNT_NAME.file.core.windows.net/$SHARE_NAME $MOUNT_POINT cifs credentials=/etc/samba/credentials,dir_mode=0777,file_mode=0777,serverino,nosharesock,actimeo=30 0 0" | sudo tee -a /etc/fstab

echo "Azure file share mounted successfully at $MOUNT_POINT"

# Test write access
echo "Test file created on $(date)" | sudo tee $MOUNT_POINT/linux-test-file.txt
```

---

## Queue Storage Services

Azure Queue Storage provides reliable messaging between application components.

### Queue Storage Implementation

```python
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage
import json
import base64
from datetime import datetime, timedelta

# Initialize queue service client
queue_service_client = QueueServiceClient.from_connection_string(connection_string)

def implement_message_queue_system():
    """
    Implement a comprehensive message queue system for distributed applications
    """
    
    # Create different queues for different purposes
    queue_configs = [
        {
            "name": "order-processing-queue",
            "description": "E-commerce order processing messages",
            "message_ttl": 604800,  # 7 days
            "visibility_timeout": 300  # 5 minutes
        },
        {
            "name": "email-notification-queue", 
            "description": "Email notification messages",
            "message_ttl": 86400,  # 1 day
            "visibility_timeout": 60  # 1 minute
        },
        {
            "name": "image-processing-queue",
            "description": "Image processing job messages", 
            "message_ttl": 259200,  # 3 days
            "visibility_timeout": 1800  # 30 minutes
        }
    ]
    
    created_queues = {}
    
    for config in queue_configs:
        # Create queue
        queue_client = queue_service_client.create_queue(
            name=config["name"],
            metadata={
                "description": config["description"],
                "created_date": datetime.now().isoformat(),
                "purpose": "distributed_processing"
            }
        )
        
        created_queues[config["name"]] = queue_client
        print(f"Created queue: {config['name']}")
    
    return created_queues

def demonstrate_message_patterns(queues):
    """
    Demonstrate different message queue patterns
    """
    
    # 1. Order Processing Pattern
    print("=== ORDER PROCESSING PATTERN ===")
    
    order_queue = queues["order-processing-queue"]
    
    # Send order messages
    orders = [
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-123",
            "items": [
                {"product_id": "PROD-001", "quantity": 2, "price": 29.99},
                {"product_id": "PROD-002", "quantity": 1, "price": 49.99}
            ],
            "total_amount": 109.97,
            "status": "pending",
            "priority": "normal",
            "created_at": datetime.now().isoformat()
        },
        {
            "order_id": "ORD-002", 
            "customer_id": "CUST-456",
            "items": [
                {"product_id": "PROD-003", "quantity": 1, "price": 199.99}
            ],
            "total_amount": 199.99,
            "status": "pending",
            "priority": "high",
            "created_at": datetime.now().isoformat()
        }
    ]
    
    for order in orders:
        # Encode message as JSON
        message_content = json.dumps(order)
        
        # Send message with TTL and visibility timeout
        order_queue.send_message(
            content=message_content,
            visibility_timeout=300,  # 5 minutes to process
            time_to_live=604800  # 7 days TTL
        )
        
        print(f"Sent order message: {order['order_id']}")
    
    # 2. Email Notification Pattern
    print("\n=== EMAIL NOTIFICATION PATTERN ===")
    
    email_queue = queues["email-notification-queue"]
    
    # Send email notifications
    notifications = [
        {
            "notification_id": "EMAIL-001",
            "type": "order_confirmation",
            "recipient": "customer@example.com",
            "subject": "Order Confirmation - ORD-001",
            "template": "order_confirmation_template",
            "data": {
                "order_id": "ORD-001",
                "customer_name": "John Doe",
                "total_amount": 109.97
            },
            "priority": "normal",
            "retry_count": 0,
            "max_retries": 3
        },
        {
            "notification_id": "EMAIL-002",
            "type": "welcome_email",
            "recipient": "newuser@example.com", 
            "subject": "Welcome to Our Platform",
            "template": "welcome_email_template",
            "data": {
                "user_name": "Jane Smith",
                "activation_link": "https://example.com/activate/abc123"
            },
            "priority": "high",
            "retry_count": 0,
            "max_retries": 3
        }
    ]
    
    for notification in notifications:
        message_content = json.dumps(notification)
        
        # Send with appropriate TTL for email notifications
        email_queue.send_message(
            content=message_content,
            visibility_timeout=60,  # 1 minute to process
            time_to_live=86400  # 1 day TTL
        )
        
        print(f"Sent email notification: {notification['notification_id']}")
    
    # 3. Batch Processing Pattern
    print("\n=== IMAGE PROCESSING PATTERN ===")
    
    image_queue = queues["image-processing-queue"]
    
    # Send image processing jobs
    image_jobs = [
        {
            "job_id": "IMG-JOB-001",
            "source_blob_url": "https://mystorageaccount.blob.core.windows.net/images/original/photo1.jpg",
            "operations": [
                {"type": "resize", "width": 800, "height": 600},
                {"type": "watermark", "text": "© Company Name"},
                {"type": "format_convert", "target_format": "webp"}
            ],
            "output_container": "processed-images",
            "callback_url": "https://api.example.com/webhooks/image-processed",
            "priority": "normal",
            "created_at": datetime.now().isoformat()
        }
    ]
    
    for job in image_jobs:
        message_content = json.dumps(job)
        
        # Send with longer visibility timeout for processing-intensive tasks
        image_queue.send_message(
            content=message_content,
            visibility_timeout=1800,  # 30 minutes to process
            time_to_live=259200  # 3 days TTL
        )
        
        print(f"Sent image processing job: {job['job_id']}")

def implement_message_consumer():
    """
    Implement message consumer patterns
    """
    
    print("\n=== MESSAGE CONSUMER PATTERNS ===")
    
    # Get queue client
    order_queue = queue_service_client.get_queue_client("order-processing-queue")
    
    # Consumer with error handling and retry logic
    def process_order_message(message):
        try:
            # Parse message content
            order_data = json.loads(message.content)
            
            print(f"Processing order: {order_data['order_id']}")
            
            # Simulate order processing
            if order_data.get("priority") == "high":
                print(f"  High priority order - expedited processing")
            
            # Process order items
            total_items = sum(item["quantity"] for item in order_data["items"])
            print(f"  Total items: {total_items}")
            print(f"  Total amount: ${order_data['total_amount']}")
            
            # Simulate processing time
            import time
            time.sleep(2)
            
            print(f"  Order {order_data['order_id']} processed successfully")
            
            # Delete message after successful processing
            order_queue.delete_message(message)
            
            return True
            
        except json.JSONDecodeError:
            print(f"  Error: Invalid JSON in message {message.id}")
            return False
        except Exception as e:
            print(f"  Error processing message {message.id}: {str(e)}")
            return False
    
    # Receive and process messages
    print("Starting message consumer...")
    
    # Get messages (up to 32 at a time)
    messages = order_queue.receive_messages(max_messages=5, visibility_timeout=300)
    
    for message in messages:
        success = process_order_message(message)
        
        if not success:
            # Update message for retry or move to poison queue
            print(f"  Message {message.id} will be retried")

# Queue storage use cases and patterns
queue_storage_use_cases = {
    "distributed_processing": {
        "description": "Decouple application components",
        "example": "Web app queuing background tasks",
        "benefits": ["Scalability", "Reliability", "Loose coupling"],
        "pattern": "Producer-Consumer"
    },
    "load_leveling": {
        "description": "Handle traffic spikes",
        "example": "E-commerce order processing during sales",
        "benefits": ["Smooth load distribution", "Prevent system overload"],
        "pattern": "Queue-based Load Leveling"
    },
    "batch_processing": {
        "description": "Collect items for batch processing",
        "example": "Image processing, data transformation",
        "benefits": ["Efficient resource utilization", "Scheduled processing"],
        "pattern": "Batch Processing"
    },
    "event_driven": {
        "description": "Event-driven architecture",
        "example": "Microservices communication",
        "benefits": ["Loose coupling", "Event sourcing", "Async processing"],
        "pattern": "Publish-Subscribe (via multiple queues)"
    }
}

# Execute queue examples
created_queues = implement_message_queue_system()
demonstrate_message_patterns(created_queues)
implement_message_consumer()
```

---

## Table Storage Services

Azure Table Storage provides NoSQL key-value storage for structured data.

### Table Storage Implementation

```python
from azure.data.tables import TableServiceClient, TableClient, TableEntity
from datetime import datetime, timedelta
import uuid

# Initialize table service client
table_service_client = TableServiceClient.from_connection_string(connection_string)

def create_table_storage_examples():
    """
    Demonstrate Azure Table Storage for different use cases
    """
    
    # 1. Customer Data Table
    print("=== CUSTOMER DATA TABLE ===")
    
    customer_table_name = "customers"
    customer_table = table_service_client.create_table_if_not_exists(customer_table_name)
    
    # Insert customer entities
    customers = [
        {
            "PartitionKey": "US-EAST",  # Geographic partition
            "RowKey": "CUST-001",       # Unique customer ID
            "CustomerName": "Acme Corporation",
            "ContactEmail": "contact@acme.com",
            "Phone": "+1-555-0123",
            "Address": "123 Business Ave, New York, NY 10001",
            "CustomerSince": datetime(2020, 1, 15),
            "AccountType": "Enterprise",
            "CreditLimit": 50000.00,
            "IsActive": True,
            "LastOrderDate": datetime(2024, 1, 10),
            "TotalOrders": 156,
            "TotalRevenue": 234567.89
        },
        {
            "PartitionKey": "US-WEST",
            "RowKey": "CUST-002", 
            "CustomerName": "Tech Innovations Inc",
            "ContactEmail": "info@techinnovations.com",
            "Phone": "+1-555-0456",
            "Address": "456 Innovation Dr, San Francisco, CA 94105",
            "CustomerSince": datetime(2021, 3, 20),
            "AccountType": "Standard",
            "CreditLimit": 25000.00,
            "IsActive": True,
            "LastOrderDate": datetime(2024, 1, 8),
            "TotalOrders": 89,
            "TotalRevenue": 123456.78
        },
        {
            "PartitionKey": "EUROPE",
            "RowKey": "CUST-003",
            "CustomerName": "Global Solutions Ltd",
            "ContactEmail": "sales@globalsolutions.co.uk",
            "Phone": "+44-20-1234-5678",
            "Address": "789 Business Park, London, UK SW1A 1AA",
            "CustomerSince": datetime(2019, 8, 10),
            "AccountType": "Premium",
            "CreditLimit": 100000.00,
            "IsActive": True,
            "LastOrderDate": datetime(2024, 1, 12),
            "TotalOrders": 278,
            "TotalRevenue": 567890.12
        }
    ]
    
    # Insert entities
    for customer in customers:
        customer_table.create_entity(entity=customer)
        print(f"Inserted customer: {customer['CustomerName']} ({customer['RowKey']})")
    
    # 2. IoT Telemetry Data Table
    print("\n=== IOT TELEMETRY DATA TABLE ===")
    
    telemetry_table_name = "iottelemetry"
    telemetry_table = table_service_client.create_table_if_not_exists(telemetry_table_name)
    
    # Generate IoT telemetry data
    devices = ["DEVICE-001", "DEVICE-002", "DEVICE-003"]
    base_time = datetime.now() - timedelta(hours=24)
    
    telemetry_data = []
    for i in range(100):  # Generate 100 data points
        device_id = devices[i % len(devices)]
        timestamp = base_time + timedelta(minutes=i * 15)  # Every 15 minutes
        
        telemetry_entity = {
            "PartitionKey": device_id,  # Partition by device
            "RowKey": timestamp.strftime("%Y%m%d%H%M%S"),  # Timestamp as row key
            "DeviceId": device_id,
            "Timestamp": timestamp,
            "Temperature": 20.0 + (i % 20),  # Simulate temperature variation
            "Humidity": 40.0 + (i % 30),     # Simulate humidity variation
            "Pressure": 1013.25 + (i % 10),  # Simulate pressure variation
            "BatteryLevel": max(0, 100 - (i % 101)),  # Simulate battery drain
            "SignalStrength": -50 - (i % 40),  # Simulate signal variation
            "Status": "Online" if i % 10 != 0 else "Offline",
            "Location": f"Building-{(i % 5) + 1}",
            "SensorVersion": "v1.2.3"
        }
        
        telemetry_data.append(telemetry_entity)
    
    # Batch insert telemetry data
    batch_size = 10
    for i in range(0, len(telemetry_data), batch_size):
        batch = telemetry_data[i:i + batch_size]
        
        # Group by partition key for batch operations
        partition_batches = {}
        for entity in batch:
            partition_key = entity["PartitionKey"]
            if partition_key not in partition_batches:
                partition_batches[partition_key] = []
            partition_batches[partition_key].append(entity)
        
        # Execute batch operations per partition
        for partition_key, entities in partition_batches.items():
            for entity in entities:
                telemetry_table.create_entity(entity=entity)
    
    print(f"Inserted {len(telemetry_data)} telemetry records")
    
    # 3. Session State Table
    print("\n=== SESSION STATE TABLE ===")
    
    session_table_name = "usersessions"
    session_table = table_service_client.create_table_if_not_exists(session_table_name)
    
    # Create user session entities
    sessions = []
    for i in range(20):
        session_id = str(uuid.uuid4())
        user_id = f"USER-{(i % 5) + 1:03d}"
        
        session_entity = {
            "PartitionKey": user_id,  # Partition by user
            "RowKey": session_id,     # Unique session ID
            "UserId": user_id,
            "SessionId": session_id,
            "CreatedAt": datetime.now() - timedelta(hours=i),
            "LastAccessedAt": datetime.now() - timedelta(minutes=i * 30),
            "IPAddress": f"192.168.1.{(i % 254) + 1}",
            "UserAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "IsActive": i % 3 != 0,  # Some inactive sessions
            "LoginMethod": "password" if i % 2 == 0 else "oauth",
            "DeviceType": "desktop" if i % 3 == 0 else "mobile",
            "SessionData": f'{{"preferences": {{"theme": "dark", "language": "en"}}, "cart_items": {i % 5}}}',
            "ExpiresAt": datetime.now() + timedelta(hours=24 - i)
        }
        
        sessions.append(session_entity)
    
    # Insert session entities
    for session in sessions:
        session_table.create_entity(entity=session)
    
    print(f"Inserted {len(sessions)} session records")
    
    return {
        "customers": customer_table,
        "telemetry": telemetry_table,
        "sessions": session_table
    }

def demonstrate_table_queries(tables):
    """
    Demonstrate different query patterns for Table Storage
    """
    
    print("\n=== TABLE STORAGE QUERY PATTERNS ===")
    
    # 1. Point Query (most efficient)
    print("1. Point Query - Get specific customer:")
    customer_table = tables["customers"]
    
    specific_customer = customer_table.get_entity(
        partition_key="US-EAST",
        row_key="CUST-001"
    )
    print(f"   Customer: {specific_customer['CustomerName']}")
    print(f"   Revenue: ${specific_customer['TotalRevenue']:,.2f}")
    
    # 2. Partition Query - Get all customers in a region
    print("\n2. Partition Query - All US-EAST customers:")
    east_customers = customer_table.query_entities(
        query_filter="PartitionKey eq 'US-EAST'"
    )
    
    for customer in east_customers:
        print(f"   {customer['CustomerName']} - {customer['AccountType']}")
    
    # 3. Range Query - Get recent telemetry data
    print("\n3. Range Query - Recent telemetry from DEVICE-001:")
    telemetry_table = tables["telemetry"]
    
    # Query last 24 hours of data for specific device
    recent_time = (datetime.now() - timedelta(hours=1)).strftime("%Y%m%d%H%M%S")
    
    recent_telemetry = telemetry_table.query_entities(
        query_filter=f"PartitionKey eq 'DEVICE-001' and RowKey ge '{recent_time}'"
    )
    
    for record in recent_telemetry:
        print(f"   {record['Timestamp']}: Temp={record['Temperature']}°C, "
              f"Humidity={record['Humidity']}%, Status={record['Status']}")
    
    # 4. Filter Query - Active sessions
    print("\n4. Filter Query - Active user sessions:")
    session_table = tables["sessions"]
    
    active_sessions = session_table.query_entities(
        query_filter="IsActive eq true"
    )
    
    session_count = 0
    for session in active_sessions:
        session_count += 1
        print(f"   User: {session['UserId']}, Device: {session['DeviceType']}, "
              f"Last Access: {session['LastAccessedAt']}")
    
    print(f"   Total active sessions: {session_count}")
    
    # 5. Complex Query - High-value customers
    print("\n5. Complex Query - High-value enterprise customers:")
    high_value_customers = customer_table.query_entities(
        query_filter="AccountType eq 'Enterprise' and TotalRevenue ge 200000"
    )
    
    for customer in high_value_customers:
        print(f"   {customer['CustomerName']}: ${customer['TotalRevenue']:,.2f} "
              f"({customer['TotalOrders']} orders)")

def implement_table_maintenance(tables):
    """
    Demonstrate table maintenance operations
    """
    
    print("\n=== TABLE MAINTENANCE OPERATIONS ===")
    
    # 1. Update entity
    print("1. Updating customer information:")
    customer_table = tables["customers"]
    
    # Get and update customer
    customer = customer_table.get_entity(
        partition_key="US-WEST",
        row_key="CUST-002"
    )
    
    # Update fields
    customer["Phone"] = "+1-555-0999"  # Updated phone
    customer["LastOrderDate"] = datetime.now()
    customer["TotalOrders"] += 1
    customer["TotalRevenue"] += 1500.00
    
    # Update entity
    customer_table.update_entity(entity=customer, mode="replace")
    print(f"   Updated customer {customer['CustomerName']}")
    
    # 2. Cleanup old sessions
    print("\n2. Cleaning up expired sessions:")
    session_table = tables["sessions"]
    
    # Find expired sessions
    current_time = datetime.now()
    all_sessions = session_table.list_entities()
    
    expired_count = 0
    for session in all_sessions:
        if session.get("ExpiresAt") and session["ExpiresAt"] < current_time:
            # Delete expired session
            session_table.delete_entity(
                partition_key=session["PartitionKey"],
                row_key=session["RowKey"]
            )
            expired_count += 1
    
    print(f"   Deleted {expired_count} expired sessions")
    
    # 3. Archive old telemetry data
    print("\n3. Archiving old telemetry data:")
    telemetry_table = tables["telemetry"]
    
    # Archive data older than 7 days
    archive_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d%H%M%S")
    
    old_telemetry = telemetry_table.query_entities(
        query_filter=f"RowKey lt '{archive_cutoff}'"
    )
    
    archived_count = 0
    for record in old_telemetry:
        # In practice, you would copy to archive storage before deleting
        # For demo, we'll just count
        archived_count += 1
    
    print(f"   Found {archived_count} records for archiving")

# Table storage use cases and characteristics
table_storage_characteristics = {
    "scalability": {
        "max_entities_per_table": "Unlimited",
        "max_entity_size": "1 MB",
        "max_property_size": "64 KB (string), 1 MB (binary)",
        "max_properties_per_entity": "255",
        "partition_key_importance": "Critical for performance and scalability"
    },
    "performance": {
        "point_queries": "Fastest (PartitionKey + RowKey)",
        "partition_queries": "Fast (PartitionKey only)",
        "range_queries": "Good (PartitionKey + RowKey range)",
        "filter_queries": "Slower (cross-partition scanning)",
        "throughput_targets": "Up to 20,000 entities/sec per partition"
    },
    "use_cases": {
        "user_data": {
            "description": "User profiles, preferences, session state",
            "partition_strategy": "By user ID or geographic region",
            "benefits": ["Fast user lookups", "Scalable user base", "Cost-effective"]
        },
        "iot_telemetry": {
            "description": "Device telemetry and sensor data",
            "partition_strategy": "By device ID or time period",
            "benefits": ["High ingestion rate", "Time-series queries", "Cost-effective storage"]
        },
        "catalog_data": {
            "description": "Product catalogs, configuration data",
            "partition_strategy": "By category or region",
            "benefits": ["Fast lookups", "Flexible schema", "Global distribution"]
        },
        "logging_metadata": {
            "description": "Log file metadata, audit trails",
            "partition_strategy": "By date or application",
            "benefits": ["Fast metadata queries", "Scalable logging", "Cost-effective"]
        }
    }
}

# Execute table storage examples
created_tables = create_table_storage_examples()
demonstrate_table_queries(created_tables)
implement_table_maintenance(created_tables)
```

---

## Disk Storage Services

Azure provides various disk storage options for virtual machines with different performance characteristics.

### Managed Disk Types

#### 1. Ultra Disk SSD

```json
{
  "diskType": "UltraSSD_LRS",
  "diskSizeGB": 1024,
  "location": "East US 2",
  "properties": {
    "creationData": {
      "createOption": "Empty"
    },
    "diskIOPSReadWrite": 80000,
    "diskMBpsReadWrite": 1200,
    "diskSizeBytes": 1099511627776
  },
  "performance": {
    "maxIOPS": "160,000 IOPS (64 TiB disk)",
    "maxThroughput": "2,000 MB/s (64 TiB disk)",
    "latency": "< 2ms",
    "adjustablePerformance": "Yes, without detaching disk"
  },
  "useCases": [
    "SAP HANA",
    "Top-tier databases (SQL Server, Oracle)",
    "Transaction-heavy workloads",
    "IO-intensive applications"
  ],
  "limitations": [
    "Available in select regions",
    "Requires specific VM types",
    "Higher cost per GB"
  ]
}
```

**Ultra Disk Implementation Example:**

```bash
# Create Ultra Disk using Azure CLI
az disk create \
  --resource-group myResourceGroup \
  --name myUltraDisk \
  --size-gb 1024 \
  --location eastus2 \
  --sku UltraSSD_LRS \
  --disk-iops-read-write 80000 \
  --disk-mbps-read-write 1200 \
  --tags Environment=Production Workload=Database

# Attach to VM (VM must support Ultra Disks)
az vm disk attach \
  --resource-group myResourceGroup \
  --vm-name myDatabaseVM \
  --name myUltraDisk
```

#### 2. Premium SSD

```json
{
  "diskType": "Premium_LRS",
  "diskSizeGB": 512,
  "location": "West US 2",
  "properties": {
    "creationData": {
      "createOption": "Empty"
    },
    "tier": "P20"
  },
  "performance": {
    "provisionedIOPS": 2300,
    "provisionedThroughputMBps": 150,
    "latency": "Single-digit milliseconds",
    "burstingSupport": "Yes (P1-P20)"
  },
  "tiers": {
    "P4": {"size": "32 GB", "iops": 120, "throughput": "25 MB/s"},
    "P6": {"size": "64 GB", "iops": 240, "throughput": "50 MB/s"},
    "P10": {"size": "128 GB", "iops": 500, "throughput": "100 MB/s"},
    "P15": {"size": "256 GB", "iops": 1100, "throughput": "125 MB/s"},
    "P20": {"size": "512 GB", "iops": 2300, "throughput": "150 MB/s"},
    "P30": {"size": "1 TB", "iops": 5000, "throughput": "200 MB/s"},
    "P40": {"size": "2 TB", "iops": 7500, "throughput": "250 MB/s"},
    "P50": {"size": "4 TB", "iops": 7500, "throughput": "250 MB/s"},
    "P60": {"size": "8 TB", "iops": 16000, "throughput": "500 MB/s"},
    "P70": {"size": "16 TB", "iops": 18000, "throughput": "750 MB/s"},
    "P80": {"size": "32 TB", "iops": 20000, "throughput": "900 MB/s"}
  },
  "useCases": [
    "Production workloads",
    "Business-critical applications", 
    "Database servers",
    "High-performance web servers"
  ]
}
```

#### 3. Standard SSD

```json
{
  "diskType": "StandardSSD_LRS",
  "diskSizeGB": 256,
  "location": "Central US",
  "properties": {
    "creationData": {
      "createOption": "Empty"
    },
    "tier": "E15"
  },
  "performance": {
    "maxIOPS": 500,
    "maxThroughputMBps": 60,
    "latency": "Single-digit milliseconds",
    "consistentPerformance": "Yes"
  },
  "tiers": {
    "E4": {"size": "32 GB", "iops": 120, "throughput": "25 MB/s"},
    "E6": {"size": "64 GB", "iops": 240, "throughput": "50 MB/s"},
    "E10": {"size": "128 GB", "iops": 500, "throughput": "60 MB/s"},
    "E15": {"size": "256 GB", "iops": 500, "throughput": "60 MB/s"},
    "E20": {"size": "512 GB", "iops": 500, "throughput": "60 MB/s"},
    "E30": {"size": "1 TB", "iops": 500, "throughput": "60 MB/s"},
    "E40": {"size": "2 TB", "iops": 500, "throughput": "60 MB/s"},
    "E50": {"size": "4 TB", "iops": 500, "throughput": "60 MB/s"},
    "E60": {"size": "8 TB", "iops": 1300, "throughput": "300 MB/s"},
    "E70": {"size": "16 TB", "iops": 2000, "throughput": "500 MB/s"},
    "E80": {"size": "32 TB", "iops": 2000, "throughput": "500 MB/s"}
  },
  "useCases": [
    "Development and test environments",
    "Backup storage",
    "Lightly used production workloads",
    "Cost-sensitive applications requiring SSD performance"
  ]
}
```

#### 4. Standard HDD

```json
{
  "diskType": "Standard_LRS",
  "diskSizeGB": 1024,
  "location": "South Central US",
  "properties": {
    "creationData": {
      "createOption": "Empty"
    },
    "tier": "S30"
  },
  "performance": {
    "maxIOPS": 500,
    "maxThroughputMBps": 60,
    "latency": "Variable",
    "costOptimized": "Yes"
  },
  "tiers": {
    "S4": {"size": "32 GB", "iops": 500, "throughput": "60 MB/s"},
    "S6": {"size": "64 GB", "iops": 500, "throughput": "60 MB/s"},
    "S10": {"size": "128 GB", "iops": 500, "throughput": "60 MB/s"},
    "S15": {"size": "256 GB", "iops": 500, "throughput": "60 MB/s"},
    "S20": {"size": "512 GB", "iops": 500, "throughput": "60 MB/s"},
    "S30": {"size": "1 TB", "iops": 500, "throughput": "60 MB/s"},
    "S40": {"size": "2 TB", "iops": 500, "throughput": "60 MB/s"},
    "S50": {"size": "4 TB", "iops": 500, "throughput": "60 MB/s"},
    "S60": {"size": "8 TB", "iops": 1300, "throughput": "300 MB/s"},
    "S70": {"size": "16 TB", "iops": 2000, "throughput": "500 MB/s"},
    "S80": {"size": "32 TB", "iops": 2000, "throughput": "500 MB/s"}
  },
  "useCases": [
    "Backup and archival storage",
    "Infrequently accessed data",
    "Development environments",
    "Cost-sensitive workloads"
  ]
}
```

### Disk Configuration Examples

```bash
#!/bin/bash
# Comprehensive disk management script

RESOURCE_GROUP="myResourceGroup"
VM_NAME="myProductionVM"
LOCATION="eastus2"

echo "=== AZURE DISK MANAGEMENT DEMO ==="

# 1. Create different types of managed disks
echo "1. Creating different disk types..."

# Ultra Disk for database
az disk create \
  --resource-group $RESOURCE_GROUP \
  --name "${VM_NAME}-ultra-db-disk" \
  --size-gb 1024 \
  --location $LOCATION \
  --sku UltraSSD_LRS \
  --disk-iops-read-write 50000 \
  --disk-mbps-read-write 1000 \
  --tags Purpose=Database Performance=Ultra

echo "Created Ultra Disk for database workload"

# Premium SSD for OS
az disk create \
  --resource-group $RESOURCE_GROUP \
  --name "${VM_NAME}-premium-os-disk" \
  --size-gb 128 \
  --location $LOCATION \
  --sku Premium_LRS \
  --tags Purpose=OS Performance=Premium

echo "Created Premium SSD for OS"

# Standard SSD for application data
az disk create \
  --resource-group $RESOURCE_GROUP \
  --name "${VM_NAME}-standard-ssd-data" \
  --size-gb 512 \
  --location $LOCATION \
  --sku StandardSSD_LRS \
  --tags Purpose=ApplicationData Performance=Standard

echo "Created Standard SSD for application data"

# Standard HDD for backup
az disk create \
  --resource-group $RESOURCE_GROUP \
  --name "${VM_NAME}-hdd-backup" \
  --size-gb 2048 \
  --location $LOCATION \
  --sku Standard_LRS \
  --tags Purpose=Backup Performance=Economy

echo "Created Standard HDD for backup"

# 2. List created disks
echo -e "\n2. Listing created disks:"
az disk list \
  --resource-group $RESOURCE_GROUP \
  --query "[].{Name:name, Size:diskSizeGb, Sku:sku.name, State:diskState}" \
  --output table

# 3. Attach disks to VM (assuming VM exists)
echo -e "\n3. Attaching disks to VM..."

# Note: Ultra Disks require special VM configuration
# Attach Premium SSD as data disk
az vm disk attach \
  --resource-group $RESOURCE_GROUP \
  --vm-name $VM_NAME \
  --name "${VM_NAME}-standard-ssd-data" \
  --lun 1

echo "Attached Standard SSD data disk"

# 4. Create disk snapshot for backup
echo -e "\n4. Creating disk snapshot..."
az snapshot create \
  --resource-group $RESOURCE_GROUP \
  --name "${VM_NAME}-premium-os-snapshot-$(date +%Y%m%d)" \
  --source "${VM_NAME}-premium-os-disk" \
  --tags Purpose=Backup CreatedBy=AutomatedScript

echo "Created disk snapshot"

# 5. Show disk performance tiers
echo -e "\n5. Disk performance comparison:"
cat << EOF
Disk Type        | IOPS      | Throughput | Latency | Use Case
-----------------|-----------|------------|---------|------------------
Ultra SSD        | 160,000   | 2,000 MB/s | <2ms    | Mission-critical
Premium SSD      | 20,000    | 900 MB/s   | <10ms   | Production workloads
Standard SSD     | 2,000     | 500 MB/s   | <10ms   | Dev/Test, light prod
Standard HDD     | 2,000     | 500 MB/s   | Variable| Backup, archival
EOF
```

---

## Use Case Decision Matrix

### Storage Service Selection Guide

```python
def storage_service_selector():
    """
    Interactive storage service selection based on requirements
    """
    
    storage_decision_matrix = {
        "unstructured_data": {
            "hot_frequent_access": {
                "service": "Blob Storage - Hot Tier",
                "cost": "$$",
                "performance": "High",
                "use_cases": ["Web content", "Active datasets", "Frequently accessed files"]
            },
            "cool_infrequent_access": {
                "service": "Blob Storage - Cool Tier", 
                "cost": "$",
                "performance": "Medium",
                "use_cases": ["Backup data", "Monthly reports", "Disaster recovery"]
            },
            "archive_long_term": {
                "service": "Blob Storage - Archive Tier",
                "cost": "¢",
                "performance": "Low (15hr retrieval)",
                "use_cases": ["Long-term backup", "Compliance data", "Historical records"]
            }
        },
        "structured_data": {
            "relational_oltp": {
                "service": "Azure SQL Database",
                "cost": "$$$",
                "performance": "Very High",
                "use_cases": ["Business applications", "E-commerce", "CRM systems"]
            },
            "nosql_keyvalue": {
                "service": "Table Storage",
                "cost": "$",
                "performance": "High",
                "use_cases": ["User profiles", "IoT telemetry", "Session state"]
            },
            "nosql_document": {
                "service": "Cosmos DB",
                "cost": "$$$$",
                "performance": "Very High",
                "use_cases": ["Global applications", "Real-time analytics", "Gaming"]
            }
        },
        "file_shares": {
            "standard_smb": {
                "service": "Azure Files - Standard",
                "cost": "$$",
                "performance": "Medium",
                "use_cases": ["Lift-and-shift", "Shared configuration", "Content repositories"]
            },
            "premium_smb": {
                "service": "Azure Files - Premium",
                "cost": "$$$",
                "performance": "Very High", 
                "use_cases": ["Database files", "HPC workloads", "High-performance apps"]
            },
            "nfs_linux": {
                "service": "Azure Files - NFS",
                "cost": "$$$",
                "performance": "High",
                "use_cases": ["Linux workloads", "Container storage", "HPC clusters"]
            }
        },
        "messaging": {
            "simple_queuing": {
                "service": "Queue Storage",
                "cost": "$",
                "performance": "High",
                "use_cases": ["Decoupling components", "Background processing", "Load leveling"]
            },
            "advanced_messaging": {
                "service": "Service Bus",
                "cost": "$$",
                "performance": "Very High",
                "use_cases": ["Enterprise messaging", "Pub/sub patterns", "Complex routing"]
            },
            "event_streaming": {
                "service": "Event Hubs",
                "cost": "$$$",
                "performance": "Very High",
                "use_cases": ["IoT telemetry", "Log aggregation", "Real-time analytics"]
            }
        },
        "vm_storage": {
            "ultra_performance": {
                "service": "Ultra Disk SSD",
                "cost": "$$$$",
                "performance": "Extreme (160K IOPS)",
                "use_cases": ["SAP HANA", "Top-tier databases", "Mission-critical apps"]
            },
            "high_performance": {
                "service": "Premium SSD",
                "cost": "$$$",
                "performance": "High (20K IOPS)",
                "use_cases": ["Production VMs", "Database servers", "Business apps"]
            },
            "balanced": {
                "service": "Standard SSD",
                "cost": "$$",
                "performance": "Medium (2K IOPS)",
                "use_cases": ["Dev/test VMs", "Light production", "Web servers"]
            },
            "cost_optimized": {
                "service": "Standard HDD",
                "cost": "$",
                "performance": "Basic (500 IOPS)",
                "use_cases": ["Backup storage", "Infrequent access", "Archive"]
            }
        },
        "analytics": {
            "big_data": {
                "service": "Data Lake Storage Gen2",
                "cost": "$$",
                "performance": "Very High",
                "use_cases": ["Big data analytics", "Data science", "ETL pipelines"]
            },
            "data_warehouse": {
                "service": "Synapse Analytics",
                "cost": "$$$",
                "performance": "Very High", 
                "use_cases": ["Enterprise DW", "Complex analytics", "BI reporting"]
            }
        }
    }
    
    return storage_decision_matrix

# Cost comparison matrix
cost_comparison = {
    "storage_costs_per_gb_month": {
        "blob_hot": "$0.0184",
        "blob_cool": "$0.0115", 
        "blob_archive": "$0.00099",
        "files_standard": "$0.06",
        "files_premium": "$0.15",
        "table_storage": "$0.045",
        "queue_storage": "$0.045",
        "premium_ssd": "$0.135",
        "standard_ssd": "$0.075",
        "standard_hdd": "$0.045"
    },
    "transaction_costs": {
        "blob_hot": "$0.0004 per 10K ops",
        "blob_cool": "$0.01 per 10K ops",
        "blob_archive": "$5.00 per 10K ops",
        "table_storage": "$0.0004 per 10K ops",
        "queue_storage": "$0.0004 per 10K ops"
    }
}

# Performance comparison matrix  
performance_comparison = {
    "iops_capabilities": {
        "ultra_disk": "Up to 160,000 IOPS",
        "premium_ssd": "Up to 20,000 IOPS",
        "standard_ssd": "Up to 2,000 IOPS", 
        "standard_hdd": "Up to 500 IOPS",
        "blob_storage": "Up to 20,000 requests/sec",
        "table_storage": "Up to 20,000 entities/sec per partition"
    },
    "throughput_capabilities": {
        "ultra_disk": "Up to 2,000 MB/s",
        "premium_ssd": "Up to 900 MB/s",
        "standard_ssd": "Up to 500 MB/s",
        "standard_hdd": "Up to 500 MB/s",
        "blob_storage": "Up to 60 Gbps egress",
        "files_premium": "Up to 10 Gbps"
    }
}

# Execute decision matrix
decision_matrix = storage_service_selector()
print("Azure Storage Decision Matrix created successfully")
```

### Conclusion

This comprehensive guide covers all major Azure storage services with their characteristics, use cases, and implementation examples. The key to successful Azure storage implementation is understanding your specific requirements for:

1. **Performance**: IOPS, throughput, and latency requirements
2. **Scalability**: Current and future capacity needs
3. **Access Patterns**: Frequency and type of data access
4. **Cost Sensitivity**: Budget constraints and cost optimization needs
5. **Compliance**: Data residency, retention, and regulatory requirements
6. **Integration**: Compatibility with existing applications and workflows

Choose the appropriate storage service based on these factors, and consider hybrid approaches that leverage multiple storage types for optimal cost and performance balance.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*