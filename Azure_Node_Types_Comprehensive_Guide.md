# Azure Node Types Comprehensive Guide
## Complete Guide to All Types of Nodes in Azure Services

---

### Table of Contents

1. [Overview](#overview)
2. [Azure Virtual Machine Nodes](#azure-virtual-machine-nodes)
3. [Azure Kubernetes Service (AKS) Nodes](#azure-kubernetes-service-aks-nodes)
4. [Azure HDInsight Cluster Nodes](#azure-hdinsight-cluster-nodes)
5. [Azure Databricks Cluster Nodes](#azure-databricks-cluster-nodes)
6. [Azure Service Fabric Nodes](#azure-service-fabric-nodes)
7. [Azure Batch Nodes](#azure-batch-nodes)
8. [Azure Container Instances](#azure-container-instances)
9. [Azure Virtual Machine Scale Sets](#azure-virtual-machine-scale-sets)
10. [Node Management and Orchestration](#node-management-and-orchestration)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)

---

## Overview

Azure nodes are the fundamental compute units that provide processing power, memory, and storage resources across various Azure services. Understanding different node types is crucial for designing scalable, cost-effective, and high-performance cloud solutions.

### Node Classification Framework

```json
{
  "azure_node_classification": {
    "compute_nodes": {
      "description": "General-purpose computing resources",
      "types": ["Virtual Machines", "VM Scale Sets", "Container Instances"],
      "use_cases": ["Web applications", "Databases", "Enterprise workloads"],
      "characteristics": ["Persistent", "Customizable", "Full OS control"]
    },
    "container_orchestration_nodes": {
      "description": "Nodes for containerized workloads",
      "types": ["AKS nodes", "Service Fabric nodes", "Container Groups"],
      "use_cases": ["Microservices", "Container orchestration", "DevOps workflows"],
      "characteristics": ["Kubernetes-managed", "Auto-scaling", "Container-optimized"]
    },
    "big_data_nodes": {
      "description": "Nodes optimized for data processing",
      "types": ["HDInsight nodes", "Databricks nodes", "Analytics clusters"],
      "use_cases": ["Big data analytics", "Machine learning", "ETL processes"],
      "characteristics": ["Data-optimized", "Distributed computing", "Framework-specific"]
    },
    "batch_processing_nodes": {
      "description": "Nodes for batch and HPC workloads",
      "types": ["Azure Batch nodes", "HPC clusters", "Spot instances"],
      "use_cases": ["Scientific computing", "Rendering", "Parallel processing"],
      "characteristics": ["Task-oriented", "Cost-optimized", "Temporary"]
    },
    "specialized_nodes": {
      "description": "Nodes with specific hardware or software",
      "types": ["GPU nodes", "FPGA nodes", "Confidential computing"],
      "use_cases": ["AI/ML training", "Graphics rendering", "Secure computing"],
      "characteristics": ["Hardware-accelerated", "High-performance", "Specialized"]
    }
  }
}
```

### Node Architecture Patterns

```json
{
  "node_architecture_patterns": {
    "single_node_architecture": {
      "description": "Single VM or container for simple workloads",
      "components": ["Single compute node", "Local storage", "Network interface"],
      "benefits": ["Simple management", "Low cost", "Quick deployment"],
      "limitations": ["No high availability", "Limited scalability", "Single point of failure"],
      "use_cases": ["Development environments", "Small applications", "Testing"]
    },
    "multi_node_cluster": {
      "description": "Multiple nodes working together as a cluster",
      "components": ["Master nodes", "Worker nodes", "Load balancer", "Shared storage"],
      "benefits": ["High availability", "Horizontal scaling", "Load distribution"],
      "complexity": ["Cluster management", "Network configuration", "Data consistency"],
      "use_cases": ["Production applications", "Distributed databases", "Big data processing"]
    },
    "auto_scaling_node_pools": {
      "description": "Dynamic node provisioning based on demand",
      "components": ["Auto-scaler", "Node pools", "Metrics monitoring", "Scaling policies"],
      "benefits": ["Cost optimization", "Performance adaptation", "Automated management"],
      "considerations": ["Scaling latency", "Resource limits", "Cost monitoring"],
      "use_cases": ["Variable workloads", "Cost-sensitive applications", "Event-driven processing"]
    },
    "hybrid_node_deployment": {
      "description": "Combination of different node types",
      "components": ["Persistent nodes", "Spot instances", "Reserved capacity", "On-demand nodes"],
      "benefits": ["Cost optimization", "Performance balance", "Risk mitigation"],
      "complexity": ["Mixed management", "Workload distribution", "Failure handling"],
      "use_cases": ["Enterprise applications", "Cost-optimized workloads", "Mixed SLA requirements"]
    }
  }
}
```

---

## Azure Virtual Machine Nodes

### VM Node Types and Sizes

Azure Virtual Machines provide the foundation for compute nodes with various families optimized for different workloads.

```json
{
  "azure_vm_families": {
    "general_purpose": {
      "series": ["B", "D", "E"],
      "characteristics": "Balanced CPU-to-memory ratio",
      "use_cases": ["Web servers", "Small to medium databases", "Development environments"],
      "examples": {
        "B_series": {
          "name": "Burstable performance",
          "sizes": ["B1s", "B1ms", "B2s", "B4ms", "B8ms"],
          "cpu_credits": "Accumulates when under baseline",
          "ideal_for": "Variable workloads"
        },
        "D_series": {
          "name": "General purpose",
          "sizes": ["D2s_v3", "D4s_v3", "D8s_v3", "D16s_v3"],
          "storage": "Premium SSD supported",
          "ideal_for": "Production workloads"
        }
      }
    },
    "compute_optimized": {
      "series": ["F"],
      "characteristics": "High CPU-to-memory ratio",
      "use_cases": ["Web servers", "Network appliances", "Batch processing"],
      "examples": {
        "F_series": {
          "name": "Compute optimized",
          "sizes": ["F2s_v2", "F4s_v2", "F8s_v2", "F16s_v2"],
          "cpu_performance": "High-performance Intel processors",
          "ideal_for": "CPU-intensive applications"
        }
      }
    },
    "memory_optimized": {
      "series": ["E", "M", "X"],
      "characteristics": "High memory-to-CPU ratio",
      "use_cases": ["In-memory databases", "Analytics", "Large caches"],
      "examples": {
        "E_series": {
          "name": "Memory optimized",
          "sizes": ["E2s_v3", "E4s_v3", "E8s_v3", "E16s_v3"],
          "memory": "Up to 432 GB RAM",
          "ideal_for": "Memory-intensive applications"
        },
        "M_series": {
          "name": "Large memory",
          "sizes": ["M8ms", "M16ms", "M32ms", "M64ms"],
          "memory": "Up to 3.8 TB RAM",
          "ideal_for": "Large in-memory databases"
        }
      }
    },
    "storage_optimized": {
      "series": ["L"],
      "characteristics": "High disk throughput and IO",
      "use_cases": ["Big data", "Distributed databases", "Large transactional databases"],
      "examples": {
        "L_series": {
          "name": "Storage optimized",
          "sizes": ["L4s", "L8s", "L16s", "L32s"],
          "storage": "NVMe SSD local storage",
          "ideal_for": "High IOPS applications"
        }
      }
    },
    "gpu_accelerated": {
      "series": ["N"],
      "characteristics": "GPU acceleration for compute or graphics",
      "use_cases": ["AI/ML", "HPC", "Graphics rendering", "Video processing"],
      "examples": {
        "NC_series": {
          "name": "GPU compute",
          "sizes": ["NC6s_v3", "NC12s_v3", "NC24s_v3"],
          "gpu": "NVIDIA Tesla V100",
          "ideal_for": "AI/ML training and inference"
        },
        "NV_series": {
          "name": "GPU visualization",
          "sizes": ["NV6", "NV12", "NV24"],
          "gpu": "NVIDIA Tesla M60",
          "ideal_for": "Graphics workstations, CAD"
        }
      }
    }
  }
}
```

### PowerShell Script for VM Node Management

```powershell
# Azure VM Node Management Script
# Comprehensive VM node creation and management

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$Location,
    
    [Parameter(Mandatory=$false)]
    [string]$VMNamePrefix = "AzureNode",
    
    [Parameter(Mandatory=$false)]
    [int]$NodeCount = 3,
    
    [Parameter(Mandatory=$false)]
    [string]$VMSize = "Standard_D2s_v3",
    
    [Parameter(Mandatory=$false)]
    [string]$AdminUsername = "azureuser"
)

# Function to create a resource group
function New-AzureResourceGroup {
    param($Name, $Location)
    
    Write-Host "=== Creating Resource Group: $Name ===" -ForegroundColor Green
    
    $rg = Get-AzResourceGroup -Name $Name -ErrorAction SilentlyContinue
    if (-not $rg) {
        New-AzResourceGroup -Name $Name -Location $Location
        Write-Host "✓ Resource Group created: $Name" -ForegroundColor Green
    } else {
        Write-Host "✓ Resource Group already exists: $Name" -ForegroundColor Yellow
    }
}

# Function to create a virtual network
function New-AzureVirtualNetwork {
    param($ResourceGroupName, $Location, $VNetName)
    
    Write-Host "=== Creating Virtual Network: $VNetName ===" -ForegroundColor Green
    
    # Create subnet configuration
    $subnetConfig = New-AzVirtualNetworkSubnetConfig `
        -Name "default" `
        -AddressPrefix "10.0.1.0/24"
    
    # Create virtual network
    $vnet = New-AzVirtualNetwork `
        -ResourceGroupName $ResourceGroupName `
        -Location $Location `
        -Name $VNetName `
        -AddressPrefix "10.0.0.0/16" `
        -Subnet $subnetConfig
    
    Write-Host "✓ Virtual Network created: $VNetName" -ForegroundColor Green
    return $vnet
}

# Function to create a network security group
function New-AzureNetworkSecurityGroup {
    param($ResourceGroupName, $Location, $NSGName)
    
    Write-Host "=== Creating Network Security Group: $NSGName ===" -ForegroundColor Green
    
    # Create security rules
    $sshRule = New-AzNetworkSecurityRuleConfig `
        -Name "SSH" `
        -Description "Allow SSH" `
        -Access "Allow" `
        -Protocol "Tcp" `
        -Direction "Inbound" `
        -Priority 1000 `
        -SourceAddressPrefix "*" `
        -SourcePortRange "*" `
        -DestinationAddressPrefix "*" `
        -DestinationPortRange 22
    
    $httpRule = New-AzNetworkSecurityRuleConfig `
        -Name "HTTP" `
        -Description "Allow HTTP" `
        -Access "Allow" `
        -Protocol "Tcp" `
        -Direction "Inbound" `
        -Priority 1001 `
        -SourceAddressPrefix "*" `
        -SourcePortRange "*" `
        -DestinationAddressPrefix "*" `
        -DestinationPortRange 80
    
    $httpsRule = New-AzNetworkSecurityRuleConfig `
        -Name "HTTPS" `
        -Description "Allow HTTPS" `
        -Access "Allow" `
        -Protocol "Tcp" `
        -Direction "Inbound" `
        -Priority 1002 `
        -SourceAddressPrefix "*" `
        -SourcePortRange "*" `
        -DestinationAddressPrefix "*" `
        -DestinationPortRange 443
    
    # Create network security group
    $nsg = New-AzNetworkSecurityGroup `
        -ResourceGroupName $ResourceGroupName `
        -Location $Location `
        -Name $NSGName `
        -SecurityRules $sshRule, $httpRule, $httpsRule
    
    Write-Host "✓ Network Security Group created: $NSGName" -ForegroundColor Green
    return $nsg
}

# Function to create a single VM node
function New-AzureVMNode {
    param(
        $ResourceGroupName,
        $Location,
        $VMName,
        $VMSize,
        $AdminUsername,
        $VNet,
        $NSG,
        $NodeIndex
    )
    
    Write-Host "=== Creating VM Node: $VMName ===" -ForegroundColor Green
    
    # Create public IP
    $publicIpName = "$VMName-ip"
    $publicIp = New-AzPublicIpAddress `
        -ResourceGroupName $ResourceGroupName `
        -Location $Location `
        -Name $publicIpName `
        -AllocationMethod "Dynamic" `
        -DomainNameLabel ($VMName.ToLower() + "-" + (Get-Random -Maximum 10000))
    
    # Create network interface
    $nicName = "$VMName-nic"
    $nic = New-AzNetworkInterface `
        -ResourceGroupName $ResourceGroupName `
        -Location $Location `
        -Name $nicName `
        -SubnetId $VNet.Subnets[0].Id `
        -PublicIpAddressId $publicIp.Id `
        -NetworkSecurityGroupId $NSG.Id
    
    # Create VM configuration
    $vmConfig = New-AzVMConfig `
        -VMName $VMName `
        -VMSize $VMSize `
        -Tags @{
            "Environment" = "Production"
            "NodeType" = "ComputeNode"
            "NodeIndex" = $NodeIndex
            "CreatedBy" = "PowerShell"
            "CreatedDate" = (Get-Date).ToString("yyyy-MM-dd")
        }
    
    # Set operating system
    $vmConfig = Set-AzVMOperatingSystem `
        -VM $vmConfig `
        -Linux `
        -ComputerName $VMName `
        -Credential (Get-Credential -UserName $AdminUsername -Message "Enter password for $AdminUsername")
    
    # Set source image
    $vmConfig = Set-AzVMSourceImage `
        -VM $vmConfig `
        -PublisherName "Canonical" `
        -Offer "0001-com-ubuntu-server-focal" `
        -Skus "20_04-lts-gen2" `
        -Version "latest"
    
    # Add network interface
    $vmConfig = Add-AzVMNetworkInterface `
        -VM $vmConfig `
        -Id $nic.Id
    
    # Set OS disk
    $vmConfig = Set-AzVMOSDisk `
        -VM $vmConfig `
        -Name "$VMName-osdisk" `
        -CreateOption "FromImage" `
        -StorageAccountType "Premium_LRS"
    
    # Create the VM
    try {
        New-AzVM -ResourceGroupName $ResourceGroupName -Location $Location -VM $vmConfig
        Write-Host "✓ VM Node created successfully: $VMName" -ForegroundColor Green
        
        # Return VM details
        return @{
            "VMName" = $VMName
            "PublicIP" = $publicIp.DnsSettings.Fqdn
            "PrivateIP" = $nic.IpConfigurations[0].PrivateIpAddress
            "Status" = "Created"
            "Size" = $VMSize
            "Location" = $Location
        }
    }
    catch {
        Write-Host "✗ Failed to create VM: $VMName - $($_.Exception.Message)" -ForegroundColor Red
        return @{
            "VMName" = $VMName
            "Status" = "Failed"
            "Error" = $_.Exception.Message
        }
    }
}

# Function to create availability set
function New-AzureAvailabilitySet {
    param($ResourceGroupName, $Location, $AvailabilitySetName)
    
    Write-Host "=== Creating Availability Set: $AvailabilitySetName ===" -ForegroundColor Green
    
    $availabilitySet = New-AzAvailabilitySet `
        -ResourceGroupName $ResourceGroupName `
        -Location $Location `
        -Name $AvailabilitySetName `
        -PlatformFaultDomainCount 2 `
        -PlatformUpdateDomainCount 5 `
        -Sku "Aligned"
    
    Write-Host "✓ Availability Set created: $AvailabilitySetName" -ForegroundColor Green
    return $availabilitySet
}

# Function to install monitoring agent
function Install-MonitoringAgent {
    param($ResourceGroupName, $VMName)
    
    Write-Host "Installing monitoring agent on: $VMName" -ForegroundColor Blue
    
    # Install Azure Monitor Agent
    $extensionName = "AzureMonitorLinuxAgent"
    
    Set-AzVMExtension `
        -ResourceGroupName $ResourceGroupName `
        -VMName $VMName `
        -Name $extensionName `
        -Publisher "Microsoft.Azure.Monitor" `
        -Type "AzureMonitorLinuxAgent" `
        -TypeHandlerVersion "1.0" `
        -Location (Get-AzVM -ResourceGroupName $ResourceGroupName -Name $VMName).Location
    
    Write-Host "✓ Monitoring agent installed on: $VMName" -ForegroundColor Green
}

# Main execution function
function New-AzureVMNodeCluster {
    Write-Host "=== Azure VM Node Cluster Creation Started ===" -ForegroundColor Cyan
    Write-Host "Resource Group: $ResourceGroupName" -ForegroundColor White
    Write-Host "Location: $Location" -ForegroundColor White
    Write-Host "Node Count: $NodeCount" -ForegroundColor White
    Write-Host "VM Size: $VMSize" -ForegroundColor White
    
    $results = @{
        "ClusterInfo" = @{
            "ResourceGroup" = $ResourceGroupName
            "Location" = $Location
            "NodeCount" = $NodeCount
            "VMSize" = $VMSize
            "CreationTime" = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
        }
        "Nodes" = @()
        "Infrastructure" = @{}
    }
    
    try {
        # Step 1: Create Resource Group
        New-AzureResourceGroup -Name $ResourceGroupName -Location $Location
        
        # Step 2: Create Virtual Network
        $vnetName = "$VMNamePrefix-vnet"
        $vnet = New-AzureVirtualNetwork -ResourceGroupName $ResourceGroupName -Location $Location -VNetName $vnetName
        $results.Infrastructure["VirtualNetwork"] = $vnetName
        
        # Step 3: Create Network Security Group
        $nsgName = "$VMNamePrefix-nsg"
        $nsg = New-AzureNetworkSecurityGroup -ResourceGroupName $ResourceGroupName -Location $Location -NSGName $nsgName
        $results.Infrastructure["NetworkSecurityGroup"] = $nsgName
        
        # Step 4: Create Availability Set
        $availabilitySetName = "$VMNamePrefix-as"
        $availabilitySet = New-AzureAvailabilitySet -ResourceGroupName $ResourceGroupName -Location $Location -AvailabilitySetName $availabilitySetName
        $results.Infrastructure["AvailabilitySet"] = $availabilitySetName
        
        # Step 5: Create VM Nodes
        Write-Host "=== Creating $NodeCount VM Nodes ===" -ForegroundColor Cyan
        
        for ($i = 1; $i -le $NodeCount; $i++) {
            $vmName = "$VMNamePrefix$i"
            Write-Host "Creating node $i of $NodeCount : $vmName" -ForegroundColor Blue
            
            $nodeResult = New-AzureVMNode `
                -ResourceGroupName $ResourceGroupName `
                -Location $Location `
                -VMName $vmName `
                -VMSize $VMSize `
                -AdminUsername $AdminUsername `
                -VNet $vnet `
                -NSG $nsg `
                -NodeIndex $i
            
            $results.Nodes += $nodeResult
            
            # Install monitoring agent if VM creation was successful
            if ($nodeResult.Status -eq "Created") {
                Install-MonitoringAgent -ResourceGroupName $ResourceGroupName -VMName $vmName
            }
            
            # Add small delay between VM creations
            Start-Sleep -Seconds 5
        }
        
        # Step 6: Generate summary
        $successfulNodes = ($results.Nodes | Where-Object { $_.Status -eq "Created" }).Count
        $failedNodes = ($results.Nodes | Where-Object { $_.Status -eq "Failed" }).Count
        
        Write-Host "=== VM Node Cluster Creation Summary ===" -ForegroundColor Cyan
        Write-Host "✓ Successful nodes: $successfulNodes" -ForegroundColor Green
        if ($failedNodes -gt 0) {
            Write-Host "✗ Failed nodes: $failedNodes" -ForegroundColor Red
        }
        Write-Host "✓ Infrastructure components created" -ForegroundColor Green
        
        # Display connection information
        Write-Host "=== Connection Information ===" -ForegroundColor Cyan
        foreach ($node in $results.Nodes) {
            if ($node.Status -eq "Created") {
                Write-Host "Node: $($node.VMName)" -ForegroundColor White
                Write-Host "  Public IP: $($node.PublicIP)" -ForegroundColor Yellow
                Write-Host "  SSH Command: ssh $AdminUsername@$($node.PublicIP)" -ForegroundColor Yellow
            }
        }
        
        return $results
        
    }
    catch {
        Write-Host "✗ Cluster creation failed: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

# Function to remove the entire cluster
function Remove-AzureVMNodeCluster {
    param($ResourceGroupName)
    
    Write-Host "=== Removing VM Node Cluster ===" -ForegroundColor Red
    Write-Host "This will delete the entire resource group: $ResourceGroupName" -ForegroundColor Yellow
    
    $confirmation = Read-Host "Are you sure you want to delete the cluster? (yes/no)"
    if ($confirmation -eq "yes") {
        Remove-AzResourceGroup -Name $ResourceGroupName -Force
        Write-Host "✓ Cluster removed successfully" -ForegroundColor Green
    } else {
        Write-Host "Cluster removal cancelled" -ForegroundColor Yellow
    }
}

# Example usage
Write-Host "Azure VM Node Management Script" -ForegroundColor Cyan
Write-Host "Available functions:" -ForegroundColor White
Write-Host "  New-AzureVMNodeCluster - Create a complete VM node cluster" -ForegroundColor Yellow
Write-Host "  Remove-AzureVMNodeCluster - Remove the entire cluster" -ForegroundColor Yellow
Write-Host "" -ForegroundColor White

# Uncomment the following line to create a cluster
# $clusterResults = New-AzureVMNodeCluster

# Uncomment the following line to remove a cluster
# Remove-AzureVMNodeCluster -ResourceGroupName $ResourceGroupName
```

### Python Script for VM Node Automation

```python
# Azure VM Node Management with Python SDK
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
import json
import time
from datetime import datetime
from typing import Dict, List, Any

class AzureVMNodeManager:
    """Comprehensive Azure VM Node Management"""
    
    def __init__(self, subscription_id: str):
        self.subscription_id = subscription_id
        self.credential = DefaultAzureCredential()
        
        # Initialize Azure clients
        self.compute_client = ComputeManagementClient(self.credential, subscription_id)
        self.network_client = NetworkManagementClient(self.credential, subscription_id)
        self.resource_client = ResourceManagementClient(self.credential, subscription_id)
        self.storage_client = StorageManagementClient(self.credential, subscription_id)
    
    def create_vm_node_cluster(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a complete VM node cluster"""
        
        print("=== Azure VM Node Cluster Creation Started ===")
        print(f"Resource Group: {config['resource_group']}")
        print(f"Location: {config['location']}")
        print(f"Node Count: {config['node_count']}")
        print(f"VM Size: {config['vm_size']}")
        
        results = {
            "cluster_info": {
                "resource_group": config["resource_group"],
                "location": config["location"],
                "node_count": config["node_count"],
                "vm_size": config["vm_size"],
                "creation_time": datetime.now().isoformat()
            },
            "nodes": [],
            "infrastructure": {}
        }
        
        try:
            # Step 1: Create Resource Group
            self._create_resource_group(config["resource_group"], config["location"])
            
            # Step 2: Create Virtual Network
            vnet_name = f"{config['vm_prefix']}-vnet"
            vnet = self._create_virtual_network(
                config["resource_group"], 
                config["location"], 
                vnet_name
            )
            results["infrastructure"]["virtual_network"] = vnet_name
            
            # Step 3: Create Network Security Group
            nsg_name = f"{config['vm_prefix']}-nsg"
            nsg = self._create_network_security_group(
                config["resource_group"],
                config["location"],
                nsg_name
            )
            results["infrastructure"]["network_security_group"] = nsg_name
            
            # Step 4: Create Availability Set
            availability_set_name = f"{config['vm_prefix']}-as"
            availability_set = self._create_availability_set(
                config["resource_group"],
                config["location"],
                availability_set_name
            )
            results["infrastructure"]["availability_set"] = availability_set_name
            
            # Step 5: Create VM Nodes
            print(f"=== Creating {config['node_count']} VM Nodes ===")
            
            for i in range(1, config["node_count"] + 1):
                vm_name = f"{config['vm_prefix']}{i:02d}"
                print(f"Creating node {i} of {config['node_count']}: {vm_name}")
                
                node_result = self._create_vm_node(
                    config["resource_group"],
                    config["location"],
                    vm_name,
                    config["vm_size"],
                    config["admin_username"],
                    vnet,
                    nsg,
                    availability_set,
                    i
                )
                
                results["nodes"].append(node_result)
                
                # Small delay between VM creations
                time.sleep(2)
            
            # Step 6: Generate summary
            successful_nodes = len([n for n in results["nodes"] if n["status"] == "created"])
            failed_nodes = len([n for n in results["nodes"] if n["status"] == "failed"])
            
            print("=== VM Node Cluster Creation Summary ===")
            print(f"✓ Successful nodes: {successful_nodes}")
            if failed_nodes > 0:
                print(f"✗ Failed nodes: {failed_nodes}")
            print("✓ Infrastructure components created")
            
            return results
            
        except Exception as e:
            print(f"✗ Cluster creation failed: {str(e)}")
            raise
    
    def _create_resource_group(self, name: str, location: str) -> None:
        """Create a resource group"""
        
        print(f"=== Creating Resource Group: {name} ===")
        
        try:
            # Check if resource group exists
            try:
                self.resource_client.resource_groups.get(name)
                print(f"✓ Resource Group already exists: {name}")
                return
            except:
                pass
            
            # Create resource group
            rg_params = {
                "location": location,
                "tags": {
                    "CreatedBy": "PythonSDK",
                    "CreatedDate": datetime.now().strftime("%Y-%m-%d"),
                    "Purpose": "VMNodeCluster"
                }
            }
            
            self.resource_client.resource_groups.create_or_update(name, rg_params)
            print(f"✓ Resource Group created: {name}")
            
        except Exception as e:
            print(f"✗ Failed to create Resource Group: {str(e)}")
            raise
    
    def _create_virtual_network(self, resource_group: str, location: str, vnet_name: str) -> Dict:
        """Create a virtual network"""
        
        print(f"=== Creating Virtual Network: {vnet_name} ===")
        
        try:
            # Define VNet parameters
            vnet_params = {
                "location": location,
                "address_space": {
                    "address_prefixes": ["10.0.0.0/16"]
                },
                "subnets": [
                    {
                        "name": "default",
                        "address_prefix": "10.0.1.0/24"
                    }
                ],
                "tags": {
                    "CreatedBy": "PythonSDK",
                    "Purpose": "VMNodeCluster"
                }
            }
            
            # Create VNet
            operation = self.network_client.virtual_networks.begin_create_or_update(
                resource_group,
                vnet_name,
                vnet_params
            )
            
            vnet = operation.result()
            print(f"✓ Virtual Network created: {vnet_name}")
            
            return {
                "name": vnet_name,
                "id": vnet.id,
                "subnets": [{"name": subnet.name, "id": subnet.id} for subnet in vnet.subnets]
            }
            
        except Exception as e:
            print(f"✗ Failed to create Virtual Network: {str(e)}")
            raise
    
    def _create_network_security_group(self, resource_group: str, location: str, nsg_name: str) -> Dict:
        """Create a network security group"""
        
        print(f"=== Creating Network Security Group: {nsg_name} ===")
        
        try:
            # Define security rules
            security_rules = [
                {
                    "name": "SSH",
                    "protocol": "Tcp",
                    "source_port_range": "*",
                    "destination_port_range": "22",
                    "source_address_prefix": "*",
                    "destination_address_prefix": "*",
                    "access": "Allow",
                    "priority": 1000,
                    "direction": "Inbound"
                },
                {
                    "name": "HTTP",
                    "protocol": "Tcp",
                    "source_port_range": "*",
                    "destination_port_range": "80",
                    "source_address_prefix": "*",
                    "destination_address_prefix": "*",
                    "access": "Allow",
                    "priority": 1001,
                    "direction": "Inbound"
                },
                {
                    "name": "HTTPS",
                    "protocol": "Tcp",
                    "source_port_range": "*",
                    "destination_port_range": "443",
                    "source_address_prefix": "*",
                    "destination_address_prefix": "*",
                    "access": "Allow",
                    "priority": 1002,
                    "direction": "Inbound"
                }
            ]
            
            # NSG parameters
            nsg_params = {
                "location": location,
                "security_rules": security_rules,
                "tags": {
                    "CreatedBy": "PythonSDK",
                    "Purpose": "VMNodeCluster"
                }
            }
            
            # Create NSG
            operation = self.network_client.network_security_groups.begin_create_or_update(
                resource_group,
                nsg_name,
                nsg_params
            )
            
            nsg = operation.result()
            print(f"✓ Network Security Group created: {nsg_name}")
            
            return {
                "name": nsg_name,
                "id": nsg.id
            }
            
        except Exception as e:
            print(f"✗ Failed to create Network Security Group: {str(e)}")
            raise
    
    def _create_availability_set(self, resource_group: str, location: str, as_name: str) -> Dict:
        """Create an availability set"""
        
        print(f"=== Creating Availability Set: {as_name} ===")
        
        try:
            # Availability Set parameters
            as_params = {
                "location": location,
                "platform_fault_domain_count": 2,
                "platform_update_domain_count": 5,
                "sku": {
                    "name": "Aligned"
                },
                "tags": {
                    "CreatedBy": "PythonSDK",
                    "Purpose": "VMNodeCluster"
                }
            }
            
            # Create Availability Set
            availability_set = self.compute_client.availability_sets.create_or_update(
                resource_group,
                as_name,
                as_params
            )
            
            print(f"✓ Availability Set created: {as_name}")
            
            return {
                "name": as_name,
                "id": availability_set.id
            }
            
        except Exception as e:
            print(f"✗ Failed to create Availability Set: {str(e)}")
            raise
    
    def _create_vm_node(self, resource_group: str, location: str, vm_name: str,
                       vm_size: str, admin_username: str, vnet: Dict, nsg: Dict,
                       availability_set: Dict, node_index: int) -> Dict:
        """Create a single VM node"""
        
        print(f"=== Creating VM Node: {vm_name} ===")
        
        try:
            # Step 1: Create Public IP
            public_ip_name = f"{vm_name}-ip"
            public_ip = self._create_public_ip(resource_group, location, public_ip_name, vm_name)
            
            # Step 2: Create Network Interface
            nic_name = f"{vm_name}-nic"
            nic = self._create_network_interface(
                resource_group, location, nic_name, 
                vnet["subnets"][0]["id"], public_ip["id"], nsg["id"]
            )
            
            # Step 3: Create VM
            vm_params = {
                "location": location,
                "os_profile": {
                    "computer_name": vm_name,
                    "admin_username": admin_username,
                    "linux_configuration": {
                        "disable_password_authentication": False
                    }
                },
                "hardware_profile": {
                    "vm_size": vm_size
                },
                "storage_profile": {
                    "image_reference": {
                        "publisher": "Canonical",
                        "offer": "0001-com-ubuntu-server-focal",
                        "sku": "20_04-lts-gen2",
                        "version": "latest"
                    },
                    "os_disk": {
                        "name": f"{vm_name}-osdisk",
                        "create_option": "FromImage",
                        "managed_disk": {
                            "storage_account_type": "Premium_LRS"
                        }
                    }
                },
                "network_profile": {
                    "network_interfaces": [
                        {
                            "id": nic["id"]
                        }
                    ]
                },
                "availability_set": {
                    "id": availability_set["id"]
                },
                "tags": {
                    "Environment": "Production",
                    "NodeType": "ComputeNode",
                    "NodeIndex": str(node_index),
                    "CreatedBy": "PythonSDK",
                    "CreatedDate": datetime.now().strftime("%Y-%m-%d")
                }
            }
            
            # Create VM
            operation = self.compute_client.virtual_machines.begin_create_or_update(
                resource_group,
                vm_name,
                vm_params
            )
            
            vm = operation.result()
            print(f"✓ VM Node created successfully: {vm_name}")
            
            return {
                "vm_name": vm_name,
                "public_ip": public_ip["fqdn"],
                "private_ip": nic["private_ip"],
                "status": "created",
                "size": vm_size,
                "location": location,
                "node_index": node_index
            }
            
        except Exception as e:
            print(f"✗ Failed to create VM: {vm_name} - {str(e)}")
            return {
                "vm_name": vm_name,
                "status": "failed",
                "error": str(e),
                "node_index": node_index
            }
    
    def _create_public_ip(self, resource_group: str, location: str, 
                         public_ip_name: str, vm_name: str) -> Dict:
        """Create a public IP address"""
        
        public_ip_params = {
            "location": location,
            "public_ip_allocation_method": "Dynamic",
            "dns_settings": {
                "domain_name_label": f"{vm_name.lower()}-{int(time.time())}"
            },
            "tags": {
                "CreatedBy": "PythonSDK",
                "Purpose": "VMNodeCluster"
            }
        }
        
        operation = self.network_client.public_ip_addresses.begin_create_or_update(
            resource_group,
            public_ip_name,
            public_ip_params
        )
        
        public_ip = operation.result()
        
        return {
            "name": public_ip_name,
            "id": public_ip.id,
            "fqdn": public_ip.dns_settings.fqdn if public_ip.dns_settings else None
        }
    
    def _create_network_interface(self, resource_group: str, location: str, nic_name: str,
                                 subnet_id: str, public_ip_id: str, nsg_id: str) -> Dict:
        """Create a network interface"""
        
        nic_params = {
            "location": location,
            "ip_configurations": [
                {
                    "name": "ipconfig1",
                    "subnet": {
                        "id": subnet_id
                    },
                    "public_ip_address": {
                        "id": public_ip_id
                    }
                }
            ],
            "network_security_group": {
                "id": nsg_id
            },
            "tags": {
                "CreatedBy": "PythonSDK",
                "Purpose": "VMNodeCluster"
            }
        }
        
        operation = self.network_client.network_interfaces.begin_create_or_update(
            resource_group,
            nic_name,
            nic_params
        )
        
        nic = operation.result()
        
        return {
            "name": nic_name,
            "id": nic.id,
            "private_ip": nic.ip_configurations[0].private_ip_address
        }
    
    def get_vm_node_status(self, resource_group: str, vm_name: str) -> Dict:
        """Get VM node status and details"""
        
        try:
            # Get VM details
            vm = self.compute_client.virtual_machines.get(
                resource_group, 
                vm_name, 
                expand="instanceView"
            )
            
            # Get power state
            power_state = "unknown"
            if vm.instance_view and vm.instance_view.statuses:
                for status in vm.instance_view.statuses:
                    if status.code.startswith("PowerState/"):
                        power_state = status.code.replace("PowerState/", "")
                        break
            
            return {
                "vm_name": vm_name,
                "power_state": power_state,
                "provisioning_state": vm.provisioning_state,
                "vm_size": vm.hardware_profile.vm_size,
                "location": vm.location,
                "tags": vm.tags or {},
                "os_type": vm.storage_profile.os_disk.os_type.value if vm.storage_profile.os_disk.os_type else "unknown"
            }
            
        except Exception as e:
            return {
                "vm_name": vm_name,
                "error": str(e),
                "status": "error"
            }
    
    def list_vm_nodes(self, resource_group: str) -> List[Dict]:
        """List all VM nodes in a resource group"""
        
        try:
            vms = self.compute_client.virtual_machines.list(resource_group)
            
            vm_list = []
            for vm in vms:
                vm_info = self.get_vm_node_status(resource_group, vm.name)
                vm_list.append(vm_info)
            
            return vm_list
            
        except Exception as e:
            print(f"Error listing VMs: {str(e)}")
            return []
    
    def delete_vm_node_cluster(self, resource_group: str) -> bool:
        """Delete the entire VM node cluster (resource group)"""
        
        print(f"=== Removing VM Node Cluster ===")
        print(f"This will delete the entire resource group: {resource_group}")
        
        try:
            # Delete resource group
            operation = self.resource_client.resource_groups.begin_delete(resource_group)
            operation.result()  # Wait for completion
            
            print(f"✓ Cluster removed successfully: {resource_group}")
            return True
            
        except Exception as e:
            print(f"✗ Failed to remove cluster: {str(e)}")
            return False

# Example usage and demonstration
def demonstrate_vm_node_management():
    """Comprehensive demonstration of VM node management"""
    
    print("=== Azure VM Node Management Demonstration ===")
    
    # Configuration
    subscription_id = "your-subscription-id"
    
    config = {
        "resource_group": "vm-node-cluster-rg",
        "location": "East US",
        "vm_prefix": "node",
        "node_count": 3,
        "vm_size": "Standard_B2s",
        "admin_username": "azureuser"
    }
    
    try:
        # Initialize VM manager
        vm_manager = AzureVMNodeManager(subscription_id)
        
        # Create VM node cluster
        cluster_results = vm_manager.create_vm_node_cluster(config)
        
        print("\n=== Cluster Creation Results ===")
        print(json.dumps(cluster_results, indent=2, default=str))
        
        # List VM nodes
        print("\n=== VM Node Status ===")
        vm_list = vm_manager.list_vm_nodes(config["resource_group"])
        for vm in vm_list:
            print(f"VM: {vm['vm_name']} - Power State: {vm.get('power_state', 'unknown')}")
        
        # Note: Uncomment the following line to delete the cluster
        # vm_manager.delete_vm_node_cluster(config["resource_group"])
        
        return {
            "status": "success",
            "cluster_results": cluster_results,
            "vm_list": vm_list
        }
        
    except Exception as e:
        print(f"Demonstration failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

# Run demonstration
# demo_results = demonstrate_vm_node_management()
```

---

## Azure Kubernetes Service (AKS) Nodes

### AKS Node Types and Configurations

AKS provides managed Kubernetes clusters with various node pool types optimized for different workloads.

```json
{
  "aks_node_types": {
    "system_node_pools": {
      "description": "Nodes dedicated to system pods and cluster operations",
      "characteristics": [
        "Required for cluster operation",
        "Minimum 1 node recommended",
        "Runs system pods (kube-system)",
        "Can run user workloads with taints/tolerations"
      ],
      "recommended_vm_sizes": ["Standard_DS2_v2", "Standard_D4s_v3", "Standard_D8s_v3"],
      "node_configuration": {
        "min_count": 1,
        "max_count": 100,
        "default_count": 3,
        "os_type": "Linux",
        "mode": "System"
      }
    },
    "user_node_pools": {
      "description": "Nodes dedicated to user application workloads",
      "characteristics": [
        "Optional but recommended for production",
        "Isolated from system workloads",
        "Can be scaled independently",
        "Supports multiple node pools"
      ],
      "vm_size_categories": {
        "general_purpose": ["Standard_D2s_v3", "Standard_D4s_v3", "Standard_D8s_v3"],
        "compute_optimized": ["Standard_F4s_v2", "Standard_F8s_v2", "Standard_F16s_v2"],
        "memory_optimized": ["Standard_E4s_v3", "Standard_E8s_v3", "Standard_E16s_v3"],
        "gpu_accelerated": ["Standard_NC6s_v3", "Standard_NC12s_v3", "Standard_ND40rs_v2"]
      },
      "node_configuration": {
        "min_count": 0,
        "max_count": 1000,
        "default_count": 2,
        "os_type": "Linux or Windows",
        "mode": "User"
      }
    },
    "spot_node_pools": {
      "description": "Cost-optimized nodes using Azure Spot instances",
      "characteristics": [
        "Up to 90% cost savings",
        "Can be evicted when Azure needs capacity",
        "Best for fault-tolerant workloads",
        "Supports automatic scaling"
      ],
      "use_cases": [
        "Batch processing",
        "Development/testing",
        "Fault-tolerant applications",
        "Big data processing"
      ],
      "limitations": [
        "No SLA guarantee",
        "Can be evicted with 30-second notice",
        "Not suitable for critical workloads"
      ]
    },
    "virtual_node_pools": {
      "description": "Serverless nodes using Azure Container Instances",
      "characteristics": [
        "Pay-per-second billing",
        "Rapid scaling (seconds)",
        "No node management required",
        "Automatic pod scheduling"
      ],
      "benefits": [
        "Infinite scale",
        "No pre-provisioned capacity",
        "Ideal for burst workloads",
        "Simplified operations"
      ],
      "limitations": [
        "Linux containers only",
        "Limited networking features",
        "Higher per-pod cost for long-running workloads"
      ]
    }
  }
}
```

### Azure CLI Script for AKS Node Management

```bash
#!/bin/bash
# Azure Kubernetes Service (AKS) Node Management Script
# Comprehensive AKS cluster and node pool management

# Configuration variables
RESOURCE_GROUP="aks-node-cluster-rg"
CLUSTER_NAME="aks-node-cluster"
LOCATION="eastus"
NODE_COUNT=3
VM_SIZE="Standard_D2s_v3"
KUBERNETES_VERSION="1.28.0"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_header() {
    echo -e "${CYAN}=== $1 ===${NC}"
}

# Function to create resource group
create_resource_group() {
    print_header "Creating Resource Group"
    
    if az group show --name "$RESOURCE_GROUP" >/dev/null 2>&1; then
        print_warning "Resource group '$RESOURCE_GROUP' already exists"
    else
        az group create \
            --name "$RESOURCE_GROUP" \
            --location "$LOCATION" \
            --tags CreatedBy="AzureCLI" Purpose="AKSNodeCluster" CreatedDate="$(date +%Y-%m-%d)"
        
        if [ $? -eq 0 ]; then
            print_status "Resource group created: $RESOURCE_GROUP"
        else
            print_error "Failed to create resource group"
            exit 1
        fi
    fi
}

# Function to create AKS cluster
create_aks_cluster() {
    print_header "Creating AKS Cluster"
    
    print_info "Cluster Name: $CLUSTER_NAME"
    print_info "Location: $LOCATION"
    print_info "Node Count: $NODE_COUNT"
    print_info "VM Size: $VM_SIZE"
    print_info "Kubernetes Version: $KUBERNETES_VERSION"
    
    # Create AKS cluster with system node pool
    az aks create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$CLUSTER_NAME" \
        --location "$LOCATION" \
        --node-count $NODE_COUNT \
        --node-vm-size "$VM_SIZE" \
        --kubernetes-version "$KUBERNETES_VERSION" \
        --enable-addons monitoring \
        --enable-managed-identity \
        --enable-cluster-autoscaler \
        --min-count 1 \
        --max-count 10 \
        --nodepool-name "systempool" \
        --nodepool-tags NodeType="System" Environment="Production" \
        --tags CreatedBy="AzureCLI" Purpose="AKSNodeCluster" \
        --generate-ssh-keys \
        --yes
    
    if [ $? -eq 0 ]; then
        print_status "AKS cluster created successfully: $CLUSTER_NAME"
    else
        print_error "Failed to create AKS cluster"
        exit 1
    fi
}

# Function to create user node pool
create_user_node_pool() {
    local pool_name=$1
    local vm_size=$2
    local node_count=$3
    local max_count=${4:-20}
    local min_count=${5:-1}
    
    print_header "Creating User Node Pool: $pool_name"
    
    az aks nodepool add \
        --resource-group "$RESOURCE_GROUP" \
        --cluster-name "$CLUSTER_NAME" \
        --name "$pool_name" \
        --node-count $node_count \
        --node-vm-size "$vm_size" \
        --enable-cluster-autoscaler \
        --min-count $min_count \
        --max-count $max_count \
        --mode User \
        --node-taints workload="$pool_name":NoSchedule \
        --tags NodeType="User" WorkloadType="$pool_name" Environment="Production"
    
    if [ $? -eq 0 ]; then
        print_status "User node pool created: $pool_name"
    else
        print_error "Failed to create user node pool: $pool_name"
    fi
}

# Function to create spot node pool
create_spot_node_pool() {
    local pool_name=$1
    local vm_size=$2
    local node_count=$3
    local max_price=${4:-0.5}
    
    print_header "Creating Spot Node Pool: $pool_name"
    
    az aks nodepool add \
        --resource-group "$RESOURCE_GROUP" \
        --cluster-name "$CLUSTER_NAME" \
        --name "$pool_name" \
        --node-count $node_count \
        --node-vm-size "$vm_size" \
        --priority Spot \
        --eviction-policy Delete \
        --spot-max-price $max_price \
        --enable-cluster-autoscaler \
        --min-count 0 \
        --max-count 10 \
        --mode User \
        --node-taints kubernetes.azure.com/scalesetpriority=spot:NoSchedule \
        --tags NodeType="Spot" CostOptimized="true" Environment="Production"
    
    if [ $? -eq 0 ]; then
        print_status "Spot node pool created: $pool_name (max price: \$$max_price/hour)"
    else
        print_error "Failed to create spot node pool: $pool_name"
    fi
}

# Function to create GPU node pool
create_gpu_node_pool() {
    local pool_name=$1
    local vm_size=${2:-"Standard_NC6s_v3"}
    local node_count=${3:-1}
    
    print_header "Creating GPU Node Pool: $pool_name"
    
    az aks nodepool add \
        --resource-group "$RESOURCE_GROUP" \
        --cluster-name "$CLUSTER_NAME" \
        --name "$pool_name" \
        --node-count $node_count \
        --node-vm-size "$vm_size" \
        --enable-cluster-autoscaler \
        --min-count 0 \
        --max-count 5 \
        --mode User \
        --node-taints sku=gpu:NoSchedule \
        --tags NodeType="GPU" Accelerated="true" Environment="Production"
    
    if [ $? -eq 0 ]; then
        print_status "GPU node pool created: $pool_name"
        
        # Install NVIDIA device plugin
        print_info "Installing NVIDIA device plugin..."
        kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.14.1/nvidia-device-plugin.yml
    else
        print_error "Failed to create GPU node pool: $pool_name"
    fi
}

# Function to enable virtual nodes
enable_virtual_nodes() {
    print_header "Enabling Virtual Nodes"
    
    # Create subnet for virtual nodes
    VNET_NAME=$(az aks show --resource-group "$RESOURCE_GROUP" --name "$CLUSTER_NAME" --query "agentPoolProfiles[0].vnetSubnetId" -o tsv | cut -d'/' -f9)
    VNET_RESOURCE_GROUP=$(az aks show --resource-group "$RESOURCE_GROUP" --name "$CLUSTER_NAME" --query "nodeResourceGroup" -o tsv)
    
    az network vnet subnet create \
        --resource-group "$VNET_RESOURCE_GROUP" \
        --vnet-name "$VNET_NAME" \
        --name virtual-node-subnet \
        --address-prefixes 10.241.0.0/16
    
    # Enable virtual nodes addon
    az aks enable-addons \
        --resource-group "$RESOURCE_GROUP" \
        --name "$CLUSTER_NAME" \
        --addons virtual-node \
        --subnet-name virtual-node-subnet
    
    if [ $? -eq 0 ]; then
        print_status "Virtual nodes enabled successfully"
    else
        print_error "Failed to enable virtual nodes"
    fi
}

# Function to get cluster credentials
get_cluster_credentials() {
    print_header "Getting Cluster Credentials"
    
    az aks get-credentials \
        --resource-group "$RESOURCE_GROUP" \
        --name "$CLUSTER_NAME" \
        --overwrite-existing
    
    if [ $? -eq 0 ]; then
        print_status "Cluster credentials configured"
        
        # Test kubectl connection
        print_info "Testing kubectl connection..."
        kubectl get nodes
    else
        print_error "Failed to get cluster credentials"
    fi
}

# Function to list node pools
list_node_pools() {
    print_header "Node Pools Information"
    
    echo "Cluster: $CLUSTER_NAME"
    echo "Resource Group: $RESOURCE_GROUP"
    echo ""
    
    az aks nodepool list \
        --resource-group "$RESOURCE_GROUP" \
        --cluster-name "$CLUSTER_NAME" \
        --output table
}

# Function to show node pool details
show_node_pool_details() {
    local pool_name=$1
    
    print_header "Node Pool Details: $pool_name"
    
    az aks nodepool show \
        --resource-group "$RESOURCE_GROUP" \
        --cluster-name "$CLUSTER_NAME" \
        --name "$pool_name" \
        --output json | jq '{
            name: .name,
            vmSize: .vmSize,
            count: .count,
            minCount: .minCount,
            maxCount: .maxCount,
            mode: .mode,
            osType: .osType,
            provisioningState: .provisioningState,
            scaleSetPriority: .scaleSetPriority,
            spotMaxPrice: .spotMaxPrice,
            tags: .tags
        }'
}

# Function to scale node pool
scale_node_pool() {
    local pool_name=$1
    local node_count=$2
    
    print_header "Scaling Node Pool: $pool_name to $node_count nodes"
    
    az aks nodepool scale \
        --resource-group "$RESOURCE_GROUP" \
        --cluster-name "$CLUSTER_NAME" \
        --name "$pool_name" \
        --node-count $node_count
    
    if [ $? -eq 0 ]; then
        print_status "Node pool scaled successfully: $pool_name"
    else
        print_error "Failed to scale node pool: $pool_name"
    fi
}

# Function to update node pool
update_node_pool() {
    local pool_name=$1
    local min_count=$2
    local max_count=$3
    
    print_header "Updating Node Pool: $pool_name"
    
    az aks nodepool update \
        --resource-group "$RESOURCE_GROUP" \
        --cluster-name "$CLUSTER_NAME" \
        --name "$pool_name" \
        --update-cluster-autoscaler \
        --min-count $min_count \
        --max-count $max_count
    
    if [ $? -eq 0 ]; then
        print_status "Node pool updated successfully: $pool_name"
    else
        print_error "Failed to update node pool: $pool_name"
    fi
}

# Function to delete node pool
delete_node_pool() {
    local pool_name=$1
    
    print_header "Deleting Node Pool: $pool_name"
    print_warning "This action cannot be undone!"
    
    read -p "Are you sure you want to delete node pool '$pool_name'? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
        az aks nodepool delete \
            --resource-group "$RESOURCE_GROUP" \
            --cluster-name "$CLUSTER_NAME" \
            --name "$pool_name" \
            --no-wait
        
        if [ $? -eq 0 ]; then
            print_status "Node pool deletion initiated: $pool_name"
        else
            print_error "Failed to delete node pool: $pool_name"
        fi
    else
        print_info "Node pool deletion cancelled"
    fi
}

# Function to create comprehensive node pool suite
create_comprehensive_node_pools() {
    print_header "Creating Comprehensive Node Pool Suite"
    
    # Create different types of node pools
    create_user_node_pool "webapps" "Standard_D4s_v3" 2 10 1
    create_user_node_pool "compute" "Standard_F8s_v2" 1 5 0
    create_user_node_pool "memory" "Standard_E8s_v3" 1 3 0
    create_spot_node_pool "batchspot" "Standard_D8s_v3" 2 0.3
    create_gpu_node_pool "gpuworkloads" "Standard_NC6s_v3" 1
    
    print_status "Comprehensive node pool suite created"
}

# Function to deploy sample workloads
deploy_sample_workloads() {
    print_header "Deploying Sample Workloads"
    
    # Web application on web apps node pool
    cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: webapp-deployment
  labels:
    app: webapp
spec:
  replicas: 3
  selector:
    matchLabels:
      app: webapp
  template:
    metadata:
      labels:
        app: webapp
    spec:
      nodeSelector:
        agentpool: webapps
      tolerations:
      - key: workload
        operator: Equal
        value: webapps
        effect: NoSchedule
      containers:
      - name: webapp
        image: nginx:latest
        ports:
        - containerPort: 80
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 512Mi
---
apiVersion: v1
kind: Service
metadata:
  name: webapp-service
spec:
  selector:
    app: webapp
  ports:
  - port: 80
    targetPort: 80
  type: LoadBalancer
EOF

    # Batch job on spot nodes
    cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: batch-job-spot
spec:
  template:
    spec:
      nodeSelector:
        agentpool: batchspot
      tolerations:
      - key: kubernetes.azure.com/scalesetpriority
        operator: Equal
        value: spot
        effect: NoSchedule
      containers:
      - name: batch-worker
        image: busybox:latest
        command: ["sh", "-c", "echo 'Processing batch job on spot node'; sleep 300; echo 'Batch job completed'"]
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
      restartPolicy: Never
  backoffLimit: 3
EOF

    # GPU workload (if GPU node pool exists)
    cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gpu-workload
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gpu-workload
  template:
    metadata:
      labels:
        app: gpu-workload
    spec:
      nodeSelector:
        agentpool: gpuworkloads
      tolerations:
      - key: sku
        operator: Equal
        value: gpu
        effect: NoSchedule
      containers:
      - name: gpu-container
        image: nvidia/cuda:11.0-runtime-ubuntu20.04
        command: ["nvidia-smi"]
        resources:
          limits:
            nvidia.com/gpu: 1
          requests:
            nvidia.com/gpu: 1
EOF

    print_status "Sample workloads deployed"
}

# Function to monitor cluster
monitor_cluster() {
    print_header "Cluster Monitoring"
    
    echo "Cluster Overview:"
    kubectl get nodes -o wide
    echo ""
    
    echo "Node Pool Status:"
    list_node_pools
    echo ""
    
    echo "Pod Distribution:"
    kubectl get pods -o wide --all-namespaces | grep -E "(webapp|batch|gpu)"
    echo ""
    
    echo "Resource Usage:"
    kubectl top nodes 2>/dev/null || print_warning "Metrics server not available"
}

# Function to cleanup cluster
cleanup_cluster() {
    print_header "Cleanup Cluster"
    print_warning "This will delete the entire AKS cluster and all resources!"
    
    read -p "Are you sure you want to delete the cluster '$CLUSTER_NAME'? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
        print_info "Deleting AKS cluster..."
        az aks delete \
            --resource-group "$RESOURCE_GROUP" \
            --name "$CLUSTER_NAME" \
            --yes \
            --no-wait
        
        print_info "Deleting resource group..."
        az group delete \
            --name "$RESOURCE_GROUP" \
            --yes \
            --no-wait
        
        print_status "Cleanup initiated"
    else
        print_info "Cleanup cancelled"
    fi
}

# Main menu function
show_menu() {
    echo ""
    print_header "AKS Node Management Menu"
    echo "1. Create AKS cluster with system node pool"
    echo "2. Create comprehensive node pool suite"
    echo "3. List node pools"
    echo "4. Show node pool details"
    echo "5. Scale node pool"
    echo "6. Update node pool"
    echo "7. Delete node pool"
    echo "8. Enable virtual nodes"
    echo "9. Deploy sample workloads"
    echo "10. Monitor cluster"
    echo "11. Get cluster credentials"
    echo "12. Cleanup cluster"
    echo "0. Exit"
    echo ""
}

# Main execution
main() {
    print_header "Azure Kubernetes Service (AKS) Node Management"
    print_info "Resource Group: $RESOURCE_GROUP"
    print_info "Cluster Name: $CLUSTER_NAME"
    print_info "Location: $LOCATION"
    echo ""
    
    # Check if Azure CLI is installed and logged in
    if ! command -v az &> /dev/null; then
        print_error "Azure CLI is not installed"
        exit 1
    fi
    
    if ! az account show &> /dev/null; then
        print_error "Not logged in to Azure CLI. Please run 'az login'"
        exit 1
    fi
    
    # Create resource group
    create_resource_group
    
    while true; do
        show_menu
        read -p "Select an option (0-12): " choice
        
        case $choice in
            1)
                create_aks_cluster
                get_cluster_credentials
                ;;
            2)
                create_comprehensive_node_pools
                ;;
            3)
                list_node_pools
                ;;
            4)
                read -p "Enter node pool name: " pool_name
                show_node_pool_details "$pool_name"
                ;;
            5)
                read -p "Enter node pool name: " pool_name
                read -p "Enter new node count: " node_count
                scale_node_pool "$pool_name" "$node_count"
                ;;
            6)
                read -p "Enter node pool name: " pool_name
                read -p "Enter min count: " min_count
                read -p "Enter max count: " max_count
                update_node_pool "$pool_name" "$min_count" "$max_count"
                ;;
            7)
                read -p "Enter node pool name to delete: " pool_name
                delete_node_pool "$pool_name"
                ;;
            8)
                enable_virtual_nodes
                ;;
            9)
                deploy_sample_workloads
                ;;
            10)
                monitor_cluster
                ;;
            11)
                get_cluster_credentials
                ;;
            12)
                cleanup_cluster
                ;;
            0)
                print_info "Exiting..."
                exit 0
                ;;
            *)
                print_error "Invalid option. Please select 0-12."
                ;;
        esac
        
        echo ""
        read -p "Press Enter to continue..."
    done
}

# Run main function
main
```

This comprehensive guide provides everything needed to understand, implement, and manage various types of nodes across Azure services, ensuring effective compute resource management and optimization in your cloud applications.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*