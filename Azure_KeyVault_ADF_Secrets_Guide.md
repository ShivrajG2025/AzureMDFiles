# Azure Key Vault Secrets in ADF Guide
## Comprehensive Guide to Managing Secrets in Azure Data Factory using Azure Key Vault

---

### Table of Contents

1. [Overview](#overview)
2. [Azure Key Vault Setup](#azure-key-vault-setup)
3. [ADF Key Vault Integration](#adf-key-vault-integration)
4. [Linked Services Configuration](#linked-services-configuration)
5. [Secret Management in Pipelines](#secret-management-in-pipelines)
6. [Advanced Security Configurations](#advanced-security-configurations)
7. [Secret Rotation and Lifecycle](#secret-rotation-and-lifecycle)
8. [Monitoring and Auditing](#monitoring-and-auditing)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)
11. [Real-World Examples](#real-world-examples)

---

## Overview

Azure Key Vault integration with Azure Data Factory provides a secure way to manage secrets, connection strings, API keys, and other sensitive information used in data pipelines. This eliminates the need to store credentials directly in ADF configurations.

### Key Benefits

```json
{
  "key_vault_adf_benefits": {
    "security": {
      "centralized_secret_management": "Single source of truth for all secrets",
      "encryption_at_rest": "Secrets encrypted using Azure-managed keys",
      "access_control": "Fine-grained RBAC and access policies",
      "audit_logging": "Comprehensive audit trail for secret access"
    },
    "compliance": {
      "regulatory_compliance": "Meets various compliance standards (SOC, ISO, HIPAA)",
      "data_residency": "Control over data location and residency",
      "key_governance": "Centralized key and secret governance",
      "separation_of_duties": "Role-based access separation"
    },
    "operational": {
      "secret_rotation": "Automated secret rotation capabilities",
      "version_management": "Multiple versions of secrets maintained",
      "disaster_recovery": "Built-in backup and recovery",
      "integration": "Seamless integration with Azure services"
    },
    "development": {
      "environment_separation": "Different secrets for dev/test/prod",
      "dynamic_configuration": "Runtime secret resolution",
      "reduced_hardcoding": "No secrets in source code or configurations",
      "simplified_deployment": "Environment-agnostic deployments"
    }
  }
}
```

### Architecture Overview

```json
{
  "architecture_components": {
    "azure_key_vault": {
      "description": "Secure storage for secrets, keys, and certificates",
      "features": ["Hardware Security Module (HSM) backing", "Access policies", "Network isolation", "Soft delete"],
      "secret_types": ["Database connection strings", "API keys", "Storage account keys", "Service principal credentials"]
    },
    "azure_data_factory": {
      "description": "Data integration service with Key Vault integration",
      "integration_points": ["Linked services", "Pipeline parameters", "Dataset properties", "Activity configurations"],
      "authentication_methods": ["Managed Identity", "Service Principal", "User-assigned managed identity"]
    },
    "security_layer": {
      "access_control": ["Azure RBAC", "Key Vault access policies", "Network security groups", "Private endpoints"],
      "monitoring": ["Azure Monitor", "Key Vault logs", "ADF monitoring", "Activity logs"],
      "compliance": ["Encryption", "Audit trails", "Access reviews", "Policy enforcement"]
    }
  }
}
```

---

## Azure Key Vault Setup

### PowerShell Setup Script

```powershell
# Azure Key Vault Setup for ADF Integration
# Comprehensive setup script for Key Vault and ADF integration

# Variables
$resourceGroupName = "rg-adf-keyvault-demo"
$location = "East US 2"
$keyVaultName = "kv-adf-demo-$(Get-Random -Minimum 1000 -Maximum 9999)"
$dataFactoryName = "adf-demo-$(Get-Random -Minimum 1000 -Maximum 9999)"
$servicePrincipalName = "sp-adf-keyvault-demo"
$currentUserObjectId = (Get-AzADUser -UserPrincipalName (Get-AzContext).Account.Id).Id

# Login to Azure
Write-Host "Logging into Azure..." -ForegroundColor Green
Connect-AzAccount

# Create Resource Group
Write-Host "Creating Resource Group: $resourceGroupName" -ForegroundColor Green
New-AzResourceGroup -Name $resourceGroupName -Location $location

# Create Azure Key Vault
Write-Host "Creating Azure Key Vault: $keyVaultName" -ForegroundColor Green
$keyVault = New-AzKeyVault `
    -ResourceGroupName $resourceGroupName `
    -VaultName $keyVaultName `
    -Location $location `
    -Sku "Standard" `
    -EnableSoftDelete `
    -SoftDeleteRetentionInDays 90 `
    -EnablePurgeProtection

# Enable Key Vault for template deployment and disk encryption
Write-Host "Configuring Key Vault policies..." -ForegroundColor Green
Set-AzKeyVaultAccessPolicy `
    -VaultName $keyVaultName `
    -ResourceGroupName $resourceGroupName `
    -EnabledForTemplateDeployment `
    -EnabledForDiskEncryption `
    -EnabledForDeployment

# Create Azure Data Factory
Write-Host "Creating Azure Data Factory: $dataFactoryName" -ForegroundColor Green
$dataFactory = New-AzDataFactoryV2 `
    -ResourceGroupName $resourceGroupName `
    -Name $dataFactoryName `
    -Location $location

# Get Data Factory Managed Identity
Write-Host "Retrieving Data Factory Managed Identity..." -ForegroundColor Green
$adfIdentity = Get-AzDataFactoryV2 -ResourceGroupName $resourceGroupName -Name $dataFactoryName
$adfPrincipalId = $adfIdentity.Identity.PrincipalId

Write-Host "ADF Managed Identity Principal ID: $adfPrincipalId" -ForegroundColor Yellow

# Grant Key Vault access to ADF Managed Identity
Write-Host "Granting Key Vault access to ADF Managed Identity..." -ForegroundColor Green
Set-AzKeyVaultAccessPolicy `
    -VaultName $keyVaultName `
    -ObjectId $adfPrincipalId `
    -PermissionsToSecrets Get,List

# Grant current user full access to Key Vault for setup
Write-Host "Granting current user access to Key Vault..." -ForegroundColor Green
Set-AzKeyVaultAccessPolicy `
    -VaultName $keyVaultName `
    -ObjectId $currentUserObjectId `
    -PermissionsToSecrets Get,Set,Delete,List,Recover,Backup,Restore `
    -PermissionsToKeys Get,List,Update,Create,Import,Delete,Recover,Backup,Restore,Decrypt,Encrypt,UnwrapKey,WrapKey,Verify,Sign `
    -PermissionsToCertificates Get,List,Update,Create,Import,Delete,Recover,Backup,Restore,ManageContacts,ManageIssuers,GetIssuers,ListIssuers,SetIssuers,DeleteIssuers

# Create sample secrets for demonstration
Write-Host "Creating sample secrets..." -ForegroundColor Green

# Database connection strings
$sqlConnectionString = "Server=tcp:your-server.database.windows.net,1433;Initial Catalog=your-database;Persist Security Info=False;User ID=your-username;Password=your-password;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "sql-connection-string" -SecretValue (ConvertTo-SecureString $sqlConnectionString -AsPlainText -Force)

$cosmosConnectionString = "AccountEndpoint=https://your-cosmos.documents.azure.com:443/;AccountKey=your-cosmos-key;"
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "cosmos-connection-string" -SecretValue (ConvertTo-SecureString $cosmosConnectionString -AsPlainText -Force)

# Storage account keys
$storageAccountKey = "your-storage-account-key"
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "storage-account-key" -SecretValue (ConvertTo-SecureString $storageAccountKey -AsPlainText -Force)

# API keys
$apiKey = "your-api-key-value"
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "external-api-key" -SecretValue (ConvertTo-SecureString $apiKey -AsPlainText -Force)

# Service Principal credentials
$servicePrincipalClientId = "your-sp-client-id"
$servicePrincipalClientSecret = "your-sp-client-secret"
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "sp-client-id" -SecretValue (ConvertTo-SecureString $servicePrincipalClientId -AsPlainText -Force)
Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "sp-client-secret" -SecretValue (ConvertTo-SecureString $servicePrincipalClientSecret -AsPlainText -Force)

# Create additional secrets for different environments
$environments = @("dev", "test", "prod")
foreach ($env in $environments) {
    $envConnectionString = "Server=tcp:$env-server.database.windows.net,1433;Initial Catalog=$env-database;Persist Security Info=False;User ID=$env-user;Password=$env-password;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
    Set-AzKeyVaultSecret -VaultName $keyVaultName -Name "sql-connection-string-$env" -SecretValue (ConvertTo-SecureString $envConnectionString -AsPlainText -Force)
}

# Output configuration summary
Write-Host "`n=== Configuration Summary ===" -ForegroundColor Cyan
Write-Host "Resource Group: $resourceGroupName" -ForegroundColor White
Write-Host "Key Vault Name: $keyVaultName" -ForegroundColor White
Write-Host "Key Vault URI: https://$keyVaultName.vault.azure.net/" -ForegroundColor Yellow
Write-Host "Data Factory Name: $dataFactoryName" -ForegroundColor White
Write-Host "ADF Managed Identity ID: $adfPrincipalId" -ForegroundColor White

Write-Host "`n=== Created Secrets ===" -ForegroundColor Cyan
$secrets = Get-AzKeyVaultSecret -VaultName $keyVaultName
foreach ($secret in $secrets) {
    Write-Host "  - $($secret.Name)" -ForegroundColor White
}

Write-Host "`n=== Next Steps ===" -ForegroundColor Green
Write-Host "1. Update the sample secret values with your actual credentials" -ForegroundColor White
Write-Host "2. Configure ADF linked services to use Key Vault secrets" -ForegroundColor White
Write-Host "3. Test the integration with sample pipelines" -ForegroundColor White
Write-Host "4. Set up monitoring and alerting" -ForegroundColor White

# Export configuration for later use
$config = @{
    ResourceGroup = $resourceGroupName
    KeyVaultName = $keyVaultName
    KeyVaultUri = "https://$keyVaultName.vault.azure.net/"
    DataFactoryName = $dataFactoryName
    ADFManagedIdentityId = $adfPrincipalId
    Location = $location
}

$config | ConvertTo-Json | Out-File "adf-keyvault-config.json"
Write-Host "`nConfiguration exported to: adf-keyvault-config.json" -ForegroundColor Yellow
```

### Azure CLI Alternative Setup

```bash
#!/bin/bash
# Azure Key Vault and ADF Setup using Azure CLI

# Variables
RESOURCE_GROUP="rg-adf-keyvault-demo"
LOCATION="eastus2"
KEY_VAULT_NAME="kv-adf-demo-$RANDOM"
DATA_FACTORY_NAME="adf-demo-$RANDOM"

echo "=== Azure Key Vault and ADF Setup ==="

# Create Resource Group
echo "Creating resource group: $RESOURCE_GROUP"
az group create --name $RESOURCE_GROUP --location $LOCATION

# Create Key Vault
echo "Creating Key Vault: $KEY_VAULT_NAME"
az keyvault create \
    --resource-group $RESOURCE_GROUP \
    --name $KEY_VAULT_NAME \
    --location $LOCATION \
    --sku standard \
    --enable-soft-delete true \
    --soft-delete-retention 90 \
    --enable-purge-protection true

# Create Data Factory
echo "Creating Data Factory: $DATA_FACTORY_NAME"
az datafactory factory create \
    --resource-group $RESOURCE_GROUP \
    --factory-name $DATA_FACTORY_NAME \
    --location $LOCATION

# Get ADF Managed Identity
echo "Retrieving ADF Managed Identity..."
ADF_PRINCIPAL_ID=$(az datafactory factory show \
    --resource-group $RESOURCE_GROUP \
    --factory-name $DATA_FACTORY_NAME \
    --query identity.principalId -o tsv)

echo "ADF Managed Identity Principal ID: $ADF_PRINCIPAL_ID"

# Grant Key Vault access to ADF
echo "Granting Key Vault access to ADF Managed Identity..."
az keyvault set-policy \
    --name $KEY_VAULT_NAME \
    --object-id $ADF_PRINCIPAL_ID \
    --secret-permissions get list

# Create sample secrets
echo "Creating sample secrets..."

az keyvault secret set \
    --vault-name $KEY_VAULT_NAME \
    --name "sql-connection-string" \
    --value "Server=tcp:your-server.database.windows.net,1433;Initial Catalog=your-database;Persist Security Info=False;User ID=your-username;Password=your-password;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"

az keyvault secret set \
    --vault-name $KEY_VAULT_NAME \
    --name "storage-account-key" \
    --value "your-storage-account-key"

az keyvault secret set \
    --vault-name $KEY_VAULT_NAME \
    --name "external-api-key" \
    --value "your-api-key-value"

# Output configuration
echo ""
echo "=== Configuration Summary ==="
echo "Resource Group: $RESOURCE_GROUP"
echo "Key Vault Name: $KEY_VAULT_NAME"
echo "Key Vault URI: https://$KEY_VAULT_NAME.vault.azure.net/"
echo "Data Factory Name: $DATA_FACTORY_NAME"
echo "ADF Managed Identity ID: $ADF_PRINCIPAL_ID"

# Export configuration
cat > adf-keyvault-config.json << EOF
{
    "resourceGroup": "$RESOURCE_GROUP",
    "keyVaultName": "$KEY_VAULT_NAME",
    "keyVaultUri": "https://$KEY_VAULT_NAME.vault.azure.net/",
    "dataFactoryName": "$DATA_FACTORY_NAME",
    "adfManagedIdentityId": "$ADF_PRINCIPAL_ID",
    "location": "$LOCATION"
}
EOF

echo "Configuration exported to: adf-keyvault-config.json"
```

---

## ADF Key Vault Integration

### Key Vault Linked Service Configuration

```json
{
  "name": "AzureKeyVaultLinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "AzureKeyVault",
    "typeProperties": {
      "baseUrl": "https://your-keyvault-name.vault.azure.net/"
    },
    "description": "Azure Key Vault linked service for secure secret management",
    "annotations": [
      "production",
      "secrets"
    ]
  }
}
```

### ARM Template for Key Vault Linked Service

```json
{
  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "dataFactoryName": {
      "type": "string",
      "metadata": {
        "description": "Name of the Azure Data Factory"
      }
    },
    "keyVaultName": {
      "type": "string",
      "metadata": {
        "description": "Name of the Azure Key Vault"
      }
    },
    "keyVaultResourceGroup": {
      "type": "string",
      "metadata": {
        "description": "Resource group of the Key Vault"
      }
    }
  },
  "variables": {
    "keyVaultUri": "[concat('https://', parameters('keyVaultName'), '.vault.azure.net/')]"
  },
  "resources": [
    {
      "type": "Microsoft.DataFactory/factories/linkedServices",
      "apiVersion": "2018-06-01",
      "name": "[concat(parameters('dataFactoryName'), '/AzureKeyVaultLinkedService')]",
      "properties": {
        "type": "AzureKeyVault",
        "typeProperties": {
          "baseUrl": "[variables('keyVaultUri')]"
        },
        "description": "Azure Key Vault linked service for secure secret management"
      }
    }
  ],
  "outputs": {
    "keyVaultLinkedServiceName": {
      "type": "string",
      "value": "AzureKeyVaultLinkedService"
    },
    "keyVaultUri": {
      "type": "string",
      "value": "[variables('keyVaultUri')]"
    }
  }
}
```

### PowerShell Script to Create Key Vault Linked Service

```powershell
# Create Key Vault Linked Service in ADF using PowerShell

# Parameters
$resourceGroupName = "your-resource-group"
$dataFactoryName = "your-data-factory"
$keyVaultName = "your-key-vault"
$linkedServiceName = "AzureKeyVaultLinkedService"

# Key Vault URI
$keyVaultUri = "https://$keyVaultName.vault.azure.net/"

Write-Host "Creating Key Vault Linked Service..." -ForegroundColor Green

# Create the linked service definition
$linkedServiceDefinition = @{
    name = $linkedServiceName
    properties = @{
        type = "AzureKeyVault"
        typeProperties = @{
            baseUrl = $keyVaultUri
        }
        description = "Azure Key Vault linked service for secure secret management"
        annotations = @("production", "secrets")
    }
}

# Convert to JSON
$jsonDefinition = $linkedServiceDefinition | ConvertTo-Json -Depth 10

# Create temporary file
$tempFile = [System.IO.Path]::GetTempFileName()
$jsonDefinition | Out-File -FilePath $tempFile -Encoding UTF8

try {
    # Create the linked service
    Set-AzDataFactoryV2LinkedService `
        -ResourceGroupName $resourceGroupName `
        -DataFactoryName $dataFactoryName `
        -Name $linkedServiceName `
        -File $tempFile

    Write-Host "✓ Key Vault Linked Service created successfully" -ForegroundColor Green
    Write-Host "  Name: $linkedServiceName" -ForegroundColor White
    Write-Host "  Key Vault URI: $keyVaultUri" -ForegroundColor White
}
catch {
    Write-Error "Failed to create Key Vault Linked Service: $_"
}
finally {
    # Clean up temporary file
    Remove-Item -Path $tempFile -Force -ErrorAction SilentlyContinue
}

# Verify the linked service
Write-Host "`nVerifying linked service..." -ForegroundColor Green
$linkedService = Get-AzDataFactoryV2LinkedService `
    -ResourceGroupName $resourceGroupName `
    -DataFactoryName $dataFactoryName `
    -Name $linkedServiceName

if ($linkedService) {
    Write-Host "✓ Linked service verified successfully" -ForegroundColor Green
    Write-Host "  Type: $($linkedService.Properties.Type)" -ForegroundColor White
    Write-Host "  Base URL: $($linkedService.Properties.TypeProperties.baseUrl)" -ForegroundColor White
} else {
    Write-Warning "Linked service verification failed"
}
```

---

## Linked Services Configuration

### SQL Database with Key Vault Secrets

```json
{
  "name": "AzureSqlDatabaseLinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "AzureSqlDatabase",
    "typeProperties": {
      "connectionString": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVaultLinkedService",
          "type": "LinkedServiceReference"
        },
        "secretName": "sql-connection-string"
      }
    },
    "description": "Azure SQL Database with connection string from Key Vault",
    "annotations": [
      "database",
      "secure"
    ]
  }
}
```

### Azure Storage with Key Vault Secrets

```json
{
  "name": "AzureStorageLinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "AzureBlobStorage",
    "typeProperties": {
      "connectionString": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVaultLinkedService",
          "type": "LinkedServiceReference"
        },
        "secretName": "storage-connection-string"
      }
    },
    "description": "Azure Blob Storage with connection string from Key Vault"
  }
}
```

### REST API with Key Vault Authentication

```json
{
  "name": "RestApiLinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "RestService",
    "typeProperties": {
      "url": "https://api.example.com",
      "authenticationType": "ApiKeyAuthentication",
      "apiKey": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVaultLinkedService",
          "type": "LinkedServiceReference"
        },
        "secretName": "external-api-key"
      },
      "headerName": "X-API-Key"
    },
    "description": "REST API service with API key from Key Vault"
  }
}
```

### Comprehensive Linked Service Creation Script

```python
# Python script to create multiple linked services with Key Vault integration
import json
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *

class ADFKeyVaultLinkedServiceManager:
    """Manage ADF Linked Services with Key Vault integration"""
    
    def __init__(self, subscription_id: str, resource_group: str, 
                 data_factory_name: str, key_vault_name: str):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.data_factory_name = data_factory_name
        self.key_vault_name = key_vault_name
        self.key_vault_uri = f"https://{key_vault_name}.vault.azure.net/"
        
        # Initialize ADF client
        self.credential = DefaultAzureCredential()
        self.adf_client = DataFactoryManagementClient(self.credential, subscription_id)
        
        print(f"Initialized ADF Key Vault Manager for:")
        print(f"  Data Factory: {data_factory_name}")
        print(f"  Key Vault: {key_vault_name}")
    
    def create_key_vault_linked_service(self) -> str:
        """Create Key Vault linked service"""
        
        linked_service_name = "AzureKeyVaultLinkedService"
        
        print(f"\n=== Creating Key Vault Linked Service ===")
        
        # Define Key Vault linked service
        key_vault_linked_service = AzureKeyVaultLinkedService(
            base_url=self.key_vault_uri,
            description="Azure Key Vault linked service for secure secret management"
        )
        
        try:
            # Create the linked service
            self.adf_client.linked_services.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                linked_service_name=linked_service_name,
                linked_service=LinkedServiceResource(properties=key_vault_linked_service)
            )
            
            print(f"✓ Key Vault linked service created: {linked_service_name}")
            print(f"  Key Vault URI: {self.key_vault_uri}")
            
            return linked_service_name
            
        except Exception as e:
            print(f"✗ Failed to create Key Vault linked service: {str(e)}")
            raise
    
    def create_sql_database_linked_service(self, service_name: str, secret_name: str) -> str:
        """Create SQL Database linked service using Key Vault secret"""
        
        print(f"\n=== Creating SQL Database Linked Service ===")
        
        # Key Vault reference
        key_vault_reference = LinkedServiceReference(
            reference_name="AzureKeyVaultLinkedService",
            type="LinkedServiceReference"
        )
        
        # Connection string from Key Vault
        connection_string = AzureKeyVaultSecretReference(
            store=key_vault_reference,
            secret_name=secret_name
        )
        
        # SQL Database linked service
        sql_linked_service = AzureSqlDatabaseLinkedService(
            connection_string=connection_string,
            description=f"Azure SQL Database with connection string from Key Vault ({secret_name})"
        )
        
        try:
            # Create the linked service
            self.adf_client.linked_services.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                linked_service_name=service_name,
                linked_service=LinkedServiceResource(properties=sql_linked_service)
            )
            
            print(f"✓ SQL Database linked service created: {service_name}")
            print(f"  Secret reference: {secret_name}")
            
            return service_name
            
        except Exception as e:
            print(f"✗ Failed to create SQL Database linked service: {str(e)}")
            raise
    
    def create_storage_linked_service(self, service_name: str, secret_name: str) -> str:
        """Create Azure Storage linked service using Key Vault secret"""
        
        print(f"\n=== Creating Storage Linked Service ===")
        
        # Key Vault reference
        key_vault_reference = LinkedServiceReference(
            reference_name="AzureKeyVaultLinkedService",
            type="LinkedServiceReference"
        )
        
        # Connection string from Key Vault
        connection_string = AzureKeyVaultSecretReference(
            store=key_vault_reference,
            secret_name=secret_name
        )
        
        # Storage linked service
        storage_linked_service = AzureBlobStorageLinkedService(
            connection_string=connection_string,
            description=f"Azure Blob Storage with connection string from Key Vault ({secret_name})"
        )
        
        try:
            # Create the linked service
            self.adf_client.linked_services.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                linked_service_name=service_name,
                linked_service=LinkedServiceResource(properties=storage_linked_service)
            )
            
            print(f"✓ Storage linked service created: {service_name}")
            print(f"  Secret reference: {secret_name}")
            
            return service_name
            
        except Exception as e:
            print(f"✗ Failed to create Storage linked service: {str(e)}")
            raise
    
    def create_rest_api_linked_service(self, service_name: str, base_url: str, 
                                     api_key_secret: str, header_name: str = "X-API-Key") -> str:
        """Create REST API linked service using Key Vault for API key"""
        
        print(f"\n=== Creating REST API Linked Service ===")
        
        # Key Vault reference
        key_vault_reference = LinkedServiceReference(
            reference_name="AzureKeyVaultLinkedService",
            type="LinkedServiceReference"
        )
        
        # API key from Key Vault
        api_key = AzureKeyVaultSecretReference(
            store=key_vault_reference,
            secret_name=api_key_secret
        )
        
        # REST API linked service
        rest_linked_service = RestServiceLinkedService(
            url=base_url,
            authentication_type="ApiKeyAuthentication",
            api_key=api_key,
            header_name=header_name,
            description=f"REST API service with API key from Key Vault ({api_key_secret})"
        )
        
        try:
            # Create the linked service
            self.adf_client.linked_services.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                linked_service_name=service_name,
                linked_service=LinkedServiceResource(properties=rest_linked_service)
            )
            
            print(f"✓ REST API linked service created: {service_name}")
            print(f"  Base URL: {base_url}")
            print(f"  API Key Secret: {api_key_secret}")
            
            return service_name
            
        except Exception as e:
            print(f"✗ Failed to create REST API linked service: {str(e)}")
            raise
    
    def create_cosmos_db_linked_service(self, service_name: str, secret_name: str) -> str:
        """Create Cosmos DB linked service using Key Vault secret"""
        
        print(f"\n=== Creating Cosmos DB Linked Service ===")
        
        # Key Vault reference
        key_vault_reference = LinkedServiceReference(
            reference_name="AzureKeyVaultLinkedService",
            type="LinkedServiceReference"
        )
        
        # Connection string from Key Vault
        connection_string = AzureKeyVaultSecretReference(
            store=key_vault_reference,
            secret_name=secret_name
        )
        
        # Cosmos DB linked service
        cosmos_linked_service = CosmosDbLinkedService(
            connection_string=connection_string,
            description=f"Cosmos DB with connection string from Key Vault ({secret_name})"
        )
        
        try:
            # Create the linked service
            self.adf_client.linked_services.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                linked_service_name=service_name,
                linked_service=LinkedServiceResource(properties=cosmos_linked_service)
            )
            
            print(f"✓ Cosmos DB linked service created: {service_name}")
            print(f"  Secret reference: {secret_name}")
            
            return service_name
            
        except Exception as e:
            print(f"✗ Failed to create Cosmos DB linked service: {str(e)}")
            raise
    
    def create_all_linked_services(self) -> dict:
        """Create all linked services with Key Vault integration"""
        
        print("=== Creating All Linked Services with Key Vault Integration ===")
        
        created_services = {}
        
        try:
            # 1. Create Key Vault linked service first
            kv_service = self.create_key_vault_linked_service()
            created_services["key_vault"] = kv_service
            
            # 2. Create SQL Database linked services
            sql_services = [
                ("AzureSqlDatabase_Dev", "sql-connection-string-dev"),
                ("AzureSqlDatabase_Test", "sql-connection-string-test"),
                ("AzureSqlDatabase_Prod", "sql-connection-string-prod")
            ]
            
            for service_name, secret_name in sql_services:
                try:
                    service = self.create_sql_database_linked_service(service_name, secret_name)
                    created_services[service_name] = service
                except Exception as e:
                    print(f"Warning: Failed to create {service_name}: {str(e)}")
            
            # 3. Create Storage linked service
            try:
                storage_service = self.create_storage_linked_service(
                    "AzureStorage_Secure", 
                    "storage-connection-string"
                )
                created_services["storage"] = storage_service
            except Exception as e:
                print(f"Warning: Failed to create storage service: {str(e)}")
            
            # 4. Create REST API linked service
            try:
                rest_service = self.create_rest_api_linked_service(
                    "ExternalAPI_Secure",
                    "https://api.example.com",
                    "external-api-key"
                )
                created_services["rest_api"] = rest_service
            except Exception as e:
                print(f"Warning: Failed to create REST API service: {str(e)}")
            
            # 5. Create Cosmos DB linked service
            try:
                cosmos_service = self.create_cosmos_db_linked_service(
                    "CosmosDB_Secure",
                    "cosmos-connection-string"
                )
                created_services["cosmos_db"] = cosmos_service
            except Exception as e:
                print(f"Warning: Failed to create Cosmos DB service: {str(e)}")
            
            print(f"\n=== Summary ===")
            print(f"Successfully created {len(created_services)} linked services:")
            for key, service in created_services.items():
                print(f"  ✓ {key}: {service}")
            
            return created_services
            
        except Exception as e:
            print(f"✗ Failed to create linked services: {str(e)}")
            raise
    
    def test_linked_services(self) -> dict:
        """Test all linked services"""
        
        print("\n=== Testing Linked Services ===")
        
        test_results = {}
        
        # Get all linked services
        linked_services = self.adf_client.linked_services.list_by_factory(
            resource_group_name=self.resource_group,
            factory_name=self.data_factory_name
        )
        
        for service in linked_services:
            service_name = service.name
            service_type = service.properties.type
            
            print(f"\nTesting: {service_name} ({service_type})")
            
            try:
                # Get detailed service information
                service_detail = self.adf_client.linked_services.get(
                    resource_group_name=self.resource_group,
                    factory_name=self.data_factory_name,
                    linked_service_name=service_name
                )
                
                test_results[service_name] = {
                    "status": "accessible",
                    "type": service_type,
                    "description": getattr(service_detail.properties, 'description', ''),
                    "has_key_vault_reference": self.has_key_vault_reference(service_detail.properties)
                }
                
                print(f"  ✓ Service accessible")
                print(f"  ✓ Type: {service_type}")
                
                if test_results[service_name]["has_key_vault_reference"]:
                    print(f"  ✓ Uses Key Vault secrets")
                else:
                    print(f"  ℹ No Key Vault references found")
                
            except Exception as e:
                test_results[service_name] = {
                    "status": "error",
                    "error": str(e)
                }
                print(f"  ✗ Error: {str(e)}")
        
        return test_results
    
    def has_key_vault_reference(self, service_properties) -> bool:
        """Check if service uses Key Vault references"""
        
        # Convert to dictionary for easier inspection
        props_dict = service_properties.as_dict() if hasattr(service_properties, 'as_dict') else {}
        props_str = json.dumps(props_dict, default=str)
        
        # Look for Key Vault reference patterns
        return "AzureKeyVaultSecret" in props_str or "AzureKeyVaultLinkedService" in props_str

# Example usage
if __name__ == "__main__":
    # Configuration
    subscription_id = "your-subscription-id"
    resource_group = "your-resource-group"
    data_factory_name = "your-data-factory"
    key_vault_name = "your-key-vault"
    
    # Initialize manager
    manager = ADFKeyVaultLinkedServiceManager(
        subscription_id=subscription_id,
        resource_group=resource_group,
        data_factory_name=data_factory_name,
        key_vault_name=key_vault_name
    )
    
    # Create all linked services
    created_services = manager.create_all_linked_services()
    
    # Test linked services
    test_results = manager.test_linked_services()
    
    print("\n=== Final Summary ===")
    print(f"Created Services: {len(created_services)}")
    print(f"Test Results: {len(test_results)}")
```

---

## Secret Management in Pipelines

### Pipeline with Key Vault Parameter

```json
{
  "name": "SecureDataProcessingPipeline",
  "properties": {
    "activities": [
      {
        "name": "CopySecureData",
        "type": "Copy",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT * FROM customers WHERE created_date >= '@{pipeline().parameters.startDate}'"
          },
          "sink": {
            "type": "AzureBlobSink"
          }
        },
        "inputs": [
          {
            "referenceName": "SourceSqlDataset",
            "type": "DatasetReference"
          }
        ],
        "outputs": [
          {
            "referenceName": "SinkBlobDataset", 
            "type": "DatasetReference"
          }
        ],
        "linkedServiceName": {
          "referenceName": "AzureSqlDatabaseLinkedService",
          "type": "LinkedServiceReference"
        }
      },
      {
        "name": "CallSecureAPI",
        "type": "WebActivity",
        "typeProperties": {
          "url": "https://api.example.com/data",
          "method": "POST",
          "headers": {
            "Content-Type": "application/json"
          },
          "body": {
            "query": "SELECT * FROM products",
            "format": "json"
          },
          "authentication": {
            "type": "MSI"
          }
        },
        "linkedServiceName": {
          "referenceName": "RestApiLinkedService",
          "type": "LinkedServiceReference"
        }
      }
    ],
    "parameters": {
      "startDate": {
        "type": "String",
        "defaultValue": "2023-01-01"
      }
    }
  }
}
```

### Dynamic Secret Selection Pipeline

```json
{
  "name": "EnvironmentAwarePipeline",
  "properties": {
    "activities": [
      {
        "name": "DetermineEnvironment",
        "type": "SetVariable",
        "typeProperties": {
          "variableName": "environmentSuffix",
          "value": {
            "value": "@if(equals(pipeline().globalParameters.environment, 'prod'), 'prod', if(equals(pipeline().globalParameters.environment, 'test'), 'test', 'dev'))",
            "type": "Expression"
          }
        }
      },
      {
        "name": "CopyDataWithEnvironmentSecret",
        "type": "Copy",
        "dependsOn": [
          {
            "activity": "DetermineEnvironment",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT * FROM orders"
          },
          "sink": {
            "type": "AzureBlobSink"
          }
        },
        "inputs": [
          {
            "referenceName": "DynamicSqlDataset",
            "type": "DatasetReference",
            "parameters": {
              "environment": "@variables('environmentSuffix')"
            }
          }
        ],
        "outputs": [
          {
            "referenceName": "OutputBlobDataset",
            "type": "DatasetReference"
          }
        ]
      }
    ],
    "variables": {
      "environmentSuffix": {
        "type": "String"
      }
    }
  }
}
```

### Python Script for Pipeline Creation with Key Vault

```python
# Create ADF Pipeline with Key Vault integration
from azure.mgmt.datafactory.models import *

class SecurePipelineManager:
    """Create and manage secure ADF pipelines with Key Vault integration"""
    
    def __init__(self, adf_client, resource_group: str, data_factory_name: str):
        self.adf_client = adf_client
        self.resource_group = resource_group
        self.data_factory_name = data_factory_name
    
    def create_secure_copy_pipeline(self, pipeline_name: str) -> str:
        """Create a secure copy pipeline using Key Vault secrets"""
        
        print(f"=== Creating Secure Copy Pipeline: {pipeline_name} ===")
        
        # Copy activity with secure linked service
        copy_activity = CopyActivity(
            name="SecureCopyActivity",
            source=AzureSqlSource(
                sql_reader_query="SELECT * FROM customers WHERE active = 1"
            ),
            sink=AzureBlobSink(),
            inputs=[
                DatasetReference(
                    reference_name="SecureSqlDataset",
                    type="DatasetReference"
                )
            ],
            outputs=[
                DatasetReference(
                    reference_name="SecureBlobDataset",
                    type="DatasetReference"
                )
            ]
        )
        
        # Web activity with secure API call
        web_activity = WebActivity(
            name="SecureAPICall",
            method="GET",
            url="https://api.example.com/status",
            headers={
                "Content-Type": "application/json"
            },
            linked_service_name=LinkedServiceReference(
                reference_name="RestApiLinkedService",
                type="LinkedServiceReference"
            )
        )
        
        # Set dependency
        web_activity.depends_on = [
            ActivityDependency(
                activity=copy_activity.name,
                dependency_conditions=["Succeeded"]
            )
        ]
        
        # Pipeline definition
        pipeline = PipelineResource(
            activities=[copy_activity, web_activity],
            parameters={
                "processingDate": ParameterSpecification(
                    type="String",
                    default_value="@utcnow('yyyy-MM-dd')"
                )
            },
            description="Secure data processing pipeline using Key Vault secrets"
        )
        
        try:
            # Create the pipeline
            self.adf_client.pipelines.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                pipeline_name=pipeline_name,
                pipeline=pipeline
            )
            
            print(f"✓ Secure pipeline created: {pipeline_name}")
            return pipeline_name
            
        except Exception as e:
            print(f"✗ Failed to create pipeline: {str(e)}")
            raise
    
    def create_environment_aware_pipeline(self, pipeline_name: str) -> str:
        """Create pipeline that dynamically selects secrets based on environment"""
        
        print(f"=== Creating Environment-Aware Pipeline: {pipeline_name} ===")
        
        # Set variable activity to determine environment
        set_env_activity = SetVariableActivity(
            name="DetermineEnvironment",
            variable_name="environmentSuffix",
            value="@if(equals(pipeline().globalParameters.environment, 'prod'), 'prod', if(equals(pipeline().globalParameters.environment, 'test'), 'test', 'dev'))"
        )
        
        # Copy activity with dynamic linked service reference
        dynamic_copy_activity = CopyActivity(
            name="EnvironmentAwareCopy",
            source=AzureSqlSource(
                sql_reader_query="SELECT * FROM products WHERE category = '@{pipeline().parameters.category}'"
            ),
            sink=AzureBlobSink(),
            inputs=[
                DatasetReference(
                    reference_name="DynamicSqlDataset",
                    type="DatasetReference",
                    parameters={
                        "environment": "@variables('environmentSuffix')"
                    }
                )
            ],
            outputs=[
                DatasetReference(
                    reference_name="OutputBlobDataset",
                    type="DatasetReference"
                )
            ],
            depends_on=[
                ActivityDependency(
                    activity=set_env_activity.name,
                    dependency_conditions=["Succeeded"]
                )
            ]
        )
        
        # Pipeline definition
        pipeline = PipelineResource(
            activities=[set_env_activity, dynamic_copy_activity],
            parameters={
                "category": ParameterSpecification(
                    type="String",
                    default_value="electronics"
                )
            },
            variables={
                "environmentSuffix": VariableSpecification(type="String")
            },
            description="Environment-aware pipeline with dynamic secret selection"
        )
        
        try:
            # Create the pipeline
            self.adf_client.pipelines.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                pipeline_name=pipeline_name,
                pipeline=pipeline
            )
            
            print(f"✓ Environment-aware pipeline created: {pipeline_name}")
            return pipeline_name
            
        except Exception as e:
            print(f"✗ Failed to create environment-aware pipeline: {str(e)}")
            raise
    
    def create_monitoring_pipeline(self, pipeline_name: str) -> str:
        """Create pipeline that monitors Key Vault secret usage"""
        
        print(f"=== Creating Key Vault Monitoring Pipeline: {pipeline_name} ===")
        
        # Web activity to check Key Vault health
        kv_health_check = WebActivity(
            name="CheckKeyVaultHealth",
            method="GET",
            url="https://management.azure.com/subscriptions/@{pipeline().globalParameters.subscriptionId}/resourceGroups/@{pipeline().globalParameters.resourceGroup}/providers/Microsoft.KeyVault/vaults/@{pipeline().globalParameters.keyVaultName}/secrets?api-version=2019-09-01",
            headers={
                "Content-Type": "application/json"
            },
            authentication={
                "type": "MSI"
            }
        )
        
        # Stored procedure activity to log secret usage
        log_usage_activity = SqlServerStoredProcedureActivity(
            name="LogSecretUsage",
            stored_procedure_name="sp_LogKeyVaultUsage",
            stored_procedure_parameters={
                "PipelineName": {
                    "value": "@pipeline().Pipeline",
                    "type": "String"
                },
                "RunId": {
                    "value": "@pipeline().RunId",
                    "type": "String"
                },
                "Timestamp": {
                    "value": "@utcnow()",
                    "type": "DateTime"
                },
                "KeyVaultChecked": {
                    "value": "@activity('CheckKeyVaultHealth').output.value",
                    "type": "String"
                }
            },
            linked_service_name=LinkedServiceReference(
                reference_name="AzureSqlDatabaseLinkedService",
                type="LinkedServiceReference"
            ),
            depends_on=[
                ActivityDependency(
                    activity=kv_health_check.name,
                    dependency_conditions=["Succeeded"]
                )
            ]
        )
        
        # Pipeline definition
        pipeline = PipelineResource(
            activities=[kv_health_check, log_usage_activity],
            description="Pipeline to monitor Key Vault secret usage and health"
        )
        
        try:
            # Create the pipeline
            self.adf_client.pipelines.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.data_factory_name,
                pipeline_name=pipeline_name,
                pipeline=pipeline
            )
            
            print(f"✓ Monitoring pipeline created: {pipeline_name}")
            return pipeline_name
            
        except Exception as e:
            print(f"✗ Failed to create monitoring pipeline: {str(e)}")
            raise

# Example usage
# pipeline_manager = SecurePipelineManager(adf_client, resource_group, data_factory_name)
# secure_pipeline = pipeline_manager.create_secure_copy_pipeline("SecureDataProcessing")
# env_pipeline = pipeline_manager.create_environment_aware_pipeline("EnvironmentAwarePipeline")
# monitor_pipeline = pipeline_manager.create_monitoring_pipeline("KeyVaultMonitoring")
```

This comprehensive guide provides everything needed to securely integrate Azure Key Vault with Azure Data Factory, ensuring proper secret management and security best practices across your data pipelines.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*