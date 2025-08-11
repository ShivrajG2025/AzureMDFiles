# Azure Data Factory: Global & Local Parameters - Comprehensive Guide

## Table of Contents
1. [Overview](#overview)
2. [Global Parameters](#global-parameters)
3. [Local Parameters](#local-parameters)
4. [Parameter Expressions and Functions](#parameter-expressions-and-functions)
5. [Parameter Passing Between Components](#parameter-passing-between-components)
6. [Environment Management](#environment-management)
7. [Security and Key Vault Integration](#security-and-key-vault-integration)
8. [Best Practices](#best-practices)
9. [Real-World Examples](#real-world-examples)
10. [Troubleshooting](#troubleshooting)
11. [Advanced Scenarios](#advanced-scenarios)

---

## Overview

Azure Data Factory (ADF) parameters provide a flexible way to make your data pipelines dynamic and reusable across different environments and scenarios. Parameters allow you to:

- **Create Reusable Pipelines**: Write once, use across multiple environments
- **Dynamic Configuration**: Change behavior without modifying pipeline code
- **Environment Separation**: Maintain different configurations for dev/test/prod
- **Security**: Store sensitive values in Azure Key Vault
- **Flexibility**: Pass values between activities and pipelines

### Parameter Types in ADF

| Parameter Type | Scope | Definition Location | Usage |
|---------------|-------|-------------------|-------|
| **Global Parameters** | Data Factory | Factory level | Shared across all pipelines |
| **Pipeline Parameters** | Pipeline | Pipeline definition | Within specific pipeline |
| **Dataset Parameters** | Dataset | Dataset definition | Dataset configuration |
| **Linked Service Parameters** | Linked Service | Linked service definition | Connection configuration |
| **Activity Parameters** | Activity | Activity definition | Activity-specific settings |

---

## Global Parameters

Global parameters are factory-level parameters that can be used across all pipelines, datasets, and linked services within a Data Factory instance.

### Defining Global Parameters

#### Via Azure Portal
1. Navigate to your Data Factory instance
2. Go to **Manage** → **Global parameters**
3. Click **+ New** to add parameters
4. Define parameter name, type, and default value

#### Via ARM Template
```json
{
    "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
    "contentVersion": "1.0.0.0",
    "parameters": {
        "factoryName": {
            "type": "string",
            "metadata": "Data Factory name"
        }
    },
    "resources": [
        {
            "type": "Microsoft.DataFactory/factories",
            "apiVersion": "2018-06-01",
            "name": "[parameters('factoryName')]",
            "location": "[resourceGroup().location]",
            "properties": {
                "globalParameters": {
                    "environment": {
                        "type": "string",
                        "value": "development"
                    },
                    "dataLakeStorageAccount": {
                        "type": "string",
                        "value": "devdatalakestorage"
                    },
                    "batchSize": {
                        "type": "int",
                        "value": 1000
                    },
                    "enableLogging": {
                        "type": "bool",
                        "value": true
                    },
                    "processingDate": {
                        "type": "string",
                        "value": "2024-01-01"
                    },
                    "retryCount": {
                        "type": "int",
                        "value": 3
                    },
                    "timeoutMinutes": {
                        "type": "int",
                        "value": 30
                    }
                }
            }
        }
    ]
}
```

### Global Parameter Management Class (PowerShell)
```powershell
# PowerShell class for managing ADF Global Parameters
class ADFGlobalParameterManager {
    [string]$SubscriptionId
    [string]$ResourceGroupName
    [string]$DataFactoryName
    [hashtable]$GlobalParameters
    
    ADFGlobalParameterManager([string]$subscriptionId, [string]$resourceGroupName, [string]$dataFactoryName) {
        $this.SubscriptionId = $subscriptionId
        $this.ResourceGroupName = $resourceGroupName
        $this.DataFactoryName = $dataFactoryName
        $this.GlobalParameters = @{}
    }
    
    [void]AddParameter([string]$name, [string]$type, [object]$value) {
        $this.GlobalParameters[$name] = @{
            type = $type
            value = $value
        }
        Write-Host "✅ Added global parameter: $name = $value ($type)"
    }
    
    [void]UpdateParameter([string]$name, [object]$newValue) {
        if ($this.GlobalParameters.ContainsKey($name)) {
            $this.GlobalParameters[$name].value = $newValue
            Write-Host "🔄 Updated global parameter: $name = $newValue"
        } else {
            Write-Warning "⚠️ Parameter '$name' not found"
        }
    }
    
    [hashtable]GetAllParameters() {
        return $this.GlobalParameters
    }
    
    [void]DeployGlobalParameters() {
        Write-Host "🚀 Deploying global parameters to ADF: $($this.DataFactoryName)"
        
        $factory = Get-AzDataFactoryV2 -ResourceGroupName $this.ResourceGroupName -Name $this.DataFactoryName
        
        foreach ($paramName in $this.GlobalParameters.Keys) {
            $param = $this.GlobalParameters[$paramName]
            
            try {
                Set-AzDataFactoryV2GlobalParameter -ResourceGroupName $this.ResourceGroupName `
                    -DataFactoryName $this.DataFactoryName `
                    -Name $paramName `
                    -Value $param.value `
                    -Type $param.type
                
                Write-Host "✅ Successfully deployed parameter: $paramName"
            }
            catch {
                Write-Error "❌ Failed to deploy parameter $paramName`: $_"
            }
        }
    }
    
    [void]ExportToJson([string]$filePath) {
        $exportData = @{
            dataFactory = $this.DataFactoryName
            globalParameters = $this.GlobalParameters
            exportDate = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        }
        
        $exportData | ConvertTo-Json -Depth 10 | Out-File -FilePath $filePath -Encoding UTF8
        Write-Host "📄 Global parameters exported to: $filePath"
    }
    
    [void]ImportFromJson([string]$filePath) {
        if (Test-Path $filePath) {
            $importData = Get-Content -Path $filePath | ConvertFrom-Json
            
            foreach ($paramName in $importData.globalParameters.PSObject.Properties.Name) {
                $param = $importData.globalParameters.$paramName
                $this.GlobalParameters[$paramName] = @{
                    type = $param.type
                    value = $param.value
                }
            }
            
            Write-Host "📥 Global parameters imported from: $filePath"
        } else {
            Write-Error "❌ File not found: $filePath"
        }
    }
}

# Example usage
$paramManager = [ADFGlobalParameterManager]::new(
    "your-subscription-id",
    "your-resource-group",
    "your-data-factory-name"
)

# Add global parameters
$paramManager.AddParameter("environment", "string", "production")
$paramManager.AddParameter("dataLakeAccount", "string", "proddatalake")
$paramManager.AddParameter("batchSize", "int", 5000)
$paramManager.AddParameter("enableDebug", "bool", $false)
$paramManager.AddParameter("processingDate", "string", "2024-01-15")

# Export configuration
$paramManager.ExportToJson("C:\ADF\global-parameters-prod.json")

# Deploy to ADF
$paramManager.DeployGlobalParameters()
```

### Using Global Parameters in Pipelines

#### Pipeline JSON with Global Parameter References
```json
{
    "name": "DataProcessingPipeline",
    "properties": {
        "activities": [
            {
                "name": "CopyDataActivity",
                "type": "Copy",
                "typeProperties": {
                    "source": {
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                            "type": "AzureBlobFSReadSettings",
                            "recursive": true,
                            "wildcardFolderPath": {
                                "value": "@concat('raw-data/', pipeline().globalParameters.environment, '/', formatDateTime(pipeline().globalParameters.processingDate, 'yyyy/MM/dd'))",
                                "type": "Expression"
                            }
                        }
                    },
                    "sink": {
                        "type": "DelimitedTextSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings",
                            "copyBehavior": "PreserveHierarchy"
                        }
                    },
                    "enableStaging": false,
                    "dataIntegrationUnits": {
                        "value": "@pipeline().globalParameters.batchSize",
                        "type": "Expression"
                    }
                },
                "inputs": [
                    {
                        "referenceName": "SourceDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "storageAccount": {
                                "value": "@pipeline().globalParameters.dataLakeStorageAccount",
                                "type": "Expression"
                            },
                            "environment": {
                                "value": "@pipeline().globalParameters.environment",
                                "type": "Expression"
                            }
                        }
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "SinkDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "storageAccount": {
                                "value": "@pipeline().globalParameters.dataLakeStorageAccount",
                                "type": "Expression"
                            }
                        }
                    }
                ]
            },
            {
                "name": "LoggingActivity",
                "type": "WebActivity",
                "dependsOn": [
                    {
                        "activity": "CopyDataActivity",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "url": {
                        "value": "@concat('https://logging-service.com/api/log?environment=', pipeline().globalParameters.environment)",
                        "type": "Expression"
                    },
                    "method": "POST",
                    "body": {
                        "value": "@concat('{\"pipeline\":\"', pipeline().Pipeline, '\",\"status\":\"completed\",\"environment\":\"', pipeline().globalParameters.environment, '\",\"processingDate\":\"', pipeline().globalParameters.processingDate, '\",\"recordsProcessed\":', activity('CopyDataActivity').output.rowsCopied, '}')",
                        "type": "Expression"
                    },
                    "headers": {
                        "Content-Type": "application/json"
                    }
                }
            }
        ],
        "parameters": {
            "customBatchSize": {
                "type": "int",
                "defaultValue": {
                    "value": "@pipeline().globalParameters.batchSize",
                    "type": "Expression"
                }
            }
        },
        "variables": {
            "currentEnvironment": {
                "type": "String",
                "defaultValue": {
                    "value": "@pipeline().globalParameters.environment",
                    "type": "Expression"
                }
            }
        }
    }
}
```

---

## Local Parameters

Local parameters are defined at specific component levels (pipeline, dataset, linked service) and have scope limited to that component.

### Pipeline Parameters

#### Basic Pipeline Parameter Definition
```json
{
    "name": "FlexibleETLPipeline",
    "properties": {
        "parameters": {
            "sourceContainer": {
                "type": "string",
                "defaultValue": "raw-data"
            },
            "targetContainer": {
                "type": "string",
                "defaultValue": "processed-data"
            },
            "processingDate": {
                "type": "string",
                "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
            },
            "batchSize": {
                "type": "int",
                "defaultValue": 1000
            },
            "enableValidation": {
                "type": "bool",
                "defaultValue": true
            },
            "filePattern": {
                "type": "string",
                "defaultValue": "*.csv"
            },
            "compressionType": {
                "type": "string",
                "defaultValue": "none"
            }
        },
        "activities": [
            {
                "name": "ValidateInputData",
                "type": "Validation",
                "typeProperties": {
                    "dataset": {
                        "referenceName": "InputDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "containerName": {
                                "value": "@pipeline().parameters.sourceContainer",
                                "type": "Expression"
                            },
                            "fileName": {
                                "value": "@pipeline().parameters.filePattern",
                                "type": "Expression"
                            }
                        }
                    },
                    "timeout": "00:10:00",
                    "sleep": 10,
                    "minimumSize": 1
                },
                "policy": {
                    "retry": 3,
                    "retryIntervalInSeconds": 30
                }
            },
            {
                "name": "ProcessData",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "ValidateInputData",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "source": {
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                            "type": "AzureBlobFSReadSettings",
                            "recursive": true,
                            "wildcardFileName": {
                                "value": "@pipeline().parameters.filePattern",
                                "type": "Expression"
                            }
                        }
                    },
                    "sink": {
                        "type": "DelimitedTextSink",
                        "storeSettings": {
                            "type": "AzureBlobFSWriteSettings",
                            "copyBehavior": "PreserveHierarchy"
                        }
                    },
                    "dataIntegrationUnits": {
                        "value": "@pipeline().parameters.batchSize",
                        "type": "Expression"
                    }
                }
            }
        ]
    }
}
```

### Dataset Parameters

#### Parameterized Dataset Definition
```json
{
    "name": "ParameterizedCSVDataset",
    "properties": {
        "linkedServiceName": {
            "referenceName": "AzureDataLakeStorage",
            "type": "LinkedServiceReference",
            "parameters": {
                "storageAccountName": {
                    "value": "@dataset().storageAccount",
                    "type": "Expression"
                }
            }
        },
        "parameters": {
            "storageAccount": {
                "type": "string",
                "defaultValue": "defaultstorage"
            },
            "containerName": {
                "type": "string",
                "defaultValue": "data"
            },
            "folderPath": {
                "type": "string",
                "defaultValue": "raw"
            },
            "fileName": {
                "type": "string",
                "defaultValue": "data.csv"
            },
            "columnDelimiter": {
                "type": "string",
                "defaultValue": ","
            },
            "encoding": {
                "type": "string",
                "defaultValue": "UTF-8"
            },
            "firstRowAsHeader": {
                "type": "bool",
                "defaultValue": true
            }
        },
        "type": "DelimitedText",
        "typeProperties": {
            "location": {
                "type": "AzureBlobFSLocation",
                "container": {
                    "value": "@dataset().containerName",
                    "type": "Expression"
                },
                "folderPath": {
                    "value": "@dataset().folderPath",
                    "type": "Expression"
                },
                "fileName": {
                    "value": "@dataset().fileName",
                    "type": "Expression"
                }
            },
            "columnDelimiter": {
                "value": "@dataset().columnDelimiter",
                "type": "Expression"
            },
            "encoding": {
                "value": "@dataset().encoding",
                "type": "Expression"
            },
            "firstRowAsHeader": {
                "value": "@dataset().firstRowAsHeader",
                "type": "Expression"
            }
        },
        "schema": []
    }
}
```

### Linked Service Parameters

#### Parameterized SQL Server Linked Service
```json
{
    "name": "ParameterizedSQLServer",
    "properties": {
        "parameters": {
            "serverName": {
                "type": "string",
                "defaultValue": "localhost"
            },
            "databaseName": {
                "type": "string",
                "defaultValue": "DefaultDB"
            },
            "authenticationType": {
                "type": "string",
                "defaultValue": "SQL"
            },
            "userName": {
                "type": "string",
                "defaultValue": "sa"
            },
            "passwordKeyVaultSecret": {
                "type": "string",
                "defaultValue": "sql-server-password"
            }
        },
        "type": "SqlServer",
        "typeProperties": {
            "connectionString": {
                "value": "@concat('Server=', linkedService().serverName, ';Database=', linkedService().databaseName, ';User ID=', linkedService().userName, ';')",
                "type": "Expression"
            },
            "password": {
                "type": "AzureKeyVaultSecret",
                "store": {
                    "referenceName": "AzureKeyVault",
                    "type": "LinkedServiceReference"
                },
                "secretName": {
                    "value": "@linkedService().passwordKeyVaultSecret",
                    "type": "Expression"
                }
            }
        }
    }
}
```

---

## Parameter Expressions and Functions

### Common ADF Functions for Parameters

#### String Functions
```json
{
    "expressions": {
        "concatenation": "@concat('prefix-', parameters('value'), '-suffix')",
        "substring": "@substring(parameters('fullString'), 0, 10)",
        "replace": "@replace(parameters('text'), 'old', 'new')",
        "split": "@split(parameters('csvString'), ',')",
        "toLower": "@toLower(parameters('upperCaseText'))",
        "toUpper": "@toUpper(parameters('lowerCaseText'))",
        "trim": "@trim(parameters('textWithSpaces'))",
        "length": "@length(parameters('textString'))",
        "indexOf": "@indexOf(parameters('text'), 'search')",
        "guid": "@guid()",
        "base64": "@base64(parameters('textToEncode'))",
        "base64ToString": "@base64ToString(parameters('encodedText'))"
    }
}
```

#### Date/Time Functions
```json
{
    "dateTimeExpressions": {
        "currentDateTime": "@utcnow()",
        "currentDateOnly": "@formatDateTime(utcnow(), 'yyyy-MM-dd')",
        "addDays": "@addDays(utcnow(), -7)",
        "addHours": "@addHours(utcnow(), -2)",
        "formatDateTime": "@formatDateTime(parameters('dateValue'), 'yyyy-MM-dd HH:mm:ss')",
        "dayOfWeek": "@dayOfWeek(parameters('dateValue'))",
        "dayOfMonth": "@dayOfMonth(parameters('dateValue'))",
        "dayOfYear": "@dayOfYear(parameters('dateValue'))",
        "year": "@formatDateTime(parameters('dateValue'), 'yyyy')",
        "month": "@formatDateTime(parameters('dateValue'), 'MM')",
        "startOfDay": "@startOfDay(parameters('dateValue'))",
        "startOfMonth": "@startOfMonth(parameters('dateValue'))",
        "getPastTime": "@getPastTime(7, 'Day', 'yyyy-MM-dd')",
        "getFutureTime": "@getFutureTime(1, 'Month', 'yyyy-MM-dd')"
    }
}
```

#### Mathematical Functions
```json
{
    "mathExpressions": {
        "addition": "@add(parameters('value1'), parameters('value2'))",
        "subtraction": "@sub(parameters('value1'), parameters('value2'))",
        "multiplication": "@mul(parameters('value1'), parameters('value2'))",
        "division": "@div(parameters('value1'), parameters('value2'))",
        "modulo": "@mod(parameters('value1'), parameters('value2'))",
        "maximum": "@max(parameters('value1'), parameters('value2'))",
        "minimum": "@min(parameters('value1'), parameters('value2'))",
        "random": "@rand(1, 100)",
        "range": "@range(0, parameters('count'))"
    }
}
```

#### Logical Functions
```json
{
    "logicalExpressions": {
        "ifCondition": "@if(equals(parameters('environment'), 'prod'), 'production-config', 'development-config')",
        "equals": "@equals(parameters('value1'), parameters('value2'))",
        "greater": "@greater(parameters('value1'), parameters('value2'))",
        "less": "@less(parameters('value1'), parameters('value2'))",
        "and": "@and(equals(parameters('env'), 'prod'), equals(parameters('region'), 'us'))",
        "or": "@or(equals(parameters('type'), 'full'), equals(parameters('type'), 'incremental'))",
        "not": "@not(equals(parameters('skipProcessing'), true))",
        "empty": "@empty(parameters('optionalValue'))",
        "coalesce": "@coalesce(parameters('primaryValue'), parameters('fallbackValue'), 'default')"
    }
}
```

### Advanced Parameter Expression Examples

#### Dynamic Path Construction
```json
{
    "name": "DynamicPathExample",
    "properties": {
        "parameters": {
            "environment": {"type": "string"},
            "year": {"type": "string"},
            "month": {"type": "string"},
            "day": {"type": "string"},
            "dataType": {"type": "string"}
        },
        "variables": {
            "dynamicPath": {
                "type": "String",
                "defaultValue": {
                    "value": "@concat(\n    parameters('environment'), '/',\n    parameters('dataType'), '/',\n    'year=', parameters('year'), '/',\n    'month=', parameters('month'), '/',\n    'day=', parameters('day')\n)",
                    "type": "Expression"
                }
            },
            "partitionedPath": {
                "type": "String",
                "defaultValue": {
                    "value": "@concat(\n    'data/',\n    parameters('environment'), '/',\n    formatDateTime(utcnow(), 'yyyy'), '/',\n    formatDateTime(utcnow(), 'MM'), '/',\n    formatDateTime(utcnow(), 'dd'), '/',\n    parameters('dataType')\n)",
                    "type": "Expression"
                }
            }
        }
    }
}
```

#### Conditional Processing Logic
```json
{
    "name": "ConditionalProcessingActivity",
    "type": "IfCondition",
    "typeProperties": {
        "expression": {
            "value": "@and(\n    equals(pipeline().parameters.environment, 'production'),\n    greater(int(pipeline().parameters.recordCount), 1000)\n)",
            "type": "Expression"
        },
        "ifTrueActivities": [
            {
                "name": "HighVolumeProcessing",
                "type": "Copy",
                "typeProperties": {
                    "dataIntegrationUnits": {
                        "value": "@mul(int(pipeline().parameters.recordCount), 2)",
                        "type": "Expression"
                    }
                }
            }
        ],
        "ifFalseActivities": [
            {
                "name": "StandardProcessing",
                "type": "Copy",
                "typeProperties": {
                    "dataIntegrationUnits": {
                        "value": "@pipeline().globalParameters.batchSize",
                        "type": "Expression"
                    }
                }
            }
        ]
    }
}
```

---

## Parameter Passing Between Components

### Parent-Child Pipeline Parameter Passing

#### Master Pipeline with Child Pipeline Execution
```json
{
    "name": "MasterDataPipeline",
    "properties": {
        "parameters": {
            "masterExecutionDate": {
                "type": "string",
                "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
            },
            "processingMode": {
                "type": "string",
                "defaultValue": "incremental"
            },
            "dataSourceList": {
                "type": "array",
                "defaultValue": ["sales", "inventory", "customers"]
            }
        },
        "activities": [
            {
                "name": "ProcessEachDataSource",
                "type": "ForEach",
                "typeProperties": {
                    "items": {
                        "value": "@pipeline().parameters.dataSourceList",
                        "type": "Expression"
                    },
                    "isSequential": false,
                    "batchCount": 3,
                    "activities": [
                        {
                            "name": "ExecuteChildPipeline",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "ChildDataProcessingPipeline",
                                    "type": "PipelineReference"
                                },
                                "parameters": {
                                    "dataSource": {
                                        "value": "@item()",
                                        "type": "Expression"
                                    },
                                    "executionDate": {
                                        "value": "@pipeline().parameters.masterExecutionDate",
                                        "type": "Expression"
                                    },
                                    "mode": {
                                        "value": "@pipeline().parameters.processingMode",
                                        "type": "Expression"
                                    },
                                    "parentPipelineRunId": {
                                        "value": "@pipeline().RunId",
                                        "type": "Expression"
                                    },
                                    "globalBatchSize": {
                                        "value": "@pipeline().globalParameters.batchSize",
                                        "type": "Expression"
                                    }
                                },
                                "waitOnCompletion": true
                            }
                        }
                    ]
                }
            },
            {
                "name": "ConsolidateResults",
                "type": "ExecutePipeline",
                "dependsOn": [
                    {
                        "activity": "ProcessEachDataSource",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "pipeline": {
                        "referenceName": "ConsolidationPipeline",
                        "type": "PipelineReference"
                    },
                    "parameters": {
                        "executionDate": {
                            "value": "@pipeline().parameters.masterExecutionDate",
                            "type": "Expression"
                        },
                        "sourceCount": {
                            "value": "@length(pipeline().parameters.dataSourceList)",
                            "type": "Expression"
                        }
                    }
                }
            }
        ]
    }
}
```

#### Child Pipeline Receiving Parameters
```json
{
    "name": "ChildDataProcessingPipeline",
    "properties": {
        "parameters": {
            "dataSource": {"type": "string"},
            "executionDate": {"type": "string"},
            "mode": {"type": "string"},
            "parentPipelineRunId": {"type": "string"},
            "globalBatchSize": {"type": "int"}
        },
        "variables": {
            "sourceTableName": {
                "type": "String",
                "defaultValue": {
                    "value": "@concat(pipeline().parameters.dataSource, '_', replace(pipeline().parameters.executionDate, '-', ''))",
                    "type": "Expression"
                }
            },
            "outputPath": {
                "type": "String", 
                "defaultValue": {
                    "value": "@concat('processed/', pipeline().parameters.dataSource, '/', pipeline().parameters.executionDate)",
                    "type": "Expression"
                }
            }
        },
        "activities": [
            {
                "name": "LogPipelineStart",
                "type": "WebActivity",
                "typeProperties": {
                    "url": "https://logging-api.com/log",
                    "method": "POST",
                    "body": {
                        "value": "@concat('{\"parentRunId\":\"', pipeline().parameters.parentPipelineRunId, '\",\"childRunId\":\"', pipeline().RunId, '\",\"dataSource\":\"', pipeline().parameters.dataSource, '\",\"executionDate\":\"', pipeline().parameters.executionDate, '\",\"mode\":\"', pipeline().parameters.mode, '\",\"status\":\"started\"}')",
                        "type": "Expression"
                    }
                }
            },
            {
                "name": "ProcessDataSource",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "LogPipelineStart",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "source": {
                        "type": "SqlServerSource",
                        "sqlReaderQuery": {
                            "value": "@concat('SELECT * FROM ', variables('sourceTableName'), ' WHERE ModifiedDate >= ''', pipeline().parameters.executionDate, '''')",
                            "type": "Expression"
                        }
                    },
                    "sink": {
                        "type": "DelimitedTextSink"
                    },
                    "dataIntegrationUnits": {
                        "value": "@pipeline().parameters.globalBatchSize",
                        "type": "Expression"
                    }
                }
            }
        ]
    }
}
```

### Activity Output to Parameter Mapping

#### Using Activity Outputs as Parameters
```json
{
    "name": "ActivityOutputParameterMapping",
    "properties": {
        "activities": [
            {
                "name": "GetMetadata",
                "type": "GetMetadata",
                "typeProperties": {
                    "dataset": {
                        "referenceName": "InputDataset",
                        "type": "DatasetReference"
                    },
                    "fieldList": ["size", "lastModified", "itemName"]
                }
            },
            {
                "name": "ConditionalProcessing",
                "type": "IfCondition",
                "dependsOn": [
                    {
                        "activity": "GetMetadata",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "expression": {
                        "value": "@greater(activity('GetMetadata').output.size, 1000000)",
                        "type": "Expression"
                    },
                    "ifTrueActivities": [
                        {
                            "name": "LargeFileProcessing",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "LargeFileProcessor",
                                    "type": "PipelineReference"
                                },
                                "parameters": {
                                    "fileName": {
                                        "value": "@activity('GetMetadata').output.itemName",
                                        "type": "Expression"
                                    },
                                    "fileSize": {
                                        "value": "@string(activity('GetMetadata').output.size)",
                                        "type": "Expression"
                                    },
                                    "lastModified": {
                                        "value": "@activity('GetMetadata').output.lastModified",
                                        "type": "Expression"
                                    }
                                }
                            }
                        }
                    ],
                    "ifFalseActivities": [
                        {
                            "name": "StandardProcessing",
                            "type": "Copy",
                            "typeProperties": {
                                "source": {
                                    "type": "DelimitedTextSource"
                                },
                                "sink": {
                                    "type": "DelimitedTextSink"
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

## Environment Management

### Environment-Specific Parameter Configuration

#### Development Environment Parameters
```json
{
    "environment": "development",
    "globalParameters": {
        "environment": {
            "type": "string",
            "value": "dev"
        },
        "dataLakeStorageAccount": {
            "type": "string",
            "value": "devdatalakestorage"
        },
        "sqlServerName": {
            "type": "string",
            "value": "dev-sql-server.database.windows.net"
        },
        "batchSize": {
            "type": "int",
            "value": 100
        },
        "enableDebugLogging": {
            "type": "bool",
            "value": true
        },
        "retryAttempts": {
            "type": "int",
            "value": 1
        },
        "timeoutMinutes": {
            "type": "int",
            "value": 15
        },
        "dataRetentionDays": {
            "type": "int",
            "value": 30
        }
    },
    "linkedServiceParameters": {
        "AzureSqlDatabase": {
            "serverName": "dev-sql-server.database.windows.net",
            "databaseName": "DevDatabase",
            "connectionTimeout": 30
        },
        "AzureDataLakeStorage": {
            "accountName": "devdatalakestorage",
            "containerName": "dev-data"
        }
    }
}
```

#### Production Environment Parameters
```json
{
    "environment": "production",
    "globalParameters": {
        "environment": {
            "type": "string",
            "value": "prod"
        },
        "dataLakeStorageAccount": {
            "type": "string",
            "value": "proddatalakestorage"
        },
        "sqlServerName": {
            "type": "string",
            "value": "prod-sql-server.database.windows.net"
        },
        "batchSize": {
            "type": "int",
            "value": 5000
        },
        "enableDebugLogging": {
            "type": "bool",
            "value": false
        },
        "retryAttempts": {
            "type": "int",
            "value": 3
        },
        "timeoutMinutes": {
            "type": "int",
            "value": 60
        },
        "dataRetentionDays": {
            "type": "int",
            "value": 365
        }
    },
    "linkedServiceParameters": {
        "AzureSqlDatabase": {
            "serverName": "prod-sql-server.database.windows.net",
            "databaseName": "ProductionDatabase",
            "connectionTimeout": 60
        },
        "AzureDataLakeStorage": {
            "accountName": "proddatalakestorage",
            "containerName": "prod-data"
        }
    }
}
```

### Environment Parameter Management Script
```powershell
# PowerShell script for environment-specific parameter management

class EnvironmentParameterManager {
    [string]$ConfigPath
    [hashtable]$Environments
    
    EnvironmentParameterManager([string]$configPath) {
        $this.ConfigPath = $configPath
        $this.Environments = @{}
        $this.LoadEnvironmentConfigs()
    }
    
    [void]LoadEnvironmentConfigs() {
        $configFiles = Get-ChildItem -Path $this.ConfigPath -Filter "*.json"
        
        foreach ($file in $configFiles) {
            $config = Get-Content -Path $file.FullName | ConvertFrom-Json
            $envName = $config.environment
            $this.Environments[$envName] = $config
            Write-Host "📁 Loaded configuration for environment: $envName"
        }
    }
    
    [void]DeployToEnvironment([string]$environmentName, [string]$subscriptionId, [string]$resourceGroupName, [string]$dataFactoryName) {
        if (-not $this.Environments.ContainsKey($environmentName)) {
            Write-Error "❌ Environment configuration not found: $environmentName"
            return
        }
        
        $config = $this.Environments[$environmentName]
        Write-Host "🚀 Deploying parameters to $environmentName environment..."
        
        # Set Azure context
        Set-AzContext -SubscriptionId $subscriptionId
        
        # Deploy global parameters
        foreach ($paramName in $config.globalParameters.PSObject.Properties.Name) {
            $param = $config.globalParameters.$paramName
            
            try {
                Set-AzDataFactoryV2GlobalParameter -ResourceGroupName $resourceGroupName `
                    -DataFactoryName $dataFactoryName `
                    -Name $paramName `
                    -Value $param.value `
                    -Type $param.type
                
                Write-Host "✅ Global parameter deployed: $paramName = $($param.value)"
            }
            catch {
                Write-Error "❌ Failed to deploy global parameter $paramName`: $_"
            }
        }
        
        Write-Host "🎉 Environment deployment completed for: $environmentName"
    }
    
    [hashtable]GetEnvironmentConfig([string]$environmentName) {
        return $this.Environments[$environmentName]
    }
    
    [void]ValidateEnvironmentConfig([string]$environmentName) {
        if (-not $this.Environments.ContainsKey($environmentName)) {
            Write-Error "❌ Environment not found: $environmentName"
            return
        }
        
        $config = $this.Environments[$environmentName]
        $validationResults = @{
            "environment" = $environmentName
            "validationPassed" = $true
            "issues" = @()
        }
        
        # Validate required global parameters
        $requiredParams = @("environment", "dataLakeStorageAccount", "batchSize")
        foreach ($param in $requiredParams) {
            if (-not $config.globalParameters.$param) {
                $validationResults.issues += "Missing required parameter: $param"
                $validationResults.validationPassed = $false
            }
        }
        
        # Validate parameter types
        if ($config.globalParameters.batchSize -and $config.globalParameters.batchSize.type -ne "int") {
            $validationResults.issues += "batchSize must be of type 'int'"
            $validationResults.validationPassed = $false
        }
        
        if ($validationResults.validationPassed) {
            Write-Host "✅ Environment configuration validation passed: $environmentName"
        } else {
            Write-Warning "⚠️ Environment configuration validation failed: $environmentName"
            foreach ($issue in $validationResults.issues) {
                Write-Warning "  - $issue"
            }
        }
        
        return $validationResults
    }
    
    [void]CompareEnvironments([string]$env1, [string]$env2) {
        $config1 = $this.Environments[$env1]
        $config2 = $this.Environments[$env2]
        
        Write-Host "🔍 Comparing environments: $env1 vs $env2"
        
        $params1 = $config1.globalParameters.PSObject.Properties.Name
        $params2 = $config2.globalParameters.PSObject.Properties.Name
        
        # Find differences
        $onlyInEnv1 = $params1 | Where-Object { $_ -notin $params2 }
        $onlyInEnv2 = $params2 | Where-Object { $_ -notin $params1 }
        $common = $params1 | Where-Object { $_ -in $params2 }
        
        if ($onlyInEnv1) {
            Write-Host "📊 Parameters only in $env1`:"
            foreach ($param in $onlyInEnv1) {
                Write-Host "  + $param"
            }
        }
        
        if ($onlyInEnv2) {
            Write-Host "📊 Parameters only in $env2`:"
            foreach ($param in $onlyInEnv2) {
                Write-Host "  + $param"
            }
        }
        
        Write-Host "📊 Value differences in common parameters:"
        foreach ($param in $common) {
            $value1 = $config1.globalParameters.$param.value
            $value2 = $config2.globalParameters.$param.value
            
            if ($value1 -ne $value2) {
                Write-Host "  🔄 $param`: $env1='$value1' vs $env2='$value2'"
            }
        }
    }
}

# Example usage
$paramManager = [EnvironmentParameterManager]::new("C:\ADF\Environments")

# Validate all environments
$environments = @("development", "staging", "production")
foreach ($env in $environments) {
    $paramManager.ValidateEnvironmentConfig($env)
}

# Compare environments
$paramManager.CompareEnvironments("development", "production")

# Deploy to specific environment
$paramManager.DeployToEnvironment("production", "your-subscription-id", "your-rg", "your-adf")
```

---

## Security and Key Vault Integration

### Azure Key Vault Integration for Parameters

#### Key Vault Linked Service
```json
{
    "name": "AzureKeyVault",
    "properties": {
        "type": "AzureKeyVault",
        "typeProperties": {
            "baseUrl": "https://your-keyvault.vault.azure.net/"
        }
    }
}
```

#### Secure Parameter Configuration
```json
{
    "name": "SecureParameterPipeline",
    "properties": {
        "parameters": {
            "environment": {
                "type": "string",
                "defaultValue": "production"
            },
            "databaseServer": {
                "type": "string",
                "defaultValue": "prod-sql-server.database.windows.net"
            }
        },
        "activities": [
            {
                "name": "SecureDataProcessing",
                "type": "Copy",
                "typeProperties": {
                    "source": {
                        "type": "SqlServerSource",
                        "sqlReaderQuery": "SELECT * FROM SensitiveData"
                    },
                    "sink": {
                        "type": "DelimitedTextSink"
                    }
                },
                "inputs": [
                    {
                        "referenceName": "SecureSQLDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "serverName": {
                                "value": "@pipeline().parameters.databaseServer",
                                "type": "Expression"
                            },
                            "environment": {
                                "value": "@pipeline().parameters.environment",
                                "type": "Expression"
                            }
                        }
                    }
                ]
            }
        ]
    }
}
```

#### Secure Dataset with Key Vault Integration
```json
{
    "name": "SecureSQLDataset",
    "properties": {
        "linkedServiceName": {
            "referenceName": "SecureSQLServerLinkedService",
            "type": "LinkedServiceReference",
            "parameters": {
                "serverName": {
                    "value": "@dataset().serverName",
                    "type": "Expression"
                },
                "environment": {
                    "value": "@dataset().environment",
                    "type": "Expression"
                }
            }
        },
        "parameters": {
            "serverName": {"type": "string"},
            "environment": {"type": "string"}
        },
        "type": "SqlServerTable",
        "typeProperties": {
            "schema": "dbo",
            "table": "SensitiveData"
        }
    }
}
```

#### Secure Linked Service with Key Vault
```json
{
    "name": "SecureSQLServerLinkedService",
    "properties": {
        "parameters": {
            "serverName": {"type": "string"},
            "environment": {"type": "string"}
        },
        "type": "SqlServer",
        "typeProperties": {
            "connectionString": {
                "value": "@concat('Server=', linkedService().serverName, ';Database=', if(equals(linkedService().environment, 'prod'), 'ProductionDB', 'DevelopmentDB'), ';')",
                "type": "Expression"
            },
            "userName": {
                "type": "AzureKeyVaultSecret",
                "store": {
                    "referenceName": "AzureKeyVault",
                    "type": "LinkedServiceReference"
                },
                "secretName": {
                    "value": "@concat('sql-username-', linkedService().environment)",
                    "type": "Expression"
                }
            },
            "password": {
                "type": "AzureKeyVaultSecret",
                "store": {
                    "referenceName": "AzureKeyVault",
                    "type": "LinkedServiceReference"
                },
                "secretName": {
                    "value": "@concat('sql-password-', linkedService().environment)",
                    "type": "Expression"
                }
            }
        }
    }
}
```

### Key Vault Management Script
```powershell
# PowerShell script for managing Key Vault secrets for ADF parameters

class ADFKeyVaultManager {
    [string]$KeyVaultName
    [string]$SubscriptionId
    [hashtable]$SecretCategories
    
    ADFKeyVaultManager([string]$keyVaultName, [string]$subscriptionId) {
        $this.KeyVaultName = $keyVaultName
        $this.SubscriptionId = $subscriptionId
        $this.SecretCategories = @{
            "database" = @("username", "password", "connectionstring")
            "storage" = @("accountkey", "sastoken", "connectionstring")
            "api" = @("apikey", "clientsecret", "accesstoken")
            "certificate" = @("thumbprint", "password")
        }
    }
    
    [void]CreateEnvironmentSecrets([string]$environment, [hashtable]$secrets) {
        Write-Host "🔐 Creating secrets for environment: $environment"
        
        foreach ($secretName in $secrets.Keys) {
            $fullSecretName = "$secretName-$environment"
            $secretValue = $secrets[$secretName]
            
            try {
                $secretObj = ConvertTo-SecureString -String $secretValue -AsPlainText -Force
                Set-AzKeyVaultSecret -VaultName $this.KeyVaultName -Name $fullSecretName -SecretValue $secretObj
                Write-Host "✅ Created secret: $fullSecretName"
            }
            catch {
                Write-Error "❌ Failed to create secret $fullSecretName`: $_"
            }
        }
    }
    
    [void]RotateSecrets([string]$environment, [array]$secretNames) {
        Write-Host "🔄 Rotating secrets for environment: $environment"
        
        foreach ($secretName in $secretNames) {
            $fullSecretName = "$secretName-$environment"
            
            # Generate new password (example for database passwords)
            if ($secretName -like "*password*") {
                $newPassword = $this.GenerateSecurePassword()
                $secretObj = ConvertTo-SecureString -String $newPassword -AsPlainText -Force
                
                try {
                    Set-AzKeyVaultSecret -VaultName $this.KeyVaultName -Name $fullSecretName -SecretValue $secretObj
                    Write-Host "✅ Rotated secret: $fullSecretName"
                }
                catch {
                    Write-Error "❌ Failed to rotate secret $fullSecretName`: $_"
                }
            }
        }
    }
    
    [string]GenerateSecurePassword() {
        $length = 16
        $characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
        $password = ""
        
        for ($i = 0; $i -lt $length; $i++) {
            $password += $characters[(Get-Random -Maximum $characters.Length)]
        }
        
        return $password
    }
    
    [hashtable]GetSecretInventory() {
        $secrets = Get-AzKeyVaultSecret -VaultName $this.KeyVaultName
        $inventory = @{
            "total" = $secrets.Count
            "by_environment" = @{}
            "by_category" = @{}
        }
        
        foreach ($secret in $secrets) {
            $secretName = $secret.Name
            
            # Extract environment from secret name
            if ($secretName -match "-(\w+)$") {
                $environment = $matches[1]
                if (-not $inventory.by_environment.ContainsKey($environment)) {
                    $inventory.by_environment[$environment] = 0
                }
                $inventory.by_environment[$environment]++
            }
            
            # Categorize secrets
            foreach ($category in $this.SecretCategories.Keys) {
                $keywords = $this.SecretCategories[$category]
                foreach ($keyword in $keywords) {
                    if ($secretName -like "*$keyword*") {
                        if (-not $inventory.by_category.ContainsKey($category)) {
                            $inventory.by_category[$category] = 0
                        }
                        $inventory.by_category[$category]++
                        break
                    }
                }
            }
        }
        
        return $inventory
    }
    
    [void]ValidateSecretAccess([string]$dataFactoryName, [string]$resourceGroupName) {
        Write-Host "🔍 Validating ADF access to Key Vault secrets..."
        
        # Get ADF managed identity
        $adf = Get-AzDataFactoryV2 -ResourceGroupName $resourceGroupName -Name $dataFactoryName
        $principalId = $adf.Identity.PrincipalId
        
        # Check Key Vault access policy
        $keyVault = Get-AzKeyVault -VaultName $this.KeyVaultName
        $accessPolicies = $keyVault.AccessPolicies
        
        $adfHasAccess = $false
        foreach ($policy in $accessPolicies) {
            if ($policy.ObjectId -eq $principalId) {
                $adfHasAccess = $true
                Write-Host "✅ ADF has Key Vault access with permissions: $($policy.PermissionsToSecrets -join ', ')"
                break
            }
        }
        
        if (-not $adfHasAccess) {
            Write-Warning "⚠️ ADF does not have access to Key Vault. Adding access policy..."
            Set-AzKeyVaultAccessPolicy -VaultName $this.KeyVaultName -ObjectId $principalId -PermissionsToSecrets Get,List
            Write-Host "✅ Access policy added for ADF"
        }
    }
    
    [void]ExportSecretConfiguration([string]$filePath) {
        $secrets = Get-AzKeyVaultSecret -VaultName $this.KeyVaultName
        $configuration = @{
            "keyVault" = $this.KeyVaultName
            "exportDate" = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            "secrets" = @()
        }
        
        foreach ($secret in $secrets) {
            $configuration.secrets += @{
                "name" = $secret.Name
                "enabled" = $secret.Enabled
                "created" = $secret.Created
                "updated" = $secret.Updated
                "expires" = $secret.Expires
            }
        }
        
        $configuration | ConvertTo-Json -Depth 10 | Out-File -FilePath $filePath -Encoding UTF8
        Write-Host "📄 Secret configuration exported to: $filePath"
    }
}

# Example usage
$kvManager = [ADFKeyVaultManager]::new("your-keyvault-name", "your-subscription-id")

# Create environment-specific secrets
$devSecrets = @{
    "sql-username" = "dev_admin"
    "sql-password" = "DevPassword123!"
    "storage-accountkey" = "dev_storage_key_here"
    "api-key" = "dev_api_key_here"
}

$prodSecrets = @{
    "sql-username" = "prod_admin"
    "sql-password" = "ProdPassword456!"
    "storage-accountkey" = "prod_storage_key_here"
    "api-key" = "prod_api_key_here"
}

$kvManager.CreateEnvironmentSecrets("dev", $devSecrets)
$kvManager.CreateEnvironmentSecrets("prod", $prodSecrets)

# Validate ADF access
$kvManager.ValidateSecretAccess("your-adf-name", "your-resource-group")

# Get inventory
$inventory = $kvManager.GetSecretInventory()
Write-Host "📊 Secret Inventory:"
Write-Host "  Total secrets: $($inventory.total)"
Write-Host "  By environment: $($inventory.by_environment | ConvertTo-Json)"
Write-Host "  By category: $($inventory.by_category | ConvertTo-Json)"
```

---

## Best Practices

### Parameter Design Principles

#### 1. Naming Conventions
```json
{
    "namingConventions": {
        "globalParameters": {
            "pattern": "camelCase",
            "examples": [
                "dataLakeStorageAccount",
                "defaultBatchSize",
                "enableDebugLogging",
                "maxRetryAttempts"
            ],
            "avoid": [
                "DataLakeStorageAccount",
                "data_lake_storage_account",
                "datalakestorageaccount"
            ]
        },
        "pipelineParameters": {
            "pattern": "camelCase with descriptive prefixes",
            "examples": [
                "sourceTableName",
                "targetContainerPath",
                "processingStartDate",
                "validationEnabled"
            ]
        },
        "datasetParameters": {
            "pattern": "descriptive and specific",
            "examples": [
                "containerName",
                "folderPath",
                "fileName",
                "columnDelimiter"
            ]
        }
    }
}
```

#### 2. Parameter Validation Framework
```json
{
    "name": "ParameterValidationPipeline",
    "properties": {
        "parameters": {
            "inputDate": {
                "type": "string",
                "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
            },
            "batchSize": {
                "type": "int",
                "defaultValue": 1000
            },
            "environment": {
                "type": "string",
                "defaultValue": "development"
            }
        },
        "activities": [
            {
                "name": "ValidateParameters",
                "type": "IfCondition",
                "typeProperties": {
                    "expression": {
                        "value": "@and(\n    and(\n        not(empty(pipeline().parameters.inputDate)),\n        greater(length(pipeline().parameters.inputDate), 8)\n    ),\n    and(\n        greater(pipeline().parameters.batchSize, 0),\n        less(pipeline().parameters.batchSize, 10000)\n    ),\n    or(\n        equals(pipeline().parameters.environment, 'development'),\n        or(\n            equals(pipeline().parameters.environment, 'staging'),\n            equals(pipeline().parameters.environment, 'production')\n        )\n    )\n)",
                        "type": "Expression"
                    },
                    "ifTrueActivities": [
                        {
                            "name": "LogValidationSuccess",
                            "type": "WebActivity",
                            "typeProperties": {
                                "url": "https://logging-api.com/validation",
                                "method": "POST",
                                "body": {
                                    "value": "{\"status\":\"validation_passed\",\"pipeline\":\"@{pipeline().Pipeline}\",\"runId\":\"@{pipeline().RunId}\"}",
                                    "type": "Expression"
                                }
                            }
                        }
                    ],
                    "ifFalseActivities": [
                        {
                            "name": "FailValidation",
                            "type": "WebActivity",
                            "typeProperties": {
                                "url": "https://logging-api.com/validation",
                                "method": "POST",
                                "body": {
                                    "value": "{\"status\":\"validation_failed\",\"pipeline\":\"@{pipeline().Pipeline}\",\"runId\":\"@{pipeline().RunId}\",\"parameters\":\"@{pipeline().parameters}\"}",
                                    "type": "Expression"
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

#### 3. Parameter Documentation Template
```json
{
    "parameterDocumentation": {
        "pipeline": "DataProcessingPipeline",
        "version": "1.0",
        "lastUpdated": "2024-01-15",
        "parameters": [
            {
                "name": "sourceContainer",
                "type": "string",
                "required": true,
                "defaultValue": "raw-data",
                "description": "Source container name in Azure Data Lake Storage",
                "validation": "Must be a valid container name (lowercase, no spaces)",
                "examples": ["raw-data", "staging-data", "archive-data"],
                "environmentValues": {
                    "development": "dev-raw-data",
                    "staging": "stg-raw-data", 
                    "production": "prod-raw-data"
                }
            },
            {
                "name": "processingDate",
                "type": "string",
                "required": false,
                "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')",
                "description": "Date for which to process data in YYYY-MM-DD format",
                "validation": "Must be in YYYY-MM-DD format and not in the future",
                "examples": ["2024-01-15", "2024-12-31"],
                "notes": "Defaults to current date if not provided"
            },
            {
                "name": "batchSize",
                "type": "int",
                "required": false,
                "defaultValue": 1000,
                "description": "Number of records to process in each batch",
                "validation": "Must be between 100 and 10000",
                "examples": [500, 1000, 2000, 5000],
                "performanceNotes": "Higher values may improve throughput but increase memory usage"
            }
        ]
    }
}
```

### Performance Optimization for Parameters

#### Parameter Caching Strategy
```json
{
    "name": "ParameterCachingPipeline",
    "properties": {
        "parameters": {
            "configurationPath": {
                "type": "string",
                "defaultValue": "config/processing-config.json"
            }
        },
        "variables": {
            "cachedConfig": {
                "type": "Object"
            },
            "configLastModified": {
                "type": "String"
            }
        },
        "activities": [
            {
                "name": "CheckConfigCache",
                "type": "GetMetadata",
                "typeProperties": {
                    "dataset": {
                        "referenceName": "ConfigFileDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "filePath": {
                                "value": "@pipeline().parameters.configurationPath",
                                "type": "Expression"
                            }
                        }
                    },
                    "fieldList": ["lastModified"]
                }
            },
            {
                "name": "LoadConfigIfNeeded",
                "type": "IfCondition",
                "dependsOn": [
                    {
                        "activity": "CheckConfigCache",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "expression": {
                        "value": "@or(\n    empty(variables('configLastModified')),\n    not(equals(activity('CheckConfigCache').output.lastModified, variables('configLastModified')))\n)",
                        "type": "Expression"
                    },
                    "ifTrueActivities": [
                        {
                            "name": "LoadConfiguration",
                            "type": "Lookup",
                            "typeProperties": {
                                "source": {
                                    "type": "JsonSource"
                                },
                                "dataset": {
                                    "referenceName": "ConfigFileDataset",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "filePath": {
                                            "value": "@pipeline().parameters.configurationPath",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            }
                        },
                        {
                            "name": "CacheConfiguration",
                            "type": "SetVariable",
                            "dependsOn": [
                                {
                                    "activity": "LoadConfiguration",
                                    "dependencyConditions": ["Succeeded"]
                                }
                            ],
                            "typeProperties": {
                                "variableName": "cachedConfig",
                                "value": {
                                    "value": "@activity('LoadConfiguration').output.firstRow",
                                    "type": "Expression"
                                }
                            }
                        },
                        {
                            "name": "UpdateCacheTimestamp",
                            "type": "SetVariable",
                            "dependsOn": [
                                {
                                    "activity": "CacheConfiguration",
                                    "dependencyConditions": ["Succeeded"]
                                }
                            ],
                            "typeProperties": {
                                "variableName": "configLastModified",
                                "value": {
                                    "value": "@activity('CheckConfigCache').output.lastModified",
                                    "type": "Expression"
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

## Real-World Examples

### Example 1: Multi-Environment Data Pipeline

#### Master Configuration Pipeline
```json
{
    "name": "MultiEnvironmentDataPipeline",
    "properties": {
        "parameters": {
            "targetEnvironment": {
                "type": "string",
                "defaultValue": "development"
            },
            "dataSourceType": {
                "type": "string",
                "defaultValue": "full"
            },
            "processingDate": {
                "type": "string",
                "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
            }
        },
        "variables": {
            "environmentConfig": {
                "type": "Object"
            },
            "sourceConnectionString": {
                "type": "String"
            },
            "targetStorageAccount": {
                "type": "String"
            }
        },
        "activities": [
            {
                "name": "LoadEnvironmentConfig",
                "type": "Switch",
                "typeProperties": {
                    "on": {
                        "value": "@pipeline().parameters.targetEnvironment",
                        "type": "Expression"
                    },
                    "cases": [
                        {
                            "value": "development",
                            "activities": [
                                {
                                    "name": "SetDevConfig",
                                    "type": "SetVariable",
                                    "typeProperties": {
                                        "variableName": "environmentConfig",
                                        "value": {
                                            "value": "{\n  \"batchSize\": 100,\n  \"retryAttempts\": 1,\n  \"storageAccount\": \"@{pipeline().globalParameters.devStorageAccount}\",\n  \"sqlServer\": \"@{pipeline().globalParameters.devSqlServer}\",\n  \"enableLogging\": true,\n  \"dataRetentionDays\": 30\n}",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            ]
                        },
                        {
                            "value": "production",
                            "activities": [
                                {
                                    "name": "SetProdConfig",
                                    "type": "SetVariable",
                                    "typeProperties": {
                                        "variableName": "environmentConfig",
                                        "value": {
                                            "value": "{\n  \"batchSize\": 5000,\n  \"retryAttempts\": 3,\n  \"storageAccount\": \"@{pipeline().globalParameters.prodStorageAccount}\",\n  \"sqlServer\": \"@{pipeline().globalParameters.prodSqlServer}\",\n  \"enableLogging\": false,\n  \"dataRetentionDays\": 365\n}",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            ]
                        }
                    ],
                    "defaultActivities": [
                        {
                            "name": "SetDefaultConfig",
                            "type": "SetVariable",
                            "typeProperties": {
                                "variableName": "environmentConfig",
                                "value": {
                                    "value": "{\n  \"batchSize\": 1000,\n  \"retryAttempts\": 2,\n  \"storageAccount\": \"@{pipeline().globalParameters.defaultStorageAccount}\",\n  \"sqlServer\": \"@{pipeline().globalParameters.defaultSqlServer}\",\n  \"enableLogging\": true,\n  \"dataRetentionDays\": 90\n}",
                                    "type": "Expression"
                                }
                            }
                        }
                    ]
                }
            },
            {
                "name": "ExecuteDataProcessing",
                "type": "ExecutePipeline",
                "dependsOn": [
                    {
                        "activity": "LoadEnvironmentConfig",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "pipeline": {
                        "referenceName": "DataProcessingWorkerPipeline",
                        "type": "PipelineReference"
                    },
                    "parameters": {
                        "batchSize": {
                            "value": "@json(variables('environmentConfig')).batchSize",
                            "type": "Expression"
                        },
                        "retryAttempts": {
                            "value": "@json(variables('environmentConfig')).retryAttempts",
                            "type": "Expression"
                        },
                        "storageAccount": {
                            "value": "@json(variables('environmentConfig')).storageAccount",
                            "type": "Expression"
                        },
                        "sqlServer": {
                            "value": "@json(variables('environmentConfig')).sqlServer",
                            "type": "Expression"
                        },
                        "processingDate": {
                            "value": "@pipeline().parameters.processingDate",
                            "type": "Expression"
                        },
                        "sourceType": {
                            "value": "@pipeline().parameters.dataSourceType",
                            "type": "Expression"
                        }
                    }
                }
            }
        ]
    }
}
```

### Example 2: Dynamic Schema Processing Pipeline

#### Schema-Driven Data Processing
```json
{
    "name": "DynamicSchemaProcessingPipeline",
    "properties": {
        "parameters": {
            "schemaConfigPath": {
                "type": "string",
                "defaultValue": "config/table-schemas.json"
            },
            "targetEnvironment": {
                "type": "string",
                "defaultValue": "development"
            }
        },
        "variables": {
            "schemaDefinitions": {
                "type": "Array"
            },
            "currentSchema": {
                "type": "Object"
            }
        },
        "activities": [
            {
                "name": "LoadSchemaDefinitions",
                "type": "Lookup",
                "typeProperties": {
                    "source": {
                        "type": "JsonSource"
                    },
                    "dataset": {
                        "referenceName": "ConfigJsonDataset",
                        "type": "DatasetReference",
                        "parameters": {
                            "filePath": {
                                "value": "@pipeline().parameters.schemaConfigPath",
                                "type": "Expression"
                            }
                        }
                    }
                },
                "firstRowOnly": false
            },
            {
                "name": "ProcessEachSchema",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "LoadSchemaDefinitions",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "items": {
                        "value": "@activity('LoadSchemaDefinitions').output.value",
                        "type": "Expression"
                    },
                    "isSequential": false,
                    "batchCount": 5,
                    "activities": [
                        {
                            "name": "ProcessSchemaTable",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "TableProcessingPipeline",
                                    "type": "PipelineReference"
                                },
                                "parameters": {
                                    "tableName": {
                                        "value": "@item().tableName",
                                        "type": "Expression"
                                    },
                                    "sourceSchema": {
                                        "value": "@item().sourceSchema",
                                        "type": "Expression"
                                    },
                                    "targetSchema": {
                                        "value": "@item().targetSchema",
                                        "type": "Expression"
                                    },
                                    "columnMappings": {
                                        "value": "@string(item().columnMappings)",
                                        "type": "Expression"
                                    },
                                    "transformationRules": {
                                        "value": "@string(item().transformationRules)",
                                        "type": "Expression"
                                    },
                                    "partitionColumn": {
                                        "value": "@item().partitionColumn",
                                        "type": "Expression"
                                    },
                                    "targetEnvironment": {
                                        "value": "@pipeline().parameters.targetEnvironment",
                                        "type": "Expression"
                                    },
                                    "batchSize": {
                                        "value": "@if(equals(pipeline().parameters.targetEnvironment, 'production'), item().prodBatchSize, item().devBatchSize)",
                                        "type": "Expression"
                                    }
                                },
                                "waitOnCompletion": true
                            }
                        }
                    ]
                }
            }
        ]
    }
}
```

#### Schema Configuration JSON
```json
[
    {
        "tableName": "Customer",
        "sourceSchema": "dbo",
        "targetSchema": "processed",
        "columnMappings": {
            "customer_id": "CustomerId",
            "first_name": "FirstName",
            "last_name": "LastName",
            "email_address": "Email",
            "registration_date": "RegistrationDate"
        },
        "transformationRules": {
            "Email": "LOWER(TRIM({Email}))",
            "FirstName": "INITCAP(TRIM({FirstName}))",
            "LastName": "INITCAP(TRIM({LastName}))"
        },
        "partitionColumn": "RegistrationDate",
        "devBatchSize": 100,
        "prodBatchSize": 2000
    },
    {
        "tableName": "Order",
        "sourceSchema": "dbo",
        "targetSchema": "processed",
        "columnMappings": {
            "order_id": "OrderId",
            "customer_id": "CustomerId",
            "order_date": "OrderDate",
            "total_amount": "TotalAmount",
            "order_status": "Status"
        },
        "transformationRules": {
            "Status": "UPPER(TRIM({Status}))",
            "TotalAmount": "ROUND({TotalAmount}, 2)"
        },
        "partitionColumn": "OrderDate",
        "devBatchSize": 200,
        "prodBatchSize": 5000
    }
]
```

### Example 3: Parameter-Driven ETL Framework

#### ETL Framework Master Pipeline
```json
{
    "name": "ETLFrameworkMaster",
    "properties": {
        "parameters": {
            "configurationTable": {
                "type": "string",
                "defaultValue": "ETL_Configuration"
            },
            "executionMode": {
                "type": "string",
                "defaultValue": "full"
            },
            "executionDate": {
                "type": "string",
                "defaultValue": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
            }
        },
        "activities": [
            {
                "name": "GetETLConfigurations",
                "type": "Lookup",
                "typeProperties": {
                    "source": {
                        "type": "SqlServerSource",
                        "sqlReaderQuery": {
                            "value": "SELECT \n    ConfigId,\n    SourceSystem,\n    SourceTable,\n    TargetTable,\n    ExecutionOrder,\n    IsActive,\n    Parameters\nFROM @{pipeline().parameters.configurationTable}\nWHERE IsActive = 1\n    AND (@{pipeline().parameters.executionMode} = 'full' \n         OR LastExecutionDate < '@{pipeline().parameters.executionDate}')\nORDER BY ExecutionOrder",
                            "type": "Expression"
                        }
                    },
                    "dataset": {
                        "referenceName": "ConfigurationDatabase",
                        "type": "DatasetReference"
                    }
                },
                "firstRowOnly": false
            },
            {
                "name": "ExecuteETLJobs",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "GetETLConfigurations",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "items": {
                        "value": "@activity('GetETLConfigurations').output.value",
                        "type": "Expression"
                    },
                    "isSequential": false,
                    "batchCount": 10,
                    "activities": [
                        {
                            "name": "ExecuteETLPipeline",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": "GenericETLPipeline",
                                    "type": "PipelineReference"
                                },
                                "parameters": {
                                    "configId": {
                                        "value": "@item().ConfigId",
                                        "type": "Expression"
                                    },
                                    "sourceSystem": {
                                        "value": "@item().SourceSystem",
                                        "type": "Expression"
                                    },
                                    "sourceTable": {
                                        "value": "@item().SourceTable",
                                        "type": "Expression"
                                    },
                                    "targetTable": {
                                        "value": "@item().TargetTable",
                                        "type": "Expression"
                                    },
                                    "executionDate": {
                                        "value": "@pipeline().parameters.executionDate",
                                        "type": "Expression"
                                    },
                                    "executionMode": {
                                        "value": "@pipeline().parameters.executionMode",
                                        "type": "Expression"
                                    },
                                    "customParameters": {
                                        "value": "@item().Parameters",
                                        "type": "Expression"
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

## Troubleshooting

### Common Parameter Issues and Solutions

#### Issue 1: Parameter Expression Evaluation Errors
```json
{
    "commonErrors": {
        "invalidExpression": {
            "error": "Expression evaluation failed",
            "causes": [
                "Syntax errors in expressions",
                "Undefined parameters or variables",
                "Type mismatches",
                "Null reference errors"
            ],
            "solutions": [
                "Validate expression syntax",
                "Check parameter existence",
                "Use coalesce() for null handling",
                "Add type conversion functions"
            ],
            "examples": {
                "incorrect": "@pipeline().parameters.nonExistentParam",
                "correct": "@coalesce(pipeline().parameters.optionalParam, 'defaultValue')"
            }
        },
        "typeConversionError": {
            "error": "Unable to convert parameter type",
            "causes": [
                "String to integer conversion failure",
                "Boolean parameter as string",
                "Date format mismatches"
            ],
            "solutions": [
                "Use int(), string(), bool() functions",
                "Validate input formats",
                "Use formatDateTime() for dates"
            ],
            "examples": {
                "incorrect": "@pipeline().parameters.stringNumber",
                "correct": "@int(pipeline().parameters.stringNumber)"
            }
        }
    }
}
```

#### Issue 2: Global Parameter Access Problems
```json
{
    "globalParameterIssues": {
        "parameterNotFound": {
            "error": "Global parameter not accessible",
            "troubleshooting": [
                "Verify parameter exists in Data Factory",
                "Check parameter name spelling",
                "Ensure parameter is published",
                "Verify access permissions"
            ],
            "diagnosticQuery": "@pipeline().globalParameters",
            "testExpression": "@contains(string(pipeline().globalParameters), 'parameterName')"
        },
        "environmentMismatch": {
            "error": "Global parameter values differ between environments",
            "troubleshooting": [
                "Compare parameter values across environments",
                "Check deployment scripts",
                "Verify ARM template parameters",
                "Review CI/CD pipeline configuration"
            ]
        }
    }
}
```

### Parameter Debugging Pipeline
```json
{
    "name": "ParameterDebuggingPipeline",
    "properties": {
        "parameters": {
            "debugMode": {
                "type": "bool",
                "defaultValue": true
            }
        },
        "variables": {
            "debugOutput": {
                "type": "String"
            }
        },
        "activities": [
            {
                "name": "DebugParameters",
                "type": "IfCondition",
                "typeProperties": {
                    "expression": {
                        "value": "@pipeline().parameters.debugMode",
                        "type": "Expression"
                    },
                    "ifTrueActivities": [
                        {
                            "name": "LogAllParameters",
                            "type": "SetVariable",
                            "typeProperties": {
                                "variableName": "debugOutput",
                                "value": {
                                    "value": "@concat(\n    'Pipeline Parameters: ', string(pipeline().parameters), '\\n',\n    'Global Parameters: ', string(pipeline().globalParameters), '\\n',\n    'Pipeline Run ID: ', pipeline().RunId, '\\n',\n    'Trigger Name: ', pipeline().TriggerName, '\\n',\n    'Trigger Time: ', pipeline().TriggerTime, '\\n',\n    'Data Factory Name: ', pipeline().DataFactory\n)",
                                    "type": "Expression"
                                }
                            }
                        },
                        {
                            "name": "OutputDebugInfo",
                            "type": "WebActivity",
                            "dependsOn": [
                                {
                                    "activity": "LogAllParameters",
                                    "dependencyConditions": ["Succeeded"]
                                }
                            ],
                            "typeProperties": {
                                "url": "https://httpbin.org/post",
                                "method": "POST",
                                "body": {
                                    "value": "@variables('debugOutput')",
                                    "type": "Expression"
                                },
                                "headers": {
                                    "Content-Type": "text/plain"
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

### Parameter Validation Utility
```powershell
# PowerShell utility for validating ADF parameters

class ADFParameterValidator {
    [string]$DataFactoryName
    [string]$ResourceGroupName
    [string]$SubscriptionId
    [hashtable]$ValidationResults
    
    ADFParameterValidator([string]$subscriptionId, [string]$resourceGroupName, [string]$dataFactoryName) {
        $this.SubscriptionId = $subscriptionId
        $this.ResourceGroupName = $resourceGroupName
        $this.DataFactoryName = $dataFactoryName
        $this.ValidationResults = @{}
    }
    
    [hashtable]ValidateGlobalParameters() {
        Write-Host "🔍 Validating global parameters..."
        
        $results = @{
            "globalParametersFound" = 0
            "validationIssues" = @()
            "recommendations" = @()
        }
        
        try {
            $factory = Get-AzDataFactoryV2 -ResourceGroupName $this.ResourceGroupName -Name $this.DataFactoryName
            $globalParams = $factory.GlobalParameters
            
            if ($globalParams) {
                $results.globalParametersFound = $globalParams.Count
                
                foreach ($paramName in $globalParams.Keys) {
                    $param = $globalParams[$paramName]
                    
                    # Check naming convention
                    if ($paramName -cmatch '^[A-Z]') {
                        $results.validationIssues += "Parameter '$paramName' should start with lowercase (camelCase convention)"
                    }
                    
                    # Check for empty values
                    if ([string]::IsNullOrEmpty($param.Value)) {
                        $results.validationIssues += "Parameter '$paramName' has empty value"
                    }
                    
                    # Check for hardcoded environment values
                    if ($param.Value -match '(dev|test|prod|staging)' -and $paramName -notlike "*environment*") {
                        $results.recommendations += "Parameter '$paramName' contains environment-specific value. Consider using environment parameter."
                    }
                }
            } else {
                $results.validationIssues += "No global parameters found"
            }
            
        }
        catch {
            $results.validationIssues += "Failed to retrieve global parameters: $_"
        }
        
        $this.ValidationResults["globalParameters"] = $results
        return $results
    }
    
    [hashtable]ValidatePipelineParameters([string]$pipelineName) {
        Write-Host "🔍 Validating pipeline parameters for: $pipelineName"
        
        $results = @{
            "pipelineName" = $pipelineName
            "parametersFound" = 0
            "validationIssues" = @()
            "recommendations" = @()
        }
        
        try {
            $pipeline = Get-AzDataFactoryV2Pipeline -ResourceGroupName $this.ResourceGroupName -DataFactoryName $this.DataFactoryName -Name $pipelineName
            
            if ($pipeline.Parameters) {
                $results.parametersFound = $pipeline.Parameters.Count
                
                foreach ($paramName in $pipeline.Parameters.Keys) {
                    $param = $pipeline.Parameters[$paramName]
                    
                    # Check for default values
                    if (-not $param.DefaultValue) {
                        $results.recommendations += "Parameter '$paramName' has no default value"
                    }
                    
                    # Check parameter types
                    if (-not $param.Type) {
                        $results.validationIssues += "Parameter '$paramName' has no type specified"
                    }
                    
                    # Check naming convention
                    if ($paramName -cmatch '^[A-Z]') {
                        $results.validationIssues += "Parameter '$paramName' should start with lowercase"
                    }
                }
            }
            
        }
        catch {
            $results.validationIssues += "Failed to retrieve pipeline parameters: $_"
        }
        
        return $results
    }
    
    [void]GenerateValidationReport([string]$outputPath) {
        $report = @{
            "dataFactory" = $this.DataFactoryName
            "validationDate" = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            "results" = $this.ValidationResults
            "summary" = @{
                "totalIssues" = 0
                "totalRecommendations" = 0
            }
        }
        
        # Calculate totals
        foreach ($result in $this.ValidationResults.Values) {
            $report.summary.totalIssues += $result.validationIssues.Count
            $report.summary.totalRecommendations += $result.recommendations.Count
        }
        
        $report | ConvertTo-Json -Depth 10 | Out-File -FilePath $outputPath -Encoding UTF8
        Write-Host "📄 Validation report generated: $outputPath"
    }
}

# Example usage
$validator = [ADFParameterValidator]::new("your-subscription-id", "your-resource-group", "your-adf-name")

# Validate global parameters
$globalValidation = $validator.ValidateGlobalParameters()

# Validate specific pipeline parameters
$pipelineValidation = $validator.ValidatePipelineParameters("YourPipelineName")

# Generate comprehensive report
$validator.GenerateValidationReport("C:\ADF\parameter-validation-report.json")

Write-Host "📊 Validation Summary:"
Write-Host "  Global Parameters Found: $($globalValidation.globalParametersFound)"
Write-Host "  Issues Found: $($globalValidation.validationIssues.Count)"
Write-Host "  Recommendations: $($globalValidation.recommendations.Count)"
```

---

## Advanced Scenarios

### Dynamic Parameter Generation

#### Runtime Parameter Discovery Pipeline
```json
{
    "name": "DynamicParameterGenerationPipeline",
    "properties": {
        "parameters": {
            "metadataSource": {
                "type": "string",
                "defaultValue": "configuration_metadata"
            }
        },
        "variables": {
            "dynamicParameters": {
                "type": "Object"
            },
            "generatedConfig": {
                "type": "String"
            }
        },
        "activities": [
            {
                "name": "DiscoverParameters",
                "type": "Lookup",
                "typeProperties": {
                    "source": {
                        "type": "SqlServerSource",
                        "sqlReaderQuery": {
                            "value": "SELECT \n    ParameterName,\n    ParameterType,\n    DefaultValue,\n    Environment,\n    IsRequired,\n    ValidationRule\nFROM @{pipeline().parameters.metadataSource}\nWHERE IsActive = 1\nORDER BY Environment, ParameterName",
                            "type": "Expression"
                        }
                    },
                    "dataset": {
                        "referenceName": "MetadataDatabase",
                        "type": "DatasetReference"
                    }
                },
                "firstRowOnly": false
            },
            {
                "name": "GenerateParameterConfig",
                "type": "ForEach",
                "dependsOn": [
                    {
                        "activity": "DiscoverParameters",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "items": {
                        "value": "@activity('DiscoverParameters').output.value",
                        "type": "Expression"
                    },
                    "activities": [
                        {
                            "name": "ProcessParameterDefinition",
                            "type": "WebActivity",
                            "typeProperties": {
                                "url": "https://parameter-generator-api.azurewebsites.net/generate",
                                "method": "POST",
                                "body": {
                                    "value": "@concat('{\"parameterName\":\"', item().ParameterName, '\",\"parameterType\":\"', item().ParameterType, '\",\"defaultValue\":\"', item().DefaultValue, '\",\"environment\":\"', item().Environment, '\",\"isRequired\":', item().IsRequired, ',\"validationRule\":\"', item().ValidationRule, '\"}')",
                                    "type": "Expression"
                                },
                                "headers": {
                                    "Content-Type": "application/json"
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

### Parameter Inheritance Hierarchy

#### Hierarchical Parameter Management
```json
{
    "parameterHierarchy": {
        "global": {
            "level": 1,
            "scope": "factory",
            "inheritance": "none",
            "examples": {
                "environment": "production",
                "dataLakeAccount": "proddatalake",
                "defaultTimeZone": "UTC"
            }
        },
        "pipeline": {
            "level": 2,
            "scope": "pipeline",
            "inheritance": "global",
            "examples": {
                "processingMode": "incremental",
                "batchSize": "@pipeline().globalParameters.defaultBatchSize",
                "logLevel": "@if(equals(pipeline().globalParameters.environment, 'prod'), 'ERROR', 'INFO')"
            }
        },
        "activity": {
            "level": 3,
            "scope": "activity",
            "inheritance": "pipeline",
            "examples": {
                "timeout": "@pipeline().parameters.defaultTimeout",
                "retryAttempts": "@pipeline().parameters.maxRetries"
            }
        },
        "dataset": {
            "level": 4,
            "scope": "dataset",
            "inheritance": "activity",
            "examples": {
                "containerName": "@dataset().containerName",
                "filePath": "@concat(pipeline().globalParameters.basePath, '/', dataset().relativePath)"
            }
        }
    }
}
```

---

## Conclusion

This comprehensive guide covers all aspects of Azure Data Factory global and local parameters:

### Key Takeaways:

1. **Parameter Types**: Understanding the different scopes and use cases for global, pipeline, dataset, and linked service parameters
2. **Expression Language**: Mastering ADF's expression functions for dynamic parameter evaluation
3. **Environment Management**: Implementing proper parameter management across development, staging, and production environments
4. **Security**: Integrating with Azure Key Vault for secure parameter storage
5. **Best Practices**: Following naming conventions, validation patterns, and performance optimization techniques
6. **Real-World Examples**: Practical implementations for multi-environment pipelines, schema-driven processing, and ETL frameworks

### Recommended Implementation Approach:

1. **Start with Global Parameters**: Define factory-wide settings and environment configurations
2. **Implement Parameter Validation**: Add validation logic to ensure parameter integrity
3. **Use Key Vault Integration**: Secure sensitive parameters using Azure Key Vault
4. **Establish Naming Conventions**: Maintain consistent parameter naming across all components
5. **Document Parameters**: Create comprehensive parameter documentation for team collaboration
6. **Monitor and Validate**: Implement monitoring and validation utilities for ongoing parameter management

This guide provides production-ready patterns and code examples that can be adapted to your specific Azure Data Factory parameter management needs.

---

*Generated on: $(date)*
*Azure Data Factory Global & Local Parameters - Comprehensive Guide*

