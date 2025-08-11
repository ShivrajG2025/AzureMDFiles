# Azure Databricks: Calling One Notebook from Another - Comprehensive Guide

## Table of Contents
1. [Overview](#overview)
2. [Methods for Calling Notebooks](#methods-for-calling-notebooks)
3. [dbutils.notebook.run() Method](#dbutilsnotebookrun-method)
4. [%run Magic Command](#run-magic-command)
5. [REST API Approach](#rest-api-approach)
6. [Databricks Workflows Integration](#databricks-workflows-integration)
7. [Parameter Passing](#parameter-passing)
8. [Error Handling](#error-handling)
9. [Best Practices](#best-practices)
10. [Real-World Examples](#real-world-examples)
11. [Performance Considerations](#performance-considerations)
12. [Troubleshooting](#troubleshooting)

---

## Overview

In Azure Databricks, calling one notebook from another is a common pattern for:
- **Modular Code Organization**: Breaking complex workflows into manageable components
- **Code Reusability**: Sharing common functions across multiple notebooks
- **Workflow Orchestration**: Creating data pipelines with dependent steps
- **Environment Management**: Separating development, testing, and production logic

### Key Benefits
- **Maintainability**: Easier to update and debug individual components
- **Scalability**: Parallel execution of independent notebook tasks
- **Collaboration**: Multiple team members can work on different notebook components
- **Testing**: Individual notebooks can be tested in isolation

---

## Methods for Calling Notebooks

### 1. **dbutils.notebook.run()** - Primary Method
- Executes notebooks programmatically
- Supports parameter passing
- Returns values from called notebooks
- Handles execution context properly

### 2. **%run Magic Command** - Import Style
- Imports notebook content into current context
- Shares variables and functions
- No return value support
- Limited parameter passing

### 3. **REST API** - External Control
- Programmatic execution from external systems
- Full control over execution parameters
- Monitoring and status tracking
- Integration with CI/CD pipelines

### 4. **Databricks Workflows** - Orchestration
- Visual workflow designer
- Dependency management
- Scheduling and monitoring
- Multi-notebook coordination

---

## dbutils.notebook.run() Method

### Basic Syntax
```python
result = dbutils.notebook.run(
    path="notebook_path",
    timeout_seconds=3600,
    arguments={"param1": "value1", "param2": "value2"}
)
```

### Complete Notebook Execution Manager
```python
import json
from typing import Dict, Any, Optional
from datetime import datetime
import traceback

class NotebookExecutionManager:
    """
    Comprehensive manager for executing Databricks notebooks
    """
    
    def __init__(self, base_path: str = "/"):
        self.base_path = base_path
        self.execution_history = []
    
    def execute_notebook(self, 
                        notebook_path: str, 
                        parameters: Dict[str, Any] = None,
                        timeout_seconds: int = 3600,
                        retry_count: int = 3) -> Dict[str, Any]:
        """
        Execute a notebook with comprehensive error handling and logging
        """
        execution_id = f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        execution_record = {
            "execution_id": execution_id,
            "notebook_path": notebook_path,
            "parameters": parameters or {},
            "start_time": datetime.now(),
            "status": "running"
        }
        
        self.execution_history.append(execution_record)
        
        try:
            print(f"🚀 Starting execution: {notebook_path}")
            print(f"📋 Parameters: {json.dumps(parameters or {}, indent=2)}")
            
            # Execute the notebook
            result = dbutils.notebook.run(
                path=notebook_path,
                timeout_seconds=timeout_seconds,
                arguments=parameters or {}
            )
            
            execution_record.update({
                "status": "completed",
                "end_time": datetime.now(),
                "result": result,
                "duration_seconds": (datetime.now() - execution_record["start_time"]).total_seconds()
            })
            
            print(f"✅ Completed: {notebook_path}")
            print(f"⏱️ Duration: {execution_record['duration_seconds']:.2f} seconds")
            print(f"📤 Result: {result}")
            
            return {
                "success": True,
                "result": result,
                "execution_record": execution_record
            }
            
        except Exception as e:
            execution_record.update({
                "status": "failed",
                "end_time": datetime.now(),
                "error": str(e),
                "traceback": traceback.format_exc(),
                "duration_seconds": (datetime.now() - execution_record["start_time"]).total_seconds()
            })
            
            print(f"❌ Failed: {notebook_path}")
            print(f"🔥 Error: {str(e)}")
            
            if retry_count > 0:
                print(f"🔄 Retrying... ({retry_count} attempts remaining)")
                return self.execute_notebook(notebook_path, parameters, timeout_seconds, retry_count - 1)
            
            return {
                "success": False,
                "error": str(e),
                "execution_record": execution_record
            }
    
    def execute_parallel_notebooks(self, notebook_configs: list) -> Dict[str, Any]:
        """
        Execute multiple notebooks in parallel using Databricks threading
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        
        results = {}
        
        def execute_single(config):
            thread_id = threading.current_thread().name
            print(f"🧵 Thread {thread_id}: Executing {config['path']}")
            
            return {
                "config": config,
                "result": self.execute_notebook(
                    notebook_path=config["path"],
                    parameters=config.get("parameters", {}),
                    timeout_seconds=config.get("timeout", 3600)
                )
            }
        
        with ThreadPoolExecutor(max_workers=len(notebook_configs)) as executor:
            future_to_config = {
                executor.submit(execute_single, config): config 
                for config in notebook_configs
            }
            
            for future in as_completed(future_to_config):
                config = future_to_config[future]
                try:
                    execution_result = future.result()
                    results[config["name"]] = execution_result["result"]
                except Exception as exc:
                    print(f"❌ {config['name']} generated an exception: {exc}")
                    results[config["name"]] = {"success": False, "error": str(exc)}
        
        return results
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """
        Get summary of all executions
        """
        total_executions = len(self.execution_history)
        successful = len([e for e in self.execution_history if e["status"] == "completed"])
        failed = len([e for e in self.execution_history if e["status"] == "failed"])
        
        return {
            "total_executions": total_executions,
            "successful": successful,
            "failed": failed,
            "success_rate": (successful / total_executions * 100) if total_executions > 0 else 0,
            "execution_history": self.execution_history
        }

# Initialize the execution manager
notebook_manager = NotebookExecutionManager()
```

### Example: Data Pipeline Orchestration
```python
# Master notebook orchestrating a data pipeline

# Configuration for pipeline notebooks
pipeline_config = {
    "data_ingestion": {
        "path": "/data_pipeline/01_data_ingestion",
        "parameters": {
            "source_path": "/mnt/raw_data/sales_data",
            "target_path": "/mnt/bronze/sales",
            "date": "2024-01-01"
        }
    },
    "data_transformation": {
        "path": "/data_pipeline/02_data_transformation",
        "parameters": {
            "input_path": "/mnt/bronze/sales",
            "output_path": "/mnt/silver/sales_processed",
            "transformation_rules": "standard_cleansing"
        }
    },
    "data_aggregation": {
        "path": "/data_pipeline/03_data_aggregation",
        "parameters": {
            "input_path": "/mnt/silver/sales_processed",
            "output_path": "/mnt/gold/sales_summary",
            "aggregation_level": "daily"
        }
    }
}

# Execute pipeline sequentially
print("🏭 Starting Data Pipeline Execution")
pipeline_results = {}

for step_name, config in pipeline_config.items():
    print(f"\n📊 Executing step: {step_name}")
    
    result = notebook_manager.execute_notebook(
        notebook_path=config["path"],
        parameters=config["parameters"],
        timeout_seconds=1800  # 30 minutes
    )
    
    pipeline_results[step_name] = result
    
    if not result["success"]:
        print(f"❌ Pipeline failed at step: {step_name}")
        break
    else:
        print(f"✅ Step completed successfully: {step_name}")

# Display pipeline summary
print("\n📈 Pipeline Execution Summary:")
for step, result in pipeline_results.items():
    status = "✅ SUCCESS" if result["success"] else "❌ FAILED"
    print(f"  {step}: {status}")
```

---

## %run Magic Command

### Basic Usage
```python
# Import all functions and variables from another notebook
%run "/shared/utility_functions"

# Now you can use functions defined in utility_functions notebook
result = my_utility_function(parameter1, parameter2)
```

### Utility Functions Notebook Example
```python
# Notebook: /shared/utility_functions

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_spark_session(app_name: str = "DataProcessing"):
    """
    Setup optimized Spark session for Azure Databricks
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    logger.info(f"Spark session configured for: {app_name}")
    return spark

def read_delta_table(table_path: str, version: int = None):
    """
    Read Delta table with optional version
    """
    try:
        if version:
            df = spark.read.format("delta").option("versionAsOf", version).load(table_path)
            logger.info(f"Read Delta table {table_path} at version {version}")
        else:
            df = spark.read.format("delta").load(table_path)
            logger.info(f"Read Delta table {table_path} (latest version)")
        
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta table {table_path}: {str(e)}")
        raise

def write_delta_table(df, table_path: str, mode: str = "overwrite", partition_by: list = None):
    """
    Write DataFrame to Delta table with partitioning
    """
    try:
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.save(table_path)
        logger.info(f"Written Delta table to {table_path} with mode {mode}")
        
    except Exception as e:
        logger.error(f"Failed to write Delta table to {table_path}: {str(e)}")
        raise

def data_quality_check(df, required_columns: list, null_check_columns: list = None):
    """
    Perform basic data quality checks
    """
    quality_report = {
        "total_rows": df.count(),
        "total_columns": len(df.columns),
        "missing_columns": [],
        "null_counts": {},
        "quality_score": 100.0
    }
    
    # Check for required columns
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        quality_report["missing_columns"] = list(missing_cols)
        quality_report["quality_score"] -= len(missing_cols) * 10
    
    # Check for null values
    if null_check_columns:
        for col in null_check_columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                quality_report["null_counts"][col] = null_count
                if null_count > 0:
                    quality_report["quality_score"] -= (null_count / quality_report["total_rows"]) * 20
    
    logger.info(f"Data quality score: {quality_report['quality_score']:.2f}")
    return quality_report

# Global variables that can be used in notebooks that import this
DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
BRONZE_PATH = "/mnt/datalake/bronze"
SILVER_PATH = "/mnt/datalake/silver"
GOLD_PATH = "/mnt/datalake/gold"

print("✅ Utility functions loaded successfully")
```

### Main Processing Notebook Using Utilities
```python
# Import utility functions
%run "/shared/utility_functions"

# Use imported functions and variables
spark_session = setup_spark_session("Sales Data Processing")

# Read data using utility function
sales_df = read_delta_table(f"{BRONZE_PATH}/sales_data")

# Perform data quality check
quality_report = data_quality_check(
    df=sales_df,
    required_columns=["customer_id", "product_id", "sale_amount", "sale_date"],
    null_check_columns=["customer_id", "product_id"]
)

print("📊 Data Quality Report:")
print(f"  Total Rows: {quality_report['total_rows']:,}")
print(f"  Quality Score: {quality_report['quality_score']:.2f}%")

# Process data
processed_df = sales_df.withColumn(
    "sale_month", 
    F.date_format(F.col("sale_date"), "yyyy-MM")
)

# Write processed data
write_delta_table(
    df=processed_df,
    table_path=f"{SILVER_PATH}/sales_processed",
    mode="overwrite",
    partition_by=["sale_month"]
)

print("✅ Sales data processing completed")
```

---

## REST API Approach

### Databricks REST API Client
```python
import requests
import json
import time
from typing import Dict, Any, Optional

class DatabricksNotebookAPI:
    """
    Client for executing Databricks notebooks via REST API
    """
    
    def __init__(self, workspace_url: str, access_token: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.access_token = access_token
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
    
    def submit_notebook_run(self, 
                           notebook_path: str,
                           cluster_id: str,
                           notebook_params: Dict[str, str] = None,
                           timeout_seconds: int = 3600) -> Dict[str, Any]:
        """
        Submit a notebook for execution
        """
        url = f"{self.workspace_url}/api/2.1/jobs/runs/submit"
        
        payload = {
            "run_name": f"API_Run_{notebook_path.split('/')[-1]}_{int(time.time())}",
            "existing_cluster_id": cluster_id,
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": notebook_params or {}
            },
            "timeout_seconds": timeout_seconds
        }
        
        response = requests.post(url, headers=self.headers, data=json.dumps(payload))
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to submit notebook: {response.status_code} - {response.text}")
    
    def get_run_status(self, run_id: int) -> Dict[str, Any]:
        """
        Get the status of a notebook run
        """
        url = f"{self.workspace_url}/api/2.1/jobs/runs/get"
        params = {"run_id": run_id}
        
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get run status: {response.status_code} - {response.text}")
    
    def wait_for_completion(self, run_id: int, poll_interval: int = 30) -> Dict[str, Any]:
        """
        Wait for notebook execution to complete
        """
        while True:
            run_info = self.get_run_status(run_id)
            state = run_info.get("state", {})
            life_cycle_state = state.get("life_cycle_state")
            
            print(f"📊 Run {run_id} status: {life_cycle_state}")
            
            if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                result_state = state.get("result_state")
                print(f"🏁 Final state: {result_state}")
                
                if result_state == "SUCCESS":
                    return {"success": True, "run_info": run_info}
                else:
                    return {"success": False, "run_info": run_info}
            
            time.sleep(poll_interval)
    
    def execute_notebook_sync(self, 
                             notebook_path: str,
                             cluster_id: str,
                             notebook_params: Dict[str, str] = None,
                             timeout_seconds: int = 3600) -> Dict[str, Any]:
        """
        Execute notebook synchronously and return results
        """
        print(f"🚀 Submitting notebook: {notebook_path}")
        
        # Submit the run
        submit_response = self.submit_notebook_run(
            notebook_path=notebook_path,
            cluster_id=cluster_id,
            notebook_params=notebook_params,
            timeout_seconds=timeout_seconds
        )
        
        run_id = submit_response["run_id"]
        print(f"📋 Run ID: {run_id}")
        
        # Wait for completion
        result = self.wait_for_completion(run_id)
        
        return result

# Example usage
def execute_data_pipeline_via_api():
    """
    Execute a data pipeline using REST API
    """
    # Initialize API client
    api_client = DatabricksNotebookAPI(
        workspace_url="https://adb-<workspace-id>.<random-number>.azuredatabricks.net",
        access_token=dbutils.secrets.get(scope="databricks-scope", key="access-token")
    )
    
    cluster_id = "your-cluster-id"
    
    # Pipeline notebooks
    pipeline_notebooks = [
        {
            "name": "Data Ingestion",
            "path": "/data_pipeline/01_data_ingestion",
            "params": {
                "input_path": "/mnt/raw/sales",
                "output_path": "/mnt/bronze/sales",
                "date": "2024-01-01"
            }
        },
        {
            "name": "Data Transformation",
            "path": "/data_pipeline/02_data_transformation",
            "params": {
                "input_path": "/mnt/bronze/sales",
                "output_path": "/mnt/silver/sales"
            }
        }
    ]
    
    results = {}
    
    for notebook_config in pipeline_notebooks:
        print(f"\n🔄 Executing: {notebook_config['name']}")
        
        result = api_client.execute_notebook_sync(
            notebook_path=notebook_config["path"],
            cluster_id=cluster_id,
            notebook_params=notebook_config["params"]
        )
        
        results[notebook_config["name"]] = result
        
        if not result["success"]:
            print(f"❌ Pipeline failed at: {notebook_config['name']}")
            break
    
    return results
```

---

## Databricks Workflows Integration

### Workflow Definition (JSON)
```json
{
  "name": "Data Processing Workflow",
  "email_notifications": {
    "on_start": ["team@company.com"],
    "on_success": ["team@company.com"],
    "on_failure": ["team@company.com", "alerts@company.com"]
  },
  "webhook_notifications": {
    "on_failure": [
      {
        "id": "slack-webhook"
      }
    ]
  },
  "timeout_seconds": 7200,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "data_ingestion",
      "description": "Ingest raw data from source systems",
      "existing_cluster_id": "cluster-id",
      "notebook_task": {
        "notebook_path": "/workflows/01_data_ingestion",
        "base_parameters": {
          "environment": "production",
          "source_path": "/mnt/raw/sales",
          "target_path": "/mnt/bronze/sales"
        }
      },
      "timeout_seconds": 1800
    },
    {
      "task_key": "data_validation",
      "description": "Validate ingested data quality",
      "depends_on": [
        {
          "task_key": "data_ingestion"
        }
      ],
      "existing_cluster_id": "cluster-id",
      "notebook_task": {
        "notebook_path": "/workflows/02_data_validation",
        "base_parameters": {
          "input_path": "/mnt/bronze/sales",
          "validation_rules": "strict"
        }
      },
      "timeout_seconds": 900
    },
    {
      "task_key": "data_transformation",
      "description": "Transform and cleanse data",
      "depends_on": [
        {
          "task_key": "data_validation"
        }
      ],
      "existing_cluster_id": "cluster-id",
      "notebook_task": {
        "notebook_path": "/workflows/03_data_transformation",
        "base_parameters": {
          "input_path": "/mnt/bronze/sales",
          "output_path": "/mnt/silver/sales",
          "transformation_config": "standard"
        }
      },
      "timeout_seconds": 2400
    },
    {
      "task_key": "data_aggregation",
      "description": "Create aggregated views",
      "depends_on": [
        {
          "task_key": "data_transformation"
        }
      ],
      "existing_cluster_id": "cluster-id",
      "notebook_task": {
        "notebook_path": "/workflows/04_data_aggregation",
        "base_parameters": {
          "input_path": "/mnt/silver/sales",
          "output_path": "/mnt/gold/sales_summary"
        }
      },
      "timeout_seconds": 1800
    },
    {
      "task_key": "data_quality_report",
      "description": "Generate data quality report",
      "depends_on": [
        {
          "task_key": "data_aggregation"
        }
      ],
      "existing_cluster_id": "cluster-id",
      "notebook_task": {
        "notebook_path": "/workflows/05_data_quality_report",
        "base_parameters": {
          "gold_path": "/mnt/gold/sales_summary",
          "report_path": "/mnt/reports/quality"
        }
      },
      "timeout_seconds": 600
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}
```

### Workflow Management Python Class
```python
class DatabricksWorkflowManager:
    """
    Manager for Databricks workflows with notebook orchestration
    """
    
    def __init__(self, workspace_url: str, access_token: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.access_token = access_token
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
    
    def create_workflow(self, workflow_definition: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new workflow
        """
        url = f"{self.workspace_url}/api/2.1/jobs/create"
        
        response = requests.post(
            url, 
            headers=self.headers, 
            data=json.dumps(workflow_definition)
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to create workflow: {response.status_code} - {response.text}")
    
    def trigger_workflow(self, job_id: int, notebook_params: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Trigger a workflow run
        """
        url = f"{self.workspace_url}/api/2.1/jobs/run-now"
        
        payload = {"job_id": job_id}
        if notebook_params:
            payload["notebook_params"] = notebook_params
        
        response = requests.post(
            url, 
            headers=self.headers, 
            data=json.dumps(payload)
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to trigger workflow: {response.status_code} - {response.text}")
    
    def monitor_workflow_run(self, run_id: int) -> Dict[str, Any]:
        """
        Monitor workflow run progress
        """
        url = f"{self.workspace_url}/api/2.1/jobs/runs/get"
        params = {"run_id": run_id}
        
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            run_info = response.json()
            
            # Extract task status
            tasks = run_info.get("tasks", [])
            task_summary = {}
            
            for task in tasks:
                task_key = task.get("task_key")
                state = task.get("state", {})
                task_summary[task_key] = {
                    "life_cycle_state": state.get("life_cycle_state"),
                    "result_state": state.get("result_state"),
                    "start_time": task.get("start_time"),
                    "end_time": task.get("end_time")
                }
            
            return {
                "run_info": run_info,
                "task_summary": task_summary
            }
        else:
            raise Exception(f"Failed to get workflow run info: {response.status_code} - {response.text}")

# Example usage
workflow_manager = DatabricksWorkflowManager(
    workspace_url="https://your-workspace.azuredatabricks.net",
    access_token="your-access-token"
)
```

---

## Parameter Passing

### Advanced Parameter Management
```python
class NotebookParameterManager:
    """
    Advanced parameter management for notebook communication
    """
    
    def __init__(self):
        self.parameter_store = {}
    
    def prepare_parameters(self, **kwargs) -> Dict[str, str]:
        """
        Prepare parameters for notebook execution
        All parameters must be strings for dbutils.notebook.run()
        """
        parameters = {}
        
        for key, value in kwargs.items():
            if isinstance(value, (dict, list)):
                # Convert complex objects to JSON strings
                parameters[key] = json.dumps(value)
            elif isinstance(value, (int, float, bool)):
                # Convert numbers and booleans to strings
                parameters[key] = str(value)
            else:
                # Keep strings as-is
                parameters[key] = str(value)
        
        return parameters
    
    def parse_received_parameters(self) -> Dict[str, Any]:
        """
        Parse parameters received in the called notebook
        """
        parsed_params = {}
        
        # Get all widgets (parameters)
        try:
            # This would typically be done in the called notebook
            for widget_name in dbutils.widgets.getAll().keys():
                raw_value = dbutils.widgets.get(widget_name)
                
                # Try to parse as JSON first
                try:
                    parsed_value = json.loads(raw_value)
                    parsed_params[widget_name] = parsed_value
                except json.JSONDecodeError:
                    # If not JSON, try to parse as number or boolean
                    if raw_value.lower() in ['true', 'false']:
                        parsed_params[widget_name] = raw_value.lower() == 'true'
                    elif raw_value.isdigit():
                        parsed_params[widget_name] = int(raw_value)
                    elif raw_value.replace('.', '', 1).isdigit():
                        parsed_params[widget_name] = float(raw_value)
                    else:
                        parsed_params[widget_name] = raw_value
        
        except Exception as e:
            print(f"Warning: Could not parse parameters: {e}")
            parsed_params = {}
        
        return parsed_params

# Parameter manager instance
param_manager = NotebookParameterManager()

# Example: Master notebook calling child notebooks with complex parameters
def execute_ml_pipeline():
    """
    Execute ML pipeline with complex parameter passing
    """
    
    # Complex configuration object
    ml_config = {
        "model_type": "random_forest",
        "hyperparameters": {
            "n_estimators": 100,
            "max_depth": 10,
            "random_state": 42
        },
        "feature_columns": ["feature1", "feature2", "feature3"],
        "target_column": "target",
        "validation_split": 0.2
    }
    
    data_config = {
        "input_path": "/mnt/ml_data/training_data",
        "output_path": "/mnt/ml_models/trained_model",
        "date_range": {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31"
        }
    }
    
    # Prepare parameters
    training_params = param_manager.prepare_parameters(
        ml_config=ml_config,
        data_config=data_config,
        environment="production",
        debug_mode=False,
        max_iterations=1000
    )
    
    print("🤖 Starting ML Training Pipeline")
    print(f"📋 Parameters: {json.dumps(training_params, indent=2)}")
    
    # Execute training notebook
    training_result = dbutils.notebook.run(
        path="/ml_pipeline/model_training",
        timeout_seconds=3600,
        arguments=training_params
    )
    
    print(f"✅ Training completed: {training_result}")
    
    # Parse training result
    try:
        training_output = json.loads(training_result)
        model_path = training_output.get("model_path")
        accuracy = training_output.get("accuracy")
        
        print(f"📊 Model Accuracy: {accuracy}")
        print(f"💾 Model saved to: {model_path}")
        
        # Execute evaluation notebook with training results
        evaluation_params = param_manager.prepare_parameters(
            model_path=model_path,
            test_data_path="/mnt/ml_data/test_data",
            evaluation_metrics=["accuracy", "precision", "recall", "f1"],
            generate_report=True
        )
        
        evaluation_result = dbutils.notebook.run(
            path="/ml_pipeline/model_evaluation",
            timeout_seconds=1800,
            arguments=evaluation_params
        )
        
        print(f"📈 Evaluation completed: {evaluation_result}")
        
        return {
            "training_result": training_output,
            "evaluation_result": json.loads(evaluation_result)
        }
        
    except json.JSONDecodeError:
        print("⚠️ Could not parse training result as JSON")
        return {"training_result": training_result}

# Execute the pipeline
ml_results = execute_ml_pipeline()
```

### Child Notebook Parameter Handling
```python
# This code would be in the called notebook (/ml_pipeline/model_training)

# Parse received parameters
received_params = param_manager.parse_received_parameters()

print("📥 Received Parameters:")
for key, value in received_params.items():
    print(f"  {key}: {value} ({type(value).__name__})")

# Extract configuration objects
ml_config = received_params.get("ml_config", {})
data_config = received_params.get("data_config", {})
environment = received_params.get("environment", "development")
debug_mode = received_params.get("debug_mode", True)

# Use the parameters in processing
print(f"🔧 Running in {environment} environment")
print(f"🐛 Debug mode: {debug_mode}")

# Simulate model training
model_accuracy = 0.85
model_path = f"{data_config.get('output_path', '/tmp')}/model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# Return results as JSON string
result = {
    "status": "success",
    "model_path": model_path,
    "accuracy": model_accuracy,
    "training_time_minutes": 45,
    "hyperparameters_used": ml_config.get("hyperparameters", {}),
    "environment": environment
}

# Return the result (this will be the return value of dbutils.notebook.run())
dbutils.notebook.exit(json.dumps(result))
```

---

## Error Handling

### Comprehensive Error Handling Framework
```python
import traceback
from enum import Enum
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

class NotebookErrorType(Enum):
    TIMEOUT = "timeout"
    EXECUTION_ERROR = "execution_error"
    PARAMETER_ERROR = "parameter_error"
    RESOURCE_ERROR = "resource_error"
    DEPENDENCY_ERROR = "dependency_error"

@dataclass
class NotebookError:
    error_type: NotebookErrorType
    message: str
    notebook_path: str
    traceback: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    timestamp: Optional[str] = None

class NotebookErrorHandler:
    """
    Comprehensive error handling for notebook execution
    """
    
    def __init__(self):
        self.error_log = []
        self.retry_strategies = {
            NotebookErrorType.TIMEOUT: self._retry_with_increased_timeout,
            NotebookErrorType.RESOURCE_ERROR: self._retry_with_different_cluster,
            NotebookErrorType.EXECUTION_ERROR: self._retry_with_fallback,
        }
    
    def execute_with_error_handling(self, 
                                   notebook_path: str,
                                   parameters: Dict[str, Any] = None,
                                   max_retries: int = 3,
                                   timeout_seconds: int = 3600) -> Dict[str, Any]:
        """
        Execute notebook with comprehensive error handling
        """
        attempt = 0
        last_error = None
        
        while attempt < max_retries:
            try:
                print(f"🔄 Attempt {attempt + 1}/{max_retries}: {notebook_path}")
                
                result = dbutils.notebook.run(
                    path=notebook_path,
                    timeout_seconds=timeout_seconds,
                    arguments=parameters or {}
                )
                
                print(f"✅ Success on attempt {attempt + 1}")
                return {
                    "success": True,
                    "result": result,
                    "attempts": attempt + 1
                }
                
            except Exception as e:
                attempt += 1
                error_message = str(e)
                
                # Classify error type
                error_type = self._classify_error(error_message)
                
                notebook_error = NotebookError(
                    error_type=error_type,
                    message=error_message,
                    notebook_path=notebook_path,
                    traceback=traceback.format_exc(),
                    parameters=parameters,
                    timestamp=datetime.now().isoformat()
                )
                
                self.error_log.append(notebook_error)
                last_error = notebook_error
                
                print(f"❌ Attempt {attempt} failed: {error_message}")
                print(f"🏷️ Error type: {error_type.value}")
                
                if attempt < max_retries:
                    # Apply retry strategy
                    retry_params = self._apply_retry_strategy(
                        error_type, parameters, timeout_seconds
                    )
                    parameters = retry_params.get("parameters", parameters)
                    timeout_seconds = retry_params.get("timeout_seconds", timeout_seconds)
                    
                    print(f"🔄 Applying retry strategy for {error_type.value}")
                    time.sleep(min(2 ** attempt, 60))  # Exponential backoff
        
        # All retries failed
        print(f"💥 All {max_retries} attempts failed for {notebook_path}")
        return {
            "success": False,
            "error": last_error,
            "attempts": max_retries
        }
    
    def _classify_error(self, error_message: str) -> NotebookErrorType:
        """
        Classify error based on error message
        """
        error_lower = error_message.lower()
        
        if "timeout" in error_lower or "timed out" in error_lower:
            return NotebookErrorType.TIMEOUT
        elif "parameter" in error_lower or "widget" in error_lower:
            return NotebookErrorType.PARAMETER_ERROR
        elif "cluster" in error_lower or "resource" in error_lower:
            return NotebookErrorType.RESOURCE_ERROR
        elif "import" in error_lower or "module" in error_lower:
            return NotebookErrorType.DEPENDENCY_ERROR
        else:
            return NotebookErrorType.EXECUTION_ERROR
    
    def _apply_retry_strategy(self, 
                             error_type: NotebookErrorType, 
                             parameters: Dict[str, Any], 
                             timeout_seconds: int) -> Dict[str, Any]:
        """
        Apply retry strategy based on error type
        """
        strategy = self.retry_strategies.get(error_type, lambda p, t: {"parameters": p, "timeout_seconds": t})
        return strategy(parameters, timeout_seconds)
    
    def _retry_with_increased_timeout(self, parameters: Dict[str, Any], timeout_seconds: int) -> Dict[str, Any]:
        """
        Retry with increased timeout
        """
        new_timeout = min(timeout_seconds * 2, 7200)  # Max 2 hours
        print(f"⏰ Increasing timeout from {timeout_seconds}s to {new_timeout}s")
        return {"parameters": parameters, "timeout_seconds": new_timeout}
    
    def _retry_with_different_cluster(self, parameters: Dict[str, Any], timeout_seconds: int) -> Dict[str, Any]:
        """
        Retry with different cluster configuration
        """
        # In a real implementation, you might switch to a different cluster
        print("🖥️ Would retry with different cluster configuration")
        return {"parameters": parameters, "timeout_seconds": timeout_seconds}
    
    def _retry_with_fallback(self, parameters: Dict[str, Any], timeout_seconds: int) -> Dict[str, Any]:
        """
        Retry with fallback parameters
        """
        # Modify parameters to use fallback values
        fallback_params = parameters.copy()
        fallback_params["use_fallback"] = "true"
        print("🔄 Using fallback parameters")
        return {"parameters": fallback_params, "timeout_seconds": timeout_seconds}
    
    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get summary of all errors
        """
        if not self.error_log:
            return {"total_errors": 0}
        
        error_counts = {}
        for error in self.error_log:
            error_type = error.error_type.value
            error_counts[error_type] = error_counts.get(error_type, 0) + 1
        
        return {
            "total_errors": len(self.error_log),
            "error_breakdown": error_counts,
            "recent_errors": [
                {
                    "notebook": error.notebook_path,
                    "type": error.error_type.value,
                    "message": error.message,
                    "timestamp": error.timestamp
                }
                for error in self.error_log[-5:]  # Last 5 errors
            ]
        }

# Initialize error handler
error_handler = NotebookErrorHandler()

# Example usage with error handling
def robust_pipeline_execution():
    """
    Execute pipeline with robust error handling
    """
    pipeline_steps = [
        {
            "name": "Data Ingestion",
            "path": "/pipeline/data_ingestion",
            "params": {"source": "database", "target": "/mnt/bronze"}
        },
        {
            "name": "Data Validation",
            "path": "/pipeline/data_validation",
            "params": {"input_path": "/mnt/bronze", "validation_level": "strict"}
        },
        {
            "name": "Data Transformation",
            "path": "/pipeline/data_transformation",
            "params": {"input_path": "/mnt/bronze", "output_path": "/mnt/silver"}
        }
    ]
    
    results = {}
    
    for step in pipeline_steps:
        print(f"\n📊 Executing: {step['name']}")
        
        result = error_handler.execute_with_error_handling(
            notebook_path=step["path"],
            parameters=step["params"],
            max_retries=3,
            timeout_seconds=1800
        )
        
        results[step["name"]] = result
        
        if not result["success"]:
            print(f"💥 Pipeline failed at step: {step['name']}")
            
            # Log error details
            error_summary = error_handler.get_error_summary()
            print(f"📊 Error Summary: {json.dumps(error_summary, indent=2)}")
            
            break
        else:
            print(f"✅ {step['name']} completed successfully")
    
    return results

# Execute robust pipeline
pipeline_results = robust_pipeline_execution()
```

---

## Best Practices

### 1. **Notebook Organization**
```python
# Best practice structure for calling notebooks

class NotebookBestPractices:
    """
    Best practices for notebook organization and calling
    """
    
    @staticmethod
    def organize_notebook_structure():
        """
        Recommended notebook organization structure
        """
        structure = {
            "/shared/": {
                "description": "Shared utilities and common functions",
                "examples": [
                    "utility_functions.py",
                    "data_quality_checks.py", 
                    "spark_configurations.py"
                ]
            },
            "/pipelines/": {
                "description": "Data pipeline notebooks",
                "examples": [
                    "01_data_ingestion.py",
                    "02_data_transformation.py",
                    "03_data_aggregation.py"
                ]
            },
            "/workflows/": {
                "description": "Workflow orchestration notebooks",
                "examples": [
                    "master_pipeline.py",
                    "ml_training_workflow.py"
                ]
            },
            "/analytics/": {
                "description": "Analysis and reporting notebooks",
                "examples": [
                    "sales_analysis.py",
                    "customer_segmentation.py"
                ]
            },
            "/tests/": {
                "description": "Test notebooks for validation",
                "examples": [
                    "data_quality_tests.py",
                    "unit_tests.py"
                ]
            }
        }
        
        print("📁 Recommended Notebook Structure:")
        for path, info in structure.items():
            print(f"\n{path}")
            print(f"  📝 {info['description']}")
            for example in info['examples']:
                print(f"    - {example}")
    
    @staticmethod
    def notebook_calling_guidelines():
        """
        Guidelines for calling notebooks effectively
        """
        guidelines = [
            {
                "principle": "Single Responsibility",
                "description": "Each notebook should have one clear purpose",
                "example": "Separate data ingestion, transformation, and validation into different notebooks"
            },
            {
                "principle": "Parameter Validation",
                "description": "Always validate input parameters in called notebooks",
                "example": "Check for required parameters and validate data types"
            },
            {
                "principle": "Error Handling",
                "description": "Implement comprehensive error handling in both caller and called notebooks",
                "example": "Use try-catch blocks and return meaningful error messages"
            },
            {
                "principle": "Resource Management",
                "description": "Properly manage Spark sessions and cluster resources",
                "example": "Don't create new Spark sessions unnecessarily"
            },
            {
                "principle": "Logging and Monitoring",
                "description": "Implement consistent logging across all notebooks",
                "example": "Log start/end times, parameters, and results"
            },
            {
                "principle": "Version Control",
                "description": "Use version control for notebook dependencies",
                "example": "Tag stable versions of utility notebooks"
            }
        ]
        
        print("📋 Notebook Calling Guidelines:")
        for i, guideline in enumerate(guidelines, 1):
            print(f"\n{i}. {guideline['principle']}")
            print(f"   📝 {guideline['description']}")
            print(f"   💡 Example: {guideline['example']}")

# Display best practices
practices = NotebookBestPractices()
practices.organize_notebook_structure()
print("\n" + "="*80 + "\n")
practices.notebook_calling_guidelines()
```

### 2. **Performance Optimization**
```python
class NotebookPerformanceOptimizer:
    """
    Performance optimization techniques for notebook calling
    """
    
    @staticmethod
    def optimize_spark_context_sharing():
        """
        Optimize Spark context sharing between notebooks
        """
        # Best practice: Don't create new Spark sessions in called notebooks
        optimization_tips = [
            "Use the existing Spark session from the calling notebook",
            "Configure Spark settings in the master notebook only",
            "Avoid stopping and starting Spark contexts unnecessarily",
            "Use broadcast variables for small lookup tables",
            "Cache frequently accessed DataFrames"
        ]
        
        print("⚡ Spark Context Optimization:")
        for tip in optimization_tips:
            print(f"  • {tip}")
    
    @staticmethod
    def optimize_parameter_passing():
        """
        Optimize parameter passing between notebooks
        """
        # Efficient parameter passing example
        def efficient_parameter_example():
            # Instead of passing large objects as parameters
            # Use shared storage locations
            
            # ❌ Inefficient - passing large JSON
            large_config = {"key": "value"} * 1000
            
            # ✅ Efficient - use shared location
            config_path = "/tmp/shared_config.json"
            with open(config_path, 'w') as f:
                json.dump(large_config, f)
            
            # Pass only the path
            parameters = {"config_path": config_path}
            
            return parameters
        
        print("📊 Parameter Passing Optimization:")
        print("  • Use shared storage for large objects")
        print("  • Pass file paths instead of large JSON strings")
        print("  • Validate parameters early in called notebooks")
        print("  • Use typed parameters when possible")
    
    @staticmethod
    def optimize_parallel_execution():
        """
        Optimize parallel notebook execution
        """
        def parallel_execution_example():
            from concurrent.futures import ThreadPoolExecutor
            import threading
            
            # Configuration for parallel notebooks
            parallel_configs = [
                {"path": "/analysis/customer_analysis", "params": {"segment": "premium"}},
                {"path": "/analysis/product_analysis", "params": {"category": "electronics"}},
                {"path": "/analysis/sales_analysis", "params": {"region": "north"}}
            ]
            
            def execute_notebook(config):
                thread_name = threading.current_thread().name
                print(f"🧵 {thread_name}: Starting {config['path']}")
                
                result = dbutils.notebook.run(
                    path=config["path"],
                    timeout_seconds=1800,
                    arguments=config["params"]
                )
                
                print(f"🧵 {thread_name}: Completed {config['path']}")
                return {"config": config, "result": result}
            
            # Execute in parallel
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(execute_notebook, config) for config in parallel_configs]
                results = [future.result() for future in futures]
            
            return results
        
        print("🚀 Parallel Execution Optimization:")
        print("  • Use ThreadPoolExecutor for independent notebooks")
        print("  • Limit concurrent executions based on cluster capacity")
        print("  • Monitor resource usage during parallel execution")
        print("  • Implement proper error handling for parallel tasks")

# Display performance optimizations
optimizer = NotebookPerformanceOptimizer()
optimizer.optimize_spark_context_sharing()
print("\n" + "-"*60 + "\n")
optimizer.optimize_parameter_passing()
print("\n" + "-"*60 + "\n")
optimizer.optimize_parallel_execution()
```

---

## Real-World Examples

### Example 1: ETL Pipeline with Data Quality Checks
```python
# Master ETL Pipeline Notebook

class ETLPipelineOrchestrator:
    """
    Real-world ETL pipeline orchestrator
    """
    
    def __init__(self):
        self.pipeline_config = {
            "source_systems": ["salesforce", "mysql", "api"],
            "target_layers": ["bronze", "silver", "gold"],
            "quality_thresholds": {
                "completeness": 0.95,
                "accuracy": 0.98,
                "timeliness": 24  # hours
            }
        }
    
    def execute_full_etl_pipeline(self, execution_date: str):
        """
        Execute complete ETL pipeline with quality gates
        """
        print(f"🏭 Starting ETL Pipeline for {execution_date}")
        
        pipeline_results = {
            "execution_date": execution_date,
            "start_time": datetime.now(),
            "steps": {}
        }
        
        try:
            # Step 1: Data Ingestion
            ingestion_result = self._execute_data_ingestion(execution_date)
            pipeline_results["steps"]["ingestion"] = ingestion_result
            
            if not ingestion_result["success"]:
                raise Exception("Data ingestion failed")
            
            # Step 2: Data Quality Assessment
            quality_result = self._execute_quality_assessment(execution_date)
            pipeline_results["steps"]["quality_assessment"] = quality_result
            
            if not self._quality_gate_passed(quality_result):
                raise Exception("Data quality gate failed")
            
            # Step 3: Data Transformation
            transformation_result = self._execute_data_transformation(execution_date)
            pipeline_results["steps"]["transformation"] = transformation_result
            
            if not transformation_result["success"]:
                raise Exception("Data transformation failed")
            
            # Step 4: Data Aggregation
            aggregation_result = self._execute_data_aggregation(execution_date)
            pipeline_results["steps"]["aggregation"] = aggregation_result
            
            if not aggregation_result["success"]:
                raise Exception("Data aggregation failed")
            
            # Step 5: Final Quality Check
            final_quality_result = self._execute_final_quality_check(execution_date)
            pipeline_results["steps"]["final_quality"] = final_quality_result
            
            pipeline_results["status"] = "SUCCESS"
            pipeline_results["end_time"] = datetime.now()
            
            print("✅ ETL Pipeline completed successfully")
            
        except Exception as e:
            pipeline_results["status"] = "FAILED"
            pipeline_results["error"] = str(e)
            pipeline_results["end_time"] = datetime.now()
            
            print(f"❌ ETL Pipeline failed: {str(e)}")
            
            # Execute failure notification
            self._send_failure_notification(pipeline_results)
        
        return pipeline_results
    
    def _execute_data_ingestion(self, execution_date: str) -> Dict[str, Any]:
        """
        Execute data ingestion from multiple sources
        """
        print("📥 Executing Data Ingestion...")
        
        ingestion_params = {
            "execution_date": execution_date,
            "source_systems": json.dumps(self.pipeline_config["source_systems"]),
            "target_path": "/mnt/bronze",
            "parallel_ingestion": "true"
        }
        
        result = dbutils.notebook.run(
            path="/etl_pipeline/01_data_ingestion",
            timeout_seconds=3600,
            arguments=ingestion_params
        )
        
        return json.loads(result)
    
    def _execute_quality_assessment(self, execution_date: str) -> Dict[str, Any]:
        """
        Execute comprehensive data quality assessment
        """
        print("🔍 Executing Data Quality Assessment...")
        
        quality_params = {
            "execution_date": execution_date,
            "input_path": "/mnt/bronze",
            "quality_rules": json.dumps({
                "null_checks": ["customer_id", "product_id", "transaction_date"],
                "range_checks": {"amount": {"min": 0, "max": 1000000}},
                "format_checks": {"email": "email_pattern", "phone": "phone_pattern"}
            }),
            "thresholds": json.dumps(self.pipeline_config["quality_thresholds"])
        }
        
        result = dbutils.notebook.run(
            path="/etl_pipeline/02_data_quality_assessment",
            timeout_seconds=1800,
            arguments=quality_params
        )
        
        return json.loads(result)
    
    def _quality_gate_passed(self, quality_result: Dict[str, Any]) -> bool:
        """
        Check if data quality meets thresholds
        """
        if not quality_result.get("success", False):
            return False
        
        metrics = quality_result.get("quality_metrics", {})
        thresholds = self.pipeline_config["quality_thresholds"]
        
        completeness = metrics.get("completeness_score", 0)
        accuracy = metrics.get("accuracy_score", 0)
        
        passed = (
            completeness >= thresholds["completeness"] and
            accuracy >= thresholds["accuracy"]
        )
        
        print(f"🎯 Quality Gate: {'✅ PASSED' if passed else '❌ FAILED'}")
        print(f"   Completeness: {completeness:.2%} (threshold: {thresholds['completeness']:.2%})")
        print(f"   Accuracy: {accuracy:.2%} (threshold: {thresholds['accuracy']:.2%})")
        
        return passed
    
    def _execute_data_transformation(self, execution_date: str) -> Dict[str, Any]:
        """
        Execute data transformation to silver layer
        """
        print("⚙️ Executing Data Transformation...")
        
        transformation_params = {
            "execution_date": execution_date,
            "input_path": "/mnt/bronze",
            "output_path": "/mnt/silver",
            "transformation_rules": json.dumps({
                "standardize_columns": True,
                "apply_business_rules": True,
                "deduplicate": True,
                "enrich_data": True
            })
        }
        
        result = dbutils.notebook.run(
            path="/etl_pipeline/03_data_transformation",
            timeout_seconds=2400,
            arguments=transformation_params
        )
        
        return json.loads(result)
    
    def _execute_data_aggregation(self, execution_date: str) -> Dict[str, Any]:
        """
        Execute data aggregation to gold layer
        """
        print("📊 Executing Data Aggregation...")
        
        aggregation_params = {
            "execution_date": execution_date,
            "input_path": "/mnt/silver",
            "output_path": "/mnt/gold",
            "aggregation_specs": json.dumps({
                "daily_sales_summary": {
                    "group_by": ["date", "product_category"],
                    "aggregations": {"sales_amount": "sum", "transaction_count": "count"}
                },
                "customer_metrics": {
                    "group_by": ["customer_id"],
                    "aggregations": {"total_spent": "sum", "avg_order_value": "mean"}
                }
            })
        }
        
        result = dbutils.notebook.run(
            path="/etl_pipeline/04_data_aggregation",
            timeout_seconds=1800,
            arguments=aggregation_params
        )
        
        return json.loads(result)
    
    def _execute_final_quality_check(self, execution_date: str) -> Dict[str, Any]:
        """
        Execute final quality check on gold layer
        """
        print("🏆 Executing Final Quality Check...")
        
        final_check_params = {
            "execution_date": execution_date,
            "input_path": "/mnt/gold",
            "validation_rules": json.dumps({
                "record_count_validation": True,
                "business_rule_validation": True,
                "referential_integrity_check": True
            })
        }
        
        result = dbutils.notebook.run(
            path="/etl_pipeline/05_final_quality_check",
            timeout_seconds=900,
            arguments=final_check_params
        )
        
        return json.loads(result)
    
    def _send_failure_notification(self, pipeline_results: Dict[str, Any]):
        """
        Send failure notification
        """
        print("📧 Sending Failure Notification...")
        
        notification_params = {
            "pipeline_results": json.dumps(pipeline_results),
            "notification_channels": json.dumps(["email", "slack"]),
            "escalation_level": "high"
        }
        
        try:
            dbutils.notebook.run(
                path="/notifications/pipeline_failure_notification",
                timeout_seconds=300,
                arguments=notification_params
            )
        except Exception as e:
            print(f"⚠️ Failed to send notification: {str(e)}")

# Execute ETL Pipeline
etl_orchestrator = ETLPipelineOrchestrator()
pipeline_result = etl_orchestrator.execute_full_etl_pipeline("2024-01-15")

print("\n📊 Pipeline Execution Summary:")
print(json.dumps(pipeline_result, indent=2, default=str))
```

### Example 2: Machine Learning Pipeline with Model Management
```python
# ML Pipeline Master Notebook

class MLPipelineOrchestrator:
    """
    Machine Learning pipeline with model lifecycle management
    """
    
    def __init__(self):
        self.ml_config = {
            "models": ["random_forest", "xgboost", "neural_network"],
            "validation_strategy": "time_series_split",
            "performance_threshold": 0.85,
            "model_registry": "/mnt/ml_models"
        }
    
    def execute_ml_pipeline(self, experiment_name: str, data_version: str):
        """
        Execute complete ML pipeline with model comparison
        """
        print(f"🤖 Starting ML Pipeline: {experiment_name}")
        
        pipeline_results = {
            "experiment_name": experiment_name,
            "data_version": data_version,
            "start_time": datetime.now(),
            "models": {}
        }
        
        try:
            # Step 1: Feature Engineering
            feature_result = self._execute_feature_engineering(data_version)
            pipeline_results["feature_engineering"] = feature_result
            
            # Step 2: Train Multiple Models in Parallel
            model_results = self._train_models_parallel(experiment_name, data_version)
            pipeline_results["models"] = model_results
            
            # Step 3: Model Comparison and Selection
            best_model = self._select_best_model(model_results)
            pipeline_results["best_model"] = best_model
            
            # Step 4: Model Validation
            validation_result = self._validate_model(best_model, data_version)
            pipeline_results["validation"] = validation_result
            
            # Step 5: Model Deployment (if validation passes)
            if validation_result["performance"] >= self.ml_config["performance_threshold"]:
                deployment_result = self._deploy_model(best_model)
                pipeline_results["deployment"] = deployment_result
            else:
                pipeline_results["deployment"] = {"status": "skipped", "reason": "performance_threshold_not_met"}
            
            pipeline_results["status"] = "SUCCESS"
            pipeline_results["end_time"] = datetime.now()
            
            print("✅ ML Pipeline completed successfully")
            
        except Exception as e:
            pipeline_results["status"] = "FAILED"
            pipeline_results["error"] = str(e)
            pipeline_results["end_time"] = datetime.now()
            
            print(f"❌ ML Pipeline failed: {str(e)}")
        
        return pipeline_results
    
    def _execute_feature_engineering(self, data_version: str) -> Dict[str, Any]:
        """
        Execute feature engineering pipeline
        """
        print("🔧 Executing Feature Engineering...")
        
        feature_params = {
            "data_version": data_version,
            "input_path": f"/mnt/silver/training_data_v{data_version}",
            "output_path": f"/mnt/features/features_v{data_version}",
            "feature_config": json.dumps({
                "numerical_features": ["age", "income", "credit_score"],
                "categorical_features": ["occupation", "region", "product_type"],
                "text_features": ["description"],
                "time_features": ["transaction_date"],
                "feature_transformations": {
                    "scaling": "standard",
                    "encoding": "one_hot",
                    "text_processing": "tfidf"
                }
            })
        }
        
        result = dbutils.notebook.run(
            path="/ml_pipeline/01_feature_engineering",
            timeout_seconds=2400,
            arguments=feature_params
        )
        
        return json.loads(result)
    
    def _train_models_parallel(self, experiment_name: str, data_version: str) -> Dict[str, Any]:
        """
        Train multiple models in parallel
        """
        print("🏋️ Training Models in Parallel...")
        
        from concurrent.futures import ThreadPoolExecutor
        
        model_configs = [
            {
                "model_type": "random_forest",
                "hyperparameters": {
                    "n_estimators": 100,
                    "max_depth": 10,
                    "random_state": 42
                }
            },
            {
                "model_type": "xgboost",
                "hyperparameters": {
                    "n_estimators": 100,
                    "learning_rate": 0.1,
                    "max_depth": 6
                }
            },
            {
                "model_type": "neural_network",
                "hyperparameters": {
                    "hidden_layers": [100, 50],
                    "activation": "relu",
                    "learning_rate": 0.001
                }
            }
        ]
        
        def train_single_model(config):
            model_type = config["model_type"]
            print(f"🤖 Training {model_type} model...")
            
            training_params = {
                "experiment_name": experiment_name,
                "data_version": data_version,
                "model_config": json.dumps(config),
                "input_path": f"/mnt/features/features_v{data_version}",
                "output_path": f"/mnt/ml_models/{experiment_name}/{model_type}"
            }
            
            result = dbutils.notebook.run(
                path=f"/ml_pipeline/models/train_{model_type}",
                timeout_seconds=3600,
                arguments=training_params
            )
            
            return {model_type: json.loads(result)}
        
        # Train models in parallel
        model_results = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(train_single_model, config) for config in model_configs]
            for future in futures:
                result = future.result()
                model_results.update(result)
        
        return model_results
    
    def _select_best_model(self, model_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Select best performing model
        """
        print("🏆 Selecting Best Model...")
        
        best_model = None
        best_score = 0
        
        for model_type, result in model_results.items():
            if result.get("success", False):
                score = result.get("validation_score", 0)
                print(f"  {model_type}: {score:.4f}")
                
                if score > best_score:
                    best_score = score
                    best_model = {
                        "model_type": model_type,
                        "score": score,
                        "model_path": result.get("model_path"),
                        "metrics": result.get("metrics")
                    }
        
        if best_model:
            print(f"🥇 Best Model: {best_model['model_type']} (Score: {best_model['score']:.4f})")
        else:
            raise Exception("No successful model training found")
        
        return best_model
    
    def _validate_model(self, best_model: Dict[str, Any], data_version: str) -> Dict[str, Any]:
        """
        Validate the best model on holdout test set
        """
        print("🧪 Validating Best Model...")
        
        validation_params = {
            "model_path": best_model["model_path"],
            "model_type": best_model["model_type"],
            "test_data_path": f"/mnt/features/test_features_v{data_version}",
            "validation_metrics": json.dumps(["accuracy", "precision", "recall", "f1", "auc"])
        }
        
        result = dbutils.notebook.run(
            path="/ml_pipeline/03_model_validation",
            timeout_seconds=1800,
            arguments=validation_params
        )
        
        return json.loads(result)
    
    def _deploy_model(self, best_model: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deploy the validated model
        """
        print("🚀 Deploying Model...")
        
        deployment_params = {
            "model_path": best_model["model_path"],
            "model_type": best_model["model_type"],
            "deployment_target": "production",
            "model_registry_path": self.ml_config["model_registry"],
            "enable_monitoring": "true"
        }
        
        result = dbutils.notebook.run(
            path="/ml_pipeline/04_model_deployment",
            timeout_seconds=1200,
            arguments=deployment_params
        )
        
        return json.loads(result)

# Execute ML Pipeline
ml_orchestrator = MLPipelineOrchestrator()
ml_result = ml_orchestrator.execute_ml_pipeline("customer_churn_v2", "1.0")

print("\n🤖 ML Pipeline Execution Summary:")
print(json.dumps(ml_result, indent=2, default=str))
```

---

## Performance Considerations

### Resource Management and Optimization
```python
class NotebookResourceManager:
    """
    Manage resources and optimize performance for notebook execution
    """
    
    def __init__(self):
        self.resource_metrics = []
    
    def monitor_notebook_execution(self, notebook_path: str, parameters: Dict[str, Any]):
        """
        Monitor resource usage during notebook execution
        """
        start_time = time.time()
        
        # Get initial resource state
        initial_state = self._get_resource_state()
        
        print(f"📊 Starting resource monitoring for: {notebook_path}")
        print(f"🖥️ Initial State: {initial_state}")
        
        try:
            # Execute notebook
            result = dbutils.notebook.run(
                path=notebook_path,
                timeout_seconds=3600,
                arguments=parameters
            )
            
            # Get final resource state
            final_state = self._get_resource_state()
            execution_time = time.time() - start_time
            
            # Calculate resource usage
            resource_usage = {
                "notebook_path": notebook_path,
                "execution_time_seconds": execution_time,
                "initial_state": initial_state,
                "final_state": final_state,
                "resource_delta": self._calculate_resource_delta(initial_state, final_state),
                "success": True,
                "result": result
            }
            
            self.resource_metrics.append(resource_usage)
            
            print(f"✅ Execution completed in {execution_time:.2f} seconds")
            print(f"📈 Resource Usage: {resource_usage['resource_delta']}")
            
            return resource_usage
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_usage = {
                "notebook_path": notebook_path,
                "execution_time_seconds": execution_time,
                "success": False,
                "error": str(e)
            }
            
            self.resource_metrics.append(error_usage)
            
            print(f"❌ Execution failed after {execution_time:.2f} seconds: {str(e)}")
            
            return error_usage
    
    def _get_resource_state(self) -> Dict[str, Any]:
        """
        Get current resource state
        """
        try:
            # Get Spark context information
            sc = spark.sparkContext
            
            return {
                "active_jobs": len(sc.statusTracker().getActiveJobIds()),
                "active_stages": len(sc.statusTracker().getActiveStageIds()),
                "executor_count": len(sc.statusTracker().getExecutorInfos()) - 1,  # Exclude driver
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
    
    def _calculate_resource_delta(self, initial: Dict[str, Any], final: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate resource usage delta
        """
        if "error" in initial or "error" in final:
            return {"error": "Could not calculate delta due to resource state errors"}
        
        return {
            "job_delta": final.get("active_jobs", 0) - initial.get("active_jobs", 0),
            "stage_delta": final.get("active_stages", 0) - initial.get("active_stages", 0),
            "executor_delta": final.get("executor_count", 0) - initial.get("executor_count", 0)
        }
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get performance summary across all monitored executions
        """
        if not self.resource_metrics:
            return {"message": "No execution metrics available"}
        
        successful_executions = [m for m in self.resource_metrics if m.get("success", False)]
        failed_executions = [m for m in self.resource_metrics if not m.get("success", False)]
        
        if successful_executions:
            execution_times = [m["execution_time_seconds"] for m in successful_executions]
            avg_execution_time = sum(execution_times) / len(execution_times)
            max_execution_time = max(execution_times)
            min_execution_time = min(execution_times)
        else:
            avg_execution_time = max_execution_time = min_execution_time = 0
        
        return {
            "total_executions": len(self.resource_metrics),
            "successful_executions": len(successful_executions),
            "failed_executions": len(failed_executions),
            "success_rate": len(successful_executions) / len(self.resource_metrics) * 100,
            "performance_metrics": {
                "avg_execution_time_seconds": avg_execution_time,
                "max_execution_time_seconds": max_execution_time,
                "min_execution_time_seconds": min_execution_time
            },
            "top_performers": sorted(
                successful_executions, 
                key=lambda x: x["execution_time_seconds"]
            )[:3],
            "slowest_performers": sorted(
                successful_executions, 
                key=lambda x: x["execution_time_seconds"], 
                reverse=True
            )[:3]
        }

# Initialize resource manager
resource_manager = NotebookResourceManager()

# Example: Monitor pipeline execution
pipeline_notebooks = [
    {
        "path": "/performance_test/fast_notebook",
        "params": {"data_size": "small"}
    },
    {
        "path": "/performance_test/slow_notebook", 
        "params": {"data_size": "large"}
    }
]

print("🔍 Starting Performance Monitoring...")

for notebook_config in pipeline_notebooks:
    resource_manager.monitor_notebook_execution(
        notebook_path=notebook_config["path"],
        parameters=notebook_config["params"]
    )

# Get performance summary
performance_summary = resource_manager.get_performance_summary()
print("\n📊 Performance Summary:")
print(json.dumps(performance_summary, indent=2, default=str))
```

---

## Troubleshooting

### Common Issues and Solutions
```python
class NotebookTroubleshootingGuide:
    """
    Comprehensive troubleshooting guide for notebook calling issues
    """
    
    def __init__(self):
        self.common_issues = {
            "timeout_errors": {
                "symptoms": [
                    "Notebook execution times out",
                    "Long-running operations fail",
                    "No response from called notebook"
                ],
                "causes": [
                    "Insufficient timeout setting",
                    "Resource contention",
                    "Large data processing",
                    "Inefficient code in called notebook"
                ],
                "solutions": [
                    "Increase timeout_seconds parameter",
                    "Optimize data processing logic",
                    "Use incremental processing",
                    "Monitor cluster resources"
                ]
            },
            "parameter_errors": {
                "symptoms": [
                    "Called notebook fails to start",
                    "Parameter validation errors",
                    "Type conversion errors"
                ],
                "causes": [
                    "Missing required parameters",
                    "Incorrect parameter data types",
                    "Parameter name mismatches",
                    "Complex object serialization issues"
                ],
                "solutions": [
                    "Validate parameters before calling",
                    "Use string serialization for complex objects",
                    "Implement parameter validation in called notebooks",
                    "Use consistent parameter naming conventions"
                ]
            },
            "resource_errors": {
                "symptoms": [
                    "Cluster unavailable errors",
                    "Memory out of bounds",
                    "Executor failures"
                ],
                "causes": [
                    "Insufficient cluster resources",
                    "Memory leaks in notebooks",
                    "Too many concurrent executions",
                    "Large dataset processing"
                ],
                "solutions": [
                    "Scale up cluster resources",
                    "Implement proper memory management",
                    "Limit concurrent notebook executions",
                    "Use data partitioning and caching strategies"
                ]
            }
        }
    
    def diagnose_issue(self, error_message: str, notebook_path: str) -> Dict[str, Any]:
        """
        Diagnose common notebook calling issues
        """
        error_lower = error_message.lower()
        
        diagnosis = {
            "error_message": error_message,
            "notebook_path": notebook_path,
            "likely_issues": [],
            "recommended_actions": []
        }
        
        # Check for timeout issues
        if any(keyword in error_lower for keyword in ["timeout", "timed out", "time limit"]):
            diagnosis["likely_issues"].append("timeout_errors")
            diagnosis["recommended_actions"].extend([
                "Increase timeout_seconds parameter",
                "Check for infinite loops or blocking operations",
                "Monitor cluster resource usage",
                "Consider breaking down into smaller notebooks"
            ])
        
        # Check for parameter issues
        if any(keyword in error_lower for keyword in ["parameter", "widget", "argument"]):
            diagnosis["likely_issues"].append("parameter_errors")
            diagnosis["recommended_actions"].extend([
                "Verify all required parameters are provided",
                "Check parameter data types and formats",
                "Validate parameter names match widget definitions",
                "Test parameter serialization/deserialization"
            ])
        
        # Check for resource issues
        if any(keyword in error_lower for keyword in ["cluster", "memory", "resource", "executor"]):
            diagnosis["likely_issues"].append("resource_errors")
            diagnosis["recommended_actions"].extend([
                "Check cluster status and availability",
                "Monitor memory usage in notebooks",
                "Reduce concurrent notebook executions",
                "Optimize data processing operations"
            ])
        
        return diagnosis
    
    def generate_troubleshooting_report(self, error_logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate comprehensive troubleshooting report
        """
        report = {
            "total_errors": len(error_logs),
            "error_categories": {},
            "common_patterns": [],
            "recommendations": []
        }
        
        # Categorize errors
        for error_log in error_logs:
            diagnosis = self.diagnose_issue(
                error_log.get("error_message", ""),
                error_log.get("notebook_path", "")
            )
            
            for issue_type in diagnosis["likely_issues"]:
                if issue_type not in report["error_categories"]:
                    report["error_categories"][issue_type] = 0
                report["error_categories"][issue_type] += 1
        
        # Generate recommendations based on most common issues
        if report["error_categories"]:
            most_common_issue = max(report["error_categories"], key=report["error_categories"].get)
            report["recommendations"] = self.common_issues[most_common_issue]["solutions"]
        
        return report
    
    def create_diagnostic_notebook(self) -> str:
        """
        Create diagnostic notebook code for troubleshooting
        """
        diagnostic_code = '''
# Diagnostic Notebook for Troubleshooting Notebook Calling Issues

import json
from datetime import datetime

def run_diagnostic_tests():
    """
    Run comprehensive diagnostic tests
    """
    print("🔍 Running Notebook Calling Diagnostics...")
    
    diagnostic_results = {
        "timestamp": datetime.now().isoformat(),
        "tests": {}
    }
    
    # Test 1: Basic notebook execution
    try:
        print("\\n1️⃣ Testing basic notebook execution...")
        result = dbutils.notebook.run(
            path="/diagnostics/simple_test_notebook",
            timeout_seconds=300,
            arguments={"test_param": "test_value"}
        )
        diagnostic_results["tests"]["basic_execution"] = {
            "status": "PASSED",
            "result": result
        }
        print("✅ Basic execution test PASSED")
    except Exception as e:
        diagnostic_results["tests"]["basic_execution"] = {
            "status": "FAILED",
            "error": str(e)
        }
        print(f"❌ Basic execution test FAILED: {str(e)}")
    
    # Test 2: Parameter passing
    try:
        print("\\n2️⃣ Testing parameter passing...")
        complex_params = {
            "string_param": "test_string",
            "number_param": "123",
            "boolean_param": "true",
            "json_param": json.dumps({"key": "value", "list": [1, 2, 3]})
        }
        
        result = dbutils.notebook.run(
            path="/diagnostics/parameter_test_notebook",
            timeout_seconds=300,
            arguments=complex_params
        )
        diagnostic_results["tests"]["parameter_passing"] = {
            "status": "PASSED",
            "result": result
        }
        print("✅ Parameter passing test PASSED")
    except Exception as e:
        diagnostic_results["tests"]["parameter_passing"] = {
            "status": "FAILED",
            "error": str(e)
        }
        print(f"❌ Parameter passing test FAILED: {str(e)}")
    
    # Test 3: Resource availability
    try:
        print("\\n3️⃣ Testing resource availability...")
        sc = spark.sparkContext
        resource_info = {
            "spark_version": spark.version,
            "executor_count": len(sc.statusTracker().getExecutorInfos()) - 1,
            "active_jobs": len(sc.statusTracker().getActiveJobIds()),
            "default_parallelism": sc.defaultParallelism
        }
        diagnostic_results["tests"]["resource_availability"] = {
            "status": "PASSED",
            "resource_info": resource_info
        }
        print(f"✅ Resource availability test PASSED: {resource_info}")
    except Exception as e:
        diagnostic_results["tests"]["resource_availability"] = {
            "status": "FAILED",
            "error": str(e)
        }
        print(f"❌ Resource availability test FAILED: {str(e)}")
    
    # Test 4: Concurrent execution
    try:
        print("\\n4️⃣ Testing concurrent execution...")
        from concurrent.futures import ThreadPoolExecutor
        
        def concurrent_test(i):
            return dbutils.notebook.run(
                path="/diagnostics/concurrent_test_notebook",
                timeout_seconds=300,
                arguments={"instance": str(i)}
            )
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(concurrent_test, i) for i in range(3)]
            results = [future.result() for future in futures]
        
        diagnostic_results["tests"]["concurrent_execution"] = {
            "status": "PASSED",
            "results": results
        }
        print("✅ Concurrent execution test PASSED")
    except Exception as e:
        diagnostic_results["tests"]["concurrent_execution"] = {
            "status": "FAILED",
            "error": str(e)
        }
        print(f"❌ Concurrent execution test FAILED: {str(e)}")
    
    # Generate summary
    passed_tests = len([t for t in diagnostic_results["tests"].values() if t["status"] == "PASSED"])
    total_tests = len(diagnostic_results["tests"])
    
    print(f"\\n📊 Diagnostic Summary: {passed_tests}/{total_tests} tests passed")
    
    return diagnostic_results

# Run diagnostics
diagnostic_results = run_diagnostic_tests()
print(f"\\n📋 Full Diagnostic Results:")
print(json.dumps(diagnostic_results, indent=2))
'''
        
        return diagnostic_code

# Initialize troubleshooting guide
troubleshoot_guide = NotebookTroubleshootingGuide()

# Example: Diagnose an error
sample_error = {
    "error_message": "Notebook execution timed out after 3600 seconds",
    "notebook_path": "/data_processing/large_dataset_analysis"
}

diagnosis = troubleshoot_guide.diagnose_issue(
    error_message=sample_error["error_message"],
    notebook_path=sample_error["notebook_path"]
)

print("🔍 Error Diagnosis:")
print(json.dumps(diagnosis, indent=2))

# Create diagnostic notebook
diagnostic_code = troubleshoot_guide.create_diagnostic_notebook()
print(f"\\n📝 Diagnostic notebook code created ({len(diagnostic_code)} characters)")
```

---

## Conclusion

This comprehensive guide covers all aspects of calling one Databricks notebook from another in Azure:

### Key Takeaways:
1. **Multiple Methods**: Use `dbutils.notebook.run()` for programmatic execution, `%run` for imports, REST API for external control, and workflows for orchestration
2. **Parameter Management**: Implement robust parameter validation and serialization strategies
3. **Error Handling**: Use comprehensive error handling with retry mechanisms and proper logging
4. **Performance**: Monitor resource usage and optimize for parallel execution when possible
5. **Best Practices**: Follow modular design, implement proper logging, and use version control

### Recommended Approach:
- **Start Simple**: Begin with `dbutils.notebook.run()` for basic notebook calling
- **Add Complexity Gradually**: Implement error handling, parameter validation, and monitoring
- **Use Workflows**: For complex multi-notebook pipelines, leverage Databricks Workflows
- **Monitor Performance**: Always monitor resource usage and execution times

This guide provides production-ready patterns and code examples that can be adapted to your specific Azure Databricks use cases.

---

*Generated on: $(date)*
*Azure Databricks Notebook Calling - Comprehensive Guide*

