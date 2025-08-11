# Azure Databricks Workflows Comprehensive Guide
## Complete Guide to Databricks Workflows and Job Orchestration

---

### Table of Contents

1. [Overview](#overview)
2. [Workflow Fundamentals](#workflow-fundamentals)
3. [Job Configuration and Management](#job-configuration-and-management)
4. [Task Types and Execution](#task-types-and-execution)
5. [Scheduling and Triggers](#scheduling-and-triggers)
6. [Dependency Management](#dependency-management)
7. [Monitoring and Alerting](#monitoring-and-alerting)
8. [Error Handling and Recovery](#error-handling-and-recovery)
9. [Advanced Workflow Patterns](#advanced-workflow-patterns)
10. [Integration with Azure Services](#integration-with-azure-services)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)

---

## Overview

Azure Databricks Workflows provide a comprehensive platform for orchestrating data processing, analytics, and machine learning pipelines. Workflows enable you to create, schedule, and monitor complex multi-task jobs with dependencies, error handling, and automated recovery mechanisms.

### Workflow Architecture

```json
{
  "databricks_workflow_architecture": {
    "workflow_components": {
      "jobs": {
        "description": "Top-level containers for workflow execution",
        "components": ["Job configuration", "Task definitions", "Scheduling", "Permissions"],
        "capabilities": ["Multi-task orchestration", "Conditional execution", "Parameter passing"],
        "scope": "Workspace-level"
      },
      "tasks": {
        "description": "Individual execution units within jobs",
        "types": ["Notebook tasks", "JAR tasks", "Python tasks", "SQL tasks", "DLT pipelines"],
        "features": ["Dependency management", "Resource allocation", "Retry policies"],
        "execution": "Cluster or serverless"
      },
      "clusters": {
        "description": "Compute resources for task execution",
        "types": ["Job clusters", "All-purpose clusters", "Serverless compute"],
        "characteristics": ["Auto-scaling", "Cost optimization", "Isolation"],
        "lifecycle": "Task-scoped or persistent"
      },
      "triggers": {
        "description": "Mechanisms to initiate workflow execution",
        "types": ["Scheduled", "File arrival", "Manual", "API-based"],
        "capabilities": ["Cron expressions", "Event-driven", "Conditional logic"],
        "integration": "External systems and services"
      }
    },
    "execution_flow": {
      "initialization": "Job validation and resource allocation",
      "task_orchestration": "Dependency resolution and parallel execution",
      "monitoring": "Real-time status tracking and logging",
      "completion": "Result aggregation and cleanup"
    },
    "data_flow": {
      "input_sources": ["ADLS", "Blob Storage", "SQL databases", "Streaming sources"],
      "processing_layers": ["Bronze (raw)", "Silver (cleaned)", "Gold (aggregated)"],
      "output_destinations": ["Data warehouses", "ML models", "Reports", "APIs"]
    }
  }
}
```

### Workflow Patterns

```json
{
  "workflow_patterns": {
    "linear_pipeline": {
      "description": "Sequential task execution with dependencies",
      "structure": "Task A -> Task B -> Task C -> Task D",
      "use_cases": ["ETL pipelines", "Data validation flows", "Model training sequences"],
      "benefits": ["Simple dependency management", "Clear execution order", "Easy debugging"],
      "limitations": ["No parallelization", "Longer execution time", "Single failure point"]
    },
    "parallel_processing": {
      "description": "Multiple tasks executing simultaneously",
      "structure": "Task A -> [Task B1, Task B2, Task B3] -> Task C",
      "use_cases": ["Multi-source ingestion", "Feature engineering", "Model ensemble training"],
      "benefits": ["Reduced execution time", "Resource optimization", "Scalability"],
      "considerations": ["Resource contention", "Dependency complexity", "Synchronization"]
    },
    "conditional_branching": {
      "description": "Dynamic task execution based on conditions",
      "structure": "Task A -> Condition -> [Path 1 | Path 2]",
      "use_cases": ["Data quality checks", "Environment-specific processing", "Error handling"],
      "benefits": ["Flexible execution", "Resource efficiency", "Adaptive workflows"],
      "complexity": ["Condition evaluation", "Path management", "State tracking"]
    },
    "fan_out_fan_in": {
      "description": "Distribute work and aggregate results",
      "structure": "Task A -> [Multiple parallel tasks] -> Aggregation Task",
      "use_cases": ["Batch processing", "Distributed analytics", "Parallel model training"],
      "benefits": ["Scalable processing", "Load distribution", "Result consolidation"],
      "challenges": ["Synchronization", "Error propagation", "Resource management"]
    }
  }
}
```

---

## Workflow Fundamentals

### Core Workflow Concepts

Understanding the fundamental concepts of Databricks workflows is essential for effective implementation and management.

```python
# Databricks Workflow Management SDK
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi
from databricks_cli.clusters.api import ClustersApi
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

class DatabricksWorkflowManager:
    """Comprehensive Databricks Workflow Management"""
    
    def __init__(self, databricks_host: str, access_token: str):
        self.host = databricks_host
        self.token = access_token
        
        # Initialize API client
        self.api_client = ApiClient(
            host=databricks_host,
            token=access_token
        )
        
        # Initialize service APIs
        self.jobs_api = JobsApi(self.api_client)
        self.clusters_api = ClustersApi(self.api_client)
        
        print(f"✓ Initialized Databricks Workflow Manager for {databricks_host}")
    
    def create_comprehensive_workflow(self, workflow_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a comprehensive multi-task workflow"""
        
        print(f"=== Creating Comprehensive Workflow: {workflow_config['name']} ===")
        
        workflow_result = {
            "workflow_name": workflow_config["name"],
            "job_id": None,
            "tasks_created": [],
            "configuration": workflow_config,
            "creation_timestamp": datetime.now().isoformat(),
            "status": "pending"
        }
        
        try:
            # Prepare job configuration
            job_config = self._prepare_job_configuration(workflow_config)
            
            # Create the job
            job_response = self.jobs_api.create_job(job_config)
            job_id = job_response['job_id']
            
            workflow_result["job_id"] = job_id
            workflow_result["status"] = "created"
            workflow_result["tasks_created"] = [task["task_key"] for task in job_config["tasks"]]
            
            print(f"✓ Workflow created successfully with Job ID: {job_id}")
            print(f"✓ Tasks created: {len(workflow_result['tasks_created'])}")
            
            # Set up monitoring and alerts
            monitoring_config = self._setup_workflow_monitoring(job_id, workflow_config)
            workflow_result["monitoring"] = monitoring_config
            
            return workflow_result
            
        except Exception as e:
            error_msg = f"Failed to create workflow: {str(e)}"
            workflow_result["status"] = "failed"
            workflow_result["error"] = error_msg
            print(f"❌ {error_msg}")
            return workflow_result
    
    def _prepare_job_configuration(self, workflow_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare comprehensive job configuration"""
        
        job_config = {
            "name": workflow_config["name"],
            "email_notifications": {
                "on_start": workflow_config.get("notifications", {}).get("on_start", []),
                "on_success": workflow_config.get("notifications", {}).get("on_success", []),
                "on_failure": workflow_config.get("notifications", {}).get("on_failure", []),
                "no_alert_for_skipped_runs": False
            },
            "webhook_notifications": {
                "on_start": workflow_config.get("webhooks", {}).get("on_start", []),
                "on_success": workflow_config.get("webhooks", {}).get("on_success", []),
                "on_failure": workflow_config.get("webhooks", {}).get("on_failure", [])
            },
            "timeout_seconds": workflow_config.get("timeout_seconds", 86400),  # 24 hours default
            "max_concurrent_runs": workflow_config.get("max_concurrent_runs", 1),
            "tasks": [],
            "job_clusters": [],
            "tags": workflow_config.get("tags", {})
        }
        
        # Add schedule if specified
        if "schedule" in workflow_config:
            job_config["schedule"] = {
                "quartz_cron_expression": workflow_config["schedule"]["cron"],
                "timezone_id": workflow_config["schedule"].get("timezone", "UTC"),
                "pause_status": "UNPAUSED"
            }
        
        # Create job clusters
        if "job_clusters" in workflow_config:
            for cluster_config in workflow_config["job_clusters"]:
                job_cluster = self._create_job_cluster_config(cluster_config)
                job_config["job_clusters"].append(job_cluster)
        
        # Create tasks
        for task_config in workflow_config["tasks"]:
            task = self._create_task_config(task_config)
            job_config["tasks"].append(task)
        
        return job_config
    
    def _create_job_cluster_config(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create job cluster configuration"""
        
        job_cluster = {
            "job_cluster_key": cluster_config["cluster_key"],
            "new_cluster": {
                "spark_version": cluster_config.get("spark_version", "11.3.x-scala2.12"),
                "node_type_id": cluster_config.get("node_type", "Standard_DS3_v2"),
                "driver_node_type_id": cluster_config.get("driver_node_type", "Standard_DS3_v2"),
                "num_workers": cluster_config.get("num_workers", 2),
                "autoscale": {
                    "min_workers": cluster_config.get("min_workers", 1),
                    "max_workers": cluster_config.get("max_workers", 8)
                } if cluster_config.get("autoscale", True) else None,
                "cluster_log_conf": {
                    "dbfs": {
                        "destination": f"dbfs:/cluster-logs/{cluster_config['cluster_key']}"
                    }
                },
                "init_scripts": cluster_config.get("init_scripts", []),
                "spark_conf": cluster_config.get("spark_conf", {}),
                "spark_env_vars": cluster_config.get("spark_env_vars", {}),
                "azure_attributes": {
                    "availability": cluster_config.get("availability", "ON_DEMAND_AZURE"),
                    "first_on_demand": cluster_config.get("first_on_demand", 1),
                    "spot_bid_max_price": cluster_config.get("spot_bid_max_price", -1)
                },
                "enable_elastic_disk": cluster_config.get("enable_elastic_disk", True),
                "disk_spec": {
                    "disk_type": {
                        "azure_disk_volume_type": "PREMIUM_LRS"
                    },
                    "disk_size": cluster_config.get("disk_size", 100)
                } if cluster_config.get("custom_disk", False) else None,
                "custom_tags": cluster_config.get("custom_tags", {})
            }
        }
        
        # Remove autoscale if num_workers is specified and autoscale is False
        if not cluster_config.get("autoscale", True):
            job_cluster["new_cluster"].pop("autoscale", None)
        else:
            job_cluster["new_cluster"].pop("num_workers", None)
        
        return job_cluster
    
    def _create_task_config(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create task configuration"""
        
        task = {
            "task_key": task_config["task_key"],
            "description": task_config.get("description", ""),
            "depends_on": [{"task_key": dep} for dep in task_config.get("depends_on", [])],
            "timeout_seconds": task_config.get("timeout_seconds", 3600),  # 1 hour default
            "max_retries": task_config.get("max_retries", 2),
            "min_retry_interval_millis": task_config.get("min_retry_interval_millis", 2000),
            "retry_on_timeout": task_config.get("retry_on_timeout", False),
            "email_notifications": task_config.get("email_notifications", {}),
            "webhook_notifications": task_config.get("webhook_notifications", {})
        }
        
        # Add cluster configuration
        if "job_cluster_key" in task_config:
            task["job_cluster_key"] = task_config["job_cluster_key"]
        elif "existing_cluster_id" in task_config:
            task["existing_cluster_id"] = task_config["existing_cluster_id"]
        elif "new_cluster" in task_config:
            task["new_cluster"] = task_config["new_cluster"]
        
        # Add task-specific configuration based on type
        task_type = task_config["type"]
        
        if task_type == "notebook":
            task["notebook_task"] = {
                "notebook_path": task_config["notebook_path"],
                "base_parameters": task_config.get("parameters", {}),
                "source": task_config.get("source", "WORKSPACE")
            }
        elif task_type == "python":
            task["python_wheel_task"] = {
                "package_name": task_config["package_name"],
                "entry_point": task_config["entry_point"],
                "parameters": task_config.get("parameters", [])
            }
        elif task_type == "jar":
            task["spark_jar_task"] = {
                "main_class_name": task_config["main_class_name"],
                "parameters": task_config.get("parameters", [])
            }
        elif task_type == "sql":
            task["sql_task"] = {
                "query": {
                    "query_id": task_config["query_id"]
                } if "query_id" in task_config else {
                    "query": task_config["query"]
                },
                "warehouse_id": task_config["warehouse_id"]
            }
        elif task_type == "dlt":
            task["pipeline_task"] = {
                "pipeline_id": task_config["pipeline_id"],
                "full_refresh": task_config.get("full_refresh", False)
            }
        
        # Add libraries if specified
        if "libraries" in task_config:
            task["libraries"] = task_config["libraries"]
        
        return task
    
    def _setup_workflow_monitoring(self, job_id: int, workflow_config: Dict[str, Any]) -> Dict[str, Any]:
        """Set up monitoring and alerting for the workflow"""
        
        monitoring_config = {
            "job_id": job_id,
            "monitoring_enabled": True,
            "alert_conditions": [],
            "dashboard_url": f"{self.host}/#job/{job_id}",
            "log_locations": []
        }
        
        # Define default alert conditions
        default_alerts = [
            {
                "condition": "job_failure",
                "threshold": 1,
                "action": "email_notification"
            },
            {
                "condition": "job_timeout",
                "threshold": workflow_config.get("timeout_seconds", 86400),
                "action": "email_notification"
            },
            {
                "condition": "task_retry_exceeded",
                "threshold": 3,
                "action": "webhook_notification"
            }
        ]
        
        monitoring_config["alert_conditions"] = default_alerts
        
        # Set up log collection locations
        log_locations = [
            f"dbfs:/databricks/jobs/{job_id}/logs",
            f"dbfs:/cluster-logs/job-{job_id}"
        ]
        monitoring_config["log_locations"] = log_locations
        
        return monitoring_config
    
    def create_etl_workflow_example(self) -> Dict[str, Any]:
        """Create a comprehensive ETL workflow example"""
        
        print("=== Creating ETL Workflow Example ===")
        
        etl_workflow_config = {
            "name": "comprehensive-etl-pipeline",
            "schedule": {
                "cron": "0 2 * * *",  # Daily at 2 AM
                "timezone": "UTC"
            },
            "timeout_seconds": 14400,  # 4 hours
            "max_concurrent_runs": 1,
            "tags": {
                "environment": "production",
                "team": "data-engineering",
                "project": "customer-analytics"
            },
            "notifications": {
                "on_failure": ["data-team@company.com"],
                "on_success": ["data-team@company.com"]
            },
            "job_clusters": [
                {
                    "cluster_key": "etl-cluster",
                    "spark_version": "11.3.x-scala2.12",
                    "node_type": "Standard_DS3_v2",
                    "autoscale": True,
                    "min_workers": 2,
                    "max_workers": 10,
                    "spark_conf": {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.sql.adaptive.coalescePartitions.enabled": "true",
                        "spark.databricks.delta.optimizeWrite.enabled": "true"
                    },
                    "custom_tags": {
                        "purpose": "etl-processing",
                        "cost-center": "data-engineering"
                    }
                }
            ],
            "tasks": [
                {
                    "task_key": "data-validation",
                    "type": "notebook",
                    "notebook_path": "/Shared/ETL/01_data_validation",
                    "job_cluster_key": "etl-cluster",
                    "timeout_seconds": 1800,  # 30 minutes
                    "max_retries": 2,
                    "parameters": {
                        "environment": "production",
                        "validation_date": "{{job.start_time}}",
                        "strict_mode": "true"
                    },
                    "description": "Validate incoming data quality and schema"
                },
                {
                    "task_key": "raw-data-ingestion",
                    "type": "notebook",
                    "notebook_path": "/Shared/ETL/02_raw_data_ingestion",
                    "job_cluster_key": "etl-cluster",
                    "depends_on": ["data-validation"],
                    "timeout_seconds": 3600,  # 1 hour
                    "max_retries": 3,
                    "parameters": {
                        "source_path": "/mnt/raw-data",
                        "target_path": "/mnt/bronze-data",
                        "processing_date": "{{job.start_time}}"
                    },
                    "description": "Ingest raw data from multiple sources"
                },
                {
                    "task_key": "customer-data-processing",
                    "type": "notebook",
                    "notebook_path": "/Shared/ETL/03_customer_processing",
                    "job_cluster_key": "etl-cluster",
                    "depends_on": ["raw-data-ingestion"],
                    "timeout_seconds": 2400,  # 40 minutes
                    "max_retries": 2,
                    "parameters": {
                        "input_path": "/mnt/bronze-data/customers",
                        "output_path": "/mnt/silver-data/customers",
                        "deduplication": "true"
                    },
                    "description": "Process and clean customer data"
                },
                {
                    "task_key": "transaction-data-processing",
                    "type": "notebook",
                    "notebook_path": "/Shared/ETL/03_transaction_processing",
                    "job_cluster_key": "etl-cluster",
                    "depends_on": ["raw-data-ingestion"],
                    "timeout_seconds": 2400,  # 40 minutes
                    "max_retries": 2,
                    "parameters": {
                        "input_path": "/mnt/bronze-data/transactions",
                        "output_path": "/mnt/silver-data/transactions",
                        "partition_by": "date"
                    },
                    "description": "Process and clean transaction data"
                },
                {
                    "task_key": "data-enrichment",
                    "type": "notebook",
                    "notebook_path": "/Shared/ETL/04_data_enrichment",
                    "job_cluster_key": "etl-cluster",
                    "depends_on": ["customer-data-processing", "transaction-data-processing"],
                    "timeout_seconds": 1800,  # 30 minutes
                    "max_retries": 2,
                    "parameters": {
                        "customer_path": "/mnt/silver-data/customers",
                        "transaction_path": "/mnt/silver-data/transactions",
                        "enriched_path": "/mnt/gold-data/customer_analytics"
                    },
                    "description": "Enrich data with external sources and create analytics tables"
                },
                {
                    "task_key": "data-quality-checks",
                    "type": "notebook",
                    "notebook_path": "/Shared/ETL/05_data_quality_checks",
                    "job_cluster_key": "etl-cluster",
                    "depends_on": ["data-enrichment"],
                    "timeout_seconds": 900,  # 15 minutes
                    "max_retries": 1,
                    "parameters": {
                        "data_path": "/mnt/gold-data/customer_analytics",
                        "quality_threshold": "0.95",
                        "alert_on_failure": "true"
                    },
                    "description": "Perform final data quality checks"
                },
                {
                    "task_key": "update-data-catalog",
                    "type": "notebook",
                    "notebook_path": "/Shared/ETL/06_update_catalog",
                    "job_cluster_key": "etl-cluster",
                    "depends_on": ["data-quality-checks"],
                    "timeout_seconds": 600,  # 10 minutes
                    "max_retries": 1,
                    "parameters": {
                        "catalog_name": "production_data",
                        "schema_name": "customer_analytics",
                        "table_path": "/mnt/gold-data/customer_analytics"
                    },
                    "description": "Update data catalog with new tables and metadata"
                }
            ]
        }
        
        # Create the workflow
        workflow_result = self.create_comprehensive_workflow(etl_workflow_config)
        
        return workflow_result
    
    def create_ml_workflow_example(self) -> Dict[str, Any]:
        """Create a comprehensive ML workflow example"""
        
        print("=== Creating ML Workflow Example ===")
        
        ml_workflow_config = {
            "name": "ml-model-training-pipeline",
            "schedule": {
                "cron": "0 6 * * 1",  # Weekly on Monday at 6 AM
                "timezone": "UTC"
            },
            "timeout_seconds": 21600,  # 6 hours
            "max_concurrent_runs": 1,
            "tags": {
                "environment": "production",
                "team": "data-science",
                "project": "customer-churn-prediction"
            },
            "notifications": {
                "on_failure": ["ml-team@company.com", "data-team@company.com"],
                "on_success": ["ml-team@company.com"]
            },
            "job_clusters": [
                {
                    "cluster_key": "ml-training-cluster",
                    "spark_version": "11.3.x-cpu-ml-scala2.12",
                    "node_type": "Standard_DS4_v2",
                    "driver_node_type": "Standard_DS4_v2",
                    "autoscale": True,
                    "min_workers": 2,
                    "max_workers": 8,
                    "spark_conf": {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.sql.adaptive.coalescePartitions.enabled": "true"
                    },
                    "custom_tags": {
                        "purpose": "ml-training",
                        "cost-center": "data-science"
                    }
                },
                {
                    "cluster_key": "ml-inference-cluster",
                    "spark_version": "11.3.x-cpu-ml-scala2.12",
                    "node_type": "Standard_DS3_v2",
                    "autoscale": True,
                    "min_workers": 1,
                    "max_workers": 4,
                    "spark_conf": {
                        "spark.sql.adaptive.enabled": "true"
                    },
                    "custom_tags": {
                        "purpose": "ml-inference",
                        "cost-center": "data-science"
                    }
                }
            ],
            "tasks": [
                {
                    "task_key": "feature-engineering",
                    "type": "notebook",
                    "notebook_path": "/Shared/ML/01_feature_engineering",
                    "job_cluster_key": "ml-training-cluster",
                    "timeout_seconds": 3600,  # 1 hour
                    "max_retries": 2,
                    "parameters": {
                        "input_path": "/mnt/gold-data/customer_analytics",
                        "feature_store_path": "/mnt/ml-data/features",
                        "lookback_days": "365"
                    },
                    "description": "Engineer features for model training"
                },
                {
                    "task_key": "data-splitting",
                    "type": "notebook",
                    "notebook_path": "/Shared/ML/02_data_splitting",
                    "job_cluster_key": "ml-training-cluster",
                    "depends_on": ["feature-engineering"],
                    "timeout_seconds": 1800,  # 30 minutes
                    "max_retries": 2,
                    "parameters": {
                        "feature_path": "/mnt/ml-data/features",
                        "train_path": "/mnt/ml-data/train",
                        "test_path": "/mnt/ml-data/test",
                        "validation_path": "/mnt/ml-data/validation",
                        "train_ratio": "0.7",
                        "test_ratio": "0.2",
                        "validation_ratio": "0.1"
                    },
                    "description": "Split data into train/test/validation sets"
                },
                {
                    "task_key": "model-training",
                    "type": "notebook",
                    "notebook_path": "/Shared/ML/03_model_training",
                    "job_cluster_key": "ml-training-cluster",
                    "depends_on": ["data-splitting"],
                    "timeout_seconds": 7200,  # 2 hours
                    "max_retries": 1,
                    "parameters": {
                        "train_path": "/mnt/ml-data/train",
                        "validation_path": "/mnt/ml-data/validation",
                        "model_output_path": "/mnt/ml-models/churn-prediction",
                        "experiment_name": "/Shared/Experiments/churn-prediction",
                        "max_evals": "50"
                    },
                    "description": "Train multiple models and select best performer"
                },
                {
                    "task_key": "model-evaluation",
                    "type": "notebook",
                    "notebook_path": "/Shared/ML/04_model_evaluation",
                    "job_cluster_key": "ml-training-cluster",
                    "depends_on": ["model-training"],
                    "timeout_seconds": 1800,  # 30 minutes
                    "max_retries": 2,
                    "parameters": {
                        "model_path": "/mnt/ml-models/churn-prediction",
                        "test_path": "/mnt/ml-data/test",
                        "evaluation_output_path": "/mnt/ml-results/evaluation",
                        "performance_threshold": "0.85"
                    },
                    "description": "Evaluate model performance on test set"
                },
                {
                    "task_key": "model-registration",
                    "type": "notebook",
                    "notebook_path": "/Shared/ML/05_model_registration",
                    "job_cluster_key": "ml-training-cluster",
                    "depends_on": ["model-evaluation"],
                    "timeout_seconds": 900,  # 15 minutes
                    "max_retries": 2,
                    "parameters": {
                        "model_path": "/mnt/ml-models/churn-prediction",
                        "model_name": "customer-churn-predictor",
                        "stage": "staging",
                        "description": "Customer churn prediction model"
                    },
                    "description": "Register model in MLflow Model Registry"
                },
                {
                    "task_key": "batch-inference",
                    "type": "notebook",
                    "notebook_path": "/Shared/ML/06_batch_inference",
                    "job_cluster_key": "ml-inference-cluster",
                    "depends_on": ["model-registration"],
                    "timeout_seconds": 2400,  # 40 minutes
                    "max_retries": 2,
                    "parameters": {
                        "model_name": "customer-churn-predictor",
                        "model_stage": "staging",
                        "input_path": "/mnt/gold-data/customer_analytics",
                        "output_path": "/mnt/ml-results/predictions",
                        "batch_size": "10000"
                    },
                    "description": "Generate batch predictions using trained model"
                }
            ]
        }
        
        # Create the workflow
        workflow_result = self.create_comprehensive_workflow(ml_workflow_config)
        
        return workflow_result
    
    def run_workflow(self, job_id: int, parameters: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Run a workflow and monitor its execution"""
        
        print(f"=== Running Workflow: Job ID {job_id} ===")
        
        run_config = {
            "job_id": job_id,
            "jar_params": [],
            "notebook_params": parameters or {},
            "python_params": [],
            "spark_submit_params": []
        }
        
        try:
            # Start the job run
            run_response = self.jobs_api.run_now(run_config)
            run_id = run_response['run_id']
            
            print(f"✓ Workflow started with Run ID: {run_id}")
            
            # Monitor the run
            run_result = self._monitor_workflow_run(run_id)
            
            return run_result
            
        except Exception as e:
            error_msg = f"Failed to run workflow: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "status": "failed",
                "error": error_msg,
                "job_id": job_id
            }
    
    def _monitor_workflow_run(self, run_id: int, poll_interval: int = 30) -> Dict[str, Any]:
        """Monitor workflow run execution"""
        
        print(f"=== Monitoring Workflow Run: {run_id} ===")
        
        run_result = {
            "run_id": run_id,
            "status": "running",
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "task_results": [],
            "final_state": None
        }
        
        start_time = time.time()
        
        try:
            while True:
                # Get run status
                run_info = self.jobs_api.get_run(run_id)
                current_state = run_info['state']['life_cycle_state']
                
                run_result["status"] = current_state.lower()
                run_result["start_time"] = run_info.get('start_time')
                
                # Update task results
                if 'tasks' in run_info:
                    run_result["task_results"] = []
                    for task in run_info['tasks']:
                        task_result = {
                            "task_key": task['task_key'],
                            "state": task['state']['life_cycle_state'],
                            "start_time": task.get('start_time'),
                            "end_time": task.get('end_time')
                        }
                        
                        if 'state' in task and 'result_state' in task['state']:
                            task_result["result_state"] = task['state']['result_state']
                        
                        run_result["task_results"].append(task_result)
                
                print(f"📊 Run Status: {current_state} - Tasks: {len(run_result['task_results'])}")
                
                # Check for completion
                if current_state in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                    run_result["final_state"] = run_info['state'].get('result_state', 'UNKNOWN')
                    run_result["end_time"] = run_info.get('end_time')
                    
                    if run_result["start_time"] and run_result["end_time"]:
                        duration = (run_result["end_time"] - run_result["start_time"]) / 1000
                        run_result["duration_seconds"] = duration
                    
                    print(f"✓ Workflow completed with state: {run_result['final_state']}")
                    break
                
                # Wait before next poll
                time.sleep(poll_interval)
                
                # Safety check for very long runs (4 hours)
                if time.time() - start_time > 14400:
                    print("⚠️ Monitoring timeout reached (4 hours)")
                    break
            
            return run_result
            
        except Exception as e:
            error_msg = f"Error monitoring workflow run: {str(e)}"
            run_result["error"] = error_msg
            print(f"❌ {error_msg}")
            return run_result
    
    def list_workflows(self, limit: int = 25, expand_tasks: bool = False) -> List[Dict[str, Any]]:
        """List all workflows in the workspace"""
        
        print("=== Listing Databricks Workflows ===")
        
        try:
            # Get list of jobs
            jobs_response = self.jobs_api.list_jobs(limit=limit, expand_tasks=expand_tasks)
            jobs = jobs_response.get('jobs', [])
            
            workflow_list = []
            for job in jobs:
                workflow_info = {
                    "job_id": job['job_id'],
                    "name": job['settings']['name'],
                    "created_time": job.get('created_time'),
                    "creator_user_name": job.get('creator_user_name'),
                    "task_count": len(job['settings'].get('tasks', [])),
                    "has_schedule": 'schedule' in job['settings'],
                    "max_concurrent_runs": job['settings'].get('max_concurrent_runs', 1),
                    "timeout_seconds": job['settings'].get('timeout_seconds'),
                    "tags": job['settings'].get('tags', {})
                }
                
                if 'schedule' in job['settings']:
                    workflow_info["schedule"] = {
                        "cron": job['settings']['schedule']['quartz_cron_expression'],
                        "timezone": job['settings']['schedule'].get('timezone_id', 'UTC'),
                        "paused": job['settings']['schedule'].get('pause_status') == 'PAUSED'
                    }
                
                workflow_list.append(workflow_info)
                
                print(f"📋 {workflow_info['name']} (ID: {workflow_info['job_id']}) - {workflow_info['task_count']} tasks")
            
            print(f"✓ Found {len(workflow_list)} workflows")
            return workflow_list
            
        except Exception as e:
            print(f"❌ Error listing workflows: {str(e)}")
            return []
    
    def get_workflow_runs(self, job_id: int, limit: int = 25) -> List[Dict[str, Any]]:
        """Get recent runs for a specific workflow"""
        
        print(f"=== Getting Workflow Runs for Job ID: {job_id} ===")
        
        try:
            runs_response = self.jobs_api.list_runs(job_id=job_id, limit=limit)
            runs = runs_response.get('runs', [])
            
            run_list = []
            for run in runs:
                run_info = {
                    "run_id": run['run_id'],
                    "job_id": run['job_id'],
                    "run_name": run.get('run_name'),
                    "state": run['state']['life_cycle_state'],
                    "result_state": run['state'].get('result_state'),
                    "start_time": run.get('start_time'),
                    "end_time": run.get('end_time'),
                    "setup_duration": run.get('setup_duration'),
                    "execution_duration": run.get('execution_duration'),
                    "cleanup_duration": run.get('cleanup_duration'),
                    "trigger": run.get('trigger', 'UNKNOWN'),
                    "run_type": run.get('run_type', 'UNKNOWN')
                }
                
                # Calculate total duration if available
                if run_info["start_time"] and run_info["end_time"]:
                    duration = (run_info["end_time"] - run_info["start_time"]) / 1000
                    run_info["total_duration_seconds"] = duration
                
                run_list.append(run_info)
                
                status_emoji = "✅" if run_info["result_state"] == "SUCCESS" else "❌" if run_info["result_state"] == "FAILED" else "⏳"
                print(f"{status_emoji} Run {run_info['run_id']}: {run_info['state']} - {run_info.get('result_state', 'N/A')}")
            
            print(f"✓ Found {len(run_list)} runs")
            return run_list
            
        except Exception as e:
            print(f"❌ Error getting workflow runs: {str(e)}")
            return []
    
    def delete_workflow(self, job_id: int) -> bool:
        """Delete a workflow"""
        
        print(f"=== Deleting Workflow: Job ID {job_id} ===")
        
        try:
            # Get job info first
            job_info = self.jobs_api.get_job(job_id)
            job_name = job_info['settings']['name']
            
            print(f"⚠️ Preparing to delete workflow: {job_name}")
            
            # Delete the job
            self.jobs_api.delete_job(job_id)
            
            print(f"✓ Workflow deleted successfully: {job_name} (ID: {job_id})")
            return True
            
        except Exception as e:
            print(f"❌ Error deleting workflow: {str(e)}")
            return False

# Example usage and demonstration functions
def demonstrate_databricks_workflows():
    """Comprehensive demonstration of Databricks workflows"""
    
    print("=== Databricks Workflows Demonstration ===")
    
    # Configuration (replace with your actual values)
    databricks_host = "https://your-workspace.azuredatabricks.net"
    access_token = "your-access-token"
    
    try:
        # Initialize workflow manager
        workflow_manager = DatabricksWorkflowManager(databricks_host, access_token)
        
        # Create ETL workflow example
        etl_workflow = workflow_manager.create_etl_workflow_example()
        print(f"ETL Workflow created with Job ID: {etl_workflow.get('job_id')}")
        
        # Create ML workflow example
        ml_workflow = workflow_manager.create_ml_workflow_example()
        print(f"ML Workflow created with Job ID: {ml_workflow.get('job_id')}")
        
        # List all workflows
        workflows = workflow_manager.list_workflows()
        
        # Get runs for the ETL workflow (if created successfully)
        if etl_workflow.get('job_id'):
            etl_runs = workflow_manager.get_workflow_runs(etl_workflow['job_id'])
        
        return {
            "status": "success",
            "etl_workflow": etl_workflow,
            "ml_workflow": ml_workflow,
            "all_workflows": workflows,
            "etl_runs": etl_runs if etl_workflow.get('job_id') else []
        }
        
    except Exception as e:
        print(f"Demonstration failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

# Run demonstration
# demo_results = demonstrate_databricks_workflows()
```

---

## Task Types and Execution

### Comprehensive Task Configuration

Databricks workflows support various task types, each optimized for different use cases and execution patterns.

```python
# Task Configuration Examples and Patterns
from typing import Dict, List, Any
import json

class TaskConfigurationManager:
    """Comprehensive task configuration management"""
    
    def __init__(self):
        self.task_templates = {}
        self.execution_patterns = {}
        self._initialize_templates()
    
    def _initialize_templates(self):
        """Initialize task templates for different use cases"""
        
        self.task_templates = {
            "notebook_task": {
                "basic": {
                    "task_key": "notebook-task",
                    "type": "notebook",
                    "notebook_path": "/Shared/Notebooks/example_notebook",
                    "job_cluster_key": "default-cluster",
                    "timeout_seconds": 3600,
                    "max_retries": 2,
                    "parameters": {}
                },
                "advanced": {
                    "task_key": "advanced-notebook-task",
                    "type": "notebook",
                    "notebook_path": "/Shared/Notebooks/advanced_processing",
                    "job_cluster_key": "high-memory-cluster",
                    "timeout_seconds": 7200,
                    "max_retries": 3,
                    "min_retry_interval_millis": 5000,
                    "retry_on_timeout": True,
                    "parameters": {
                        "input_path": "/mnt/data/input",
                        "output_path": "/mnt/data/output",
                        "processing_date": "{{job.start_time}}",
                        "batch_size": "10000",
                        "debug_mode": "false"
                    },
                    "libraries": [
                        {"pypi": {"package": "pandas>=1.3.0"}},
                        {"pypi": {"package": "scikit-learn>=1.0.0"}},
                        {"maven": {"coordinates": "com.databricks:spark-xml_2.12:0.14.0"}}
                    ],
                    "email_notifications": {
                        "on_start": ["team@company.com"],
                        "on_success": ["team@company.com"],
                        "on_failure": ["team@company.com", "alerts@company.com"]
                    }
                }
            },
            "python_task": {
                "wheel_package": {
                    "task_key": "python-wheel-task",
                    "type": "python",
                    "package_name": "my_data_processing_package",
                    "entry_point": "main",
                    "job_cluster_key": "python-cluster",
                    "timeout_seconds": 1800,
                    "max_retries": 2,
                    "parameters": [
                        "--input-path", "/mnt/data/input",
                        "--output-path", "/mnt/data/output",
                        "--config", "production"
                    ],
                    "libraries": [
                        {"whl": "dbfs:/mnt/libraries/my_package-1.0.0-py3-none-any.whl"}
                    ]
                },
                "script_execution": {
                    "task_key": "python-script-task",
                    "type": "python",
                    "python_file": "dbfs:/mnt/scripts/data_processor.py",
                    "job_cluster_key": "python-cluster",
                    "timeout_seconds": 2400,
                    "max_retries": 1,
                    "parameters": [
                        "process",
                        "--date", "{{job.start_time}}",
                        "--environment", "production"
                    ]
                }
            },
            "sql_task": {
                "query_execution": {
                    "task_key": "sql-query-task",
                    "type": "sql",
                    "query": """
                        INSERT OVERWRITE TABLE gold.customer_summary
                        SELECT 
                            customer_id,
                            COUNT(*) as transaction_count,
                            SUM(amount) as total_amount,
                            AVG(amount) as avg_amount,
                            MAX(transaction_date) as last_transaction_date
                        FROM silver.transactions 
                        WHERE transaction_date >= current_date() - INTERVAL 30 DAYS
                        GROUP BY customer_id
                    """,
                    "warehouse_id": "your-sql-warehouse-id",
                    "timeout_seconds": 1800,
                    "max_retries": 2
                },
                "parameterized_query": {
                    "task_key": "parameterized-sql-task",
                    "type": "sql",
                    "query": """
                        CREATE OR REPLACE TABLE gold.daily_summary AS
                        SELECT 
                            DATE('{{start_date}}') as summary_date,
                            COUNT(DISTINCT customer_id) as unique_customers,
                            COUNT(*) as total_transactions,
                            SUM(amount) as total_revenue
                        FROM silver.transactions 
                        WHERE transaction_date = '{{start_date}}'
                    """,
                    "warehouse_id": "your-sql-warehouse-id",
                    "timeout_seconds": 900,
                    "max_retries": 1
                }
            },
            "jar_task": {
                "spark_application": {
                    "task_key": "spark-jar-task",
                    "type": "jar",
                    "main_class_name": "com.company.data.SparkDataProcessor",
                    "job_cluster_key": "spark-cluster",
                    "timeout_seconds": 3600,
                    "max_retries": 2,
                    "parameters": [
                        "/mnt/data/input",
                        "/mnt/data/output",
                        "2024-01-01",
                        "production"
                    ],
                    "libraries": [
                        {"jar": "dbfs:/mnt/jars/data-processor-1.0.jar"},
                        {"maven": {"coordinates": "org.apache.spark:spark-sql_2.12:3.3.0"}}
                    ]
                }
            },
            "dlt_task": {
                "pipeline_execution": {
                    "task_key": "dlt-pipeline-task",
                    "type": "dlt",
                    "pipeline_id": "your-dlt-pipeline-id",
                    "full_refresh": False,
                    "timeout_seconds": 7200,
                    "max_retries": 1
                },
                "full_refresh": {
                    "task_key": "dlt-full-refresh-task",
                    "type": "dlt",
                    "pipeline_id": "your-dlt-pipeline-id",
                    "full_refresh": True,
                    "timeout_seconds": 14400,
                    "max_retries": 1
                }
            }
        }
    
    def create_data_ingestion_tasks(self) -> List[Dict[str, Any]]:
        """Create tasks for data ingestion workflow"""
        
        ingestion_tasks = [
            {
                "task_key": "validate-source-data",
                "type": "notebook",
                "notebook_path": "/Shared/DataIngestion/01_validate_sources",
                "job_cluster_key": "ingestion-cluster",
                "timeout_seconds": 900,
                "max_retries": 2,
                "parameters": {
                    "source_systems": "['salesforce', 'mysql', 'api_gateway']",
                    "validation_rules": "/mnt/config/validation_rules.json",
                    "alert_threshold": "0.95"
                },
                "description": "Validate data quality from all source systems"
            },
            {
                "task_key": "ingest-salesforce-data",
                "type": "notebook",
                "notebook_path": "/Shared/DataIngestion/02_ingest_salesforce",
                "job_cluster_key": "ingestion-cluster",
                "depends_on": ["validate-source-data"],
                "timeout_seconds": 1800,
                "max_retries": 3,
                "parameters": {
                    "salesforce_instance": "production",
                    "objects": "['Account', 'Contact', 'Opportunity', 'Lead']",
                    "output_path": "/mnt/bronze/salesforce",
                    "incremental": "true",
                    "last_modified_field": "LastModifiedDate"
                },
                "libraries": [
                    {"pypi": {"package": "simple-salesforce>=1.11.0"}}
                ],
                "description": "Ingest data from Salesforce CRM"
            },
            {
                "task_key": "ingest-mysql-data",
                "type": "notebook",
                "notebook_path": "/Shared/DataIngestion/02_ingest_mysql",
                "job_cluster_key": "ingestion-cluster",
                "depends_on": ["validate-source-data"],
                "timeout_seconds": 2400,
                "max_retries": 3,
                "parameters": {
                    "mysql_host": "mysql.company.com",
                    "database": "production_db",
                    "tables": "['customers', 'orders', 'products', 'inventory']",
                    "output_path": "/mnt/bronze/mysql",
                    "batch_size": "50000",
                    "parallel_connections": "4"
                },
                "description": "Ingest data from MySQL database"
            },
            {
                "task_key": "ingest-api-data",
                "type": "python",
                "python_file": "dbfs:/mnt/scripts/api_ingestion.py",
                "job_cluster_key": "ingestion-cluster",
                "depends_on": ["validate-source-data"],
                "timeout_seconds": 1800,
                "max_retries": 2,
                "parameters": [
                    "--api-endpoints", "/mnt/config/api_endpoints.json",
                    "--output-path", "/mnt/bronze/api_data",
                    "--rate-limit", "100",
                    "--retry-attempts", "3"
                ],
                "libraries": [
                    {"pypi": {"package": "requests>=2.25.0"}},
                    {"pypi": {"package": "tenacity>=8.0.0"}}
                ],
                "description": "Ingest data from REST APIs"
            },
            {
                "task_key": "merge-ingested-data",
                "type": "notebook",
                "notebook_path": "/Shared/DataIngestion/03_merge_data",
                "job_cluster_key": "ingestion-cluster",
                "depends_on": ["ingest-salesforce-data", "ingest-mysql-data", "ingest-api-data"],
                "timeout_seconds": 2400,
                "max_retries": 2,
                "parameters": {
                    "salesforce_path": "/mnt/bronze/salesforce",
                    "mysql_path": "/mnt/bronze/mysql",
                    "api_path": "/mnt/bronze/api_data",
                    "output_path": "/mnt/silver/integrated_data",
                    "deduplication": "true",
                    "schema_evolution": "true"
                },
                "description": "Merge and integrate data from all sources"
            }
        ]
        
        return ingestion_tasks
    
    def create_data_transformation_tasks(self) -> List[Dict[str, Any]]:
        """Create tasks for data transformation workflow"""
        
        transformation_tasks = [
            {
                "task_key": "data-profiling",
                "type": "notebook",
                "notebook_path": "/Shared/DataTransformation/01_data_profiling",
                "job_cluster_key": "transformation-cluster",
                "timeout_seconds": 1800,
                "max_retries": 1,
                "parameters": {
                    "input_path": "/mnt/silver/integrated_data",
                    "profile_output_path": "/mnt/gold/data_profiles",
                    "generate_statistics": "true",
                    "detect_anomalies": "true"
                },
                "description": "Profile data quality and generate statistics"
            },
            {
                "task_key": "customer-data-cleansing",
                "type": "notebook",
                "notebook_path": "/Shared/DataTransformation/02_customer_cleansing",
                "job_cluster_key": "transformation-cluster",
                "depends_on": ["data-profiling"],
                "timeout_seconds": 2400,
                "max_retries": 2,
                "parameters": {
                    "input_path": "/mnt/silver/integrated_data/customers",
                    "output_path": "/mnt/gold/clean_customers",
                    "cleansing_rules": "/mnt/config/customer_cleansing_rules.json",
                    "duplicate_resolution": "latest_record",
                    "address_standardization": "true"
                },
                "libraries": [
                    {"pypi": {"package": "phonenumbers>=8.12.0"}},
                    {"pypi": {"package": "email-validator>=1.1.0"}}
                ],
                "description": "Clean and standardize customer data"
            },
            {
                "task_key": "transaction-aggregation",
                "type": "sql",
                "query": """
                    CREATE OR REPLACE TABLE gold.customer_transaction_summary AS
                    SELECT 
                        c.customer_id,
                        c.customer_name,
                        c.customer_segment,
                        COUNT(t.transaction_id) as total_transactions,
                        SUM(t.transaction_amount) as total_spent,
                        AVG(t.transaction_amount) as avg_transaction_amount,
                        MIN(t.transaction_date) as first_transaction_date,
                        MAX(t.transaction_date) as last_transaction_date,
                        DATEDIFF(MAX(t.transaction_date), MIN(t.transaction_date)) as customer_lifetime_days
                    FROM gold.clean_customers c
                    LEFT JOIN silver.transactions t ON c.customer_id = t.customer_id
                    GROUP BY c.customer_id, c.customer_name, c.customer_segment
                """,
                "warehouse_id": "your-sql-warehouse-id",
                "depends_on": ["customer-data-cleansing"],
                "timeout_seconds": 1800,
                "max_retries": 2,
                "description": "Aggregate customer transaction data"
            },
            {
                "task_key": "customer-segmentation",
                "type": "notebook",
                "notebook_path": "/Shared/DataTransformation/04_customer_segmentation",
                "job_cluster_key": "ml-cluster",
                "depends_on": ["transaction-aggregation"],
                "timeout_seconds": 3600,
                "max_retries": 1,
                "parameters": {
                    "input_path": "/mnt/gold/customer_transaction_summary",
                    "output_path": "/mnt/gold/customer_segments",
                    "segmentation_method": "kmeans",
                    "num_segments": "5",
                    "features": "['total_spent', 'total_transactions', 'avg_transaction_amount', 'customer_lifetime_days']"
                },
                "libraries": [
                    {"pypi": {"package": "scikit-learn>=1.0.0"}},
                    {"pypi": {"package": "matplotlib>=3.5.0"}},
                    {"pypi": {"package": "seaborn>=0.11.0"}}
                ],
                "description": "Perform customer segmentation using machine learning"
            }
        ]
        
        return transformation_tasks
    
    def create_ml_pipeline_tasks(self) -> List[Dict[str, Any]]:
        """Create tasks for ML pipeline workflow"""
        
        ml_tasks = [
            {
                "task_key": "feature-store-preparation",
                "type": "notebook",
                "notebook_path": "/Shared/ML/01_feature_store_prep",
                "job_cluster_key": "ml-cluster",
                "timeout_seconds": 2400,
                "max_retries": 2,
                "parameters": {
                    "customer_data_path": "/mnt/gold/customer_segments",
                    "transaction_data_path": "/mnt/gold/customer_transaction_summary",
                    "feature_store_database": "feature_store",
                    "customer_features_table": "customer_features",
                    "lookback_windows": "[7, 30, 90, 365]"
                },
                "libraries": [
                    {"pypi": {"package": "databricks-feature-store>=0.3.0"}}
                ],
                "description": "Prepare features for ML model training"
            },
            {
                "task_key": "model-training-experiment",
                "type": "notebook",
                "notebook_path": "/Shared/ML/02_model_training",
                "job_cluster_key": "ml-cluster",
                "depends_on": ["feature-store-preparation"],
                "timeout_seconds": 7200,
                "max_retries": 1,
                "parameters": {
                    "experiment_name": "/Shared/Experiments/customer_lifetime_value",
                    "feature_store_database": "feature_store",
                    "target_variable": "customer_lifetime_value",
                    "algorithms": "['xgboost', 'lightgbm', 'random_forest']",
                    "hyperparameter_tuning": "true",
                    "cross_validation_folds": "5",
                    "test_size": "0.2"
                },
                "libraries": [
                    {"pypi": {"package": "xgboost>=1.6.0"}},
                    {"pypi": {"package": "lightgbm>=3.3.0"}},
                    {"pypi": {"package": "hyperopt>=0.2.7"}},
                    {"pypi": {"package": "mlflow>=1.28.0"}}
                ],
                "description": "Train and tune ML models with hyperparameter optimization"
            },
            {
                "task_key": "model-validation",
                "type": "notebook",
                "notebook_path": "/Shared/ML/03_model_validation",
                "job_cluster_key": "ml-cluster",
                "depends_on": ["model-training-experiment"],
                "timeout_seconds": 1800,
                "max_retries": 2,
                "parameters": {
                    "experiment_name": "/Shared/Experiments/customer_lifetime_value",
                    "validation_dataset_path": "/mnt/ml/validation_data",
                    "performance_threshold": "0.85",
                    "model_registry_name": "customer_lifetime_value_model",
                    "staging_deployment": "true"
                },
                "description": "Validate model performance and register in MLflow"
            },
            {
                "task_key": "batch-scoring",
                "type": "notebook",
                "notebook_path": "/Shared/ML/04_batch_scoring",
                "job_cluster_key": "ml-inference-cluster",
                "depends_on": ["model-validation"],
                "timeout_seconds": 3600,
                "max_retries": 2,
                "parameters": {
                    "model_name": "customer_lifetime_value_model",
                    "model_stage": "Staging",
                    "scoring_data_path": "/mnt/gold/customer_segments",
                    "predictions_output_path": "/mnt/gold/customer_predictions",
                    "batch_size": "10000"
                },
                "description": "Generate batch predictions using trained model"
            },
            {
                "task_key": "model-monitoring-setup",
                "type": "notebook",
                "notebook_path": "/Shared/ML/05_model_monitoring",
                "job_cluster_key": "ml-cluster",
                "depends_on": ["batch-scoring"],
                "timeout_seconds": 900,
                "max_retries": 1,
                "parameters": {
                    "model_name": "customer_lifetime_value_model",
                    "predictions_table": "gold.customer_predictions",
                    "monitoring_table": "gold.model_monitoring",
                    "drift_detection": "true",
                    "performance_tracking": "true"
                },
                "description": "Set up model monitoring and drift detection"
            }
        ]
        
        return ml_tasks
    
    def create_data_quality_tasks(self) -> List[Dict[str, Any]]:
        """Create tasks for data quality monitoring workflow"""
        
        quality_tasks = [
            {
                "task_key": "schema-validation",
                "type": "notebook",
                "notebook_path": "/Shared/DataQuality/01_schema_validation",
                "job_cluster_key": "quality-cluster",
                "timeout_seconds": 900,
                "max_retries": 1,
                "parameters": {
                    "data_paths": "[\'/mnt/bronze/salesforce\', \'/mnt/bronze/mysql\', \'/mnt/silver/integrated_data\']",
                    "schema_registry_path": "/mnt/config/schemas",
                    "validation_output_path": "/mnt/quality/schema_validation",
                    "strict_mode": "false"
                },
                "description": "Validate data schemas against registered schemas"
            },
            {
                "task_key": "data-completeness-check",
                "type": "sql",
                "query": """
                    CREATE OR REPLACE TABLE quality.completeness_report AS
                    WITH completeness_metrics AS (
                        SELECT 
                            'customers' as table_name,
                            COUNT(*) as total_records,
                            COUNT(customer_id) as non_null_customer_id,
                            COUNT(customer_name) as non_null_customer_name,
                            COUNT(email) as non_null_email,
                            COUNT(phone) as non_null_phone,
                            CURRENT_TIMESTAMP() as check_timestamp
                        FROM gold.clean_customers
                        
                        UNION ALL
                        
                        SELECT 
                            'transactions' as table_name,
                            COUNT(*) as total_records,
                            COUNT(transaction_id) as non_null_transaction_id,
                            COUNT(customer_id) as non_null_customer_id,
                            COUNT(transaction_amount) as non_null_amount,
                            COUNT(transaction_date) as non_null_date,
                            CURRENT_TIMESTAMP() as check_timestamp
                        FROM silver.transactions
                    )
                    SELECT 
                        *,
                        ROUND((non_null_customer_id / total_records) * 100, 2) as completeness_percentage
                    FROM completeness_metrics
                """,
                "warehouse_id": "your-sql-warehouse-id",
                "depends_on": ["schema-validation"],
                "timeout_seconds": 600,
                "max_retries": 1,
                "description": "Check data completeness across key tables"
            },
            {
                "task_key": "data-freshness-check",
                "type": "notebook",
                "notebook_path": "/Shared/DataQuality/03_freshness_check",
                "job_cluster_key": "quality-cluster",
                "depends_on": ["data-completeness-check"],
                "timeout_seconds": 600,
                "max_retries": 1,
                "parameters": {
                    "tables_to_check": "['gold.clean_customers', 'silver.transactions', 'gold.customer_segments']",
                    "freshness_thresholds": "{'gold.clean_customers': 24, 'silver.transactions': 2, 'gold.customer_segments': 168}",
                    "alert_output_path": "/mnt/quality/freshness_alerts"
                },
                "description": "Monitor data freshness and generate alerts"
            },
            {
                "task_key": "anomaly-detection",
                "type": "notebook",
                "notebook_path": "/Shared/DataQuality/04_anomaly_detection",
                "job_cluster_key": "ml-cluster",
                "depends_on": ["data-freshness-check"],
                "timeout_seconds": 1800,
                "max_retries": 1,
                "parameters": {
                    "transaction_data_path": "/mnt/silver/transactions",
                    "customer_data_path": "/mnt/gold/clean_customers",
                    "anomaly_output_path": "/mnt/quality/anomalies",
                    "detection_methods": "['isolation_forest', 'statistical_outliers']",
                    "sensitivity": "medium"
                },
                "libraries": [
                    {"pypi": {"package": "scikit-learn>=1.0.0"}},
                    {"pypi": {"package": "scipy>=1.7.0"}}
                ],
                "description": "Detect data anomalies using ML techniques"
            },
            {
                "task_key": "quality-report-generation",
                "type": "notebook",
                "notebook_path": "/Shared/DataQuality/05_quality_report",
                "job_cluster_key": "quality-cluster",
                "depends_on": ["anomaly-detection"],
                "timeout_seconds": 900,
                "max_retries": 1,
                "parameters": {
                    "schema_validation_path": "/mnt/quality/schema_validation",
                    "completeness_table": "quality.completeness_report",
                    "freshness_alerts_path": "/mnt/quality/freshness_alerts",
                    "anomalies_path": "/mnt/quality/anomalies",
                    "report_output_path": "/mnt/quality/daily_report",
                    "email_recipients": "['data-team@company.com', 'quality-team@company.com']"
                },
                "description": "Generate comprehensive data quality report"
            }
        ]
        
        return quality_tasks

# Example usage
def demonstrate_task_configurations():
    """Demonstrate various task configurations"""
    
    print("=== Task Configuration Examples ===")
    
    task_manager = TaskConfigurationManager()
    
    # Generate different task sets
    ingestion_tasks = task_manager.create_data_ingestion_tasks()
    transformation_tasks = task_manager.create_data_transformation_tasks()
    ml_tasks = task_manager.create_ml_pipeline_tasks()
    quality_tasks = task_manager.create_data_quality_tasks()
    
    print(f"✓ Data Ingestion Tasks: {len(ingestion_tasks)}")
    print(f"✓ Data Transformation Tasks: {len(transformation_tasks)}")
    print(f"✓ ML Pipeline Tasks: {len(ml_tasks)}")
    print(f"✓ Data Quality Tasks: {len(quality_tasks)}")
    
    return {
        "ingestion_tasks": ingestion_tasks,
        "transformation_tasks": transformation_tasks,
        "ml_tasks": ml_tasks,
        "quality_tasks": quality_tasks,
        "task_templates": task_manager.task_templates
    }

# Run demonstration
# task_demo_results = demonstrate_task_configurations()
```

This comprehensive guide provides everything needed to understand, implement, and manage Databricks workflows effectively, ensuring robust data processing and analytics pipelines in Azure environments.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*