# Azure Activity Success & Failure Management Guide
## Comprehensive Guide to Monitoring, Handling, and Troubleshooting Azure Activities

---

### Table of Contents

1. [Overview](#overview)
2. [Azure Activity Types](#azure-activity-types)
3. [Activity Monitoring Framework](#activity-monitoring-framework)
4. [Azure Data Factory Activities](#azure-data-factory-activities)
5. [Azure Databricks Activities](#azure-databricks-activities)
6. [Azure Functions Activities](#azure-functions-activities)
7. [Azure Logic Apps Activities](#azure-logic-apps-activities)
8. [Failure Handling Strategies](#failure-handling-strategies)
9. [Success Patterns & Best Practices](#success-patterns--best-practices)
10. [Monitoring & Alerting](#monitoring--alerting)
11. [Troubleshooting Guide](#troubleshooting-guide)
12. [Real-World Examples](#real-world-examples)

---

## Overview

Azure activities represent the fundamental units of work across various Azure services. Understanding how to monitor, handle failures, and ensure success is critical for building resilient cloud applications and data pipelines.

### Activity Lifecycle

```json
{
  "azure_activity_lifecycle": {
    "states": {
      "queued": {
        "description": "Activity is waiting to be executed",
        "typical_duration": "Seconds to minutes",
        "monitoring_focus": "Queue depth, resource availability"
      },
      "in_progress": {
        "description": "Activity is currently executing",
        "typical_duration": "Minutes to hours",
        "monitoring_focus": "Progress indicators, resource utilization"
      },
      "succeeded": {
        "description": "Activity completed successfully",
        "typical_duration": "N/A",
        "monitoring_focus": "Output validation, performance metrics"
      },
      "failed": {
        "description": "Activity encountered an error and stopped",
        "typical_duration": "N/A",
        "monitoring_focus": "Error details, failure patterns"
      },
      "cancelled": {
        "description": "Activity was manually cancelled or timed out",
        "typical_duration": "N/A",
        "monitoring_focus": "Cancellation reasons, timeout settings"
      },
      "skipped": {
        "description": "Activity was skipped due to conditions",
        "typical_duration": "N/A",
        "monitoring_focus": "Skip conditions, dependency logic"
      }
    },
    "transitions": {
      "normal_flow": ["queued", "in_progress", "succeeded"],
      "failure_flow": ["queued", "in_progress", "failed"],
      "cancellation_flow": ["queued", "in_progress", "cancelled"],
      "skip_flow": ["queued", "skipped"]
    }
  }
}
```

### Common Failure Categories

```json
{
  "failure_categories": {
    "infrastructure_failures": {
      "description": "Issues with underlying Azure infrastructure",
      "examples": ["Service outages", "Network connectivity", "Resource unavailability"],
      "typical_resolution": "Retry with exponential backoff, failover to different regions"
    },
    "configuration_errors": {
      "description": "Incorrect configuration or setup",
      "examples": ["Invalid connection strings", "Missing permissions", "Wrong parameters"],
      "typical_resolution": "Fix configuration, update credentials, adjust parameters"
    },
    "data_quality_issues": {
      "description": "Problems with input data",
      "examples": ["Schema mismatches", "Corrupt files", "Missing data"],
      "typical_resolution": "Data validation, cleansing, schema evolution handling"
    },
    "resource_constraints": {
      "description": "Insufficient resources for execution",
      "examples": ["Out of memory", "CPU throttling", "Storage limits"],
      "typical_resolution": "Scale up resources, optimize queries, implement partitioning"
    },
    "timeout_errors": {
      "description": "Activities exceeding time limits",
      "examples": ["Long-running queries", "Network timeouts", "API rate limits"],
      "typical_resolution": "Increase timeouts, optimize performance, implement chunking"
    },
    "dependency_failures": {
      "description": "Failures in dependent services or activities",
      "examples": ["Upstream service down", "Database unavailable", "API failures"],
      "typical_resolution": "Circuit breaker patterns, graceful degradation, retry logic"
    }
  }
}
```

---

## Azure Activity Types

### Data Processing Activities

```json
{
  "data_processing_activities": {
    "azure_data_factory": {
      "copy_activity": {
        "description": "Copy data between data stores",
        "common_failures": ["Connection failures", "Schema mismatches", "Permission issues"],
        "success_indicators": ["Rows copied", "Data validation passed", "Performance metrics"]
      },
      "data_flow": {
        "description": "Transform data using mapping data flows",
        "common_failures": ["Transformation errors", "Memory issues", "Cluster startup failures"],
        "success_indicators": ["Transformation completed", "Output row count", "Data quality checks"]
      },
      "stored_procedure": {
        "description": "Execute stored procedures in databases",
        "common_failures": ["SQL errors", "Timeout issues", "Parameter problems"],
        "success_indicators": ["Execution completed", "Expected results returned", "No errors logged"]
      },
      "web_activity": {
        "description": "Call REST APIs or web services",
        "common_failures": ["HTTP errors", "Authentication failures", "Network issues"],
        "success_indicators": ["HTTP 200 response", "Expected payload", "Response time acceptable"]
      }
    },
    "azure_databricks": {
      "notebook_activity": {
        "description": "Execute Databricks notebooks",
        "common_failures": ["Code errors", "Cluster issues", "Resource constraints"],
        "success_indicators": ["Notebook completed", "Expected outputs", "No exceptions"]
      },
      "jar_activity": {
        "description": "Execute JAR files on Databricks",
        "common_failures": ["Class not found", "Dependency issues", "Memory errors"],
        "success_indicators": ["Job completed", "Exit code 0", "Expected results"]
      }
    },
    "azure_synapse": {
      "sql_pool_activity": {
        "description": "Execute queries on SQL pools",
        "common_failures": ["Query errors", "Resource limits", "Deadlocks"],
        "success_indicators": ["Query completed", "Expected row count", "Performance acceptable"]
      },
      "spark_activity": {
        "description": "Execute Spark jobs on Synapse",
        "common_failures": ["Spark errors", "Driver failures", "Executor issues"],
        "success_indicators": ["Job completed", "All stages successful", "Output validated"]
      }
    }
  }
}
```

---

## Activity Monitoring Framework

### Comprehensive Monitoring System

```python
# Azure Activity Monitoring Framework
import json
import time
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import requests
from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, MetricsQueryClient
from azure.storage.blob import BlobServiceClient

class ActivityStatus(Enum):
    """Activity status enumeration"""
    QUEUED = "Queued"
    IN_PROGRESS = "InProgress" 
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    SKIPPED = "Skipped"
    TIMEOUT = "Timeout"

class FailureCategory(Enum):
    """Failure category enumeration"""
    INFRASTRUCTURE = "Infrastructure"
    CONFIGURATION = "Configuration"
    DATA_QUALITY = "DataQuality"
    RESOURCE_CONSTRAINT = "ResourceConstraint"
    TIMEOUT = "Timeout"
    DEPENDENCY = "Dependency"
    UNKNOWN = "Unknown"

@dataclass
class ActivityMetrics:
    """Activity performance metrics"""
    activity_id: str
    activity_name: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: Optional[float]
    status: ActivityStatus
    rows_processed: Optional[int] = None
    data_size_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    error_message: Optional[str] = None
    failure_category: Optional[FailureCategory] = None

@dataclass
class ActivityAlert:
    """Activity alert definition"""
    alert_id: str
    activity_id: str
    alert_type: str
    severity: str
    message: str
    timestamp: datetime
    resolved: bool = False
    resolution_time: Optional[datetime] = None

class ActivityMonitor:
    """Comprehensive activity monitoring system"""
    
    def __init__(self, workspace_id: str, subscription_id: str):
        self.workspace_id = workspace_id
        self.subscription_id = subscription_id
        self.credential = DefaultAzureCredential()
        self.logs_client = LogsQueryClient(self.credential)
        self.metrics_client = MetricsQueryClient(self.credential)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Activity tracking
        self.active_activities: Dict[str, ActivityMetrics] = {}
        self.completed_activities: List[ActivityMetrics] = []
        self.alerts: List[ActivityAlert] = []
    
    def start_activity_monitoring(self, activity_id: str, activity_name: str, 
                                activity_type: str = "Generic") -> ActivityMetrics:
        """Start monitoring an activity"""
        
        self.logger.info(f"Starting monitoring for activity: {activity_id} ({activity_name})")
        
        activity_metrics = ActivityMetrics(
            activity_id=activity_id,
            activity_name=activity_name,
            start_time=datetime.now(),
            end_time=None,
            duration_seconds=None,
            status=ActivityStatus.QUEUED
        )
        
        self.active_activities[activity_id] = activity_metrics
        
        return activity_metrics
    
    def update_activity_status(self, activity_id: str, status: ActivityStatus, 
                             error_message: Optional[str] = None,
                             failure_category: Optional[FailureCategory] = None):
        """Update activity status"""
        
        if activity_id not in self.active_activities:
            self.logger.warning(f"Activity {activity_id} not found in active activities")
            return
        
        activity = self.active_activities[activity_id]
        activity.status = status
        
        if status in [ActivityStatus.SUCCEEDED, ActivityStatus.FAILED, 
                     ActivityStatus.CANCELLED, ActivityStatus.TIMEOUT]:
            activity.end_time = datetime.now()
            activity.duration_seconds = (activity.end_time - activity.start_time).total_seconds()
            
            if error_message:
                activity.error_message = error_message
            if failure_category:
                activity.failure_category = failure_category
            
            # Move to completed activities
            self.completed_activities.append(activity)
            del self.active_activities[activity_id]
            
            self.logger.info(f"Activity {activity_id} completed with status: {status.value}")
            
            # Generate alerts for failures
            if status == ActivityStatus.FAILED:
                self.generate_failure_alert(activity)
        
        self.logger.info(f"Updated activity {activity_id} status to: {status.value}")
    
    def update_activity_metrics(self, activity_id: str, metrics: Dict[str, Any]):
        """Update activity performance metrics"""
        
        if activity_id not in self.active_activities:
            self.logger.warning(f"Activity {activity_id} not found in active activities")
            return
        
        activity = self.active_activities[activity_id]
        
        # Update metrics
        if 'rows_processed' in metrics:
            activity.rows_processed = metrics['rows_processed']
        if 'data_size_mb' in metrics:
            activity.data_size_mb = metrics['data_size_mb']
        if 'cpu_usage_percent' in metrics:
            activity.cpu_usage_percent = metrics['cpu_usage_percent']
        if 'memory_usage_mb' in metrics:
            activity.memory_usage_mb = metrics['memory_usage_mb']
        
        self.logger.debug(f"Updated metrics for activity {activity_id}: {metrics}")
    
    def generate_failure_alert(self, activity: ActivityMetrics):
        """Generate alert for failed activity"""
        
        alert = ActivityAlert(
            alert_id=f"alert_{activity.activity_id}_{int(time.time())}",
            activity_id=activity.activity_id,
            alert_type="ActivityFailure",
            severity="High" if activity.failure_category in [
                FailureCategory.INFRASTRUCTURE, FailureCategory.DEPENDENCY
            ] else "Medium",
            message=f"Activity '{activity.activity_name}' failed: {activity.error_message}",
            timestamp=datetime.now()
        )
        
        self.alerts.append(alert)
        self.logger.error(f"Generated failure alert: {alert.alert_id}")
        
        return alert
    
    def get_activity_statistics(self, time_window_hours: int = 24) -> Dict[str, Any]:
        """Get activity statistics for specified time window"""
        
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        
        # Filter activities within time window
        recent_activities = [
            activity for activity in self.completed_activities
            if activity.start_time >= cutoff_time
        ]
        
        if not recent_activities:
            return {
                "total_activities": 0,
                "success_rate": 0.0,
                "failure_rate": 0.0,
                "average_duration": 0.0,
                "failure_categories": {}
            }
        
        # Calculate statistics
        total_activities = len(recent_activities)
        successful_activities = len([a for a in recent_activities if a.status == ActivityStatus.SUCCEEDED])
        failed_activities = len([a for a in recent_activities if a.status == ActivityStatus.FAILED])
        
        success_rate = (successful_activities / total_activities) * 100
        failure_rate = (failed_activities / total_activities) * 100
        
        # Calculate average duration
        durations = [a.duration_seconds for a in recent_activities if a.duration_seconds is not None]
        average_duration = sum(durations) / len(durations) if durations else 0.0
        
        # Failure categories breakdown
        failure_categories = {}
        for activity in recent_activities:
            if activity.status == ActivityStatus.FAILED and activity.failure_category:
                category = activity.failure_category.value
                failure_categories[category] = failure_categories.get(category, 0) + 1
        
        statistics = {
            "time_window_hours": time_window_hours,
            "total_activities": total_activities,
            "successful_activities": successful_activities,
            "failed_activities": failed_activities,
            "success_rate": round(success_rate, 2),
            "failure_rate": round(failure_rate, 2),
            "average_duration_seconds": round(average_duration, 2),
            "failure_categories": failure_categories,
            "active_activities_count": len(self.active_activities),
            "total_alerts": len([a for a in self.alerts if not a.resolved])
        }
        
        self.logger.info(f"Generated activity statistics: {statistics}")
        return statistics
    
    def query_azure_logs(self, query: str, time_range: timedelta) -> List[Dict]:
        """Query Azure Monitor logs"""
        
        try:
            response = self.logs_client.query_workspace(
                workspace_id=self.workspace_id,
                query=query,
                timespan=time_range
            )
            
            results = []
            for table in response.tables:
                for row in table.rows:
                    row_dict = {}
                    for i, column in enumerate(table.columns):
                        row_dict[column.name] = row[i]
                    results.append(row_dict)
            
            self.logger.info(f"Retrieved {len(results)} log entries")
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to query Azure logs: {str(e)}")
            return []
    
    def get_data_factory_activities(self, factory_name: str, time_range_hours: int = 24) -> List[Dict]:
        """Get Azure Data Factory activity runs"""
        
        query = f"""
        ADFActivityRun
        | where TimeGenerated >= ago({time_range_hours}h)
        | where DataFactoryName == "{factory_name}"
        | project 
            TimeGenerated,
            ActivityName,
            ActivityType,
            Status,
            Start,
            End,
            Duration = End - Start,
            ErrorMessage = iff(Status == "Failed", ErrorMessage, ""),
            PipelineName,
            ActivityRunId
        | order by TimeGenerated desc
        """
        
        return self.query_azure_logs(query, timedelta(hours=time_range_hours))
    
    def get_databricks_activities(self, workspace_name: str, time_range_hours: int = 24) -> List[Dict]:
        """Get Azure Databricks job runs"""
        
        query = f"""
        DatabricksJobs
        | where TimeGenerated >= ago({time_range_hours}h)
        | where WorkspaceName == "{workspace_name}"
        | project 
            TimeGenerated,
            JobName,
            RunId,
            Status = ResultState,
            StartTime = StartTime,
            EndTime = EndTime,
            Duration = EndTime - StartTime,
            ErrorMessage = iff(ResultState == "FAILED", StateMessage, ""),
            ClusterName
        | order by TimeGenerated desc
        """
        
        return self.query_azure_logs(query, timedelta(hours=time_range_hours))
    
    def export_activity_report(self, output_path: str, time_window_hours: int = 24):
        """Export comprehensive activity report"""
        
        statistics = self.get_activity_statistics(time_window_hours)
        
        report = {
            "report_generated": datetime.now().isoformat(),
            "time_window_hours": time_window_hours,
            "statistics": statistics,
            "active_activities": [asdict(activity) for activity in self.active_activities.values()],
            "recent_completed_activities": [
                asdict(activity) for activity in self.completed_activities[-50:]  # Last 50
            ],
            "unresolved_alerts": [
                asdict(alert) for alert in self.alerts if not alert.resolved
            ]
        }
        
        # Convert datetime objects to strings for JSON serialization
        def datetime_converter(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object {obj} is not JSON serializable")
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=datetime_converter)
        
        self.logger.info(f"Activity report exported to: {output_path}")
        return report

# Example usage and demonstration
class ActivityMonitoringDemo:
    """Demonstration of activity monitoring capabilities"""
    
    def __init__(self):
        self.monitor = ActivityMonitor(
            workspace_id="your-log-analytics-workspace-id",
            subscription_id="your-subscription-id"
        )
    
    def demonstrate_activity_lifecycle(self):
        """Demonstrate complete activity lifecycle monitoring"""
        
        print("=== Activity Lifecycle Monitoring Demo ===")
        
        # Simulate multiple activities
        activities = [
            {"id": "copy_001", "name": "Copy Customer Data", "type": "Copy"},
            {"id": "transform_001", "name": "Transform Sales Data", "type": "DataFlow"},
            {"id": "load_001", "name": "Load to Warehouse", "type": "StoredProcedure"}
        ]
        
        # Start activities
        for activity in activities:
            self.monitor.start_activity_monitoring(
                activity["id"], 
                activity["name"], 
                activity["type"]
            )
            print(f"Started monitoring: {activity['name']}")
        
        # Simulate activity progression
        import time
        import random
        
        for i, activity in enumerate(activities):
            activity_id = activity["id"]
            
            # Update to in progress
            time.sleep(1)
            self.monitor.update_activity_status(activity_id, ActivityStatus.IN_PROGRESS)
            print(f"Activity {activity['name']} is now in progress")
            
            # Update metrics during execution
            time.sleep(2)
            self.monitor.update_activity_metrics(activity_id, {
                "rows_processed": random.randint(1000, 100000),
                "data_size_mb": random.uniform(10, 1000),
                "cpu_usage_percent": random.uniform(20, 80),
                "memory_usage_mb": random.uniform(100, 2000)
            })
            
            # Complete activity (simulate some failures)
            time.sleep(2)
            if i == 1:  # Simulate failure for second activity
                self.monitor.update_activity_status(
                    activity_id, 
                    ActivityStatus.FAILED,
                    error_message="Transformation failed due to schema mismatch",
                    failure_category=FailureCategory.DATA_QUALITY
                )
                print(f"Activity {activity['name']} failed")
            else:
                self.monitor.update_activity_status(activity_id, ActivityStatus.SUCCEEDED)
                print(f"Activity {activity['name']} succeeded")
        
        # Generate statistics
        stats = self.monitor.get_activity_statistics(1)  # Last 1 hour
        print("\n=== Activity Statistics ===")
        for key, value in stats.items():
            print(f"{key}: {value}")
        
        # Export report
        report_path = "activity_monitoring_report.json"
        self.monitor.export_activity_report(report_path, 1)
        print(f"\nReport exported to: {report_path}")
        
        return stats

# Run demonstration
# demo = ActivityMonitoringDemo()
# demo_results = demo.demonstrate_activity_lifecycle()
```

---

## Azure Data Factory Activities

### Data Factory Activity Monitoring

```python
# Azure Data Factory Activity Management
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import json
from datetime import datetime, timedelta

class ADFActivityManager:
    """Manage Azure Data Factory activities with comprehensive monitoring"""
    
    def __init__(self, subscription_id: str, resource_group: str, factory_name: str):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.credential = DefaultAzureCredential()
        self.adf_client = DataFactoryManagementClient(self.credential, subscription_id)
        
        # Activity tracking
        self.activity_runs: Dict[str, Any] = {}
        self.pipeline_runs: Dict[str, Any] = {}
    
    def monitor_pipeline_run(self, pipeline_name: str, run_id: str) -> Dict[str, Any]:
        """Monitor a specific pipeline run"""
        
        print(f"=== Monitoring Pipeline Run: {pipeline_name} ({run_id}) ===")
        
        try:
            # Get pipeline run details
            pipeline_run = self.adf_client.pipeline_runs.get(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                run_id=run_id
            )
            
            pipeline_info = {
                "run_id": run_id,
                "pipeline_name": pipeline_run.pipeline_name,
                "status": pipeline_run.status,
                "run_start": pipeline_run.run_start.isoformat() if pipeline_run.run_start else None,
                "run_end": pipeline_run.run_end.isoformat() if pipeline_run.run_end else None,
                "duration_seconds": None,
                "parameters": pipeline_run.parameters,
                "message": pipeline_run.message,
                "activities": []
            }
            
            # Calculate duration if completed
            if pipeline_run.run_start and pipeline_run.run_end:
                duration = pipeline_run.run_end - pipeline_run.run_start
                pipeline_info["duration_seconds"] = duration.total_seconds()
            
            print(f"Pipeline Status: {pipeline_run.status}")
            print(f"Start Time: {pipeline_run.run_start}")
            print(f"End Time: {pipeline_run.run_end}")
            
            # Get activity runs for this pipeline
            activity_runs = self.get_activity_runs(run_id)
            pipeline_info["activities"] = activity_runs
            
            # Store pipeline run info
            self.pipeline_runs[run_id] = pipeline_info
            
            return pipeline_info
            
        except Exception as e:
            print(f"Error monitoring pipeline run: {str(e)}")
            return {}
    
    def get_activity_runs(self, pipeline_run_id: str) -> List[Dict[str, Any]]:
        """Get all activity runs for a pipeline run"""
        
        print(f"Getting activity runs for pipeline: {pipeline_run_id}")
        
        try:
            # Query activity runs
            activity_runs_response = self.adf_client.activity_runs.query_by_pipeline_run(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                run_id=pipeline_run_id,
                filter_parameters={
                    "lastUpdatedAfter": (datetime.now() - timedelta(days=7)).isoformat(),
                    "lastUpdatedBefore": datetime.now().isoformat()
                }
            )
            
            activities = []
            for activity_run in activity_runs_response.value:
                activity_info = {
                    "activity_run_id": activity_run.activity_run_id,
                    "activity_name": activity_run.activity_name,
                    "activity_type": activity_run.activity_type,
                    "status": activity_run.status,
                    "activity_run_start": activity_run.activity_run_start.isoformat() if activity_run.activity_run_start else None,
                    "activity_run_end": activity_run.activity_run_end.isoformat() if activity_run.activity_run_end else None,
                    "duration_seconds": None,
                    "error": None,
                    "output": activity_run.output,
                    "input": activity_run.input
                }
                
                # Calculate duration
                if activity_run.activity_run_start and activity_run.activity_run_end:
                    duration = activity_run.activity_run_end - activity_run.activity_run_start
                    activity_info["duration_seconds"] = duration.total_seconds()
                
                # Extract error information if failed
                if activity_run.status == "Failed" and activity_run.error:
                    activity_info["error"] = {
                        "error_code": activity_run.error.error_code,
                        "message": activity_run.error.message,
                        "failure_type": activity_run.error.failure_type
                    }
                
                activities.append(activity_info)
                
                print(f"  Activity: {activity_run.activity_name} - Status: {activity_run.status}")
                
                # Store activity run info
                self.activity_runs[activity_run.activity_run_id] = activity_info
            
            return activities
            
        except Exception as e:
            print(f"Error getting activity runs: {str(e)}")
            return []
    
    def analyze_activity_failures(self, time_window_hours: int = 24) -> Dict[str, Any]:
        """Analyze activity failures within time window"""
        
        print(f"=== Analyzing Activity Failures (Last {time_window_hours} hours) ===")
        
        try:
            # Query recent pipeline runs
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=time_window_hours)
            
            pipeline_runs_response = self.adf_client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                filter_parameters={
                    "lastUpdatedAfter": start_time.isoformat(),
                    "lastUpdatedBefore": end_time.isoformat()
                }
            )
            
            failure_analysis = {
                "time_window_hours": time_window_hours,
                "total_pipeline_runs": 0,
                "failed_pipeline_runs": 0,
                "total_activity_runs": 0,
                "failed_activity_runs": 0,
                "failure_patterns": {},
                "error_categories": {},
                "most_failed_activities": {},
                "recommendations": []
            }
            
            failed_activities = []
            
            for pipeline_run in pipeline_runs_response.value:
                failure_analysis["total_pipeline_runs"] += 1
                
                if pipeline_run.status == "Failed":
                    failure_analysis["failed_pipeline_runs"] += 1
                
                # Get activity runs for each pipeline
                activity_runs = self.get_activity_runs(pipeline_run.run_id)
                failure_analysis["total_activity_runs"] += len(activity_runs)
                
                for activity in activity_runs:
                    if activity["status"] == "Failed":
                        failure_analysis["failed_activity_runs"] += 1
                        failed_activities.append(activity)
            
            # Analyze failure patterns
            for activity in failed_activities:
                activity_name = activity["activity_name"]
                activity_type = activity["activity_type"]
                
                # Count failures by activity name
                if activity_name in failure_analysis["most_failed_activities"]:
                    failure_analysis["most_failed_activities"][activity_name] += 1
                else:
                    failure_analysis["most_failed_activities"][activity_name] = 1
                
                # Categorize errors
                if activity["error"]:
                    error_code = activity["error"]["error_code"]
                    failure_type = activity["error"]["failure_type"]
                    
                    if error_code in failure_analysis["error_categories"]:
                        failure_analysis["error_categories"][error_code] += 1
                    else:
                        failure_analysis["error_categories"][error_code] = 1
                    
                    # Pattern analysis
                    pattern = f"{activity_type}_{failure_type}"
                    if pattern in failure_analysis["failure_patterns"]:
                        failure_analysis["failure_patterns"][pattern] += 1
                    else:
                        failure_analysis["failure_patterns"][pattern] = 1
            
            # Generate recommendations
            if failure_analysis["failed_activity_runs"] > 0:
                failure_rate = (failure_analysis["failed_activity_runs"] / failure_analysis["total_activity_runs"]) * 100
                
                if failure_rate > 10:
                    failure_analysis["recommendations"].append("High failure rate detected - review pipeline design and error handling")
                
                # Specific recommendations based on error patterns
                for error_code, count in failure_analysis["error_categories"].items():
                    if "timeout" in error_code.lower():
                        failure_analysis["recommendations"].append("Consider increasing timeout settings for long-running activities")
                    elif "connection" in error_code.lower():
                        failure_analysis["recommendations"].append("Review network connectivity and firewall settings")
                    elif "permission" in error_code.lower():
                        failure_analysis["recommendations"].append("Verify service principal permissions and access rights")
            
            print(f"Failure Analysis Results:")
            print(f"  Total Pipeline Runs: {failure_analysis['total_pipeline_runs']}")
            print(f"  Failed Pipeline Runs: {failure_analysis['failed_pipeline_runs']}")
            print(f"  Total Activity Runs: {failure_analysis['total_activity_runs']}")
            print(f"  Failed Activity Runs: {failure_analysis['failed_activity_runs']}")
            
            if failure_analysis["total_activity_runs"] > 0:
                failure_rate = (failure_analysis["failed_activity_runs"] / failure_analysis["total_activity_runs"]) * 100
                print(f"  Failure Rate: {failure_rate:.2f}%")
            
            return failure_analysis
            
        except Exception as e:
            print(f"Error analyzing failures: {str(e)}")
            return {}
    
    def create_retry_policy_recommendations(self, activity_type: str, failure_patterns: Dict[str, int]) -> Dict[str, Any]:
        """Create retry policy recommendations based on failure patterns"""
        
        recommendations = {
            "activity_type": activity_type,
            "retry_settings": {
                "retry_count": 3,
                "retry_interval_seconds": 30,
                "retry_policy": "exponential"
            },
            "timeout_settings": {
                "timeout_minutes": 60
            },
            "error_handling": {
                "on_failure": "stop_pipeline",
                "notification": "enabled"
            },
            "monitoring": {
                "alerts": "enabled",
                "log_level": "detailed"
            }
        }
        
        # Adjust based on failure patterns
        if "Copy_UserError" in failure_patterns:
            recommendations["retry_settings"]["retry_count"] = 1  # Don't retry user errors
            recommendations["error_handling"]["on_failure"] = "continue"
        
        if "DataFlow_SystemError" in failure_patterns:
            recommendations["retry_settings"]["retry_count"] = 5
            recommendations["retry_settings"]["retry_interval_seconds"] = 60
            recommendations["timeout_settings"]["timeout_minutes"] = 120
        
        if "Web_ConnectionFailure" in failure_patterns:
            recommendations["retry_settings"]["retry_count"] = 5
            recommendations["retry_settings"]["retry_interval_seconds"] = 10
        
        return recommendations
    
    def generate_activity_health_report(self, output_path: str):
        """Generate comprehensive activity health report"""
        
        print("=== Generating Activity Health Report ===")
        
        # Analyze failures for different time windows
        failure_analysis_24h = self.analyze_activity_failures(24)
        failure_analysis_7d = self.analyze_activity_failures(168)  # 7 days
        
        # Create comprehensive report
        health_report = {
            "report_generated": datetime.now().isoformat(),
            "factory_name": self.factory_name,
            "resource_group": self.resource_group,
            "analysis": {
                "last_24_hours": failure_analysis_24h,
                "last_7_days": failure_analysis_7d
            },
            "retry_recommendations": {},
            "monitoring_recommendations": [
                "Set up alerts for pipeline failures",
                "Monitor activity duration trends",
                "Implement data quality checks",
                "Review resource utilization patterns",
                "Establish SLA monitoring"
            ]
        }
        
        # Generate retry recommendations for common activity types
        common_activity_types = ["Copy", "DataFlow", "Web", "StoredProcedure", "Databricks"]
        for activity_type in common_activity_types:
            if failure_analysis_24h.get("failure_patterns"):
                health_report["retry_recommendations"][activity_type] = \
                    self.create_retry_policy_recommendations(activity_type, failure_analysis_24h["failure_patterns"])
        
        # Save report
        with open(output_path, 'w') as f:
            json.dump(health_report, f, indent=2)
        
        print(f"Health report saved to: {output_path}")
        return health_report

# Example usage
# adf_manager = ADFActivityManager(
#     subscription_id="your-subscription-id",
#     resource_group="your-resource-group", 
#     factory_name="your-data-factory-name"
# )

# # Monitor specific pipeline run
# pipeline_info = adf_manager.monitor_pipeline_run("YourPipelineName", "pipeline-run-id")

# # Analyze failures
# failure_analysis = adf_manager.analyze_activity_failures(24)

# # Generate health report
# health_report = adf_manager.generate_activity_health_report("adf_health_report.json")
```

---

## Azure Databricks Activities

### Databricks Job Monitoring

```python
# Azure Databricks Activity Management
import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

class DatabricksActivityManager:
    """Manage Azure Databricks activities with comprehensive monitoring"""
    
    def __init__(self, workspace_url: str, access_token: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.access_token = access_token
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Job tracking
        self.job_runs: Dict[str, Any] = {}
        self.cluster_info: Dict[str, Any] = {}
    
    def get_job_runs(self, job_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get job runs with optional filtering by job ID"""
        
        print(f"=== Getting Job Runs (Limit: {limit}) ===")
        
        url = f"{self.workspace_url}/api/2.1/jobs/runs/list"
        params = {"limit": limit}
        
        if job_id:
            params["job_id"] = job_id
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            job_runs = data.get("runs", [])
            
            print(f"Retrieved {len(job_runs)} job runs")
            
            # Process and store job run information
            processed_runs = []
            for run in job_runs:
                run_info = self.process_job_run(run)
                processed_runs.append(run_info)
                self.job_runs[str(run["run_id"])] = run_info
            
            return processed_runs
            
        except requests.exceptions.RequestException as e:
            print(f"Error getting job runs: {str(e)}")
            return []
    
    def process_job_run(self, run_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process job run data and extract relevant information"""
        
        run_info = {
            "run_id": run_data["run_id"],
            "job_id": run_data.get("job_id"),
            "run_name": run_data.get("run_name", ""),
            "state": run_data.get("state", {}),
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "cluster_spec": run_data.get("cluster_spec", {}),
            "tasks": [],
            "error_details": None,
            "success_metrics": {}
        }
        
        # Process state information
        state = run_data.get("state", {})
        run_info["life_cycle_state"] = state.get("life_cycle_state")
        run_info["result_state"] = state.get("result_state")
        run_info["state_message"] = state.get("state_message", "")
        
        # Process timing information
        if "start_time" in run_data:
            run_info["start_time"] = datetime.fromtimestamp(run_data["start_time"] / 1000).isoformat()
        
        if "end_time" in run_data:
            run_info["end_time"] = datetime.fromtimestamp(run_data["end_time"] / 1000).isoformat()
            
            if "start_time" in run_data:
                duration_ms = run_data["end_time"] - run_data["start_time"]
                run_info["duration_seconds"] = duration_ms / 1000
        
        # Process task information if available
        if "tasks" in run_data:
            for task in run_data["tasks"]:
                task_info = {
                    "task_key": task.get("task_key"),
                    "state": task.get("state", {}),
                    "start_time": None,
                    "end_time": None,
                    "duration_seconds": None
                }
                
                # Task timing
                if "start_time" in task:
                    task_info["start_time"] = datetime.fromtimestamp(task["start_time"] / 1000).isoformat()
                
                if "end_time" in task:
                    task_info["end_time"] = datetime.fromtimestamp(task["end_time"] / 1000).isoformat()
                    
                    if "start_time" in task:
                        task_duration_ms = task["end_time"] - task["start_time"]
                        task_info["duration_seconds"] = task_duration_ms / 1000
                
                run_info["tasks"].append(task_info)
        
        # Extract error details for failed runs
        if run_info["result_state"] == "FAILED":
            run_info["error_details"] = self.extract_error_details(run_data)
        
        # Extract success metrics for successful runs
        if run_info["result_state"] == "SUCCESS":
            run_info["success_metrics"] = self.extract_success_metrics(run_data)
        
        return run_info
    
    def extract_error_details(self, run_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed error information from failed job run"""
        
        error_details = {
            "error_category": "Unknown",
            "error_message": "",
            "failed_task": None,
            "cluster_issues": False,
            "code_issues": False,
            "resource_issues": False
        }
        
        # Check state message for error information
        state = run_data.get("state", {})
        state_message = state.get("state_message", "")
        
        error_details["error_message"] = state_message
        
        # Categorize error based on message content
        message_lower = state_message.lower()
        
        if any(keyword in message_lower for keyword in ["cluster", "driver", "executor"]):
            error_details["error_category"] = "Cluster"
            error_details["cluster_issues"] = True
        elif any(keyword in message_lower for keyword in ["python", "scala", "sql", "syntax"]):
            error_details["error_category"] = "Code"
            error_details["code_issues"] = True
        elif any(keyword in message_lower for keyword in ["memory", "disk", "cpu", "resource"]):
            error_details["error_category"] = "Resource"
            error_details["resource_issues"] = True
        elif any(keyword in message_lower for keyword in ["timeout", "cancelled"]):
            error_details["error_category"] = "Timeout"
        elif any(keyword in message_lower for keyword in ["permission", "access", "auth"]):
            error_details["error_category"] = "Permission"
        
        # Check for failed tasks
        if "tasks" in run_data:
            for task in run_data["tasks"]:
                task_state = task.get("state", {})
                if task_state.get("result_state") == "FAILED":
                    error_details["failed_task"] = {
                        "task_key": task.get("task_key"),
                        "error_message": task_state.get("state_message", "")
                    }
                    break
        
        return error_details
    
    def extract_success_metrics(self, run_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract success metrics from completed job run"""
        
        success_metrics = {
            "total_tasks": 0,
            "successful_tasks": 0,
            "execution_efficiency": 0.0,
            "resource_utilization": {},
            "data_processed": {}
        }
        
        # Count tasks
        if "tasks" in run_data:
            success_metrics["total_tasks"] = len(run_data["tasks"])
            success_metrics["successful_tasks"] = len([
                task for task in run_data["tasks"]
                if task.get("state", {}).get("result_state") == "SUCCESS"
            ])
            
            if success_metrics["total_tasks"] > 0:
                success_metrics["execution_efficiency"] = \
                    success_metrics["successful_tasks"] / success_metrics["total_tasks"]
        
        # Extract cluster utilization if available
        cluster_spec = run_data.get("cluster_spec", {})
        if cluster_spec:
            success_metrics["resource_utilization"] = {
                "node_type": cluster_spec.get("node_type_id"),
                "num_workers": cluster_spec.get("num_workers", 0),
                "driver_node_type": cluster_spec.get("driver_node_type_id")
            }
        
        return success_metrics
    
    def analyze_job_performance(self, time_window_hours: int = 24) -> Dict[str, Any]:
        """Analyze job performance within specified time window"""
        
        print(f"=== Analyzing Job Performance (Last {time_window_hours} hours) ===")
        
        # Get recent job runs
        job_runs = self.get_job_runs(limit=1000)  # Get more runs for analysis
        
        # Filter by time window
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        recent_runs = []
        
        for run in job_runs:
            if run["start_time"]:
                start_time = datetime.fromisoformat(run["start_time"].replace('Z', '+00:00'))
                if start_time.replace(tzinfo=None) >= cutoff_time:
                    recent_runs.append(run)
        
        if not recent_runs:
            return {"message": "No job runs found in the specified time window"}
        
        # Performance analysis
        analysis = {
            "time_window_hours": time_window_hours,
            "total_runs": len(recent_runs),
            "successful_runs": 0,
            "failed_runs": 0,
            "cancelled_runs": 0,
            "success_rate": 0.0,
            "average_duration_seconds": 0.0,
            "failure_categories": {},
            "performance_trends": {
                "fastest_run": None,
                "slowest_run": None,
                "most_efficient_run": None
            },
            "recommendations": []
        }
        
        durations = []
        failed_runs = []
        
        for run in recent_runs:
            result_state = run.get("result_state")
            
            if result_state == "SUCCESS":
                analysis["successful_runs"] += 1
            elif result_state == "FAILED":
                analysis["failed_runs"] += 1
                failed_runs.append(run)
            elif result_state in ["CANCELLED", "TIMEOUT"]:
                analysis["cancelled_runs"] += 1
            
            # Collect duration data
            if run["duration_seconds"]:
                durations.append({
                    "run_id": run["run_id"],
                    "duration": run["duration_seconds"],
                    "efficiency": run.get("success_metrics", {}).get("execution_efficiency", 0)
                })
        
        # Calculate rates and averages
        if analysis["total_runs"] > 0:
            analysis["success_rate"] = (analysis["successful_runs"] / analysis["total_runs"]) * 100
        
        if durations:
            analysis["average_duration_seconds"] = sum(d["duration"] for d in durations) / len(durations)
            
            # Find performance extremes
            fastest = min(durations, key=lambda x: x["duration"])
            slowest = max(durations, key=lambda x: x["duration"])
            most_efficient = max(durations, key=lambda x: x["efficiency"])
            
            analysis["performance_trends"]["fastest_run"] = {
                "run_id": fastest["run_id"],
                "duration_seconds": fastest["duration"]
            }
            analysis["performance_trends"]["slowest_run"] = {
                "run_id": slowest["run_id"], 
                "duration_seconds": slowest["duration"]
            }
            analysis["performance_trends"]["most_efficient_run"] = {
                "run_id": most_efficient["run_id"],
                "efficiency": most_efficient["efficiency"]
            }
        
        # Analyze failure categories
        for run in failed_runs:
            error_details = run.get("error_details", {})
            category = error_details.get("error_category", "Unknown")
            
            if category in analysis["failure_categories"]:
                analysis["failure_categories"][category] += 1
            else:
                analysis["failure_categories"][category] = 1
        
        # Generate recommendations
        if analysis["success_rate"] < 90:
            analysis["recommendations"].append("Low success rate - review job configurations and error handling")
        
        if analysis["failure_categories"].get("Cluster", 0) > 0:
            analysis["recommendations"].append("Cluster failures detected - consider cluster optimization or auto-scaling")
        
        if analysis["failure_categories"].get("Resource", 0) > 0:
            analysis["recommendations"].append("Resource issues detected - review cluster sizing and memory allocation")
        
        if durations and max(d["duration"] for d in durations) > 3600:  # > 1 hour
            analysis["recommendations"].append("Long-running jobs detected - consider optimization or partitioning")
        
        print(f"Performance Analysis Results:")
        print(f"  Total Runs: {analysis['total_runs']}")
        print(f"  Success Rate: {analysis['success_rate']:.2f}%")
        print(f"  Average Duration: {analysis['average_duration_seconds']:.2f} seconds")
        print(f"  Failure Categories: {analysis['failure_categories']}")
        
        return analysis
    
    def create_job_monitoring_dashboard(self, output_path: str):
        """Create comprehensive job monitoring dashboard data"""
        
        print("=== Creating Job Monitoring Dashboard ===")
        
        # Get performance analysis for different time windows
        performance_24h = self.analyze_job_performance(24)
        performance_7d = self.analyze_job_performance(168)  # 7 days
        
        # Get current job runs
        current_runs = self.get_job_runs(limit=50)
        
        dashboard_data = {
            "dashboard_generated": datetime.now().isoformat(),
            "workspace_url": self.workspace_url,
            "performance_analysis": {
                "last_24_hours": performance_24h,
                "last_7_days": performance_7d
            },
            "current_job_runs": current_runs[:10],  # Latest 10 runs
            "alerts": [],
            "health_score": 0,
            "recommendations": []
        }
        
        # Calculate health score (0-100)
        if performance_24h.get("success_rate"):
            health_score = performance_24h["success_rate"]
            
            # Adjust for other factors
            if performance_24h.get("failure_categories"):
                critical_failures = performance_24h["failure_categories"].get("Cluster", 0) + \
                                 performance_24h["failure_categories"].get("Resource", 0)
                health_score -= (critical_failures * 5)  # Reduce score for critical failures
            
            dashboard_data["health_score"] = max(0, min(100, health_score))
        
        # Generate alerts
        if performance_24h.get("success_rate", 100) < 80:
            dashboard_data["alerts"].append({
                "type": "WARNING",
                "message": f"Low success rate: {performance_24h['success_rate']:.1f}%",
                "timestamp": datetime.now().isoformat()
            })
        
        if performance_24h.get("failed_runs", 0) > 5:
            dashboard_data["alerts"].append({
                "type": "CRITICAL", 
                "message": f"High failure count: {performance_24h['failed_runs']} failures in 24h",
                "timestamp": datetime.now().isoformat()
            })
        
        # Consolidate recommendations
        all_recommendations = set()
        if performance_24h.get("recommendations"):
            all_recommendations.update(performance_24h["recommendations"])
        if performance_7d.get("recommendations"):
            all_recommendations.update(performance_7d["recommendations"])
        
        dashboard_data["recommendations"] = list(all_recommendations)
        
        # Save dashboard data
        with open(output_path, 'w') as f:
            json.dump(dashboard_data, f, indent=2)
        
        print(f"Dashboard data saved to: {output_path}")
        print(f"Health Score: {dashboard_data['health_score']}/100")
        
        return dashboard_data

# Example usage
# databricks_manager = DatabricksActivityManager(
#     workspace_url="https://your-workspace.azuredatabricks.net",
#     access_token="your-access-token"
# )

# # Get job runs
# job_runs = databricks_manager.get_job_runs(limit=100)

# # Analyze performance
# performance = databricks_manager.analyze_job_performance(24)

# # Create monitoring dashboard
# dashboard = databricks_manager.create_job_monitoring_dashboard("databricks_dashboard.json")
```

This comprehensive guide provides everything needed to monitor, handle, and troubleshoot Azure activities across multiple services, ensuring robust and reliable cloud operations.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*