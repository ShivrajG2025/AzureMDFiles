# Azure Triggers Comprehensive Guide
## Complete Guide to All Types of Triggers in Azure Services

---

### Table of Contents

1. [Overview](#overview)
2. [Azure Data Factory Triggers](#azure-data-factory-triggers)
3. [Azure Functions Triggers](#azure-functions-triggers)
4. [Azure Logic Apps Triggers](#azure-logic-apps-triggers)
5. [Azure Databricks Triggers](#azure-databricks-triggers)
6. [Azure Event Grid Triggers](#azure-event-grid-triggers)
7. [Azure Stream Analytics Triggers](#azure-stream-analytics-triggers)
8. [Database Triggers](#database-triggers)
9. [Custom Trigger Implementation](#custom-trigger-implementation)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

---

## Overview

Azure triggers are event-driven mechanisms that automatically initiate workflows, functions, or processes in response to specific conditions or events. They form the backbone of automation and real-time processing in Azure cloud applications.

### Trigger Categories

```json
{
  "azure_trigger_categories": {
    "time_based_triggers": {
      "description": "Execute based on time schedules",
      "types": ["Schedule triggers", "Timer triggers", "Tumbling window triggers"],
      "use_cases": ["Batch processing", "Regular maintenance", "Periodic data extraction"],
      "services": ["Azure Data Factory", "Azure Functions", "Logic Apps", "Databricks"]
    },
    "event_based_triggers": {
      "description": "Execute in response to events",
      "types": ["Blob triggers", "Queue triggers", "Event Grid triggers", "Storage triggers"],
      "use_cases": ["File processing", "Message handling", "Event-driven architectures"],
      "services": ["Azure Functions", "Event Grid", "Logic Apps", "Stream Analytics"]
    },
    "data_triggers": {
      "description": "Execute based on data changes",
      "types": ["Database triggers", "Change feed triggers", "Cosmos DB triggers"],
      "use_cases": ["Data synchronization", "Audit logging", "Real-time analytics"],
      "services": ["SQL Database", "Cosmos DB", "Functions", "Stream Analytics"]
    },
    "http_triggers": {
      "description": "Execute via HTTP requests",
      "types": ["HTTP triggers", "Webhook triggers", "API triggers"],
      "use_cases": ["API endpoints", "Webhook processing", "External integrations"],
      "services": ["Azure Functions", "Logic Apps", "API Management"]
    },
    "custom_triggers": {
      "description": "User-defined trigger mechanisms",
      "types": ["Custom event triggers", "Business logic triggers", "Conditional triggers"],
      "use_cases": ["Complex business rules", "Multi-condition workflows", "Custom automation"],
      "services": ["All Azure services with extensibility"]
    }
  }
}
```

### Trigger Architecture Patterns

```json
{
  "trigger_architecture_patterns": {
    "publisher_subscriber": {
      "description": "Event publishers notify multiple subscribers",
      "components": ["Event source", "Event Grid", "Multiple subscribers"],
      "benefits": ["Loose coupling", "Scalability", "Flexibility"],
      "example_scenario": "File upload triggers multiple processing workflows"
    },
    "request_response": {
      "description": "Synchronous trigger execution with response",
      "components": ["HTTP client", "Trigger endpoint", "Processing logic", "Response"],
      "benefits": ["Immediate feedback", "Simple implementation", "Direct control"],
      "example_scenario": "API endpoint triggering data validation and returning results"
    },
    "pipeline_orchestration": {
      "description": "Sequential trigger execution in workflows",
      "components": ["Initial trigger", "Pipeline stages", "Conditional logic", "Final actions"],
      "benefits": ["Complex workflows", "Error handling", "State management"],
      "example_scenario": "Data ingestion trigger starting ETL pipeline with multiple stages"
    },
    "fan_out_fan_in": {
      "description": "Single trigger spawning parallel processes that reconverge",
      "components": ["Single trigger", "Parallel processors", "Aggregation point"],
      "benefits": ["Parallel processing", "Performance optimization", "Resource efficiency"],
      "example_scenario": "Batch job trigger processing multiple files in parallel"
    }
  }
}
```

---

## Azure Data Factory Triggers

### Schedule Triggers

Schedule triggers execute pipelines based on wall-clock time schedules, supporting complex recurrence patterns.

```json
{
  "name": "DailyETLScheduleTrigger",
  "type": "Microsoft.DataFactory/factories/triggers",
  "properties": {
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "startTime": "2024-01-01T02:00:00Z",
        "endTime": "2024-12-31T02:00:00Z",
        "timeZone": "UTC",
        "schedule": {
          "hours": [2],
          "minutes": [0],
          "weekDays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        }
      }
    },
    "pipelines": [
      {
        "pipelineReference": {
          "type": "PipelineReference",
          "referenceName": "DailyETLPipeline"
        },
        "parameters": {
          "processingDate": "@formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')",
          "environment": "production"
        }
      }
    ],
    "description": "Daily ETL pipeline execution on weekdays at 2 AM UTC"
  }
}
```

### Tumbling Window Triggers

Tumbling window triggers execute pipelines for consecutive, non-overlapping time intervals.

```json
{
  "name": "HourlyDataProcessingTrigger",
  "type": "Microsoft.DataFactory/factories/triggers",
  "properties": {
    "type": "TumblingWindowTrigger",
    "typeProperties": {
      "frequency": "Hour",
      "interval": 1,
      "startTime": "2024-01-01T00:00:00Z",
      "endTime": "2024-12-31T23:59:59Z",
      "delay": "00:05:00",
      "maxConcurrency": 3,
      "retryPolicy": {
        "count": 3,
        "intervalInSeconds": 300
      },
      "dependsOn": [
        {
          "type": "TumblingWindowTriggerDependencyReference",
          "referenceName": "UpstreamDataTrigger",
          "offset": "-1:00:00"
        }
      ]
    },
    "pipeline": {
      "pipelineReference": {
        "type": "PipelineReference",
        "referenceName": "HourlyDataProcessing"
      },
      "parameters": {
        "windowStart": "@trigger().outputs.windowStartTime",
        "windowEnd": "@trigger().outputs.windowEndTime",
        "retryCount": "@trigger().outputs.retryCount"
      }
    },
    "description": "Hourly data processing with dependency on upstream trigger"
  }
}
```

### Event-Based Triggers (Storage Events)

Event-based triggers respond to storage account events like blob creation or deletion.

```json
{
  "name": "BlobCreatedTrigger",
  "type": "Microsoft.DataFactory/factories/triggers",
  "properties": {
    "type": "BlobEventsTrigger",
    "typeProperties": {
      "blobPathBeginsWith": "/raw-data/sales/",
      "blobPathEndsWith": ".csv",
      "ignoreEmptyBlobs": true,
      "scope": "/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Storage/storageAccounts/{storage-account}",
      "events": [
        "Microsoft.Storage.BlobCreated"
      ]
    },
    "pipelines": [
      {
        "pipelineReference": {
          "type": "PipelineReference",
          "referenceName": "ProcessNewSalesData"
        },
        "parameters": {
          "fileName": "@triggerBody().fileName",
          "folderPath": "@triggerBody().folderPath",
          "eventTime": "@triggerBody().eventTime"
        }
      }
    ],
    "description": "Trigger when new CSV files are created in sales folder"
  }
}
```

### Custom Event Triggers

Custom event triggers respond to custom events published to Event Grid.

```json
{
  "name": "CustomBusinessEventTrigger",
  "type": "Microsoft.DataFactory/factories/triggers",
  "properties": {
    "type": "CustomEventsTrigger",
    "typeProperties": {
      "subjectBeginsWith": "orders/",
      "subjectEndsWith": "/completed",
      "events": [
        "OrderCompleted",
        "OrderCancelled"
      ],
      "scope": "/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.EventGrid/topics/{topic-name}"
    },
    "pipelines": [
      {
        "pipelineReference": {
          "type": "PipelineReference",
          "referenceName": "ProcessOrderEvent"
        },
        "parameters": {
          "eventType": "@triggerBody().eventType",
          "orderId": "@triggerBody().data.orderId",
          "customerId": "@triggerBody().data.customerId",
          "eventTime": "@triggerBody().eventTime"
        }
      }
    ],
    "description": "Process order completion and cancellation events"
  }
}
```

### Python Script for ADF Trigger Management

```python
# Azure Data Factory Trigger Management
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import json
from datetime import datetime, timedelta

class ADFTriggerManager:
    """Comprehensive Azure Data Factory trigger management"""
    
    def __init__(self, subscription_id: str, resource_group: str, factory_name: str):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.credential = DefaultAzureCredential()
        self.adf_client = DataFactoryManagementClient(self.credential, subscription_id)
    
    def create_schedule_trigger(self, trigger_name: str, pipeline_name: str, 
                              schedule_config: dict) -> str:
        """Create a schedule trigger"""
        
        print(f"=== Creating Schedule Trigger: {trigger_name} ===")
        
        # Create recurrence pattern
        recurrence = ScheduleTriggerRecurrence(
            frequency=schedule_config.get("frequency", "Day"),
            interval=schedule_config.get("interval", 1),
            start_time=datetime.fromisoformat(schedule_config.get("start_time", "2024-01-01T00:00:00")),
            end_time=datetime.fromisoformat(schedule_config.get("end_time", "2024-12-31T23:59:59")) if schedule_config.get("end_time") else None,
            time_zone=schedule_config.get("time_zone", "UTC")
        )
        
        # Add schedule details if provided
        if "schedule" in schedule_config:
            schedule_details = schedule_config["schedule"]
            recurrence.schedule = RecurrenceSchedule(
                hours=schedule_details.get("hours", [0]),
                minutes=schedule_details.get("minutes", [0]),
                week_days=schedule_details.get("week_days", [])
            )
        
        # Create pipeline reference
        pipeline_ref = PipelineReference(
            type="PipelineReference",
            reference_name=pipeline_name
        )
        
        # Create trigger properties
        trigger_properties = ScheduleTrigger(
            type="ScheduleTrigger",
            description=schedule_config.get("description", f"Schedule trigger for {pipeline_name}"),
            recurrence=recurrence,
            pipelines=[
                TriggerPipelineReference(
                    pipeline_reference=pipeline_ref,
                    parameters=schedule_config.get("parameters", {})
                )
            ]
        )
        
        try:
            # Create the trigger
            trigger_resource = TriggerResource(properties=trigger_properties)
            
            self.adf_client.triggers.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                trigger_name=trigger_name,
                trigger=trigger_resource
            )
            
            print(f"✓ Schedule trigger created: {trigger_name}")
            print(f"  Pipeline: {pipeline_name}")
            print(f"  Frequency: {schedule_config.get('frequency')} every {schedule_config.get('interval')} unit(s)")
            
            return trigger_name
            
        except Exception as e:
            print(f"✗ Failed to create schedule trigger: {str(e)}")
            raise
    
    def create_tumbling_window_trigger(self, trigger_name: str, pipeline_name: str,
                                     window_config: dict) -> str:
        """Create a tumbling window trigger"""
        
        print(f"=== Creating Tumbling Window Trigger: {trigger_name} ===")
        
        # Create trigger properties
        trigger_properties = TumblingWindowTrigger(
            type="TumblingWindowTrigger",
            description=window_config.get("description", f"Tumbling window trigger for {pipeline_name}"),
            frequency=window_config.get("frequency", "Hour"),
            interval=window_config.get("interval", 1),
            start_time=datetime.fromisoformat(window_config.get("start_time", "2024-01-01T00:00:00")),
            end_time=datetime.fromisoformat(window_config.get("end_time")) if window_config.get("end_time") else None,
            delay=window_config.get("delay", "00:00:00"),
            max_concurrency=window_config.get("max_concurrency", 1),
            retry_policy=RetryPolicy(
                count=window_config.get("retry_count", 3),
                interval_in_seconds=window_config.get("retry_interval", 300)
            ) if window_config.get("retry_count") else None,
            pipeline=TriggerPipelineReference(
                pipeline_reference=PipelineReference(
                    type="PipelineReference",
                    reference_name=pipeline_name
                ),
                parameters=window_config.get("parameters", {})
            )
        )
        
        # Add dependencies if specified
        if "depends_on" in window_config:
            dependencies = []
            for dep in window_config["depends_on"]:
                dependency = TumblingWindowTriggerDependencyReference(
                    type="TumblingWindowTriggerDependencyReference",
                    reference_name=dep["trigger_name"],
                    offset=dep.get("offset", "00:00:00")
                )
                dependencies.append(dependency)
            trigger_properties.depends_on = dependencies
        
        try:
            # Create the trigger
            trigger_resource = TriggerResource(properties=trigger_properties)
            
            self.adf_client.triggers.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                trigger_name=trigger_name,
                trigger=trigger_resource
            )
            
            print(f"✓ Tumbling window trigger created: {trigger_name}")
            print(f"  Pipeline: {pipeline_name}")
            print(f"  Window: {window_config.get('frequency')} every {window_config.get('interval')} unit(s)")
            print(f"  Max Concurrency: {window_config.get('max_concurrency', 1)}")
            
            return trigger_name
            
        except Exception as e:
            print(f"✗ Failed to create tumbling window trigger: {str(e)}")
            raise
    
    def create_blob_event_trigger(self, trigger_name: str, pipeline_name: str,
                                blob_config: dict) -> str:
        """Create a blob event trigger"""
        
        print(f"=== Creating Blob Event Trigger: {trigger_name} ===")
        
        # Create trigger properties
        trigger_properties = BlobEventsTrigger(
            type="BlobEventsTrigger",
            description=blob_config.get("description", f"Blob event trigger for {pipeline_name}"),
            blob_path_begins_with=blob_config.get("path_begins_with", "/"),
            blob_path_ends_with=blob_config.get("path_ends_with", ""),
            ignore_empty_blobs=blob_config.get("ignore_empty_blobs", True),
            scope=blob_config["storage_account_scope"],
            events=blob_config.get("events", ["Microsoft.Storage.BlobCreated"]),
            pipelines=[
                TriggerPipelineReference(
                    pipeline_reference=PipelineReference(
                        type="PipelineReference",
                        reference_name=pipeline_name
                    ),
                    parameters=blob_config.get("parameters", {})
                )
            ]
        )
        
        try:
            # Create the trigger
            trigger_resource = TriggerResource(properties=trigger_properties)
            
            self.adf_client.triggers.create_or_update(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                trigger_name=trigger_name,
                trigger=trigger_resource
            )
            
            print(f"✓ Blob event trigger created: {trigger_name}")
            print(f"  Pipeline: {pipeline_name}")
            print(f"  Path filter: {blob_config.get('path_begins_with', '/')}*{blob_config.get('path_ends_with', '')}")
            print(f"  Events: {blob_config.get('events', ['Microsoft.Storage.BlobCreated'])}")
            
            return trigger_name
            
        except Exception as e:
            print(f"✗ Failed to create blob event trigger: {str(e)}")
            raise
    
    def start_trigger(self, trigger_name: str) -> bool:
        """Start a trigger"""
        
        print(f"Starting trigger: {trigger_name}")
        
        try:
            self.adf_client.triggers.start(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                trigger_name=trigger_name
            )
            
            print(f"✓ Trigger started: {trigger_name}")
            return True
            
        except Exception as e:
            print(f"✗ Failed to start trigger: {str(e)}")
            return False
    
    def stop_trigger(self, trigger_name: str) -> bool:
        """Stop a trigger"""
        
        print(f"Stopping trigger: {trigger_name}")
        
        try:
            self.adf_client.triggers.stop(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                trigger_name=trigger_name
            )
            
            print(f"✓ Trigger stopped: {trigger_name}")
            return True
            
        except Exception as e:
            print(f"✗ Failed to stop trigger: {str(e)}")
            return False
    
    def get_trigger_runs(self, trigger_name: str, days_back: int = 7) -> list:
        """Get trigger run history"""
        
        print(f"Getting trigger runs for: {trigger_name}")
        
        try:
            # Calculate time range
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days_back)
            
            # Query trigger runs
            filter_parameters = RunFilterParameters(
                last_updated_after=start_time,
                last_updated_before=end_time,
                filters=[
                    RunQueryFilter(
                        operand="TriggerName",
                        operator="Equals",
                        values=[trigger_name]
                    )
                ]
            )
            
            trigger_runs = self.adf_client.trigger_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                filter_parameters=filter_parameters
            )
            
            runs_list = []
            for run in trigger_runs.value:
                runs_list.append({
                    "run_id": run.trigger_run_id,
                    "trigger_name": run.trigger_name,
                    "trigger_type": run.trigger_type,
                    "status": run.status,
                    "trigger_time": run.trigger_run_timestamp.isoformat() if run.trigger_run_timestamp else None,
                    "message": run.message
                })
            
            print(f"✓ Retrieved {len(runs_list)} trigger runs")
            return runs_list
            
        except Exception as e:
            print(f"✗ Failed to get trigger runs: {str(e)}")
            return []
    
    def create_comprehensive_trigger_suite(self, pipeline_name: str) -> dict:
        """Create a comprehensive suite of triggers for demonstration"""
        
        print("=== Creating Comprehensive Trigger Suite ===")
        
        created_triggers = {}
        
        # 1. Daily schedule trigger
        schedule_config = {
            "frequency": "Day",
            "interval": 1,
            "start_time": "2024-01-01T02:00:00",
            "time_zone": "UTC",
            "schedule": {
                "hours": [2],
                "minutes": [0],
                "week_days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
            },
            "parameters": {
                "environment": "production",
                "processingDate": "@formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')"
            },
            "description": "Daily processing trigger for weekdays at 2 AM UTC"
        }
        
        try:
            schedule_trigger = self.create_schedule_trigger(
                "DailyScheduleTrigger",
                pipeline_name,
                schedule_config
            )
            created_triggers["schedule"] = schedule_trigger
        except Exception as e:
            print(f"Warning: Schedule trigger creation failed: {e}")
        
        # 2. Hourly tumbling window trigger
        window_config = {
            "frequency": "Hour",
            "interval": 1,
            "start_time": "2024-01-01T00:00:00",
            "delay": "00:05:00",
            "max_concurrency": 2,
            "retry_count": 3,
            "retry_interval": 300,
            "parameters": {
                "windowStart": "@trigger().outputs.windowStartTime",
                "windowEnd": "@trigger().outputs.windowEndTime"
            },
            "description": "Hourly tumbling window processing with 5-minute delay"
        }
        
        try:
            window_trigger = self.create_tumbling_window_trigger(
                "HourlyTumblingWindowTrigger",
                pipeline_name,
                window_config
            )
            created_triggers["tumbling_window"] = window_trigger
        except Exception as e:
            print(f"Warning: Tumbling window trigger creation failed: {e}")
        
        # 3. Blob event trigger (requires storage account scope)
        blob_config = {
            "path_begins_with": "/raw-data/",
            "path_ends_with": ".json",
            "ignore_empty_blobs": True,
            "events": ["Microsoft.Storage.BlobCreated"],
            "storage_account_scope": f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Storage/storageAccounts/yourstorageaccount",
            "parameters": {
                "fileName": "@triggerBody().fileName",
                "folderPath": "@triggerBody().folderPath"
            },
            "description": "Trigger on JSON file creation in raw-data folder"
        }
        
        try:
            blob_trigger = self.create_blob_event_trigger(
                "BlobCreatedTrigger",
                pipeline_name,
                blob_config
            )
            created_triggers["blob_event"] = blob_trigger
        except Exception as e:
            print(f"Warning: Blob event trigger creation failed: {e}")
        
        print(f"\n=== Trigger Suite Summary ===")
        print(f"Created {len(created_triggers)} triggers:")
        for trigger_type, trigger_name in created_triggers.items():
            print(f"  ✓ {trigger_type}: {trigger_name}")
        
        return created_triggers

# Example usage and demonstration
def demonstrate_adf_triggers():
    """Comprehensive demonstration of ADF triggers"""
    
    print("=== Azure Data Factory Triggers Demonstration ===")
    
    # Configuration
    subscription_id = "your-subscription-id"
    resource_group = "your-resource-group"
    factory_name = "your-data-factory"
    pipeline_name = "your-pipeline"
    
    try:
        # Initialize trigger manager
        trigger_manager = ADFTriggerManager(subscription_id, resource_group, factory_name)
        
        # Create comprehensive trigger suite
        created_triggers = trigger_manager.create_comprehensive_trigger_suite(pipeline_name)
        
        # Start triggers (optional - be careful with this in production)
        # for trigger_name in created_triggers.values():
        #     trigger_manager.start_trigger(trigger_name)
        
        # Get trigger run history
        for trigger_name in created_triggers.values():
            runs = trigger_manager.get_trigger_runs(trigger_name, days_back=7)
            print(f"\nTrigger runs for {trigger_name}: {len(runs)}")
        
        return {
            "status": "success",
            "created_triggers": created_triggers,
            "factory_name": factory_name
        }
        
    except Exception as e:
        print(f"Demonstration failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

# Run demonstration
# demo_results = demonstrate_adf_triggers()
```

---

## Azure Functions Triggers

### HTTP Triggers

HTTP triggers respond to HTTP requests and are commonly used for APIs and webhooks.

```python
# Azure Functions - HTTP Trigger Example
import logging
import json
import azure.functions as func
from datetime import datetime
from typing import Dict, Any

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP Trigger function for processing API requests
    """
    logging.info('HTTP trigger function processed a request.')
    
    try:
        # Get request method and headers
        method = req.method
        headers = dict(req.headers)
        
        # Process different HTTP methods
        if method == 'GET':
            return handle_get_request(req)
        elif method == 'POST':
            return handle_post_request(req)
        elif method == 'PUT':
            return handle_put_request(req)
        elif method == 'DELETE':
            return handle_delete_request(req)
        else:
            return func.HttpResponse(
                json.dumps({"error": f"Method {method} not supported"}),
                status_code=405,
                mimetype="application/json"
            )
            
    except Exception as e:
        logging.error(f"Error processing HTTP request: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": "Internal server error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )

def handle_get_request(req: func.HttpRequest) -> func.HttpResponse:
    """Handle GET requests"""
    
    # Get query parameters
    name = req.params.get('name', 'World')
    format_type = req.params.get('format', 'json')
    
    response_data = {
        "message": f"Hello, {name}!",
        "timestamp": datetime.now().isoformat(),
        "method": "GET",
        "format": format_type
    }
    
    if format_type.lower() == 'xml':
        xml_response = f"""<?xml version="1.0" encoding="UTF-8"?>
<response>
    <message>{response_data['message']}</message>
    <timestamp>{response_data['timestamp']}</timestamp>
    <method>{response_data['method']}</method>
</response>"""
        return func.HttpResponse(xml_response, mimetype="application/xml")
    else:
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            mimetype="application/json"
        )

def handle_post_request(req: func.HttpRequest) -> func.HttpResponse:
    """Handle POST requests with JSON data"""
    
    try:
        # Get JSON data from request body
        req_body = req.get_json()
        
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "Request body is required for POST requests"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Process the data (example: data validation and transformation)
        processed_data = process_post_data(req_body)
        
        response_data = {
            "status": "success",
            "message": "Data processed successfully",
            "processed_data": processed_data,
            "timestamp": datetime.now().isoformat()
        }
        
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=201,
            mimetype="application/json"
        )
        
    except ValueError as e:
        return func.HttpResponse(
            json.dumps({
                "error": "Invalid JSON in request body",
                "message": str(e)
            }),
            status_code=400,
            mimetype="application/json"
        )

def process_post_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Process POST data with validation and transformation"""
    
    processed = {
        "original_data": data,
        "validation_results": {},
        "transformations": {}
    }
    
    # Example validations
    if "email" in data:
        email = data["email"]
        is_valid_email = "@" in email and "." in email
        processed["validation_results"]["email"] = {
            "value": email,
            "is_valid": is_valid_email
        }
    
    if "age" in data:
        try:
            age = int(data["age"])
            processed["validation_results"]["age"] = {
                "value": age,
                "is_valid": 0 <= age <= 150
            }
        except (ValueError, TypeError):
            processed["validation_results"]["age"] = {
                "value": data["age"],
                "is_valid": False,
                "error": "Age must be a valid integer"
            }
    
    # Example transformations
    if "name" in data:
        processed["transformations"]["name"] = {
            "original": data["name"],
            "uppercase": data["name"].upper(),
            "lowercase": data["name"].lower(),
            "title_case": data["name"].title()
        }
    
    return processed
```

### Timer Triggers

Timer triggers execute functions on a schedule using CRON expressions.

```python
# Azure Functions - Timer Trigger Example
import logging
import json
import azure.functions as func
from datetime import datetime, timedelta
import os
import requests
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

def main(mytimer: func.TimerRequest) -> None:
    """
    Timer trigger function for scheduled data processing
    Runs every hour at minute 0: "0 0 * * * *"
    """
    utc_timestamp = datetime.utcnow().replace(tzinfo=None).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info(f'Timer trigger function executed at {utc_timestamp}')
    
    try:
        # Perform scheduled tasks
        results = {
            "execution_time": utc_timestamp,
            "tasks_completed": []
        }
        
        # Task 1: Data cleanup
        cleanup_result = perform_data_cleanup()
        results["tasks_completed"].append({
            "task": "data_cleanup",
            "status": cleanup_result["status"],
            "details": cleanup_result
        })
        
        # Task 2: Health check
        health_result = perform_health_check()
        results["tasks_completed"].append({
            "task": "health_check", 
            "status": health_result["status"],
            "details": health_result
        })
        
        # Task 3: Generate reports
        report_result = generate_hourly_report()
        results["tasks_completed"].append({
            "task": "generate_report",
            "status": report_result["status"],
            "details": report_result
        })
        
        # Log summary
        successful_tasks = len([t for t in results["tasks_completed"] if t["status"] == "success"])
        total_tasks = len(results["tasks_completed"])
        
        logging.info(f'Timer execution completed: {successful_tasks}/{total_tasks} tasks successful')
        
        # Save execution results to storage
        save_execution_results(results)
        
    except Exception as e:
        logging.error(f'Timer trigger execution failed: {str(e)}')
        raise

def perform_data_cleanup() -> dict:
    """Perform scheduled data cleanup tasks"""
    
    try:
        # Example: Clean up old temporary files
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
        if not storage_account_name:
            return {"status": "skipped", "reason": "Storage account not configured"}
        
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )
        
        container_name = "temp-data"
        cutoff_date = datetime.now() - timedelta(days=7)
        
        # List and delete old blobs
        container_client = blob_service_client.get_container_client(container_name)
        deleted_count = 0
        
        for blob in container_client.list_blobs():
            if blob.last_modified < cutoff_date:
                blob_client = container_client.get_blob_client(blob.name)
                blob_client.delete_blob()
                deleted_count += 1
        
        return {
            "status": "success",
            "deleted_files": deleted_count,
            "cutoff_date": cutoff_date.isoformat()
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }

def perform_health_check() -> dict:
    """Perform system health checks"""
    
    health_results = {
        "status": "success",
        "checks": {}
    }
    
    # Check 1: External API availability
    try:
        response = requests.get("https://api.example.com/health", timeout=10)
        health_results["checks"]["external_api"] = {
            "status": "healthy" if response.status_code == 200 else "unhealthy",
            "response_code": response.status_code,
            "response_time_ms": response.elapsed.total_seconds() * 1000
        }
    except Exception as e:
        health_results["checks"]["external_api"] = {
            "status": "unhealthy",
            "error": str(e)
        }
    
    # Check 2: Database connectivity
    try:
        # Example database check (replace with actual database connection)
        db_connection_string = os.environ.get('DATABASE_CONNECTION_STRING')
        if db_connection_string:
            # Simulate database check
            health_results["checks"]["database"] = {
                "status": "healthy",
                "connection_time_ms": 45
            }
        else:
            health_results["checks"]["database"] = {
                "status": "not_configured",
                "message": "Database connection string not provided"
            }
    except Exception as e:
        health_results["checks"]["database"] = {
            "status": "unhealthy",
            "error": str(e)
        }
    
    # Check 3: Storage account accessibility
    try:
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
        if storage_account_name:
            credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(
                account_url=f"https://{storage_account_name}.blob.core.windows.net",
                credential=credential
            )
            
            # Test storage access
            containers = list(blob_service_client.list_containers(max_results=1))
            health_results["checks"]["storage"] = {
                "status": "healthy",
                "accessible_containers": len(containers)
            }
        else:
            health_results["checks"]["storage"] = {
                "status": "not_configured",
                "message": "Storage account not configured"
            }
    except Exception as e:
        health_results["checks"]["storage"] = {
            "status": "unhealthy",
            "error": str(e)
        }
    
    # Determine overall health status
    unhealthy_checks = [check for check in health_results["checks"].values() 
                       if check["status"] in ["unhealthy", "failed"]]
    
    if unhealthy_checks:
        health_results["status"] = "degraded"
        health_results["unhealthy_count"] = len(unhealthy_checks)
    
    return health_results

def generate_hourly_report() -> dict:
    """Generate hourly summary report"""
    
    try:
        current_time = datetime.now()
        report_data = {
            "report_timestamp": current_time.isoformat(),
            "report_period": {
                "start": (current_time - timedelta(hours=1)).isoformat(),
                "end": current_time.isoformat()
            },
            "metrics": {
                "function_executions": 1,  # This execution
                "data_processed_mb": 0,    # Placeholder
                "errors_encountered": 0,   # Placeholder
                "average_execution_time_ms": 0  # Placeholder
            },
            "status": "completed"
        }
        
        # In a real scenario, you would collect actual metrics
        # from monitoring systems, databases, or log analytics
        
        return {
            "status": "success",
            "report": report_data,
            "report_size_bytes": len(json.dumps(report_data))
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }

def save_execution_results(results: dict) -> None:
    """Save execution results to storage for audit trail"""
    
    try:
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
        if not storage_account_name:
            logging.warning("Storage account not configured, skipping results save")
            return
        
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )
        
        # Create blob name with timestamp
        timestamp = datetime.now().strftime("%Y/%m/%d/%H")
        blob_name = f"timer-execution-results/{timestamp}/results-{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Upload results
        blob_client = blob_service_client.get_blob_client(
            container="function-logs",
            blob=blob_name
        )
        
        results_json = json.dumps(results, indent=2)
        blob_client.upload_blob(results_json, overwrite=True)
        
        logging.info(f"Execution results saved to: {blob_name}")
        
    except Exception as e:
        logging.error(f"Failed to save execution results: {str(e)}")
```

### Blob Triggers

Blob triggers execute when blobs are created or updated in Azure Storage.

```python
# Azure Functions - Blob Trigger Example
import logging
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import os
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime

def main(myblob: func.InputStream) -> None:
    """
    Blob trigger function that processes files when they are uploaded
    Trigger path: samples-workitems/{name}
    """
    logging.info(f"Blob trigger processed blob: {myblob.name}, Size: {myblob.length} bytes")
    
    try:
        # Extract file information
        blob_info = extract_blob_info(myblob)
        
        # Process based on file type
        processing_result = process_blob_by_type(myblob, blob_info)
        
        # Create processing summary
        summary = {
            "blob_info": blob_info,
            "processing_result": processing_result,
            "processed_at": datetime.now().isoformat()
        }
        
        # Save processing results
        save_processing_results(summary)
        
        logging.info(f"Blob processing completed successfully for: {myblob.name}")
        
    except Exception as e:
        logging.error(f"Error processing blob {myblob.name}: {str(e)}")
        # In production, you might want to move failed files to an error container
        handle_processing_error(myblob, str(e))

def extract_blob_info(myblob: func.InputStream) -> dict:
    """Extract information about the blob"""
    
    blob_name = myblob.name
    blob_size = myblob.length
    
    # Parse blob path
    path_parts = blob_name.split('/')
    container_name = path_parts[0] if path_parts else "unknown"
    file_name = path_parts[-1] if path_parts else blob_name
    folder_path = '/'.join(path_parts[1:-1]) if len(path_parts) > 2 else ""
    
    # Determine file type
    file_extension = os.path.splitext(file_name)[1].lower()
    
    return {
        "blob_name": blob_name,
        "file_name": file_name,
        "folder_path": folder_path,
        "container_name": container_name,
        "file_size_bytes": blob_size,
        "file_extension": file_extension,
        "file_type": determine_file_type(file_extension)
    }

def determine_file_type(extension: str) -> str:
    """Determine file type category based on extension"""
    
    type_mapping = {
        '.csv': 'structured_data',
        '.json': 'structured_data', 
        '.xml': 'structured_data',
        '.parquet': 'structured_data',
        '.txt': 'text',
        '.log': 'log',
        '.pdf': 'document',
        '.docx': 'document',
        '.xlsx': 'spreadsheet',
        '.jpg': 'image',
        '.jpeg': 'image',
        '.png': 'image',
        '.mp4': 'video',
        '.zip': 'archive'
    }
    
    return type_mapping.get(extension, 'unknown')

def process_blob_by_type(myblob: func.InputStream, blob_info: dict) -> dict:
    """Process blob based on its type"""
    
    file_type = blob_info["file_type"]
    
    if file_type == "structured_data":
        return process_structured_data(myblob, blob_info)
    elif file_type == "text" or file_type == "log":
        return process_text_file(myblob, blob_info)
    elif file_type == "image":
        return process_image_file(myblob, blob_info)
    elif file_type == "document":
        return process_document(myblob, blob_info)
    else:
        return process_generic_file(myblob, blob_info)

def process_structured_data(myblob: func.InputStream, blob_info: dict) -> dict:
    """Process structured data files (CSV, JSON, etc.)"""
    
    try:
        # Read blob content
        blob_content = myblob.read()
        
        if blob_info["file_extension"] == ".csv":
            return process_csv_data(blob_content, blob_info)
        elif blob_info["file_extension"] == ".json":
            return process_json_data(blob_content, blob_info)
        else:
            return {
                "status": "processed",
                "message": f"Structured data file processed: {blob_info['file_extension']}",
                "content_length": len(blob_content)
            }
            
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "file_type": "structured_data"
        }

def process_csv_data(content: bytes, blob_info: dict) -> dict:
    """Process CSV data with pandas"""
    
    try:
        # Convert bytes to string
        csv_string = content.decode('utf-8')
        
        # Read CSV with pandas
        df = pd.read_csv(StringIO(csv_string))
        
        # Perform data analysis
        analysis = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "data_types": df.dtypes.astype(str).to_dict(),
            "null_counts": df.isnull().sum().to_dict(),
            "memory_usage_bytes": df.memory_usage(deep=True).sum()
        }
        
        # Basic data quality checks
        quality_checks = {
            "has_null_values": df.isnull().any().any(),
            "has_duplicates": df.duplicated().any(),
            "numeric_columns": list(df.select_dtypes(include=['number']).columns),
            "text_columns": list(df.select_dtypes(include=['object']).columns)
        }
        
        # Generate summary statistics for numeric columns
        if quality_checks["numeric_columns"]:
            numeric_summary = df[quality_checks["numeric_columns"]].describe().to_dict()
        else:
            numeric_summary = {}
        
        return {
            "status": "processed",
            "file_type": "csv",
            "analysis": analysis,
            "quality_checks": quality_checks,
            "numeric_summary": numeric_summary,
            "processing_notes": "CSV data successfully analyzed with pandas"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "file_type": "csv"
        }

def process_json_data(content: bytes, blob_info: dict) -> dict:
    """Process JSON data"""
    
    try:
        # Parse JSON
        json_data = json.loads(content.decode('utf-8'))
        
        # Analyze JSON structure
        analysis = analyze_json_structure(json_data)
        
        return {
            "status": "processed",
            "file_type": "json",
            "analysis": analysis,
            "processing_notes": "JSON data successfully parsed and analyzed"
        }
        
    except json.JSONDecodeError as e:
        return {
            "status": "failed",
            "error": f"Invalid JSON format: {str(e)}",
            "file_type": "json"
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "file_type": "json"
        }

def analyze_json_structure(data, max_depth=3, current_depth=0):
    """Recursively analyze JSON structure"""
    
    if current_depth > max_depth:
        return "max_depth_reached"
    
    if isinstance(data, dict):
        return {
            "type": "object",
            "keys": list(data.keys()),
            "key_count": len(data),
            "structure": {k: analyze_json_structure(v, max_depth, current_depth + 1) 
                         for k, v in data.items()}
        }
    elif isinstance(data, list):
        return {
            "type": "array",
            "length": len(data),
            "element_types": list(set(type(item).__name__ for item in data)),
            "sample_structure": analyze_json_structure(data[0], max_depth, current_depth + 1) if data else None
        }
    else:
        return {
            "type": type(data).__name__,
            "value_preview": str(data)[:100] if len(str(data)) > 100 else str(data)
        }

def process_text_file(myblob: func.InputStream, blob_info: dict) -> dict:
    """Process text and log files"""
    
    try:
        # Read content
        content = myblob.read().decode('utf-8')
        
        # Basic text analysis
        lines = content.split('\n')
        words = content.split()
        
        analysis = {
            "line_count": len(lines),
            "word_count": len(words),
            "character_count": len(content),
            "average_line_length": sum(len(line) for line in lines) / len(lines) if lines else 0,
            "empty_lines": len([line for line in lines if not line.strip()])
        }
        
        # If it's a log file, perform additional analysis
        if blob_info["file_extension"] == ".log":
            log_analysis = analyze_log_content(lines)
            analysis["log_analysis"] = log_analysis
        
        return {
            "status": "processed",
            "file_type": "text",
            "analysis": analysis,
            "processing_notes": f"Text file analyzed: {blob_info['file_extension']}"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "file_type": "text"
        }

def analyze_log_content(lines: list) -> dict:
    """Analyze log file content for patterns"""
    
    log_levels = ["ERROR", "WARN", "INFO", "DEBUG", "TRACE"]
    level_counts = {level: 0 for level in log_levels}
    
    timestamp_patterns = 0
    
    for line in lines:
        # Count log levels
        for level in log_levels:
            if level in line.upper():
                level_counts[level] += 1
        
        # Look for timestamp patterns (basic check)
        if any(pattern in line for pattern in [":", "-", "T", "Z"]):
            timestamp_patterns += 1
    
    return {
        "log_level_distribution": level_counts,
        "lines_with_timestamps": timestamp_patterns,
        "error_rate": level_counts["ERROR"] / len(lines) if lines else 0,
        "warning_rate": level_counts["WARN"] / len(lines) if lines else 0
    }

def process_image_file(myblob: func.InputStream, blob_info: dict) -> dict:
    """Process image files"""
    
    try:
        # For image processing, you might want to use libraries like Pillow
        # This is a basic example
        
        content = myblob.read()
        
        return {
            "status": "processed",
            "file_type": "image",
            "analysis": {
                "file_size_bytes": len(content),
                "format": blob_info["file_extension"]
            },
            "processing_notes": "Image file processed (basic analysis only)"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "file_type": "image"
        }

def process_document(myblob: func.InputStream, blob_info: dict) -> dict:
    """Process document files"""
    
    try:
        content = myblob.read()
        
        return {
            "status": "processed",
            "file_type": "document",
            "analysis": {
                "file_size_bytes": len(content),
                "format": blob_info["file_extension"]
            },
            "processing_notes": f"Document file processed: {blob_info['file_extension']}"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "file_type": "document"
        }

def process_generic_file(myblob: func.InputStream, blob_info: dict) -> dict:
    """Process files of unknown or unsupported types"""
    
    try:
        content = myblob.read()
        
        return {
            "status": "processed",
            "file_type": "generic",
            "analysis": {
                "file_size_bytes": len(content),
                "format": blob_info["file_extension"]
            },
            "processing_notes": f"Generic file processed: {blob_info['file_extension']}"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "file_type": "generic"
        }

def save_processing_results(summary: dict) -> None:
    """Save processing results to storage"""
    
    try:
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
        if not storage_account_name:
            logging.warning("Storage account not configured, skipping results save")
            return
        
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )
        
        # Create results blob name
        timestamp = datetime.now().strftime("%Y/%m/%d")
        blob_name = f"processing-results/{timestamp}/{summary['blob_info']['file_name']}-results.json"
        
        # Upload results
        blob_client = blob_service_client.get_blob_client(
            container="function-results",
            blob=blob_name
        )
        
        results_json = json.dumps(summary, indent=2, default=str)
        blob_client.upload_blob(results_json, overwrite=True)
        
        logging.info(f"Processing results saved to: {blob_name}")
        
    except Exception as e:
        logging.error(f"Failed to save processing results: {str(e)}")

def handle_processing_error(myblob: func.InputStream, error_message: str) -> None:
    """Handle processing errors by moving files to error container"""
    
    try:
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
        if not storage_account_name:
            return
        
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )
        
        # Create error record
        error_record = {
            "original_blob": myblob.name,
            "error_message": error_message,
            "error_timestamp": datetime.now().isoformat(),
            "file_size_bytes": myblob.length
        }
        
        # Save error record
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_blob_name = f"errors/{timestamp}-{os.path.basename(myblob.name)}-error.json"
        
        blob_client = blob_service_client.get_blob_client(
            container="processing-errors",
            blob=error_blob_name
        )
        
        error_json = json.dumps(error_record, indent=2)
        blob_client.upload_blob(error_json, overwrite=True)
        
        logging.info(f"Error record saved to: {error_blob_name}")
        
    except Exception as e:
        logging.error(f"Failed to save error record: {str(e)}")
```

This comprehensive guide provides everything needed to understand, implement, and manage various types of triggers across Azure services, ensuring effective automation and event-driven processing in your cloud applications.

---

*Document Version: 1.0*  
*Last Updated: January 2025*  
*© Microsoft Azure Documentation*