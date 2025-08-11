# Azure DAG & Data Lineage Comprehensive Guide
## Complete Guide to Directed Acyclic Graphs and Data Lineage in Azure

---

### Table of Contents

1. [Overview](#overview)
2. [DAG Fundamentals](#dag-fundamentals)
3. [Data Lineage Concepts](#data-lineage-concepts)
4. [Azure Data Factory DAGs](#azure-data-factory-dags)
5. [Azure Databricks Lineage](#azure-databricks-lineage)
6. [Azure Synapse Lineage](#azure-synapse-lineage)
7. [Azure Purview Data Lineage](#azure-purview-data-lineage)
8. [Lineage Tracking Implementation](#lineage-tracking-implementation)
9. [Visualization and Reporting](#visualization-and-reporting)
10. [Data Governance](#data-governance)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)

---

## Overview

DAG (Directed Acyclic Graph) and Data Lineage are fundamental concepts in modern data engineering and governance. In Azure, these concepts are implemented across multiple services including Azure Data Factory, Azure Databricks, Azure Synapse Analytics, and Azure Purview, providing comprehensive data flow visualization, dependency management, and governance capabilities.

### Key Concepts

```json
{
  "dag_lineage_overview": {
    "directed_acyclic_graph": {
      "definition": "A finite directed graph with no directed cycles",
      "characteristics": [
        "Nodes represent data processing tasks or datasets",
        "Edges represent dependencies or data flow",
        "No circular dependencies allowed",
        "Topological ordering possible"
      ],
      "azure_implementations": [
        "Azure Data Factory pipelines",
        "Azure Databricks workflows",
        "Azure Synapse pipelines",
        "Apache Airflow on Azure"
      ]
    },
    "data_lineage": {
      "definition": "The data's lifecycle including origins, transformations, and destinations",
      "components": [
        "Data sources and origins",
        "Transformation processes",
        "Data movement and flow",
        "Data destinations and consumers",
        "Metadata and schema evolution"
      ],
      "benefits": [
        "Data governance and compliance",
        "Impact analysis for changes",
        "Root cause analysis for data quality issues",
        "Regulatory compliance and auditing"
      ]
    },
    "azure_services_integration": {
      "azure_data_factory": "Pipeline orchestration and data movement lineage",
      "azure_databricks": "Spark job execution and transformation lineage",
      "azure_synapse": "Unified analytics platform with built-in lineage",
      "azure_purview": "Centralized data catalog and lineage visualization",
      "azure_monitor": "Operational monitoring and lineage tracking"
    }
  }
}
```

---

## DAG Fundamentals

### Understanding DAGs in Azure Context

DAGs form the backbone of data processing workflows in Azure, providing a mathematical model for representing dependencies and execution order.

```python
# DAG Concepts and Implementation in Azure
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict, deque

class TaskStatus(Enum):
    """Enumeration for task execution status"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRY = "retry"

class TaskType(Enum):
    """Enumeration for different task types in Azure"""
    DATA_INGESTION = "data_ingestion"
    DATA_TRANSFORMATION = "data_transformation"
    DATA_VALIDATION = "data_validation"
    DATA_EXPORT = "data_export"
    MACHINE_LEARNING = "machine_learning"
    NOTIFICATION = "notification"

@dataclass
class Task:
    """Represents a single task in the DAG"""
    task_id: str
    task_name: str
    task_type: TaskType
    dependencies: List[str] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 3
    timeout_minutes: int = 60
    status: TaskStatus = TaskStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    output_datasets: List[str] = field(default_factory=list)
    input_datasets: List[str] = field(default_factory=list)

@dataclass
class DataLineage:
    """Represents data lineage information"""
    dataset_id: str
    dataset_name: str
    source_system: str
    creation_time: datetime
    last_modified: datetime
    schema_version: str
    upstream_datasets: List[str] = field(default_factory=list)
    downstream_datasets: List[str] = field(default_factory=list)
    transformations: List[str] = field(default_factory=list)
    quality_metrics: Dict[str, Any] = field(default_factory=dict)

class AzureDAGManager:
    """Comprehensive DAG management system for Azure data workflows"""
    
    def __init__(self, dag_name: str, description: str = ""):
        self.dag_name = dag_name
        self.description = description
        self.dag_id = str(uuid.uuid4())
        self.creation_time = datetime.now()
        
        # DAG structure
        self.tasks: Dict[str, Task] = {}
        self.graph = nx.DiGraph()
        self.execution_history: List[Dict[str, Any]] = []
        
        # Lineage tracking
        self.lineage_map: Dict[str, DataLineage] = {}
        self.dataset_dependencies: Dict[str, Set[str]] = defaultdict(set)
        
        print(f"✓ Initialized Azure DAG Manager: {dag_name}")
        print(f"✓ DAG ID: {self.dag_id}")
    
    def add_task(self, task: Task) -> bool:
        """Add a task to the DAG"""
        
        try:
            # Validate task
            if task.task_id in self.tasks:
                raise ValueError(f"Task {task.task_id} already exists in DAG")
            
            # Add task to DAG
            self.tasks[task.task_id] = task
            self.graph.add_node(task.task_id, task=task)
            
            # Add dependencies
            for dep_task_id in task.dependencies:
                if dep_task_id not in self.tasks:
                    print(f"⚠️ Warning: Dependency {dep_task_id} not found, will be added later")
                else:
                    self.graph.add_edge(dep_task_id, task.task_id)
            
            # Validate DAG remains acyclic
            if not nx.is_directed_acyclic_graph(self.graph):
                # Remove the task and its edges
                self.graph.remove_node(task.task_id)
                del self.tasks[task.task_id]
                raise ValueError(f"Adding task {task.task_id} would create a cycle in the DAG")
            
            print(f"✓ Added task: {task.task_id} ({task.task_type.value})")
            return True
            
        except Exception as e:
            print(f"❌ Error adding task {task.task_id}: {str(e)}")
            return False
    
    def add_task_dependency(self, upstream_task_id: str, downstream_task_id: str) -> bool:
        """Add a dependency between two tasks"""
        
        try:
            if upstream_task_id not in self.tasks:
                raise ValueError(f"Upstream task {upstream_task_id} not found")
            
            if downstream_task_id not in self.tasks:
                raise ValueError(f"Downstream task {downstream_task_id} not found")
            
            # Add edge to graph
            self.graph.add_edge(upstream_task_id, downstream_task_id)
            
            # Validate DAG remains acyclic
            if not nx.is_directed_acyclic_graph(self.graph):
                self.graph.remove_edge(upstream_task_id, downstream_task_id)
                raise ValueError(f"Adding dependency {upstream_task_id} -> {downstream_task_id} would create a cycle")
            
            # Update task dependencies
            if upstream_task_id not in self.tasks[downstream_task_id].dependencies:
                self.tasks[downstream_task_id].dependencies.append(upstream_task_id)
            
            print(f"✓ Added dependency: {upstream_task_id} -> {downstream_task_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error adding dependency: {str(e)}")
            return False
    
    def get_topological_order(self) -> List[str]:
        """Get topological ordering of tasks for execution"""
        
        try:
            if not nx.is_directed_acyclic_graph(self.graph):
                raise ValueError("Graph contains cycles, cannot determine topological order")
            
            topological_order = list(nx.topological_sort(self.graph))
            print(f"✓ Topological order determined: {len(topological_order)} tasks")
            return topological_order
            
        except Exception as e:
            print(f"❌ Error determining topological order: {str(e)}")
            return []
    
    def get_execution_layers(self) -> List[List[str]]:
        """Get tasks grouped by execution layers (tasks that can run in parallel)"""
        
        try:
            layers = []
            remaining_tasks = set(self.tasks.keys())
            
            while remaining_tasks:
                # Find tasks with no dependencies in remaining tasks
                current_layer = []
                for task_id in remaining_tasks.copy():
                    task_deps = set(self.tasks[task_id].dependencies)
                    if not task_deps.intersection(remaining_tasks):
                        current_layer.append(task_id)
                        remaining_tasks.remove(task_id)
                
                if not current_layer:
                    raise ValueError("Circular dependency detected")
                
                layers.append(current_layer)
            
            print(f"✓ Execution layers determined: {len(layers)} layers")
            for i, layer in enumerate(layers):
                print(f"  Layer {i+1}: {layer}")
            
            return layers
            
        except Exception as e:
            print(f"❌ Error determining execution layers: {str(e)}")
            return []
    
    def analyze_dag_structure(self) -> Dict[str, Any]:
        """Analyze DAG structure and properties"""
        
        analysis = {
            "dag_metadata": {
                "dag_id": self.dag_id,
                "dag_name": self.dag_name,
                "creation_time": self.creation_time.isoformat(),
                "total_tasks": len(self.tasks),
                "total_edges": self.graph.number_of_edges()
            },
            "structural_analysis": {},
            "task_analysis": {},
            "dependency_analysis": {},
            "complexity_metrics": {}
        }
        
        try:
            # Structural analysis
            analysis["structural_analysis"] = {
                "is_acyclic": nx.is_directed_acyclic_graph(self.graph),
                "is_connected": nx.is_weakly_connected(self.graph) if self.graph.number_of_nodes() > 0 else True,
                "number_of_components": nx.number_weakly_connected_components(self.graph),
                "longest_path_length": len(nx.dag_longest_path(self.graph)) if nx.is_directed_acyclic_graph(self.graph) else 0
            }
            
            # Task analysis
            task_types = defaultdict(int)
            task_statuses = defaultdict(int)
            
            for task in self.tasks.values():
                task_types[task.task_type.value] += 1
                task_statuses[task.status.value] += 1
            
            analysis["task_analysis"] = {
                "task_types": dict(task_types),
                "task_statuses": dict(task_statuses),
                "average_dependencies_per_task": sum(len(task.dependencies) for task in self.tasks.values()) / max(len(self.tasks), 1)
            }
            
            # Dependency analysis
            if self.graph.number_of_nodes() > 0:
                in_degrees = dict(self.graph.in_degree())
                out_degrees = dict(self.graph.out_degree())
                
                analysis["dependency_analysis"] = {
                    "root_tasks": [task_id for task_id, degree in in_degrees.items() if degree == 0],
                    "leaf_tasks": [task_id for task_id, degree in out_degrees.items() if degree == 0],
                    "max_fan_in": max(in_degrees.values()) if in_degrees else 0,
                    "max_fan_out": max(out_degrees.values()) if out_degrees else 0,
                    "average_fan_in": sum(in_degrees.values()) / len(in_degrees) if in_degrees else 0,
                    "average_fan_out": sum(out_degrees.values()) / len(out_degrees) if out_degrees else 0
                }
            
            # Complexity metrics
            analysis["complexity_metrics"] = {
                "cyclomatic_complexity": self.graph.number_of_edges() - self.graph.number_of_nodes() + 2 if self.graph.number_of_nodes() > 0 else 0,
                "density": nx.density(self.graph),
                "execution_layers": len(self.get_execution_layers()),
                "critical_path_length": analysis["structural_analysis"]["longest_path_length"]
            }
            
            print("✓ DAG structure analysis completed")
            return analysis
            
        except Exception as e:
            print(f"❌ Error analyzing DAG structure: {str(e)}")
            analysis["error"] = str(e)
            return analysis
    
    def add_data_lineage(self, lineage: DataLineage) -> bool:
        """Add data lineage information"""
        
        try:
            self.lineage_map[lineage.dataset_id] = lineage
            
            # Update dataset dependencies
            for upstream_id in lineage.upstream_datasets:
                self.dataset_dependencies[upstream_id].add(lineage.dataset_id)
            
            print(f"✓ Added data lineage for dataset: {lineage.dataset_name}")
            return True
            
        except Exception as e:
            print(f"❌ Error adding data lineage: {str(e)}")
            return False
    
    def trace_data_lineage(self, dataset_id: str, direction: str = "both") -> Dict[str, Any]:
        """Trace data lineage for a specific dataset"""
        
        lineage_trace = {
            "dataset_id": dataset_id,
            "upstream_lineage": [],
            "downstream_lineage": [],
            "transformation_chain": [],
            "quality_impact": {}
        }
        
        try:
            if dataset_id not in self.lineage_map:
                raise ValueError(f"Dataset {dataset_id} not found in lineage map")
            
            root_lineage = self.lineage_map[dataset_id]
            
            # Trace upstream lineage
            if direction in ["upstream", "both"]:
                visited = set()
                upstream_queue = deque(root_lineage.upstream_datasets)
                
                while upstream_queue:
                    current_id = upstream_queue.popleft()
                    if current_id in visited or current_id not in self.lineage_map:
                        continue
                    
                    visited.add(current_id)
                    current_lineage = self.lineage_map[current_id]
                    
                    lineage_trace["upstream_lineage"].append({
                        "dataset_id": current_id,
                        "dataset_name": current_lineage.dataset_name,
                        "source_system": current_lineage.source_system,
                        "transformations": current_lineage.transformations,
                        "quality_metrics": current_lineage.quality_metrics
                    })
                    
                    # Add upstream dependencies to queue
                    upstream_queue.extend(current_lineage.upstream_datasets)
            
            # Trace downstream lineage
            if direction in ["downstream", "both"]:
                visited = set()
                downstream_queue = deque([dataset_id])
                
                while downstream_queue:
                    current_id = downstream_queue.popleft()
                    if current_id in visited:
                        continue
                    
                    visited.add(current_id)
                    
                    # Find downstream datasets
                    for dep_id, deps in self.dataset_dependencies.items():
                        if current_id in deps and dep_id in self.lineage_map:
                            downstream_lineage = self.lineage_map[dep_id]
                            
                            lineage_trace["downstream_lineage"].append({
                                "dataset_id": dep_id,
                                "dataset_name": downstream_lineage.dataset_name,
                                "source_system": downstream_lineage.source_system,
                                "transformations": downstream_lineage.transformations,
                                "quality_metrics": downstream_lineage.quality_metrics
                            })
                            
                            downstream_queue.append(dep_id)
            
            # Build transformation chain
            transformation_chain = []
            for upstream in lineage_trace["upstream_lineage"]:
                transformation_chain.extend(upstream["transformations"])
            transformation_chain.extend(root_lineage.transformations)
            
            lineage_trace["transformation_chain"] = list(set(transformation_chain))
            
            print(f"✓ Data lineage traced for dataset: {dataset_id}")
            print(f"  Upstream datasets: {len(lineage_trace['upstream_lineage'])}")
            print(f"  Downstream datasets: {len(lineage_trace['downstream_lineage'])}")
            print(f"  Total transformations: {len(lineage_trace['transformation_chain'])}")
            
            return lineage_trace
            
        except Exception as e:
            print(f"❌ Error tracing data lineage: {str(e)}")
            lineage_trace["error"] = str(e)
            return lineage_trace
    
    def simulate_dag_execution(self, max_parallel_tasks: int = 4) -> Dict[str, Any]:
        """Simulate DAG execution with parallel processing"""
        
        execution_result = {
            "execution_id": str(uuid.uuid4()),
            "start_time": datetime.now(),
            "end_time": None,
            "total_duration_seconds": 0,
            "executed_tasks": [],
            "failed_tasks": [],
            "execution_layers": [],
            "resource_utilization": {}
        }
        
        try:
            print(f"🚀 Starting DAG execution simulation: {self.dag_name}")
            
            # Get execution layers
            layers = self.get_execution_layers()
            if not layers:
                raise ValueError("Cannot determine execution order")
            
            # Reset task statuses
            for task in self.tasks.values():
                task.status = TaskStatus.PENDING
                task.start_time = None
                task.end_time = None
                task.error_message = None
            
            total_start_time = datetime.now()
            
            # Execute layers
            for layer_index, layer_tasks in enumerate(layers):
                layer_start_time = datetime.now()
                print(f"📋 Executing Layer {layer_index + 1}: {layer_tasks}")
                
                # Simulate parallel execution within layer
                layer_results = []
                for task_id in layer_tasks[:max_parallel_tasks]:  # Limit parallelism
                    task = self.tasks[task_id]
                    
                    # Simulate task execution
                    task.status = TaskStatus.RUNNING
                    task.start_time = datetime.now()
                    
                    # Simulate execution time (random between 1-10 seconds)
                    import random
                    execution_time = random.uniform(1, 10)
                    
                    # Simulate task success/failure (90% success rate)
                    if random.random() < 0.9:
                        task.status = TaskStatus.SUCCESS
                        print(f"  ✅ Task {task_id} completed successfully")
                    else:
                        task.status = TaskStatus.FAILED
                        task.error_message = f"Simulated failure in task {task_id}"
                        execution_result["failed_tasks"].append(task_id)
                        print(f"  ❌ Task {task_id} failed: {task.error_message}")
                    
                    task.end_time = datetime.now()
                    
                    layer_results.append({
                        "task_id": task_id,
                        "status": task.status.value,
                        "execution_time_seconds": execution_time,
                        "start_time": task.start_time.isoformat(),
                        "end_time": task.end_time.isoformat()
                    })
                
                layer_end_time = datetime.now()
                layer_duration = (layer_end_time - layer_start_time).total_seconds()
                
                execution_result["execution_layers"].append({
                    "layer_index": layer_index + 1,
                    "tasks": layer_tasks,
                    "duration_seconds": layer_duration,
                    "results": layer_results
                })
                
                print(f"✓ Layer {layer_index + 1} completed in {layer_duration:.2f} seconds")
            
            total_end_time = datetime.now()
            execution_result["end_time"] = total_end_time
            execution_result["total_duration_seconds"] = (total_end_time - total_start_time).total_seconds()
            
            # Collect execution statistics
            successful_tasks = [task_id for task_id, task in self.tasks.items() if task.status == TaskStatus.SUCCESS]
            execution_result["executed_tasks"] = successful_tasks
            
            execution_result["resource_utilization"] = {
                "total_tasks": len(self.tasks),
                "successful_tasks": len(successful_tasks),
                "failed_tasks": len(execution_result["failed_tasks"]),
                "success_rate": len(successful_tasks) / len(self.tasks) * 100,
                "average_layer_duration": sum(layer["duration_seconds"] for layer in execution_result["execution_layers"]) / len(execution_result["execution_layers"])
            }
            
            # Store execution history
            self.execution_history.append(execution_result)
            
            print(f"🏁 DAG execution completed in {execution_result['total_duration_seconds']:.2f} seconds")
            print(f"📊 Success rate: {execution_result['resource_utilization']['success_rate']:.1f}%")
            
            return execution_result
            
        except Exception as e:
            print(f"❌ Error during DAG execution simulation: {str(e)}")
            execution_result["error"] = str(e)
            execution_result["end_time"] = datetime.now()
            return execution_result
    
    def generate_dag_visualization_data(self) -> Dict[str, Any]:
        """Generate data for DAG visualization"""
        
        visualization_data = {
            "nodes": [],
            "edges": [],
            "layout_hints": {},
            "styling": {}
        }
        
        try:
            # Generate nodes
            for task_id, task in self.tasks.items():
                node_data = {
                    "id": task_id,
                    "label": task.task_name,
                    "type": task.task_type.value,
                    "status": task.status.value,
                    "dependencies": len(task.dependencies),
                    "parameters": task.parameters,
                    "input_datasets": task.input_datasets,
                    "output_datasets": task.output_datasets
                }
                
                # Add execution information if available
                if task.start_time:
                    node_data["start_time"] = task.start_time.isoformat()
                if task.end_time:
                    node_data["end_time"] = task.end_time.isoformat()
                    node_data["duration_seconds"] = (task.end_time - task.start_time).total_seconds()
                if task.error_message:
                    node_data["error_message"] = task.error_message
                
                visualization_data["nodes"].append(node_data)
            
            # Generate edges
            for edge in self.graph.edges():
                edge_data = {
                    "source": edge[0],
                    "target": edge[1],
                    "type": "dependency"
                }
                visualization_data["edges"].append(edge_data)
            
            # Layout hints for better visualization
            layers = self.get_execution_layers()
            layout_hints = {}
            for layer_index, layer_tasks in enumerate(layers):
                for task_index, task_id in enumerate(layer_tasks):
                    layout_hints[task_id] = {
                        "layer": layer_index,
                        "position_in_layer": task_index,
                        "total_in_layer": len(layer_tasks)
                    }
            
            visualization_data["layout_hints"] = layout_hints
            
            # Styling information
            visualization_data["styling"] = {
                "task_colors": {
                    "data_ingestion": "#4CAF50",
                    "data_transformation": "#2196F3",
                    "data_validation": "#FF9800",
                    "data_export": "#9C27B0",
                    "machine_learning": "#F44336",
                    "notification": "#607D8B"
                },
                "status_colors": {
                    "pending": "#9E9E9E",
                    "running": "#2196F3",
                    "success": "#4CAF50",
                    "failed": "#F44336",
                    "skipped": "#FF9800",
                    "retry": "#FFC107"
                }
            }
            
            print(f"✓ DAG visualization data generated")
            print(f"  Nodes: {len(visualization_data['nodes'])}")
            print(f"  Edges: {len(visualization_data['edges'])}")
            print(f"  Layers: {len(layers)}")
            
            return visualization_data
            
        except Exception as e:
            print(f"❌ Error generating DAG visualization data: {str(e)}")
            visualization_data["error"] = str(e)
            return visualization_data
    
    def export_dag_definition(self, format_type: str = "json") -> str:
        """Export DAG definition in various formats"""
        
        try:
            dag_definition = {
                "dag_metadata": {
                    "dag_id": self.dag_id,
                    "dag_name": self.dag_name,
                    "description": self.description,
                    "creation_time": self.creation_time.isoformat(),
                    "total_tasks": len(self.tasks),
                    "total_dependencies": self.graph.number_of_edges()
                },
                "tasks": [],
                "dependencies": [],
                "data_lineage": [],
                "execution_history": self.execution_history[-5:] if self.execution_history else []  # Last 5 executions
            }
            
            # Export tasks
            for task_id, task in self.tasks.items():
                task_def = {
                    "task_id": task_id,
                    "task_name": task.task_name,
                    "task_type": task.task_type.value,
                    "dependencies": task.dependencies,
                    "parameters": task.parameters,
                    "retry_count": task.retry_count,
                    "timeout_minutes": task.timeout_minutes,
                    "input_datasets": task.input_datasets,
                    "output_datasets": task.output_datasets
                }
                dag_definition["tasks"].append(task_def)
            
            # Export dependencies
            for edge in self.graph.edges():
                dag_definition["dependencies"].append({
                    "upstream_task": edge[0],
                    "downstream_task": edge[1]
                })
            
            # Export data lineage
            for dataset_id, lineage in self.lineage_map.items():
                lineage_def = {
                    "dataset_id": dataset_id,
                    "dataset_name": lineage.dataset_name,
                    "source_system": lineage.source_system,
                    "schema_version": lineage.schema_version,
                    "upstream_datasets": lineage.upstream_datasets,
                    "downstream_datasets": lineage.downstream_datasets,
                    "transformations": lineage.transformations,
                    "creation_time": lineage.creation_time.isoformat(),
                    "last_modified": lineage.last_modified.isoformat()
                }
                dag_definition["data_lineage"].append(lineage_def)
            
            if format_type.lower() == "json":
                return json.dumps(dag_definition, indent=2)
            elif format_type.lower() == "yaml":
                import yaml
                return yaml.dump(dag_definition, default_flow_style=False)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
                
        except Exception as e:
            print(f"❌ Error exporting DAG definition: {str(e)}")
            return f"Error: {str(e)}"

# Example usage and demonstration
def create_sample_data_pipeline_dag():
    """Create a comprehensive sample data pipeline DAG"""
    
    print("=== Creating Sample Data Pipeline DAG ===")
    
    # Initialize DAG manager
    dag_manager = AzureDAGManager(
        dag_name="CustomerAnalyticsPipeline",
        description="End-to-end customer analytics data pipeline with lineage tracking"
    )
    
    # Define tasks
    tasks = [
        Task(
            task_id="ingest_customer_data",
            task_name="Ingest Customer Data from CRM",
            task_type=TaskType.DATA_INGESTION,
            parameters={
                "source_system": "CRM_Database",
                "connection_string": "encrypted_connection_string",
                "table_name": "customers",
                "incremental_column": "last_modified"
            },
            input_datasets=[],
            output_datasets=["raw_customer_data"]
        ),
        Task(
            task_id="ingest_transaction_data",
            task_name="Ingest Transaction Data from ERP",
            task_type=TaskType.DATA_INGESTION,
            parameters={
                "source_system": "ERP_Database",
                "connection_string": "encrypted_connection_string",
                "table_name": "transactions",
                "incremental_column": "transaction_date"
            },
            input_datasets=[],
            output_datasets=["raw_transaction_data"]
        ),
        Task(
            task_id="validate_customer_data",
            task_name="Validate Customer Data Quality",
            task_type=TaskType.DATA_VALIDATION,
            dependencies=["ingest_customer_data"],
            parameters={
                "validation_rules": [
                    "customer_id_not_null",
                    "email_format_valid",
                    "phone_number_format"
                ],
                "error_threshold": 0.05
            },
            input_datasets=["raw_customer_data"],
            output_datasets=["validated_customer_data"]
        ),
        Task(
            task_id="validate_transaction_data",
            task_name="Validate Transaction Data Quality",
            task_type=TaskType.DATA_VALIDATION,
            dependencies=["ingest_transaction_data"],
            parameters={
                "validation_rules": [
                    "transaction_id_not_null",
                    "amount_positive",
                    "customer_id_exists"
                ],
                "error_threshold": 0.02
            },
            input_datasets=["raw_transaction_data"],
            output_datasets=["validated_transaction_data"]
        ),
        Task(
            task_id="transform_customer_data",
            task_name="Transform and Enrich Customer Data",
            task_type=TaskType.DATA_TRANSFORMATION,
            dependencies=["validate_customer_data"],
            parameters={
                "transformations": [
                    "standardize_address",
                    "calculate_customer_age",
                    "derive_customer_segment"
                ],
                "enrichment_sources": ["geo_location_api", "demographic_data"]
            },
            input_datasets=["validated_customer_data"],
            output_datasets=["transformed_customer_data"]
        ),
        Task(
            task_id="aggregate_transaction_data",
            task_name="Aggregate Transaction Data by Customer",
            task_type=TaskType.DATA_TRANSFORMATION,
            dependencies=["validate_transaction_data"],
            parameters={
                "aggregation_window": "monthly",
                "metrics": [
                    "total_transactions",
                    "total_amount",
                    "average_transaction_amount",
                    "transaction_frequency"
                ]
            },
            input_datasets=["validated_transaction_data"],
            output_datasets=["aggregated_transaction_data"]
        ),
        Task(
            task_id="join_customer_transaction_data",
            task_name="Join Customer and Transaction Data",
            task_type=TaskType.DATA_TRANSFORMATION,
            dependencies=["transform_customer_data", "aggregate_transaction_data"],
            parameters={
                "join_type": "left",
                "join_keys": ["customer_id"],
                "output_schema": "customer_analytics_schema_v1"
            },
            input_datasets=["transformed_customer_data", "aggregated_transaction_data"],
            output_datasets=["customer_analytics_data"]
        ),
        Task(
            task_id="calculate_customer_ltv",
            task_name="Calculate Customer Lifetime Value",
            task_type=TaskType.MACHINE_LEARNING,
            dependencies=["join_customer_transaction_data"],
            parameters={
                "model_type": "regression",
                "features": [
                    "customer_age",
                    "customer_segment",
                    "total_transactions",
                    "average_transaction_amount"
                ],
                "prediction_horizon": "12_months"
            },
            input_datasets=["customer_analytics_data"],
            output_datasets=["customer_ltv_predictions"]
        ),
        Task(
            task_id="export_to_data_warehouse",
            task_name="Export to Azure Synapse Data Warehouse",
            task_type=TaskType.DATA_EXPORT,
            dependencies=["calculate_customer_ltv"],
            parameters={
                "destination": "azure_synapse",
                "target_table": "dim_customer_analytics",
                "load_type": "upsert",
                "partition_by": "customer_segment"
            },
            input_datasets=["customer_ltv_predictions"],
            output_datasets=["dw_customer_analytics"]
        ),
        Task(
            task_id="export_to_power_bi",
            task_name="Export to Power BI Dataset",
            task_type=TaskType.DATA_EXPORT,
            dependencies=["calculate_customer_ltv"],
            parameters={
                "destination": "power_bi",
                "dataset_name": "customer_analytics_dashboard",
                "refresh_mode": "incremental"
            },
            input_datasets=["customer_ltv_predictions"],
            output_datasets=["powerbi_customer_analytics"]
        ),
        Task(
            task_id="send_completion_notification",
            task_name="Send Pipeline Completion Notification",
            task_type=TaskType.NOTIFICATION,
            dependencies=["export_to_data_warehouse", "export_to_power_bi"],
            parameters={
                "notification_type": "email",
                "recipients": ["data-team@company.com"],
                "include_metrics": True
            },
            input_datasets=["dw_customer_analytics", "powerbi_customer_analytics"],
            output_datasets=[]
        )
    ]
    
    # Add tasks to DAG
    for task in tasks:
        dag_manager.add_task(task)
    
    # Create data lineage information
    lineage_data = [
        DataLineage(
            dataset_id="raw_customer_data",
            dataset_name="Raw Customer Data",
            source_system="CRM_Database",
            creation_time=datetime.now() - timedelta(days=1),
            last_modified=datetime.now(),
            schema_version="v1.0",
            transformations=["data_ingestion"]
        ),
        DataLineage(
            dataset_id="raw_transaction_data",
            dataset_name="Raw Transaction Data",
            source_system="ERP_Database",
            creation_time=datetime.now() - timedelta(days=1),
            last_modified=datetime.now(),
            schema_version="v1.0",
            transformations=["data_ingestion"]
        ),
        DataLineage(
            dataset_id="validated_customer_data",
            dataset_name="Validated Customer Data",
            source_system="Data_Pipeline",
            creation_time=datetime.now(),
            last_modified=datetime.now(),
            schema_version="v1.1",
            upstream_datasets=["raw_customer_data"],
            transformations=["data_validation", "quality_checks"]
        ),
        DataLineage(
            dataset_id="validated_transaction_data",
            dataset_name="Validated Transaction Data",
            source_system="Data_Pipeline",
            creation_time=datetime.now(),
            last_modified=datetime.now(),
            schema_version="v1.1",
            upstream_datasets=["raw_transaction_data"],
            transformations=["data_validation", "quality_checks"]
        ),
        DataLineage(
            dataset_id="transformed_customer_data",
            dataset_name="Transformed Customer Data",
            source_system="Data_Pipeline",
            creation_time=datetime.now(),
            last_modified=datetime.now(),
            schema_version="v2.0",
            upstream_datasets=["validated_customer_data"],
            transformations=["data_transformation", "enrichment", "standardization"]
        ),
        DataLineage(
            dataset_id="aggregated_transaction_data",
            dataset_name="Aggregated Transaction Data",
            source_system="Data_Pipeline",
            creation_time=datetime.now(),
            last_modified=datetime.now(),
            schema_version="v2.0",
            upstream_datasets=["validated_transaction_data"],
            transformations=["data_aggregation", "metric_calculation"]
        ),
        DataLineage(
            dataset_id="customer_analytics_data",
            dataset_name="Customer Analytics Data",
            source_system="Data_Pipeline",
            creation_time=datetime.now(),
            last_modified=datetime.now(),
            schema_version="v3.0",
            upstream_datasets=["transformed_customer_data", "aggregated_transaction_data"],
            transformations=["data_join", "feature_engineering"]
        ),
        DataLineage(
            dataset_id="customer_ltv_predictions",
            dataset_name="Customer LTV Predictions",
            source_system="ML_Pipeline",
            creation_time=datetime.now(),
            last_modified=datetime.now(),
            schema_version="v4.0",
            upstream_datasets=["customer_analytics_data"],
            transformations=["ml_prediction", "ltv_calculation"]
        ),
        DataLineage(
            dataset_id="dw_customer_analytics",
            dataset_name="Data Warehouse Customer Analytics",
            source_system="Azure_Synapse",
            creation_time=datetime.now(),
            last_modified=datetime.now(),
            schema_version="v4.0",
            upstream_datasets=["customer_ltv_predictions"],
            transformations=["data_export", "upsert_operation"]
        ),
        DataLineage(
            dataset_id="powerbi_customer_analytics",
            dataset_name="Power BI Customer Analytics Dataset",
            source_system="Power_BI",
            creation_time=datetime.now(),
            last_modified=datetime.now(),
            schema_version="v4.0",
            upstream_datasets=["customer_ltv_predictions"],
            transformations=["data_export", "incremental_refresh"]
        )
    ]
    
    # Add lineage data
    for lineage in lineage_data:
        dag_manager.add_data_lineage(lineage)
    
    return dag_manager

# Demonstration function
def demonstrate_dag_and_lineage():
    """Comprehensive demonstration of DAG and lineage functionality"""
    
    print("=== DAG and Data Lineage Demonstration ===")
    
    try:
        # Create sample DAG
        dag_manager = create_sample_data_pipeline_dag()
        
        # Analyze DAG structure
        print("\n📊 Analyzing DAG Structure...")
        dag_analysis = dag_manager.analyze_dag_structure()
        
        print(f"✓ DAG Analysis Summary:")
        print(f"  Total Tasks: {dag_analysis['dag_metadata']['total_tasks']}")
        print(f"  Total Dependencies: {dag_analysis['dag_metadata']['total_edges']}")
        print(f"  Execution Layers: {dag_analysis['complexity_metrics']['execution_layers']}")
        print(f"  Critical Path Length: {dag_analysis['complexity_metrics']['critical_path_length']}")
        
        # Demonstrate lineage tracing
        print("\n🔍 Tracing Data Lineage...")
        lineage_trace = dag_manager.trace_data_lineage("customer_ltv_predictions", "both")
        
        print(f"✓ Lineage Trace for Customer LTV Predictions:")
        print(f"  Upstream Datasets: {len(lineage_trace['upstream_lineage'])}")
        print(f"  Downstream Datasets: {len(lineage_trace['downstream_lineage'])}")
        print(f"  Total Transformations: {len(lineage_trace['transformation_chain'])}")
        
        # Simulate DAG execution
        print("\n🚀 Simulating DAG Execution...")
        execution_result = dag_manager.simulate_dag_execution(max_parallel_tasks=3)
        
        print(f"✓ Execution Summary:")
        print(f"  Total Duration: {execution_result['total_duration_seconds']:.2f} seconds")
        print(f"  Success Rate: {execution_result['resource_utilization']['success_rate']:.1f}%")
        print(f"  Failed Tasks: {len(execution_result['failed_tasks'])}")
        
        # Generate visualization data
        print("\n🎨 Generating Visualization Data...")
        viz_data = dag_manager.generate_dag_visualization_data()
        
        print(f"✓ Visualization Data Generated:")
        print(f"  Nodes: {len(viz_data['nodes'])}")
        print(f"  Edges: {len(viz_data['edges'])}")
        
        # Export DAG definition
        print("\n💾 Exporting DAG Definition...")
        dag_json = dag_manager.export_dag_definition("json")
        
        return {
            "dag_manager": dag_manager,
            "dag_analysis": dag_analysis,
            "lineage_trace": lineage_trace,
            "execution_result": execution_result,
            "visualization_data": viz_data,
            "dag_definition": dag_json
        }
        
    except Exception as e:
        print(f"❌ Error in DAG demonstration: {str(e)}")
        return {"error": str(e)}

# Run demonstration
# demo_results = demonstrate_dag_and_lineage()
```

---

## Data Lineage Concepts

### Understanding Data Lineage in Azure

Data lineage provides a comprehensive view of data flow through systems, enabling better governance, compliance, and impact analysis.

```python
# Advanced Data Lineage Implementation
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from enum import Enum
import json
import hashlib
import uuid

class LineageEventType(Enum):
    """Types of lineage events"""
    DATA_CREATION = "data_creation"
    DATA_TRANSFORMATION = "data_transformation"
    DATA_MOVEMENT = "data_movement"
    DATA_DELETION = "data_deletion"
    SCHEMA_CHANGE = "schema_change"
    QUALITY_CHECK = "quality_check"
    ACCESS_EVENT = "access_event"

class DataQualityStatus(Enum):
    """Data quality assessment status"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    UNKNOWN = "unknown"

@dataclass
class LineageEvent:
    """Represents a single lineage event"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: LineageEventType = LineageEventType.DATA_TRANSFORMATION
    timestamp: datetime = field(default_factory=datetime.now)
    source_dataset_id: Optional[str] = None
    target_dataset_id: Optional[str] = None
    transformation_details: Dict[str, Any] = field(default_factory=dict)
    user_id: Optional[str] = None
    system_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class DataQualityMetrics:
    """Comprehensive data quality metrics"""
    completeness_score: float = 0.0
    accuracy_score: float = 0.0
    consistency_score: float = 0.0
    validity_score: float = 0.0
    timeliness_score: float = 0.0
    uniqueness_score: float = 0.0
    overall_score: float = 0.0
    status: DataQualityStatus = DataQualityStatus.UNKNOWN
    assessment_timestamp: datetime = field(default_factory=datetime.now)
    issues_detected: List[str] = field(default_factory=list)

@dataclass
class DatasetSchema:
    """Represents dataset schema information"""
    schema_id: str
    schema_version: str
    columns: List[Dict[str, Any]]
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Dict[str, str]] = field(default_factory=list)
    indexes: List[str] = field(default_factory=list)
    constraints: List[Dict[str, Any]] = field(default_factory=list)
    creation_timestamp: datetime = field(default_factory=datetime.now)
    last_modified: datetime = field(default_factory=datetime.now)

@dataclass
class EnhancedDataLineage:
    """Enhanced data lineage with comprehensive tracking"""
    dataset_id: str
    dataset_name: str
    dataset_type: str  # table, file, stream, api, etc.
    source_system: str
    physical_location: str
    schema: DatasetSchema
    
    # Lineage relationships
    upstream_datasets: List[str] = field(default_factory=list)
    downstream_datasets: List[str] = field(default_factory=list)
    
    # Transformation tracking
    transformations: List[str] = field(default_factory=list)
    transformation_code: Optional[str] = None
    transformation_engine: Optional[str] = None
    
    # Quality and governance
    quality_metrics: DataQualityMetrics = field(default_factory=DataQualityMetrics)
    data_classification: str = "unclassified"
    sensitivity_level: str = "public"
    retention_policy: Optional[str] = None
    compliance_tags: List[str] = field(default_factory=list)
    
    # Operational metadata
    creation_time: datetime = field(default_factory=datetime.now)
    last_modified: datetime = field(default_factory=datetime.now)
    last_accessed: Optional[datetime] = None
    access_frequency: int = 0
    data_volume_bytes: int = 0
    record_count: int = 0
    
    # Business context
    business_owner: Optional[str] = None
    technical_owner: Optional[str] = None
    business_description: Optional[str] = None
    business_terms: List[str] = field(default_factory=list)
    
    # Events and history
    lineage_events: List[LineageEvent] = field(default_factory=list)

class AdvancedLineageTracker:
    """Advanced data lineage tracking system"""
    
    def __init__(self, organization_name: str):
        self.organization_name = organization_name
        self.lineage_catalog: Dict[str, EnhancedDataLineage] = {}
        self.lineage_graph: Dict[str, Set[str]] = {}  # dataset_id -> set of downstream datasets
        self.reverse_lineage_graph: Dict[str, Set[str]] = {}  # dataset_id -> set of upstream datasets
        self.event_history: List[LineageEvent] = []
        
        print(f"✓ Initialized Advanced Lineage Tracker for: {organization_name}")
    
    def register_dataset(self, lineage: EnhancedDataLineage) -> bool:
        """Register a new dataset in the lineage catalog"""
        
        try:
            # Validate dataset
            if lineage.dataset_id in self.lineage_catalog:
                print(f"⚠️ Dataset {lineage.dataset_id} already exists, updating...")
            
            # Register dataset
            self.lineage_catalog[lineage.dataset_id] = lineage
            
            # Initialize graph entries
            if lineage.dataset_id not in self.lineage_graph:
                self.lineage_graph[lineage.dataset_id] = set()
            if lineage.dataset_id not in self.reverse_lineage_graph:
                self.reverse_lineage_graph[lineage.dataset_id] = set()
            
            # Update lineage relationships
            for upstream_id in lineage.upstream_datasets:
                self.add_lineage_relationship(upstream_id, lineage.dataset_id)
            
            # Create registration event
            registration_event = LineageEvent(
                event_type=LineageEventType.DATA_CREATION,
                target_dataset_id=lineage.dataset_id,
                transformation_details={
                    "action": "dataset_registration",
                    "source_system": lineage.source_system,
                    "dataset_type": lineage.dataset_type
                },
                system_id="lineage_tracker"
            )
            
            self.record_lineage_event(registration_event)
            lineage.lineage_events.append(registration_event)
            
            print(f"✓ Registered dataset: {lineage.dataset_name} ({lineage.dataset_id})")
            return True
            
        except Exception as e:
            print(f"❌ Error registering dataset {lineage.dataset_id}: {str(e)}")
            return False
    
    def add_lineage_relationship(self, upstream_dataset_id: str, downstream_dataset_id: str) -> bool:
        """Add a lineage relationship between two datasets"""
        
        try:
            # Update forward lineage graph
            if upstream_dataset_id not in self.lineage_graph:
                self.lineage_graph[upstream_dataset_id] = set()
            self.lineage_graph[upstream_dataset_id].add(downstream_dataset_id)
            
            # Update reverse lineage graph
            if downstream_dataset_id not in self.reverse_lineage_graph:
                self.reverse_lineage_graph[downstream_dataset_id] = set()
            self.reverse_lineage_graph[downstream_dataset_id].add(upstream_dataset_id)
            
            # Update dataset lineage lists if datasets exist in catalog
            if upstream_dataset_id in self.lineage_catalog:
                if downstream_dataset_id not in self.lineage_catalog[upstream_dataset_id].downstream_datasets:
                    self.lineage_catalog[upstream_dataset_id].downstream_datasets.append(downstream_dataset_id)
            
            if downstream_dataset_id in self.lineage_catalog:
                if upstream_dataset_id not in self.lineage_catalog[downstream_dataset_id].upstream_datasets:
                    self.lineage_catalog[downstream_dataset_id].upstream_datasets.append(upstream_dataset_id)
            
            print(f"✓ Added lineage relationship: {upstream_dataset_id} -> {downstream_dataset_id}")
            return True
            
        except Exception as e:
            print(f"❌ Error adding lineage relationship: {str(e)}")
            return False
    
    def record_lineage_event(self, event: LineageEvent) -> bool:
        """Record a lineage event"""
        
        try:
            self.event_history.append(event)
            
            # Update dataset last_modified timestamp
            if event.target_dataset_id and event.target_dataset_id in self.lineage_catalog:
                self.lineage_catalog[event.target_dataset_id].last_modified = event.timestamp
            
            if event.source_dataset_id and event.source_dataset_id in self.lineage_catalog:
                self.lineage_catalog[event.source_dataset_id].last_accessed = event.timestamp
                self.lineage_catalog[event.source_dataset_id].access_frequency += 1
            
            print(f"✓ Recorded lineage event: {event.event_type.value} ({event.event_id})")
            return True
            
        except Exception as e:
            print(f"❌ Error recording lineage event: {str(e)}")
            return False
    
    def trace_upstream_lineage(self, dataset_id: str, max_depth: int = 10) -> Dict[str, Any]:
        """Trace upstream lineage for a dataset"""
        
        lineage_trace = {
            "root_dataset_id": dataset_id,
            "max_depth": max_depth,
            "upstream_datasets": [],
            "transformation_chain": [],
            "total_upstream_count": 0,
            "trace_timestamp": datetime.now().isoformat()
        }
        
        try:
            if dataset_id not in self.lineage_catalog:
                raise ValueError(f"Dataset {dataset_id} not found in lineage catalog")
            
            visited = set()
            current_depth = 0
            queue = [(dataset_id, current_depth)]
            
            while queue and current_depth < max_depth:
                current_dataset_id, depth = queue.pop(0)
                
                if current_dataset_id in visited:
                    continue
                
                visited.add(current_dataset_id)
                current_depth = max(current_depth, depth)
                
                # Get upstream datasets
                upstream_ids = self.reverse_lineage_graph.get(current_dataset_id, set())
                
                for upstream_id in upstream_ids:
                    if upstream_id not in visited and upstream_id in self.lineage_catalog:
                        upstream_lineage = self.lineage_catalog[upstream_id]
                        
                        upstream_info = {
                            "dataset_id": upstream_id,
                            "dataset_name": upstream_lineage.dataset_name,
                            "dataset_type": upstream_lineage.dataset_type,
                            "source_system": upstream_lineage.source_system,
                            "depth": depth + 1,
                            "transformations": upstream_lineage.transformations,
                            "quality_score": upstream_lineage.quality_metrics.overall_score,
                            "last_modified": upstream_lineage.last_modified.isoformat()
                        }
                        
                        lineage_trace["upstream_datasets"].append(upstream_info)
                        lineage_trace["transformation_chain"].extend(upstream_lineage.transformations)
                        
                        # Add to queue for further traversal
                        queue.append((upstream_id, depth + 1))
            
            # Remove duplicates from transformation chain
            lineage_trace["transformation_chain"] = list(set(lineage_trace["transformation_chain"]))
            lineage_trace["total_upstream_count"] = len(lineage_trace["upstream_datasets"])
            
            print(f"✓ Traced upstream lineage for {dataset_id}")
            print(f"  Found {lineage_trace['total_upstream_count']} upstream datasets")
            print(f"  Total transformations: {len(lineage_trace['transformation_chain'])}")
            
            return lineage_trace
            
        except Exception as e:
            print(f"❌ Error tracing upstream lineage: {str(e)}")
            lineage_trace["error"] = str(e)
            return lineage_trace
    
    def trace_downstream_lineage(self, dataset_id: str, max_depth: int = 10) -> Dict[str, Any]:
        """Trace downstream lineage for a dataset"""
        
        lineage_trace = {
            "root_dataset_id": dataset_id,
            "max_depth": max_depth,
            "downstream_datasets": [],
            "impacted_systems": set(),
            "total_downstream_count": 0,
            "trace_timestamp": datetime.now().isoformat()
        }
        
        try:
            if dataset_id not in self.lineage_catalog:
                raise ValueError(f"Dataset {dataset_id} not found in lineage catalog")
            
            visited = set()
            current_depth = 0
            queue = [(dataset_id, current_depth)]
            
            while queue and current_depth < max_depth:
                current_dataset_id, depth = queue.pop(0)
                
                if current_dataset_id in visited:
                    continue
                
                visited.add(current_dataset_id)
                current_depth = max(current_depth, depth)
                
                # Get downstream datasets
                downstream_ids = self.lineage_graph.get(current_dataset_id, set())
                
                for downstream_id in downstream_ids:
                    if downstream_id not in visited and downstream_id in self.lineage_catalog:
                        downstream_lineage = self.lineage_catalog[downstream_id]
                        
                        downstream_info = {
                            "dataset_id": downstream_id,
                            "dataset_name": downstream_lineage.dataset_name,
                            "dataset_type": downstream_lineage.dataset_type,
                            "source_system": downstream_lineage.source_system,
                            "depth": depth + 1,
                            "business_owner": downstream_lineage.business_owner,
                            "sensitivity_level": downstream_lineage.sensitivity_level,
                            "last_accessed": downstream_lineage.last_accessed.isoformat() if downstream_lineage.last_accessed else None
                        }
                        
                        lineage_trace["downstream_datasets"].append(downstream_info)
                        lineage_trace["impacted_systems"].add(downstream_lineage.source_system)
                        
                        # Add to queue for further traversal
                        queue.append((downstream_id, depth + 1))
            
            lineage_trace["impacted_systems"] = list(lineage_trace["impacted_systems"])
            lineage_trace["total_downstream_count"] = len(lineage_trace["downstream_datasets"])
            
            print(f"✓ Traced downstream lineage for {dataset_id}")
            print(f"  Found {lineage_trace['total_downstream_count']} downstream datasets")
            print(f"  Impacted systems: {len(lineage_trace['impacted_systems'])}")
            
            return lineage_trace
            
        except Exception as e:
            print(f"❌ Error tracing downstream lineage: {str(e)}")
            lineage_trace["error"] = str(e)
            return lineage_trace
    
    def perform_impact_analysis(self, dataset_id: str, change_type: str = "schema_change") -> Dict[str, Any]:
        """Perform impact analysis for changes to a dataset"""
        
        impact_analysis = {
            "dataset_id": dataset_id,
            "change_type": change_type,
            "analysis_timestamp": datetime.now().isoformat(),
            "immediate_impact": [],
            "downstream_impact": [],
            "risk_assessment": {},
            "recommendations": []
        }
        
        try:
            if dataset_id not in self.lineage_catalog:
                raise ValueError(f"Dataset {dataset_id} not found in lineage catalog")
            
            # Get immediate downstream datasets
            immediate_downstream = self.lineage_graph.get(dataset_id, set())
            
            for downstream_id in immediate_downstream:
                if downstream_id in self.lineage_catalog:
                    downstream_dataset = self.lineage_catalog[downstream_id]
                    
                    impact_info = {
                        "dataset_id": downstream_id,
                        "dataset_name": downstream_dataset.dataset_name,
                        "impact_severity": self._assess_impact_severity(dataset_id, downstream_id, change_type),
                        "business_owner": downstream_dataset.business_owner,
                        "technical_owner": downstream_dataset.technical_owner,
                        "last_accessed": downstream_dataset.last_accessed.isoformat() if downstream_dataset.last_accessed else None,
                        "access_frequency": downstream_dataset.access_frequency
                    }
                    
                    impact_analysis["immediate_impact"].append(impact_info)
            
            # Get full downstream lineage
            downstream_trace = self.trace_downstream_lineage(dataset_id, max_depth=5)
            impact_analysis["downstream_impact"] = downstream_trace["downstream_datasets"]
            
            # Risk assessment
            high_risk_count = sum(1 for impact in impact_analysis["immediate_impact"] if impact["impact_severity"] == "high")
            medium_risk_count = sum(1 for impact in impact_analysis["immediate_impact"] if impact["impact_severity"] == "medium")
            
            impact_analysis["risk_assessment"] = {
                "overall_risk_level": "high" if high_risk_count > 0 else ("medium" if medium_risk_count > 0 else "low"),
                "total_impacted_datasets": len(impact_analysis["downstream_impact"]),
                "high_risk_datasets": high_risk_count,
                "medium_risk_datasets": medium_risk_count,
                "impacted_systems": list(set(ds["source_system"] for ds in impact_analysis["downstream_impact"]))
            }
            
            # Generate recommendations
            recommendations = []
            
            if high_risk_count > 0:
                recommendations.append("Schedule change during maintenance window due to high-risk impacts")
                recommendations.append("Notify business owners of high-risk datasets before implementation")
            
            if len(impact_analysis["downstream_impact"]) > 10:
                recommendations.append("Consider phased rollout approach for large-scale impact")
            
            if change_type == "schema_change":
                recommendations.append("Update data contracts and API documentation")
                recommendations.append("Validate downstream transformations and queries")
            
            impact_analysis["recommendations"] = recommendations
            
            print(f"✓ Impact analysis completed for {dataset_id}")
            print(f"  Overall risk level: {impact_analysis['risk_assessment']['overall_risk_level']}")
            print(f"  Total impacted datasets: {impact_analysis['risk_assessment']['total_impacted_datasets']}")
            
            return impact_analysis
            
        except Exception as e:
            print(f"❌ Error performing impact analysis: {str(e)}")
            impact_analysis["error"] = str(e)
            return impact_analysis
    
    def _assess_impact_severity(self, source_dataset_id: str, target_dataset_id: str, change_type: str) -> str:
        """Assess the severity of impact between two datasets"""
        
        try:
            if target_dataset_id not in self.lineage_catalog:
                return "unknown"
            
            target_dataset = self.lineage_catalog[target_dataset_id]
            
            # Factors that increase impact severity
            severity_score = 0
            
            # High access frequency indicates critical dataset
            if target_dataset.access_frequency > 100:
                severity_score += 3
            elif target_dataset.access_frequency > 50:
                severity_score += 2
            elif target_dataset.access_frequency > 10:
                severity_score += 1
            
            # Sensitive data increases impact
            if target_dataset.sensitivity_level in ["confidential", "restricted"]:
                severity_score += 2
            elif target_dataset.sensitivity_level == "internal":
                severity_score += 1
            
            # Recent access indicates active use
            if target_dataset.last_accessed and (datetime.now() - target_dataset.last_accessed).days < 7:
                severity_score += 2
            elif target_dataset.last_accessed and (datetime.now() - target_dataset.last_accessed).days < 30:
                severity_score += 1
            
            # Schema changes are more impactful
            if change_type in ["schema_change", "data_deletion"]:
                severity_score += 2
            elif change_type in ["data_transformation", "data_movement"]:
                severity_score += 1
            
            # Convert score to severity level
            if severity_score >= 6:
                return "high"
            elif severity_score >= 3:
                return "medium"
            else:
                return "low"
                
        except Exception as e:
            print(f"⚠️ Error assessing impact severity: {str(e)}")
            return "unknown"
    
    def generate_lineage_report(self, dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate comprehensive lineage report"""
        
        report = {
            "report_metadata": {
                "organization": self.organization_name,
                "generation_timestamp": datetime.now().isoformat(),
                "report_scope": "single_dataset" if dataset_id else "full_catalog",
                "total_datasets": len(self.lineage_catalog),
                "total_events": len(self.event_history)
            },
            "dataset_summary": {},
            "lineage_analysis": {},
            "quality_overview": {},
            "governance_metrics": {},
            "recommendations": []
        }
        
        try:
            datasets_to_analyze = [dataset_id] if dataset_id else list(self.lineage_catalog.keys())
            
            # Dataset summary
            dataset_types = {}
            source_systems = {}
            sensitivity_levels = {}
            
            for ds_id in datasets_to_analyze:
                if ds_id in self.lineage_catalog:
                    dataset = self.lineage_catalog[ds_id]
                    
                    # Count by type
                    dataset_types[dataset.dataset_type] = dataset_types.get(dataset.dataset_type, 0) + 1
                    
                    # Count by source system
                    source_systems[dataset.source_system] = source_systems.get(dataset.source_system, 0) + 1
                    
                    # Count by sensitivity level
                    sensitivity_levels[dataset.sensitivity_level] = sensitivity_levels.get(dataset.sensitivity_level, 0) + 1
            
            report["dataset_summary"] = {
                "total_analyzed": len(datasets_to_analyze),
                "dataset_types": dataset_types,
                "source_systems": source_systems,
                "sensitivity_levels": sensitivity_levels
            }
            
            # Lineage analysis
            total_relationships = sum(len(downstream) for downstream in self.lineage_graph.values())
            orphaned_datasets = [ds_id for ds_id in datasets_to_analyze 
                               if ds_id in self.lineage_catalog and 
                               len(self.lineage_catalog[ds_id].upstream_datasets) == 0 and 
                               len(self.lineage_catalog[ds_id].downstream_datasets) == 0]
            
            report["lineage_analysis"] = {
                "total_relationships": total_relationships,
                "orphaned_datasets": len(orphaned_datasets),
                "average_upstream_dependencies": sum(len(self.reverse_lineage_graph.get(ds_id, set())) 
                                                   for ds_id in datasets_to_analyze) / max(len(datasets_to_analyze), 1),
                "average_downstream_dependencies": sum(len(self.lineage_graph.get(ds_id, set())) 
                                                     for ds_id in datasets_to_analyze) / max(len(datasets_to_analyze), 1)
            }
            
            # Quality overview
            quality_scores = [self.lineage_catalog[ds_id].quality_metrics.overall_score 
                            for ds_id in datasets_to_analyze 
                            if ds_id in self.lineage_catalog and self.lineage_catalog[ds_id].quality_metrics.overall_score > 0]
            
            if quality_scores:
                report["quality_overview"] = {
                    "average_quality_score": sum(quality_scores) / len(quality_scores),
                    "datasets_with_quality_metrics": len(quality_scores),
                    "high_quality_datasets": len([score for score in quality_scores if score >= 0.8]),
                    "poor_quality_datasets": len([score for score in quality_scores if score < 0.5])
                }
            
            # Governance metrics
            datasets_with_owners = sum(1 for ds_id in datasets_to_analyze 
                                     if ds_id in self.lineage_catalog and 
                                     (self.lineage_catalog[ds_id].business_owner or self.lineage_catalog[ds_id].technical_owner))
            
            datasets_with_classification = sum(1 for ds_id in datasets_to_analyze 
                                             if ds_id in self.lineage_catalog and 
                                             self.lineage_catalog[ds_id].data_classification != "unclassified")
            
            report["governance_metrics"] = {
                "datasets_with_owners": datasets_with_owners,
                "ownership_percentage": (datasets_with_owners / max(len(datasets_to_analyze), 1)) * 100,
                "datasets_with_classification": datasets_with_classification,
                "classification_percentage": (datasets_with_classification / max(len(datasets_to_analyze), 1)) * 100
            }
            
            # Recommendations
            recommendations = []
            
            if len(orphaned_datasets) > 0:
                recommendations.append(f"Review {len(orphaned_datasets)} orphaned datasets for potential cleanup or integration")
            
            if report["governance_metrics"]["ownership_percentage"] < 80:
                recommendations.append("Improve data ownership assignment - currently below 80%")
            
            if report["quality_overview"].get("poor_quality_datasets", 0) > 0:
                recommendations.append(f"Address data quality issues in {report['quality_overview']['poor_quality_datasets']} datasets")
            
            if total_relationships == 0:
                recommendations.append("Establish data lineage relationships to enable better governance and impact analysis")
            
            report["recommendations"] = recommendations
            
            print(f"✓ Generated lineage report for {report['report_metadata']['report_scope']}")
            return report
            
        except Exception as e:
            print(f"❌ Error generating lineage report: {str(e)}")
            report["error"] = str(e)
            return report

# Example usage and demonstration
def create_comprehensive_lineage_example():
    """Create a comprehensive data lineage example"""
    
    print("=== Creating Comprehensive Data Lineage Example ===")
    
    # Initialize lineage tracker
    lineage_tracker = AdvancedLineageTracker("Contoso Corporation")
    
    # Create sample schemas
    customer_schema = DatasetSchema(
        schema_id="customer_schema_v1",
        schema_version="1.0",
        columns=[
            {"name": "customer_id", "type": "int", "nullable": False, "description": "Unique customer identifier"},
            {"name": "first_name", "type": "varchar(50)", "nullable": False, "description": "Customer first name"},
            {"name": "last_name", "type": "varchar(50)", "nullable": False, "description": "Customer last name"},
            {"name": "email", "type": "varchar(255)", "nullable": False, "description": "Customer email address"},
            {"name": "registration_date", "type": "datetime", "nullable": False, "description": "Customer registration date"},
            {"name": "last_login", "type": "datetime", "nullable": True, "description": "Last login timestamp"}
        ],
        primary_keys=["customer_id"],
        constraints=[
            {"type": "unique", "columns": ["email"]},
            {"type": "check", "expression": "email LIKE '%@%'"}
        ]
    )
    
    # Create sample datasets with enhanced lineage
    datasets = [
        EnhancedDataLineage(
            dataset_id="crm_customers_raw",
            dataset_name="CRM Raw Customer Data",
            dataset_type="table",
            source_system="CRM_Database",
            physical_location="azure-sql://crm-db.database.windows.net/customers",
            schema=customer_schema,
            data_classification="internal",
            sensitivity_level="internal",
            business_owner="sales-team@contoso.com",
            technical_owner="data-engineering@contoso.com",
            business_description="Raw customer data from CRM system including registration and contact information",
            business_terms=["customer", "contact", "registration"],
            compliance_tags=["GDPR", "PII"],
            data_volume_bytes=1024*1024*100,  # 100MB
            record_count=50000,
            quality_metrics=DataQualityMetrics(
                completeness_score=0.95,
                accuracy_score=0.88,
                consistency_score=0.92,
                validity_score=0.90,
                timeliness_score=0.85,
                uniqueness_score=0.98,
                overall_score=0.91,
                status=DataQualityStatus.GOOD,
                issues_detected=["Some missing last_login values", "Email format validation needed"]
            )
        ),
        EnhancedDataLineage(
            dataset_id="customers_validated",
            dataset_name="Validated Customer Data",
            dataset_type="table",
            source_system="Data_Pipeline",
            physical_location="azure-storage://datalake.dfs.core.windows.net/validated/customers/",
            schema=customer_schema,
            upstream_datasets=["crm_customers_raw"],
            transformations=["email_validation", "data_cleansing", "deduplication"],
            transformation_code="SELECT DISTINCT * FROM crm_customers WHERE email LIKE '%@%.%'",
            transformation_engine="Azure_Data_Factory",
            data_classification="internal",
            sensitivity_level="internal",
            business_owner="sales-team@contoso.com",
            technical_owner="data-engineering@contoso.com",
            business_description="Validated and cleansed customer data ready for analytics",
            business_terms=["validated_customer", "clean_data"],
            compliance_tags=["GDPR", "PII"],
            data_volume_bytes=1024*1024*95,  # 95MB (some records removed)
            record_count=48500,
            quality_metrics=DataQualityMetrics(
                completeness_score=0.98,
                accuracy_score=0.95,
                consistency_score=0.97,
                validity_score=0.96,
                timeliness_score=0.90,
                uniqueness_score=1.0,
                overall_score=0.96,
                status=DataQualityStatus.EXCELLENT,
                issues_detected=[]
            )
        ),
        EnhancedDataLineage(
            dataset_id="customer_analytics",
            dataset_name="Customer Analytics Dataset",
            dataset_type="table",
            source_system="Analytics_Platform",
            physical_location="azure-synapse://analytics-workspace.sql.azuresynapse.net/customer_analytics",
            schema=customer_schema,
            upstream_datasets=["customers_validated"],
            transformations=["feature_engineering", "segmentation", "scoring"],
            transformation_code="CREATE TABLE customer_analytics AS SELECT *, DATEDIFF(day, registration_date, GETDATE()) as days_since_registration FROM customers_validated",
            transformation_engine="Azure_Synapse",
            data_classification="internal",
            sensitivity_level="internal",
            business_owner="marketing-team@contoso.com",
            technical_owner="data-science@contoso.com",
            business_description="Customer analytics dataset with derived features for marketing campaigns",
            business_terms=["customer_analytics", "segmentation", "marketing"],
            compliance_tags=["GDPR"],
            data_volume_bytes=1024*1024*120,  # 120MB (additional features)
            record_count=48500,
            access_frequency=150,
            last_accessed=datetime.now() - timedelta(hours=2),
            quality_metrics=DataQualityMetrics(
                completeness_score=1.0,
                accuracy_score=0.93,
                consistency_score=0.95,
                validity_score=0.94,
                timeliness_score=0.88,
                uniqueness_score=1.0,
                overall_score=0.95,
                status=DataQualityStatus.EXCELLENT
            )
        )
    ]
    
    # Register datasets
    for dataset in datasets:
        lineage_tracker.register_dataset(dataset)
    
    # Create some lineage events
    events = [
        LineageEvent(
            event_type=LineageEventType.DATA_TRANSFORMATION,
            source_dataset_id="crm_customers_raw",
            target_dataset_id="customers_validated",
            transformation_details={
                "transformation_type": "data_validation",
                "rules_applied": ["email_format_check", "deduplication", "null_value_handling"],
                "records_processed": 50000,
                "records_output": 48500,
                "quality_improvement": 0.05
            },
            user_id="data-engineer@contoso.com",
            system_id="Azure_Data_Factory"
        ),
        LineageEvent(
            event_type=LineageEventType.DATA_TRANSFORMATION,
            source_dataset_id="customers_validated",
            target_dataset_id="customer_analytics",
            transformation_details={
                "transformation_type": "feature_engineering",
                "features_added": ["days_since_registration", "customer_segment", "lifetime_value_score"],
                "ml_models_applied": ["customer_segmentation_model_v2"],
                "processing_time_seconds": 45
            },
            user_id="data-scientist@contoso.com",
            system_id="Azure_Synapse"
        ),
        LineageEvent(
            event_type=LineageEventType.QUALITY_CHECK,
            target_dataset_id="customer_analytics",
            transformation_details={
                "quality_check_type": "automated_validation",
                "checks_passed": 15,
                "checks_failed": 0,
                "overall_score": 0.95
            },
            system_id="Data_Quality_Monitor"
        )
    ]
    
    # Record events
    for event in events:
        lineage_tracker.record_lineage_event(event)
    
    return lineage_tracker

# Demonstration function
def demonstrate_advanced_lineage():
    """Demonstrate advanced lineage tracking capabilities"""
    
    print("=== Advanced Data Lineage Demonstration ===")
    
    try:
        # Create comprehensive lineage example
        lineage_tracker = create_comprehensive_lineage_example()
        
        # Trace upstream lineage
        print("\n🔍 Tracing Upstream Lineage...")
        upstream_trace = lineage_tracker.trace_upstream_lineage("customer_analytics")
        
        print(f"✓ Upstream lineage for customer_analytics:")
        print(f"  Found {upstream_trace['total_upstream_count']} upstream datasets")
        for dataset in upstream_trace['upstream_datasets']:
            print(f"    - {dataset['dataset_name']} (Quality: {dataset['quality_score']:.2f})")
        
        # Trace downstream lineage
        print("\n🔍 Tracing Downstream Lineage...")
        downstream_trace = lineage_tracker.trace_downstream_lineage("crm_customers_raw")
        
        print(f"✓ Downstream lineage for crm_customers_raw:")
        print(f"  Found {downstream_trace['total_downstream_count']} downstream datasets")
        
        # Perform impact analysis
        print("\n📊 Performing Impact Analysis...")
        impact_analysis = lineage_tracker.perform_impact_analysis("crm_customers_raw", "schema_change")
        
        print(f"✓ Impact analysis for schema change:")
        print(f"  Overall risk level: {impact_analysis['risk_assessment']['overall_risk_level']}")
        print(f"  Total impacted datasets: {impact_analysis['risk_assessment']['total_impacted_datasets']}")
        print(f"  Recommendations: {len(impact_analysis['recommendations'])}")
        
        # Generate comprehensive report
        print("\n📋 Generating Lineage Report...")
        lineage_report = lineage_tracker.generate_lineage_report()
        
        print(f"✓ Lineage report generated:")
        print(f"  Total datasets analyzed: {lineage_report['dataset_summary']['total_analyzed']}")
        print(f"  Average quality score: {lineage_report['quality_overview']['average_quality_score']:.2f}")
        print(f"  Ownership percentage: {lineage_report['governance_metrics']['ownership_percentage']:.1f}%")
        
        return {
            "lineage_tracker": lineage_tracker,
            "upstream_trace": upstream_trace,
            "downstream_trace": downstream_trace,
            "impact_analysis": impact_analysis,
            "lineage_report": lineage_report
        }
        
    except Exception as e:
        print(f"❌ Error in advanced lineage demonstration: {str(e)}")
        return {"error": str(e)}

# Run demonstration
# advanced_lineage_demo = demonstrate_advanced_lineage()
```

This comprehensive guide provides everything needed to understand, implement, and optimize DAG and Data Lineage capabilities in Azure environments, ensuring effective data governance, compliance, and impact analysis across all Azure data services.