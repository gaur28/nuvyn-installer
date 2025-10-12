# Nuvyn Executor Script - Backend Integration Guide

## 📋 Table of Contents
1. [Overview](#overview)
2. [Installation Flow](#installation-flow)
3. [Job Types and Usage](#job-types-and-usage)
4. [Databricks Jobs API Integration](#databricks-jobs-api-integration)
5. [Working Flow Explanation](#working-flow-explanation)
6. [Error Handling](#error-handling)
7. [Monitoring and Logging](#monitoring-and-logging)
8. [Security Considerations](#security-considerations)

---

## 📖 Overview

The Nuvyn Executor Script is a job-based metadata extraction and processing system designed to run in client Databricks environments. It acts as a bridge between the client's data sources and your platform.

### Key Features
- ✅ Job-based execution (only runs specific tasks, not entire script)
- ✅ Schema management for `_executor_metadata` 
- ✅ Metadata extraction from multiple data sources
- ✅ Data quality assessment
- ✅ REST API integration for sending results back to your platform
- ✅ Comprehensive audit logging

### Architecture
```
Your Backend → Databricks Jobs API → Executor Script → Client Data Sources
                                            ↓
                                    _executor_metadata Schema
                                            ↓
                                    REST API → Your Platform
```

---

## 🚀 Installation Flow

### Step 1: Install the Executor Script in Client's Databricks

Use the Databricks Commands API to install the package:

```bash
curl -X POST \
  https://<workspace-url>/api/2.0/commands/execute \
  -H "Authorization: Bearer <databricks-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "language": "python",
    "clusterId": "<cluster-id>",
    "command": "%pip install git+https://github.com/nuvyn-bldr/executor-script.git@main"
  }'
```

**What This Does:**
- Downloads the executor script from GitHub repository
- Installs all required dependencies
- Makes the `nuvyn-executor` command available
- Installs the `executor` Python module

**Response:**
```json
{
  "id": "command-execution-id",
  "status": "Queued"
}
```

### Step 2: Verify Installation

Check if the command executed successfully:

```bash
curl -X GET \
  https://<workspace-url>/api/2.0/commands/status \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "clusterId": "<cluster-id>",
    "commandId": "<command-execution-id>"
  }'
```

**Response:**
```json
{
  "id": "command-execution-id",
  "status": "Finished",
  "results": {
    "resultType": "text",
    "data": "Successfully installed nuvyn-executor-script-1.0.0"
  }
}
```

---

## 📋 Job Types and Usage

The executor script supports 6 job types:

### 1. **Schema Validation** (`schema_validation`)
**Purpose:** Validates that the `_executor_metadata` schema exists and has the correct structure.

**Usage:**
```bash
nuvyn-executor schema_validation
```

**Parameters:**
- No data source path required
- Automatically uses tenant's default schema

**What It Does:**
- Checks if `_executor_metadata` schema exists
- Validates 5 required tables: `sources`, `tables`, `columns`, `executor_runs`, `logs`
- Creates missing tables if needed
- Returns validation status

**Expected Output:**
```json
{
  "schema_name": "_executor_metadata",
  "validation_status": "valid",
  "tables_found": ["sources", "tables", "columns", "executor_runs", "logs"],
  "tables_missing": [],
  "recommendations": []
}
```

---

### 2. **Metadata Extraction** (`metadata_extraction`)
**Purpose:** Extracts metadata from client data sources.

**Usage:**
```bash
nuvyn-executor metadata_extraction <data_source_path> [source_type] [tenant_id]
```

**Parameters:**
- `data_source_path` (required): Path to data source (e.g., `/dbfs/mnt/data/`, `/Volumes/catalog/schema/table/`)
- `source_type` (optional): Type of data source (`csv`, `parquet`, `azure_blob`, `aws_s3`, `auto`)
- `tenant_id` (optional): Tenant identifier (default: `default`)

**What It Does:**
- Connects to the specified data source
- Lists all files/tables in the path
- Analyzes file structure, schema, and data types
- Collects metadata attributes (columns, types, row counts, etc.)
- Stores metadata in `_executor_metadata` schema
- Calculates quality metrics

**Expected Output:**
```json
{
  "source_path": "/dbfs/mnt/data/",
  "source_type": "azure_blob",
  "files_found": 5,
  "total_size_bytes": 1048576,
  "schema_info": {
    "tables": 5,
    "columns": 25
  },
  "quality_metrics": {
    "overall_score": 85,
    "completeness": 90
  }
}
```

---

### 3. **Data Reading** (`data_reading`)
**Purpose:** Reads actual data from client data sources.

**Usage:**
```bash
nuvyn-executor data_reading <data_source_path> [source_type] [tenant_id]
```

**Parameters:**
- `data_source_path` (required): Path to data source
- `source_type` (optional): Type of data source
- `tenant_id` (optional): Tenant identifier

**What It Does:**
- Connects to the data source
- Reads sample data from files
- Extracts data preview and statistics
- Returns file information and sample data

**Expected Output:**
```json
{
  "source_path": "/dbfs/mnt/data/",
  "files_found": 3,
  "sample_data": {
    "file1.csv": {
      "size": 1024,
      "preview": "column1,column2,column3..."
    }
  },
  "connection_status": "success"
}
```

---

### 4. **Quality Assessment** (`quality_assessment`)
**Purpose:** Assesses data quality metrics.

**Usage:**
```bash
nuvyn-executor quality_assessment <data_source_path> [source_type] [tenant_id]
```

**Parameters:**
- `data_source_path` (required): Path to data source
- `source_type` (optional): Type of data source
- `tenant_id` (optional): Tenant identifier

**What It Does:**
- Analyzes data quality across multiple dimensions
- Calculates completeness, accuracy, consistency scores
- Generates quality improvement recommendations
- Provides quality level classification

**Expected Output:**
```json
{
  "source_path": "/dbfs/mnt/data/",
  "overall_score": 87.86,
  "quality_level": "Good",
  "quality_metrics": {
    "completeness": 90,
    "accuracy": 85,
    "consistency": 80,
    "timeliness": 95,
    "validity": 88,
    "uniqueness": 92
  },
  "recommendations": [
    "Data quality is good - maintain current standards"
  ]
}
```

---

### 5. **API Transmission** (`api_transmission`)
**Purpose:** Sends collected metadata to your platform's API.

**Usage:**
```bash
nuvyn-executor api_transmission
```

**Parameters:**
- No specific parameters required
- Uses environment variables for API endpoint and credentials

**What It Does:**
- Retrieves stored metadata from `_executor_metadata` schema
- Formats data for transmission
- Sends data to your platform via REST API
- Returns transmission status

**Expected Output:**
```json
{
  "transmission_status": "success",
  "api_response": {
    "status": "received",
    "record_count": 100
  },
  "status_code": 200
}
```

---

### 6. **Full Pipeline** (`full_pipeline`)
**Purpose:** Executes the complete pipeline (all job types in sequence).

**Usage:**
```bash
nuvyn-executor full_pipeline <data_source_path> [source_type] [tenant_id]
```

**Parameters:**
- `data_source_path` (required): Path to data source
- `source_type` (optional): Type of data source
- `tenant_id` (optional): Tenant identifier

**What It Does:**
1. Validates/creates schema
2. Extracts metadata
3. Reads data samples
4. Assesses quality
5. Transmits results to your platform

**Expected Output:**
```json
{
  "schema_validation": { "status": "valid" },
  "metadata_extraction": { "files_found": 5 },
  "data_reading": { "files_read": 3 },
  "quality_assessment": { "overall_score": 85 },
  "api_transmission": { "status": "success" }
}
```

---

## 🔧 Databricks Jobs API Integration

### Installation via Databricks Commands API

**Step 1: Execute Installation Command**

```bash
curl -X POST \
  https://<workspace-url>/api/2.0/commands/execute \
  -H "Authorization: Bearer <databricks-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "language": "python",
    "clusterId": "<cluster-id>",
    "command": "%pip install git+https://github.com/nuvyn-bldr/executor-script.git@main"
  }'
```

**Response:**
```json
{
  "id": "cmd-12345-67890"
}
```

**Step 2: Check Installation Status**

```bash
curl -X GET \
  "https://<workspace-url>/api/2.0/commands/status?clusterId=<cluster-id>&commandId=cmd-12345-67890" \
  -H "Authorization: Bearer <databricks-token>"
```

**Response:**
```json
{
  "id": "cmd-12345-67890",
  "status": "Finished",
  "results": {
    "resultType": "text",
    "data": "Successfully installed nuvyn-executor-script-1.0.0"
  }
}
```

---

### Creating Databricks Jobs

#### **Job 1: Schema Validation Job**

```bash
curl -X POST \
  https://<workspace-url>/api/2.1/jobs/create \
  -H "Authorization: Bearer <databricks-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Nuvyn-Schema-Validation",
    "existing_cluster_id": "<cluster-id>",
    "spark_python_task": {
      "python_file": "dbfs:/path/to/executor/main.py",
      "parameters": ["schema_validation"]
    },
    "timeout_seconds": 1800,
    "max_concurrent_runs": 1,
    "retry_on_timeout": false,
    "max_retries": 1
  }'
```

**Response:**
```json
{
  "job_id": 12345
}
```

**What This Does:**
- Creates a Databricks job named "Nuvyn-Schema-Validation"
- Configures it to run on the specified cluster
- Sets the job to execute `schema_validation` task
- Sets 30-minute timeout
- Limits to 1 concurrent run

---

#### **Job 2: Metadata Extraction Job**

```bash
curl -X POST \
  https://<workspace-url>/api/2.1/jobs/create \
  -H "Authorization: Bearer <databricks-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Nuvyn-Metadata-Extraction",
    "existing_cluster_id": "<cluster-id>",
    "spark_python_task": {
      "python_file": "dbfs:/path/to/executor/main.py",
      "parameters": [
        "metadata_extraction",
        "/dbfs/mnt/client-data/",
        "csv",
        "client-123"
      ]
    },
    "timeout_seconds": 3600,
    "max_concurrent_runs": 1
  }'
```

**Response:**
```json
{
  "job_id": 12346
}
```

**What This Does:**
- Creates metadata extraction job
- Targets `/dbfs/mnt/client-data/` directory
- Expects CSV files
- Associates with tenant `client-123`
- Sets 60-minute timeout

---

#### **Job 3: Full Pipeline Job**

```bash
curl -X POST \
  https://<workspace-url>/api/2.1/jobs/create \
  -H "Authorization: Bearer <databricks-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Nuvyn-Full-Pipeline",
    "existing_cluster_id": "<cluster-id>",
    "spark_python_task": {
      "python_file": "dbfs:/path/to/executor/main.py",
      "parameters": [
        "full_pipeline",
        "/Volumes/main/default/sales_data/",
        "parquet",
        "production-tenant"
      ]
    },
    "timeout_seconds": 7200,
    "max_concurrent_runs": 1,
    "email_notifications": {
      "on_success": ["success@nuvyn.com"],
      "on_failure": ["alerts@nuvyn.com"]
    }
  }'
```

**Response:**
```json
{
  "job_id": 12347
}
```

**What This Does:**
- Creates full pipeline job
- Processes Unity Catalog volume data
- Handles parquet files
- Sets 2-hour timeout
- Configures email notifications for job completion/failure

---

### Triggering Job Execution

#### **Method 1: Run Job Now**

```bash
curl -X POST \
  https://<workspace-url>/api/2.1/jobs/run-now \
  -H "Authorization: Bearer <databricks-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": 12345
  }'
```

**Response:**
```json
{
  "run_id": 98765,
  "number_in_job": 1
}
```

**What This Does:**
- Triggers immediate execution of the specified job
- Returns a `run_id` for tracking
- Job executes with the parameters defined during job creation

---

#### **Method 2: Run Job with Override Parameters**

```bash
curl -X POST \
  https://<workspace-url>/api/2.1/jobs/run-now \
  -H "Authorization: Bearer <databricks-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": 12346,
    "python_params": [
      "metadata_extraction",
      "/Volumes/production/analytics/customer_data/",
      "parquet",
      "prod-tenant-456"
    ]
  }'
```

**Response:**
```json
{
  "run_id": 98766,
  "number_in_job": 2
}
```

**What This Does:**
- Triggers job execution with custom parameters
- Overrides the default parameters set during job creation
- Useful for running the same job with different data sources

---

### Monitoring Job Execution

#### **Get Job Run Status**

```bash
curl -X GET \
  https://<workspace-url>/api/2.1/jobs/runs/get \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "run_id": 98765
  }'
```

**Response:**
```json
{
  "run_id": 98765,
  "run_name": "Nuvyn-Schema-Validation",
  "state": {
    "life_cycle_state": "TERMINATED",
    "result_state": "SUCCESS",
    "state_message": ""
  },
  "start_time": 1609459200000,
  "end_time": 1609459260000,
  "setup_duration": 5000,
  "execution_duration": 55000,
  "cleanup_duration": 5000
}
```

**What This Does:**
- Retrieves detailed status of a specific job run
- Shows lifecycle state (PENDING, RUNNING, TERMINATED)
- Shows result state (SUCCESS, FAILED, CANCELLED)
- Provides execution duration metrics

**Life Cycle States:**
- `PENDING` - Job is queued
- `RUNNING` - Job is currently executing
- `TERMINATED` - Job has completed (check result_state for success/failure)
- `SKIPPED` - Job was skipped
- `INTERNAL_ERROR` - Internal error occurred

**Result States:**
- `SUCCESS` - Job completed successfully (exit code 0)
- `FAILED` - Job failed (exit code non-zero)
- `CANCELLED` - Job was cancelled by user
- `TIMEDOUT` - Job exceeded timeout limit

---

#### **Get Job Run Output**

```bash
curl -X GET \
  https://<workspace-url>/api/2.1/jobs/runs/get-output \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "run_id": 98765
  }'
```

**Response:**
```json
{
  "metadata": {
    "job_id": 12345,
    "run_id": 98765,
    "number_in_job": 1,
    "state": {
      "life_cycle_state": "TERMINATED",
      "result_state": "SUCCESS"
    },
    "task": {
      "spark_python_task": {
        "python_file": "dbfs:/path/to/executor/main.py",
        "parameters": ["schema_validation"]
      }
    }
  },
  "notebook_output": {
    "result": "Schema validation completed successfully",
    "truncated": false
  },
  "logs": "✅ Schema validation successful\n✅ All tables validated\n✅ Schema ready for use"
}
```

**What This Does:**
- Retrieves the output and logs from the job run
- Shows the actual results from the executor script
- Provides logs for debugging and audit

---

#### **List All Job Runs**

```bash
curl -X GET \
  https://<workspace-url>/api/2.1/jobs/runs/list \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "job_id": 12345,
    "active_only": false,
    "completed_only": false,
    "offset": 0,
    "limit": 25
  }'
```

**Response:**
```json
{
  "runs": [
    {
      "run_id": 98765,
      "run_name": "Nuvyn-Schema-Validation",
      "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "SUCCESS"
      },
      "start_time": 1609459200000,
      "end_time": 1609459260000
    },
    {
      "run_id": 98764,
      "run_name": "Nuvyn-Schema-Validation",
      "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "SUCCESS"
      },
      "start_time": 1609458000000,
      "end_time": 1609458055000
    }
  ],
  "has_more": false
}
```

**What This Does:**
- Lists all runs for a specific job
- Supports pagination (offset and limit)
- Can filter by active_only or completed_only
- Shows historical execution data

---

## 🔄 Working Flow Explanation

### Complete Workflow: From Installation to Results

```
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: Your Backend Initiates Installation                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    curl POST /api/2.0/commands/execute
    → pip install executor-script
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 2: Create Databricks Job                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    curl POST /api/2.1/jobs/create
    → Job created with ID: 12345
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 3: Trigger Job Execution                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    curl POST /api/2.1/jobs/run-now
    → Run ID: 98765 (Status: RUNNING)
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 4: Executor Script Runs in Databricks                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    a. Parse job parameters (job_type, data_source_path, etc.)
    b. Initialize ConfigManager and JobManager
    c. Create job instance with unique job_id
    d. Execute specific job type:
       - Schema Validation → Validates _executor_metadata schema
       - Metadata Extraction → Connects to data source, extracts metadata
       - Data Reading → Reads actual data samples
       - Quality Assessment → Calculates quality metrics
       - API Transmission → Sends results to your platform
    e. Store results in _executor_metadata schema
    f. Return execution results
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 5: Monitor Job Progress                                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    curl GET /api/2.1/jobs/runs/get?run_id=98765
    → Status: TERMINATED, Result: SUCCESS
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 6: Retrieve Job Results                                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    curl GET /api/2.1/jobs/runs/get-output?run_id=98765
    → Returns execution logs and results
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 7: Process Results in Your Backend                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Detailed Job Execution Flow

### Schema Validation Job Flow

```
1. Job Triggered
   └→ parse_parameters("schema_validation")
   
2. Initialize Components
   └→ ConfigManager (loads environment variables)
   └→ JobManager (manages job lifecycle)
   
3. Create Job Instance
   └→ job_id = "job_abc123def456"
   └→ job_type = JobType.SCHEMA_VALIDATION
   └→ Save job config in memory
   
4. Execute Job
   └→ Import SchemaValidator
   └→ Check if in Databricks environment
   └→ Validate schema existence
   └→ Check 5 required tables:
      ├→ sources (data source registry)
      ├→ tables (table metadata)
      ├→ columns (column metadata)
      ├→ executor_runs (execution audit trail)
      └→ logs (detailed logs)
   └→ Return validation result
   
5. Update Job Status
   └→ Status: COMPLETED
   └→ Store results in job_results
   
6. Return Results
   └→ Exit code: 0 (success)
   └→ Logs written to Databricks job output
```

---

### Metadata Extraction Job Flow

```
1. Job Triggered
   └→ parse_parameters("metadata_extraction", "/dbfs/mnt/data/", "csv", "tenant123")
   
2. Initialize Components
   └→ ConfigManager (loads credentials)
   └→ JobManager
   └→ DataSourceFactory
   
3. Create Job Instance
   └→ job_id = "job_xyz789abc123"
   └→ job_type = JobType.METADATA_EXTRACTION
   └→ data_source_path = "/dbfs/mnt/data/"
   └→ source_type = "csv"
   └→ tenant_id = "tenant123"
   
4. Execute Job
   └→ Import MetadataExtractor
   └→ Get data source credentials from environment
   └→ Auto-detect connector (Azure Blob, S3, Database, etc.)
   └→ Connect to data source
   └→ List files in the path
   └→ For each file:
      ├→ Get file size
      ├→ Read sample data (first 2KB)
      ├→ Detect file type (csv, parquet, excel, json)
      ├→ Extract schema information
      └→ Calculate quality metrics
   └→ Store metadata in _executor_metadata.sources
   └→ Store table info in _executor_metadata.tables
   └→ Store column info in _executor_metadata.columns
   └→ Log execution in _executor_metadata.executor_runs
   └→ Return extraction results
   
5. Update Job Status
   └→ Status: COMPLETED
   └→ Execution time: 45.67s
   
6. Return Results
   └→ Files analyzed: 5
   └→ Total size: 10.5 MB
   └→ Quality score: 85/100
   └→ Exit code: 0
```

---

### API Transmission Job Flow

```
1. Job Triggered
   └→ parse_parameters("api_transmission")
   
2. Initialize Components
   └→ ConfigManager (loads API credentials)
   └→ APIClient
   
3. Prepare Payload
   └→ Retrieve metadata from _executor_metadata schema
   └→ Format as JSON
   └→ Add authentication headers
   
4. Execute Transmission
   └→ POST https://your-platform-api/api/metadata
   └→ Headers:
      ├→ Content-Type: application/json
      └→ Authorization: Bearer <api-key>
   └→ Body:
      ├→ job_id
      ├→ tenant_id
      ├→ metadata (sources, tables, columns)
      └→ quality_metrics
   
5. Handle Response
   └→ Success (200): Store confirmation
   └→ Failure (4xx/5xx): Log error and retry
   
6. Return Results
   └→ transmission_status: "success"
   └→ api_response: {...}
   └→ Exit code: 0
```

---

## 🛡️ Error Handling

### Common Errors and Solutions

#### **Error 1: Installation Failed**

**Curl Command:**
```bash
curl -X GET \
  "https://<workspace-url>/api/2.0/commands/status?clusterId=<cluster-id>&commandId=<cmd-id>" \
  -H "Authorization: Bearer <databricks-token>"
```

**Error Response:**
```json
{
  "status": "Error",
  "results": {
    "resultType": "error",
    "data": "ERROR: Could not find a version that satisfies the requirement..."
  }
}
```

**Solution:**
- Check cluster has internet access
- Verify GitHub repository URL is correct
- Ensure cluster Python version >= 3.8

---

#### **Error 2: Job Execution Failed**

**Curl Command:**
```bash
curl -X GET \
  https://<workspace-url>/api/2.1/jobs/runs/get \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "run_id": 98765
  }'
```

**Error Response:**
```json
{
  "state": {
    "life_cycle_state": "TERMINATED",
    "result_state": "FAILED",
    "state_message": "Job failed with exit code 1"
  }
}
```

**Solution:**
- Check job output logs for detailed error
- Verify data source path is accessible
- Check environment variables are set correctly

**Get Detailed Logs:**
```bash
curl -X GET \
  https://<workspace-url>/api/2.1/jobs/runs/get-output \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "run_id": 98765
  }'
```

---

#### **Error 3: Timeout**

**Error Response:**
```json
{
  "state": {
    "life_cycle_state": "TERMINATED",
    "result_state": "TIMEDOUT"
  }
}
```

**Solution:**
- Increase timeout_seconds in job creation
- Optimize data source for faster access
- Consider splitting large jobs into smaller tasks

**Update Job Timeout:**
```bash
curl -X POST \
  https://<workspace-url>/api/2.1/jobs/update \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "job_id": 12345,
    "new_settings": {
      "timeout_seconds": 7200
    }
  }'
```

---

## 🔐 Security Considerations

### Environment Variables Setup

Set these in Databricks cluster environment or use Databricks Secrets:

```bash
# Required for executor functionality
export DATABRICKS_WORKSPACE_URL="https://your-workspace.databricks.com"
export NUVYN_API_ENDPOINT="https://your-platform-api.com"
export NUVYN_API_KEY="your-secure-api-key"

# Optional for specific data sources
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
```

### Using Databricks Secrets (Recommended)

**Create Secret Scope:**
```bash
curl -X POST \
  https://<workspace-url>/api/2.0/secrets/scopes/create \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "scope": "nuvyn-executor",
    "initial_manage_principal": "users"
  }'
```

**Add Secrets:**
```bash
curl -X POST \
  https://<workspace-url>/api/2.0/secrets/put \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "scope": "nuvyn-executor",
    "key": "api-key",
    "string_value": "your-secure-api-key"
  }'
```

**Use in Job:**
```python
# In Databricks job
import os
os.environ['NUVYN_API_KEY'] = dbutils.secrets.get(scope="nuvyn-executor", key="api-key")
```

---

## 📈 Monitoring and Logging

### Job Execution Logs

All executor jobs write logs that can be accessed via:

```bash
curl -X GET \
  https://<workspace-url>/api/2.1/jobs/runs/get-output \
  -H "Authorization: Bearer <databricks-token>" \
  -d '{
    "run_id": 98765
  }'
```

**Log Format:**
```
08:45:25 - nuvyn_executor.job_manager - INFO - ℹ️ 🚀 Starting job execution: job_abc123
08:45:25 - nuvyn_executor.schema.validator - INFO - ℹ️ 🔍 Validating schema: _executor_metadata
08:45:25 - nuvyn_executor.schema.validator - INFO - ℹ️ ✅ Schema validation successful
```

**Log Levels:**
- `INFO` - Normal execution information
- `WARNING` - Non-critical issues (e.g., skipped operations)
- `ERROR` - Execution failures
- `DEBUG` - Detailed debugging information (set EXECUTOR_LOG_LEVEL=DEBUG)

---

### Audit Trail in _executor_metadata

All job executions are logged in the client's Databricks:

**Query Execution History:**
```sql
SELECT 
    run_id,
    executor_version,
    run_mode,
    status,
    started_at,
    finished_at,
    error_message
FROM _executor_metadata.executor_runs
WHERE status = 'Success'
ORDER BY started_at DESC
LIMIT 10;
```

**Query Logs:**
```sql
SELECT 
    l.log_timestamp,
    l.log_level,
    l.log_message,
    r.run_mode
FROM _executor_metadata.logs l
JOIN _executor_metadata.executor_runs r ON l.run_id = r.run_id
WHERE r.run_id = 'job_abc123'
ORDER BY l.log_timestamp DESC;
```

---

## 🎯 Complete Backend Integration Example

### Scenario: Client Onboarding and Metadata Extraction

```bash
#!/bin/bash

# Configuration
WORKSPACE_URL="https://client-workspace.databricks.com"
DATABRICKS_TOKEN="dapi1234567890abcdef"
CLUSTER_ID="0123-456789-abcdef01"
DATA_SOURCE_PATH="/dbfs/mnt/client-data/"
TENANT_ID="client-abc-123"

echo "🚀 Starting Client Onboarding..."

# Step 1: Install Executor Script
echo "📦 Step 1: Installing executor script..."
INSTALL_CMD_ID=$(curl -s -X POST \
  "${WORKSPACE_URL}/api/2.0/commands/execute" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"language\": \"python\",
    \"clusterId\": \"${CLUSTER_ID}\",
    \"command\": \"%pip install git+https://github.com/nuvyn-bldr/executor-script.git@main\"
  }" | jq -r '.id')

echo "   Command ID: $INSTALL_CMD_ID"

# Wait for installation
sleep 30

# Check installation status
INSTALL_STATUS=$(curl -s -X GET \
  "${WORKSPACE_URL}/api/2.0/commands/status?clusterId=${CLUSTER_ID}&commandId=${INSTALL_CMD_ID}" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" | jq -r '.status')

echo "   Installation status: $INSTALL_STATUS"

if [ "$INSTALL_STATUS" != "Finished" ]; then
    echo "❌ Installation failed!"
    exit 1
fi

echo "✅ Step 1 Complete: Executor installed"

# Step 2: Create Schema Validation Job
echo "📋 Step 2: Creating schema validation job..."
SCHEMA_JOB_ID=$(curl -s -X POST \
  "${WORKSPACE_URL}/api/2.1/jobs/create" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"Nuvyn-Schema-Validation-${TENANT_ID}\",
    \"existing_cluster_id\": \"${CLUSTER_ID}\",
    \"spark_python_task\": {
      \"python_file\": \"dbfs:/executor/main.py\",
      \"parameters\": [\"schema_validation\"]
    },
    \"timeout_seconds\": 1800
  }" | jq -r '.job_id')

echo "   Schema Job ID: $SCHEMA_JOB_ID"

# Step 3: Run Schema Validation
echo "🔧 Step 3: Running schema validation..."
SCHEMA_RUN_ID=$(curl -s -X POST \
  "${WORKSPACE_URL}/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"job_id\": ${SCHEMA_JOB_ID}
  }" | jq -r '.run_id')

echo "   Run ID: $SCHEMA_RUN_ID"

# Wait for completion
sleep 10

# Check status
SCHEMA_STATUS=$(curl -s -X GET \
  "${WORKSPACE_URL}/api/2.1/jobs/runs/get" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -d "{
    \"run_id\": ${SCHEMA_RUN_ID}
  }" | jq -r '.state.result_state')

echo "   Schema validation status: $SCHEMA_STATUS"

if [ "$SCHEMA_STATUS" != "SUCCESS" ]; then
    echo "❌ Schema validation failed!"
    exit 1
fi

echo "✅ Step 3 Complete: Schema validated"

# Step 4: Create Metadata Extraction Job
echo "📊 Step 4: Creating metadata extraction job..."
METADATA_JOB_ID=$(curl -s -X POST \
  "${WORKSPACE_URL}/api/2.1/jobs/create" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"Nuvyn-Metadata-Extraction-${TENANT_ID}\",
    \"existing_cluster_id\": \"${CLUSTER_ID}\",
    \"spark_python_task\": {
      \"python_file\": \"dbfs:/executor/main.py\",
      \"parameters\": [
        \"metadata_extraction\",
        \"${DATA_SOURCE_PATH}\",
        \"csv\",
        \"${TENANT_ID}\"
      ]
    },
    \"timeout_seconds\": 3600
  }" | jq -r '.job_id')

echo "   Metadata Job ID: $METADATA_JOB_ID"

# Step 5: Run Metadata Extraction
echo "🔍 Step 5: Running metadata extraction..."
METADATA_RUN_ID=$(curl -s -X POST \
  "${WORKSPACE_URL}/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"job_id\": ${METADATA_JOB_ID}
  }" | jq -r '.run_id')

echo "   Run ID: $METADATA_RUN_ID"

# Monitor progress
echo "⏳ Monitoring job progress..."
while true; do
    STATUS=$(curl -s -X GET \
      "${WORKSPACE_URL}/api/2.1/jobs/runs/get" \
      -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
      -d "{
        \"run_id\": ${METADATA_RUN_ID}
      }" | jq -r '.state.life_cycle_state')
    
    echo "   Current status: $STATUS"
    
    if [ "$STATUS" == "TERMINATED" ]; then
        break
    fi
    
    sleep 5
done

# Get final result
METADATA_RESULT=$(curl -s -X GET \
  "${WORKSPACE_URL}/api/2.1/jobs/runs/get" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -d "{
    \"run_id\": ${METADATA_RUN_ID}
  }" | jq -r '.state.result_state')

echo "   Metadata extraction result: $METADATA_RESULT"

if [ "$METADATA_RESULT" != "SUCCESS" ]; then
    echo "❌ Metadata extraction failed!"
    
    # Get error details
    curl -X GET \
      "${WORKSPACE_URL}/api/2.1/jobs/runs/get-output" \
      -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
      -d "{
        \"run_id\": ${METADATA_RUN_ID}
      }" | jq '.logs'
    
    exit 1
fi

echo "✅ Step 5 Complete: Metadata extracted"

# Step 6: Get Results
echo "📄 Step 6: Retrieving results..."
curl -s -X GET \
  "${WORKSPACE_URL}/api/2.1/jobs/runs/get-output" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -d "{
    \"run_id\": ${METADATA_RUN_ID}
  }" | jq '.'

echo ""
echo "🎉 Client onboarding completed successfully!"
echo "   Tenant ID: ${TENANT_ID}"
echo "   Data Source: ${DATA_SOURCE_PATH}"
echo "   Metadata Job ID: ${METADATA_JOB_ID}"
echo "   Schema Job ID: ${SCHEMA_JOB_ID}"
```

---

## 🔄 Recommended Integration Pattern

### Your Backend Service Implementation

```python
# backend/services/databricks_executor_service.py

import requests
import time
import json
from typing import Dict, Any, Optional

class DatabricksExecutorService:
    """Service for managing Databricks executor jobs"""
    
    def __init__(self, workspace_url: str, token: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def install_executor(self, cluster_id: str) -> Dict[str, Any]:
        """Install executor script on client cluster"""
        url = f"{self.workspace_url}/api/2.0/commands/execute"
        payload = {
            "language": "python",
            "clusterId": cluster_id,
            "command": "%pip install git+https://github.com/nuvyn-bldr/executor-script.git@main"
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        return response.json()
    
    def create_job(self, 
                   name: str,
                   cluster_id: str, 
                   job_type: str,
                   data_source_path: str = None,
                   source_type: str = "auto",
                   tenant_id: str = "default") -> int:
        """Create a new executor job"""
        url = f"{self.workspace_url}/api/2.1/jobs/create"
        
        parameters = [job_type]
        if data_source_path:
            parameters.extend([data_source_path, source_type, tenant_id])
        
        payload = {
            "name": name,
            "existing_cluster_id": cluster_id,
            "spark_python_task": {
                "python_file": "dbfs:/executor/main.py",
                "parameters": parameters
            },
            "timeout_seconds": 3600,
            "max_concurrent_runs": 1
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        return response.json()['job_id']
    
    def run_job(self, job_id: int, override_params: list = None) -> int:
        """Trigger job execution"""
        url = f"{self.workspace_url}/api/2.1/jobs/run-now"
        
        payload = {"job_id": job_id}
        if override_params:
            payload["python_params"] = override_params
        
        response = requests.post(url, headers=self.headers, json=payload)
        return response.json()['run_id']
    
    def get_job_status(self, run_id: int) -> Dict[str, Any]:
        """Get job run status"""
        url = f"{self.workspace_url}/api/2.1/jobs/runs/get"
        payload = {"run_id": run_id}
        
        response = requests.get(url, headers=self.headers, json=payload)
        return response.json()
    
    def wait_for_completion(self, run_id: int, timeout: int = 3600) -> str:
        """Wait for job to complete and return result"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self.get_job_status(run_id)
            life_cycle_state = status['state']['life_cycle_state']
            
            if life_cycle_state == 'TERMINATED':
                return status['state']['result_state']
            
            time.sleep(5)
        
        return 'TIMEOUT'
    
    def get_job_output(self, run_id: int) -> Dict[str, Any]:
        """Get job execution output and logs"""
        url = f"{self.workspace_url}/api/2.1/jobs/runs/get-output"
        payload = {"run_id": run_id}
        
        response = requests.get(url, headers=self.headers, json=payload)
        return response.json()


# Usage Example
def onboard_client(client_config: Dict[str, Any]):
    """Complete client onboarding workflow"""
    
    service = DatabricksExecutorService(
        workspace_url=client_config['workspace_url'],
        token=client_config['databricks_token']
    )
    
    # 1. Install executor
    install_result = service.install_executor(client_config['cluster_id'])
    print(f"Installation started: {install_result['id']}")
    
    # 2. Create schema validation job
    schema_job_id = service.create_job(
        name=f"Schema-Validation-{client_config['tenant_id']}",
        cluster_id=client_config['cluster_id'],
        job_type="schema_validation",
        tenant_id=client_config['tenant_id']
    )
    print(f"Schema job created: {schema_job_id}")
    
    # 3. Run schema validation
    schema_run_id = service.run_job(schema_job_id)
    print(f"Schema validation running: {schema_run_id}")
    
    # 4. Wait for completion
    schema_result = service.wait_for_completion(schema_run_id)
    print(f"Schema validation result: {schema_result}")
    
    if schema_result != 'SUCCESS':
        raise Exception(f"Schema validation failed: {schema_result}")
    
    # 5. Create metadata extraction job
    metadata_job_id = service.create_job(
        name=f"Metadata-Extraction-{client_config['tenant_id']}",
        cluster_id=client_config['cluster_id'],
        job_type="metadata_extraction",
        data_source_path=client_config['data_source_path'],
        source_type=client_config.get('source_type', 'auto'),
        tenant_id=client_config['tenant_id']
    )
    print(f"Metadata job created: {metadata_job_id}")
    
    # 6. Run metadata extraction
    metadata_run_id = service.run_job(metadata_job_id)
    print(f"Metadata extraction running: {metadata_run_id}")
    
    # 7. Monitor progress
    metadata_result = service.wait_for_completion(metadata_run_id, timeout=7200)
    print(f"Metadata extraction result: {metadata_result}")
    
    if metadata_result != 'SUCCESS':
        # Get error details
        output = service.get_job_output(metadata_run_id)
        raise Exception(f"Metadata extraction failed: {output.get('logs', 'Unknown error')}")
    
    # 8. Get results
    output = service.get_job_output(metadata_run_id)
    print(f"Metadata extraction completed successfully!")
    print(f"Results: {json.dumps(output, indent=2)}")
    
    return {
        "schema_job_id": schema_job_id,
        "metadata_job_id": metadata_job_id,
        "metadata_run_id": metadata_run_id,
        "status": "success"
    }
```

---

## 📝 Quick Reference Card

### Installation Command
```bash
curl -X POST https://<workspace>/api/2.0/commands/execute \
  -H "Authorization: Bearer <token>" \
  -d '{"language":"python","clusterId":"<id>","command":"%pip install git+https://github.com/nuvyn-bldr/executor-script.git@main"}'
```

### Create Job Command
```bash
curl -X POST https://<workspace>/api/2.1/jobs/create \
  -H "Authorization: Bearer <token>" \
  -d '{"name":"Nuvyn-Job","existing_cluster_id":"<id>","spark_python_task":{"python_file":"dbfs:/executor/main.py","parameters":["<job_type>","<path>"]}}'
```

### Run Job Command
```bash
curl -X POST https://<workspace>/api/2.1/jobs/run-now \
  -H "Authorization: Bearer <token>" \
  -d '{"job_id":<id>}'
```

### Check Status Command
```bash
curl -X GET https://<workspace>/api/2.1/jobs/runs/get \
  -H "Authorization: Bearer <token>" \
  -d '{"run_id":<id>}'
```

### Get Output Command
```bash
curl -X GET https://<workspace>/api/2.1/jobs/runs/get-output \
  -H "Authorization: Bearer <token>" \
  -d '{"run_id":<id>}'
```

---

## 🎓 Best Practices

1. **Always validate schema first** before running metadata extraction
2. **Use appropriate timeouts** based on data source size
3. **Monitor job execution** and handle failures gracefully
4. **Use Databricks Secrets** for sensitive credentials
5. **Implement retry logic** for transient failures
6. **Log all operations** for audit trail
7. **Set up alerts** for job failures
8. **Clean up old jobs** periodically to avoid clutter

---

## 📞 Support and Troubleshooting

For issues or questions:
- Check Databricks job logs for detailed error messages
- Review `_executor_metadata.executor_runs` table for execution history
- Check `_executor_metadata.logs` table for detailed logs
- Verify environment variables are set correctly
- Ensure data source paths are accessible from Databricks cluster

---

**Document Version:** 1.0.0  
**Last Updated:** October 2025  
**Executor Script Version:** 1.0.0
