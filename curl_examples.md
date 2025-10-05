# Nuvyn Executor Script - Curl Examples

This document provides curl examples for interacting with the Nuvyn Executor Script API server.

## Starting the API Server

```bash
# Start the API server
python executor/api_server.py --host 0.0.0.0 --port 8080

# Or with custom log level
python executor/api_server.py --host 0.0.0.0 --port 8080 --log-level DEBUG
```

## API Endpoints

### 1. Health Check

```bash
curl -X GET http://localhost:8080/health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "version": "1.0.0",
  "service": "nuvyn-executor-api"
}
```

### 2. API Information

```bash
curl -X GET http://localhost:8080/info
```

**Response:**
```json
{
  "service": "nuvyn-executor-api",
  "version": "1.0.0",
  "description": "Job-based metadata extraction and processing system",
  "endpoints": {
    "health": "GET /health",
    "create_job": "POST /jobs/create",
    "execute_job": "POST /jobs/{job_id}/execute",
    "get_status": "GET /jobs/{job_id}/status",
    "get_result": "GET /jobs/{job_id}/result",
    "cancel_job": "DELETE /jobs/{job_id}/cancel",
    "list_jobs": "GET /jobs",
    "job_stats": "GET /jobs/stats"
  },
  "job_types": [
    "metadata_extraction",
    "schema_validation", 
    "data_reading",
    "quality_assessment",
    "api_transmission",
    "full_pipeline"
  ]
}
```

## Job Management

### 3. Create a New Job

```bash
curl -X POST http://localhost:8080/jobs/create \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "metadata_extraction",
    "data_source_path": "/path/to/data",
    "data_source_type": "csv",
    "tenant_id": "client_123",
    "job_metadata": {
      "description": "Extract metadata from CSV files",
      "priority": "high"
    }
  }'
```

**Response:**
```json
{
  "job_id": "job_abc123def456",
  "status": "created",
  "message": "Job created successfully"
}
```

### 4. Execute a Job

```bash
curl -X POST http://localhost:8080/jobs/job_abc123def456/execute
```

**Response:**
```json
{
  "job_id": "job_abc123def456",
  "status": "completed",
  "success": true,
  "execution_time": 45.67,
  "error": null,
  "result_data": {
    "files_processed": 5,
    "total_rows": 10000,
    "quality_score": 85
  },
  "metadata": {
    "job_type": "metadata_extraction",
    "data_source_path": "/path/to/data",
    "tenant_id": "client_123"
  }
}
```

### 5. Get Job Status

```bash
curl -X GET http://localhost:8080/jobs/job_abc123def456/status
```

**Response:**
```json
{
  "job_id": "job_abc123def456",
  "status": "completed",
  "result": {
    "files_processed": 5,
    "total_rows": 10000,
    "quality_score": 85
  },
  "execution_time": 45.67,
  "error": null,
  "metadata": {
    "job_type": "metadata_extraction",
    "started_at": "2024-01-15T10:30:00Z",
    "completed_at": "2024-01-15T10:30:45Z"
  }
}
```

### 6. Get Job Result

```bash
curl -X GET http://localhost:8080/jobs/job_abc123def456/result
```

**Response:**
```json
{
  "job_id": "job_abc123def456",
  "status": "completed",
  "result_data": {
    "files_processed": 5,
    "total_rows": 10000,
    "quality_score": 85,
    "schema_info": {
      "tables": 5,
      "columns": 25
    }
  },
  "execution_time": 45.67,
  "error": null,
  "metadata": {
    "job_type": "metadata_extraction",
    "started_at": "2024-01-15T10:30:00Z",
    "completed_at": "2024-01-15T10:30:45Z"
  }
}
```

### 7. Cancel a Job

```bash
curl -X DELETE http://localhost:8080/jobs/job_abc123def456/cancel
```

**Response:**
```json
{
  "job_id": "job_abc123def456",
  "cancelled": true,
  "status": "cancelled"
}
```

### 8. List Jobs

```bash
# List all jobs
curl -X GET http://localhost:8080/jobs

# List jobs by status
curl -X GET "http://localhost:8080/jobs?status=completed"

# List jobs by tenant
curl -X GET "http://localhost:8080/jobs?tenant_id=client_123"

# List jobs with multiple filters
curl -X GET "http://localhost:8080/jobs?status=completed&tenant_id=client_123"
```

**Response:**
```json
{
  "total_jobs": 3,
  "jobs": [
    {
      "job_id": "job_abc123def456",
      "status": "completed",
      "job_type": "metadata_extraction",
      "data_source_path": "/path/to/data",
      "tenant_id": "client_123",
      "created_at": "2024-01-15T10:30:00Z",
      "execution_time": 45.67
    }
  ],
  "filters": {
    "status": "completed",
    "tenant_id": "client_123"
  }
}
```

### 9. Get Job Statistics

```bash
curl -X GET http://localhost:8080/jobs/stats
```

**Response:**
```json
{
  "total_jobs": 25,
  "active_jobs": 2,
  "completed_jobs": 20,
  "failed_jobs": 3,
  "success_rate": 80.0,
  "average_execution_time_seconds": 42.5,
  "max_concurrent_jobs": 5
}
```

## Data Source Management

### 10. Test Data Source Connection

```bash
# Test Azure Blob Storage
curl -X POST http://localhost:8080/datasources/test \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "azure_blob",
    "credentials": {
      "connection_string": "DefaultEndpointsProtocol=https;AccountName=...",
      "account_name": "mystorageaccount",
      "account_key": "your-account-key"
    },
    "path": "https://mystorageaccount.blob.core.windows.net/container/path"
  }'
```

```bash
# Test AWS S3
curl -X POST http://localhost:8080/datasources/test \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "aws_s3",
    "credentials": {
      "access_key_id": "AKIA...",
      "secret_access_key": "your-secret-key",
      "region": "us-east-1"
    },
    "path": "s3://my-bucket/path/to/data"
  }'
```

```bash
# Test Database
curl -X POST http://localhost:8080/datasources/test \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "database",
    "credentials": {
      "host": "localhost",
      "port": "3306",
      "username": "user",
      "password": "password",
      "database": "mydb",
      "type": "mysql"
    },
    "path": "mysql://localhost:3306/mydb"
  }'
```

**Response:**
```json
{
  "success": true,
  "source_type": "azure_blob",
  "connection_status": "connected"
}
```

### 11. Get Supported Data Source Types

```bash
curl -X GET http://localhost:8080/datasources/types
```

**Response:**
```json
{
  "supported_types": ["azure_blob", "aws_s3", "database"],
  "type_info": {
    "azure_blob": {
      "source_type": "azure_blob",
      "class_name": "AzureBlobDataSource",
      "required_credentials": [
        "connection_string (or account_name + account_key, or sas_token)"
      ]
    },
    "aws_s3": {
      "source_type": "aws_s3", 
      "class_name": "AWSS3DataSource",
      "required_credentials": [
        "access_key_id",
        "secret_access_key",
        "region (optional, defaults to us-east-1)"
      ]
    }
  }
}
```

## Schema Management

### 12. Validate Schema

```bash
curl -X POST http://localhost:8080/schema/validate?tenant_id=client_123
```

**Response:**
```json
{
  "schema_name": "_executor_metadata",
  "validation_status": "valid",
  "tables_found": ["sources", "tables", "columns", "executor_runs", "logs"],
  "tables_missing": [],
  "tables_invalid": [],
  "validation_details": {
    "sources": {
      "exists": true,
      "structure_valid": true
    }
  }
}
```

### 13. Create Schema

```bash
curl -X POST http://localhost:8080/schema/create?tenant_id=client_123
```

**Response:**
```json
{
  "schema_name": "_executor_metadata",
  "creation_status": "success",
  "tables_created": ["sources", "tables", "columns", "executor_runs", "logs"],
  "tables_failed": [],
  "creation_details": {
    "sources": {
      "success": true
    }
  }
}
```

## Utility Endpoints

### 14. Ping Test

```bash
curl -X POST http://localhost:8080/ping \
  -H "Content-Type: application/json" \
  -d '{
    "test": "data",
    "timestamp": "2024-01-15T10:30:00Z"
  }'
```

**Response:**
```json
{
  "pong": true,
  "timestamp": "2024-01-15T10:30:00Z",
  "received_data": {
    "test": "data",
    "timestamp": "2024-01-15T10:30:00Z"
  },
  "message": "Pong! API is responding"
}
```

## Complete Workflow Example

Here's a complete workflow example:

```bash
#!/bin/bash

# 1. Check if API server is running
curl -X GET http://localhost:8080/health

# 2. Create a metadata extraction job
JOB_ID=$(curl -s -X POST http://localhost:8080/jobs/create \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "metadata_extraction",
    "data_source_path": "/path/to/csv/files",
    "data_source_type": "csv",
    "tenant_id": "client_123"
  }' | jq -r '.job_id')

echo "Created job: $JOB_ID"

# 3. Execute the job
curl -X POST http://localhost:8080/jobs/$JOB_ID/execute

# 4. Wait and check status
sleep 5
curl -X GET http://localhost:8080/jobs/$JOB_ID/status

# 5. Get final results
curl -X GET http://localhost:8080/jobs/$JOB_ID/result

# 6. Check job statistics
curl -X GET http://localhost:8080/jobs/stats
```

## Error Handling

All endpoints return appropriate HTTP status codes:

- `200 OK` - Successful request
- `400 Bad Request` - Invalid request data
- `404 Not Found` - Resource not found
- `500 Internal Server Error` - Server error

Error responses include:

```json
{
  "error": "Error message description",
  "status": "error",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

## Environment Variables

Set these environment variables for configuration:

```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export NUVYN_API_ENDPOINT="https://your-backend-api.com"
export NUVYN_API_KEY="your-api-key"
export EXECUTOR_LOG_LEVEL="INFO"
export EXECUTOR_VERBOSE="true"
```
