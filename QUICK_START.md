# Nuvyn Executor Script - Quick Start Guide

## 🚀 Get Started in 3 Steps

### 1. Start the API Server

```bash
# Make the script executable (if not already)
chmod +x start_server.sh

# Start the server
./start_server.sh
```

The server will start on `http://localhost:8080` by default.

### 2. Test the API

```bash
# Check if server is running
curl -X GET http://localhost:8080/health

# Get API information
curl -X GET http://localhost:8080/info
```

### 3. Run Your First Job

```bash
# Create and execute a metadata extraction job
curl -X POST http://localhost:8080/jobs/create \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "metadata_extraction",
    "data_source_path": "/path/to/your/data",
    "data_source_type": "csv",
    "tenant_id": "client_123"
  }'

# The response will include a job_id, then execute it:
curl -X POST http://localhost:8080/jobs/YOUR_JOB_ID/execute

# Check the result:
curl -X GET http://localhost:8080/jobs/YOUR_JOB_ID/result
```

## 📋 Available Job Types

- `metadata_extraction` - Extract metadata from data sources
- `schema_validation` - Validate/create executor metadata schema
- `data_reading` - Read actual data from sources
- `quality_assessment` - Assess data quality
- `api_transmission` - Send results to backend API
- `full_pipeline` - Execute complete pipeline

## 🔧 Configuration

Set environment variables for your setup:

```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export NUVYN_API_ENDPOINT="https://your-backend-api.com"
export NUVYN_API_KEY="your-api-key"
export EXECUTOR_LOG_LEVEL="INFO"
```

## 📚 More Examples

See `curl_examples.md` for comprehensive curl examples and API documentation.

## 🆘 Need Help?

- Check the logs in the terminal where you started the server
- Verify your environment variables are set correctly
- Ensure the data source path is accessible
- Check `curl_examples.md` for detailed examples
