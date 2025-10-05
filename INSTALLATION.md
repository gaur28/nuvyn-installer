# Nuvyn Executor Script - Installation Guide

## Quick Installation

The Nuvyn Executor Script can be installed directly from the Git repository using pip:

```bash
pip install git+https://github.com/nuvyn-bldr/executor-script.git@main
```

## Installation in Databricks

### Method 1: Using Databricks Notebook
```python
%pip install git+https://github.com/nuvyn-bldr/executor-script.git@main
```

### Method 2: Using Databricks CLI
```bash
databricks libraries install --cluster-id <cluster-id> --pypi-package git+https://github.com/nuvyn-bldr/executor-script.git@main
```

### Method 3: Using Databricks Jobs API
```bash
curl -X POST \
  https://<workspace-url>/api/2.0/libraries/install \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "cluster_id": "<cluster-id>",
    "libraries": [
      {
        "pypi": {
          "package": "git+https://github.com/nuvyn-bldr/executor-script.git@main"
        }
      }
    ]
  }'
```

## Usage After Installation

Once installed, the executor script can be used as a command-line tool:

```bash
# Schema validation
nuvyn-executor schema_validation

# Metadata extraction
nuvyn-executor metadata_extraction /path/to/data

# Data reading
nuvyn-executor data_reading /path/to/data

# Quality assessment
nuvyn-executor quality_assessment /path/to/data

# API transmission
nuvyn-executor api_transmission

# Full pipeline
nuvyn-executor full_pipeline /path/to/data
```

## Environment Variables

Set the following environment variables for proper configuration:

```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.databricks.com"
export NUVYN_API_ENDPOINT="https://your-api-endpoint.com"
export NUVYN_API_KEY="your-api-key"
export EXECUTOR_LOG_LEVEL="INFO"
```

## Verification

Test the installation:

```bash
nuvyn-executor schema_validation
```

You should see output indicating successful schema validation.

## Troubleshooting

### Import Errors
If you encounter import errors, ensure all dependencies are installed:
```bash
pip install -r requirements.txt
```

### Permission Issues
For Databricks installations, ensure the cluster has appropriate permissions to install packages.

### Network Issues
If the git installation fails, try using SSH instead of HTTPS:
```bash
pip install git+ssh://git@github.com/nuvyn-bldr/executor-script.git@main
```
