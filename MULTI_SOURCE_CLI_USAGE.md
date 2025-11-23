# Multi-Source CLI Usage Guide

## Overview

When processing multiple sources in a single job, the CLI command format is different from single-source mode. The `data_source_path` is not required as a positional argument because sources are provided via the `NUVYN_JOB_PAYLOAD` environment variable.

## CLI Command Format

### Multi-Source Mode

When `sources` array is provided in `NUVYN_JOB_PAYLOAD`, use one of these formats:

**Option 1: Minimal (recommended)**
```bash
nuvyn-executor metadata_extraction --write-to-db
```

**Option 2: With workflow_id as positional argument**
```bash
nuvyn-executor metadata_extraction <workflow_id> --write-to-db
```

**Option 3: With all arguments (workflow_id will be overridden by env if present)**
```bash
nuvyn-executor metadata_extraction <data_source_path> <data_source_type> <workflow_id> --write-to-db
```

> **Note:** When `sources` are in the environment variable, `data_source_path` is ignored even if provided.

### Single-Source Mode (Backward Compatible)

When `sources` is NOT in `NUVYN_JOB_PAYLOAD`, use the traditional format:

```bash
nuvyn-executor metadata_extraction <data_source_path> <data_source_type> <workflow_id> [source_id] --write-to-db
```

## Notebook Integration

### Correct Way (Multi-Source)

```python
import os
import json
import subprocess

# Set NUVYN_JOB_PAYLOAD environment variable
job_payload = {
    "job_type": "metadata_extraction",
    "workflow_id": "wf_a8fe47b7",
    "sources": [
        {
            "source_id": "source_1",
            "data_source_path": "https://...",
            "data_source_type": "CSV"
        },
        {
            "source_id": "source_2",
            "data_source_path": "https://...",
            "data_source_type": "CSV"
        }
    ],
    "job_metadata": {
        "workflow_id": "wf_a8fe47b7",
        "write_to_db": True,
        "databricks_credentials": {...}
    }
}

os.environ["NUVYN_JOB_PAYLOAD"] = json.dumps(job_payload)

# Set Databricks credentials
os.environ["DATABRICKS_SERVER_HOSTNAME"] = "..."
os.environ["DATABRICKS_HTTP_PATH"] = "..."
os.environ["DATABRICKS_ACCESS_TOKEN"] = "..."

# Execute CLI - NO data_source_path needed!
cmd = [
    'nuvyn-executor',
    'metadata_extraction',
    '--write-to-db'
]

result = subprocess.run(cmd, capture_output=True, text=True)
```

### Incorrect Way (Will Cause KeyError)

```python
# ❌ DON'T DO THIS - data_source_path doesn't exist in multi-source mode
cmd = [
    'nuvyn-executor',
    'metadata_extraction',
    job_payload['data_source_path'],  # KeyError: 'data_source_path'
    job_payload['data_source_type'],
    job_payload['workflow_id'],
    '--write-to-db'
]
```

## Environment Variable Structure

The `NUVYN_JOB_PAYLOAD` should contain:

```json
{
    "job_type": "metadata_extraction",
    "workflow_id": "wf_xxx",
    "sources": [
        {
            "source_id": "source_1",
            "data_source_path": "https://...",
            "data_source_type": "CSV"
        }
    ],
    "job_metadata": {
        "workflow_id": "wf_xxx",
        "write_to_db": true,
        "databricks_credentials": {...}
    }
}
```

## Key Points

1. **When `sources` array exists in `NUVYN_JOB_PAYLOAD`:**
   - `data_source_path` is NOT required as a CLI argument
   - Each source in the array has its own `data_source_path`
   - `workflow_id` is required (from env or CLI)
   - All sources share the same `workflow_id` but have different `source_id`

2. **When `sources` array does NOT exist:**
   - Use traditional single-source CLI format
   - `data_source_path` is required as a positional argument

3. **The CLI automatically detects multi-source mode** by checking for `sources` in the environment variable.

## Troubleshooting

### Error: `KeyError: 'data_source_path'`

**Cause:** Trying to access `job_payload['data_source_path']` when using multi-source mode.

**Solution:** Don't include `data_source_path` in the CLI command when `sources` array is present. Use:
```bash
nuvyn-executor metadata_extraction --write-to-db
```

### Error: `workflow_id is required for multi-source extraction`

**Cause:** `workflow_id` is missing from `NUVYN_JOB_PAYLOAD`.

**Solution:** Ensure `workflow_id` is present in either:
- `NUVYN_JOB_PAYLOAD["workflow_id"]` (top level)
- `NUVYN_JOB_PAYLOAD["job_metadata"]["workflow_id"]`
- Or pass it as a positional argument: `nuvyn-executor metadata_extraction <workflow_id> --write-to-db`

