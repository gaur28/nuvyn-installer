# Troubleshooting Import Error: nuvyn_executor package not found

## 🔴 Error Message

```
ImportError: nuvyn_executor package is not available after 3 attempts. 
Package installed but module/CLI not found.
```

## 🔍 Root Cause

The package is installed as `nuvyn-executor-script` (pip package name), but the **Python module name is `executor`**, not `nuvyn_executor`.

### Package Structure

- **Pip Package Name**: `nuvyn-executor-script` (with hyphens)
- **Python Module Name**: `executor` (the actual import name)
- **CLI Command**: `nuvyn-executor` (works correctly)

## ✅ Correct Usage

### Option 1: Use the CLI Command (Recommended)

```python
# In Databricks notebook
import subprocess
result = subprocess.run(
    ["nuvyn-executor", "metadata_extraction", "/path/to/data", "csv", "default", "--write-to-db"],
    capture_output=True,
    text=True
)
print(result.stdout)
```

### Option 2: Import the Executor Module

```python
# Correct import
from executor.main import main as executor_main
from executor.config import ConfigManager, JobType
from executor.job_manager import JobManager

# Or import specific functions
from executor.main import create_and_execute_job
import asyncio

# Use it
result = asyncio.run(create_and_execute_job(
    job_type="metadata_extraction",
    data_source_path="/path/to/data",
    data_source_type="csv"
))
```

### Option 3: Direct Module Import

```python
# Import the executor package
import executor
from executor import ConfigManager, JobManager, JobType

# Or use the main module
import executor.main as executor_main
```

## ❌ Incorrect Usage (What Causes the Error)

```python
# ❌ WRONG - This module doesn't exist
import nuvyn_executor

# ❌ WRONG - This package doesn't exist
from nuvyn_executor import something
```

## 🔧 Verification Steps

### Step 1: Check if Package is Installed

```python
# In Databricks notebook
import subprocess
result = subprocess.run(["pip", "show", "nuvyn-executor-script"], capture_output=True, text=True)
print(result.stdout)
```

Expected output should show:
```
Name: nuvyn-executor-script
Version: 1.0.0
Location: /local_disk0/.ephemeral_nfs/envs/...
```

### Step 2: Check if Module is Available

```python
# Test if executor module can be imported
try:
    import executor
    print("✅ executor module imported successfully")
    print(f"Module location: {executor.__file__}")
except ImportError as e:
    print(f"❌ Failed to import executor: {e}")
```

### Step 3: Check if CLI Command is Available

```python
# Test if CLI command works
import subprocess
result = subprocess.run(["nuvyn-executor", "--help"], capture_output=True, text=True)
if result.returncode == 0:
    print("✅ nuvyn-executor CLI command works")
    print(result.stdout)
else:
    print(f"❌ CLI command failed: {result.stderr}")
```

### Step 4: List Installed Packages

```python
# Check what's actually installed
import subprocess
result = subprocess.run(["pip", "list"], capture_output=True, text=True)
if "nuvyn-executor-script" in result.stdout:
    print("✅ Package is installed")
else:
    print("❌ Package is NOT installed")
```

## 🛠️ Solutions

### Solution 1: Use Correct Import (Recommended)

Update your Databricks notebook code to use the correct import:

```python
# Instead of: import nuvyn_executor
# Use:
import executor
from executor.main import create_and_execute_job
from executor.config import ConfigManager, JobType
```

### Solution 2: Use CLI Command

Instead of importing, use the CLI command:

```python
import subprocess
import json

# Set environment variables
import os
os.environ['NUVYN_JOB_PAYLOAD'] = json.dumps({
    "workflow_id": "workflow_123",
    "sources": [...]
})

# Run the executor
result = subprocess.run(
    ["nuvyn-executor", "metadata_extraction", "/path/to/data", "csv", "default", "--write-to-db"],
    capture_output=True,
    text=True
)
```

### Solution 3: Reinstall Package

If the package structure is corrupted:

```python
# Uninstall and reinstall
%pip uninstall -y nuvyn-executor-script
%pip install git+https://github.com/nuvyn-bldr/executor-script.git@main

# Restart Python kernel
dbutils.library.restartPython()
```

### Solution 4: Check Python Path

```python
import sys
print("Python path:")
for path in sys.path:
    print(f"  - {path}")

# Check if executor is in path
import os
executor_path = None
for path in sys.path:
    potential = os.path.join(path, "executor")
    if os.path.exists(potential):
        executor_path = potential
        break

if executor_path:
    print(f"✅ Found executor at: {executor_path}")
else:
    print("❌ executor module not found in Python path")
```

## 📝 Common Issues

### Issue 1: Package Installed but Module Not Found

**Symptom**: `pip show nuvyn-executor-script` works, but `import executor` fails

**Cause**: Package installed in wrong location or Python path issue

**Solution**:
```python
# Restart Python kernel
dbutils.library.restartPython()

# Or reinstall
%pip install --force-reinstall git+https://github.com/nuvyn-bldr/executor-script.git@main
```

### Issue 2: CLI Command Not Found

**Symptom**: `nuvyn-executor` command not found

**Cause**: Entry point not installed correctly

**Solution**:
```python
# Reinstall with entry points
%pip install --force-reinstall --no-deps git+https://github.com/nuvyn-bldr/executor-script.git@main
%pip install -r requirements.txt
```

### Issue 3: Import Works but Functions Not Available

**Symptom**: `import executor` works but `executor.main` doesn't exist

**Cause**: Package structure issue

**Solution**: Use direct imports:
```python
from executor.main import create_and_execute_job
from executor.config import ConfigManager
```

## 🧪 Test Script

Use this script to verify everything works:

```python
# Test script for Databricks notebook
print("=" * 80)
print("Testing Nuvyn Executor Installation")
print("=" * 80)

# Test 1: Check package installation
print("\n1. Checking package installation...")
import subprocess
result = subprocess.run(["pip", "show", "nuvyn-executor-script"], capture_output=True, text=True)
if "nuvyn-executor-script" in result.stdout:
    print("   ✅ Package is installed")
else:
    print("   ❌ Package is NOT installed")
    print("   Run: %pip install git+https://github.com/nuvyn-bldr/executor-script.git@main")

# Test 2: Check module import
print("\n2. Testing module import...")
try:
    import executor
    print("   ✅ executor module imported successfully")
except ImportError as e:
    print(f"   ❌ Failed to import executor: {e}")

# Test 3: Check CLI command
print("\n3. Testing CLI command...")
result = subprocess.run(["which", "nuvyn-executor"], capture_output=True, text=True)
if result.returncode == 0:
    print("   ✅ nuvyn-executor CLI command found")
else:
    print("   ⚠️  nuvyn-executor CLI command not in PATH")
    print("   Try: import executor.main as executor_main")

# Test 4: Check specific imports
print("\n4. Testing specific imports...")
try:
    from executor.config import ConfigManager, JobType
    from executor.job_manager import JobManager
    print("   ✅ Core modules imported successfully")
except ImportError as e:
    print(f"   ❌ Failed to import core modules: {e}")

print("\n" + "=" * 80)
print("Test Complete")
print("=" * 80)
```

## 📚 Correct Import Examples

### Example 1: Using CLI Command

```python
import subprocess
import json

# Set environment
os.environ['NUVYN_JOB_PAYLOAD'] = json.dumps({
    "workflow_id": "workflow_123",
    "sources": [{"source_id": "source_1", "data_source_path": "https://..."}]
})

# Run executor
result = subprocess.run(
    ["nuvyn-executor", "metadata_extraction", "", "auto", "default", "--write-to-db"],
    capture_output=True,
    text=True
)
print(result.stdout)
```

### Example 2: Using Python API

```python
import asyncio
from executor.main import create_and_execute_job
from executor.config import ConfigManager

async def run_extraction():
    config_manager = ConfigManager()
    result = await create_and_execute_job(
        job_type="metadata_extraction",
        data_source_path="/path/to/data",
        data_source_type="csv",
        tenant_id="default",
        config_manager=config_manager,
        job_metadata={
            "workflow_id": "workflow_123",
            "source_id": "source_1"
        }
    )
    return result

# Run it
result = asyncio.run(run_extraction())
print(result)
```

### Example 3: Direct Module Usage

```python
from executor.config import ConfigManager, JobType
from executor.job_manager import JobManager
import asyncio

config_manager = ConfigManager()
job_manager = JobManager(config_manager)

# Create and execute job
async def run():
    job_id = await job_manager.create_job(
        job_type=JobType.METADATA_EXTRACTION,
        data_source_path="/path/to/data",
        data_source_type="csv",
        tenant_id="default",
        job_metadata={"workflow_id": "workflow_123"}
    )
    result = await job_manager.execute_job(job_id)
    return result

result = asyncio.run(run())
```

## 🔗 Summary

- **Package Name**: `nuvyn-executor-script` (for pip install)
- **Module Name**: `executor` (for Python imports)
- **CLI Command**: `nuvyn-executor` (for command-line usage)
- **Correct Import**: `import executor` or `from executor import ...`
- **Wrong Import**: `import nuvyn_executor` ❌

---

**Document Version:** 1.0  
**Last Updated:** 2025

