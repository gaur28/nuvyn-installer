# Metadata Extraction Job - Requirements

## 📋 Overview

This document outlines all requirements needed to successfully run the **Metadata Extraction Job** in the Nuvyn Executor Script.

---

## 🔧 System Requirements

### 1. **Databricks Environment**
- ✅ Databricks Runtime: 11.3 LTS or higher
- ✅ Python Version: 3.8 or higher
- ✅ Cluster Type: Single node or multi-node
- ✅ Access Mode: Shared or Single User

### 2. **Python Dependencies**
```
pandas>=1.5.0
numpy>=1.21.0
pyarrow>=10.0.0
openpyxl>=3.0.0
chardet>=5.0.0
aiohttp>=3.8.0
requests>=2.28.0
sqlalchemy>=1.4.0
```

All dependencies are automatically installed with the executor script.

---

## 📦 Installation Requirements

### **Required:**
```bash
%pip install git+https://github.com/nuvyn-bldr/executor-script.git@main
```

### **Verification:**
```bash
%sh
nuvyn-executor --help
```

---

## 🔑 Access Requirements

### 1. **Data Source Access**

The executor needs access to the client's data sources:

#### **For Azure Blob Storage:**
```bash
# Environment variables required:
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."
# OR
export AZURE_STORAGE_ACCOUNT_NAME="your-account-name"
export AZURE_STORAGE_ACCOUNT_KEY="your-account-key"
# OR
export AZURE_STORAGE_SAS_TOKEN="your-sas-token"
```

#### **For AWS S3:**
```bash
# Environment variables required:
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

#### **For Databases (MySQL, PostgreSQL, Snowflake):**
```bash
# Environment variables required:
export MYSQL_HOST="localhost"
export MYSQL_PORT="3306"
export MYSQL_USERNAME="user"
export MYSQL_PASSWORD="password"
export MYSQL_DATABASE="mydb"
```

#### **For DBFS/Unity Catalog:**
- No additional credentials required (uses cluster permissions)
- Cluster must have access to the specified paths

### 2. **Schema Access**

The executor needs permissions to:
- ✅ Create schema: `_executor_metadata`
- ✅ Create tables in that schema
- ✅ Insert data into those tables
- ✅ Query data from those tables

**Grant Permissions:**
```sql
-- In Databricks SQL
GRANT CREATE SCHEMA ON CATALOG main TO `<service-principal>`;
GRANT ALL PRIVILEGES ON SCHEMA main._executor_metadata TO `<service-principal>`;
```

### 3. **Network Access**

The executor needs:
- ✅ Outbound internet access (for pip install from GitHub)
- ✅ Access to your platform's REST API (for transmitting results)
- ✅ Access to client data sources (Azure Blob, S3, databases, etc.)

---

## 📁 Input Requirements

### **Required Parameter: Data Source Path**

The metadata extraction job requires a data source path parameter:

```bash
nuvyn-executor metadata_extraction <data_source_path>
```

#### **Valid Path Formats:**

**1. DBFS Paths:**
```bash
/dbfs/mnt/storage/data/
/dbfs/FileStore/data/
```

**2. Unity Catalog Volumes:**
```bash
/Volumes/main/default/sales_data/
/Volumes/catalog_name/schema_name/volume_name/
```

**3. Azure Blob Storage:**
```bash
https://storageaccount.blob.core.windows.net/container/path/
abfss://container@account.dfs.core.windows.net/path/
```

**4. AWS S3:**
```bash
s3://bucket-name/path/to/data/
https://bucket-name.s3.region.amazonaws.com/path/
```

**5. Database Connection:**
```bash
mysql://hostname:3306/database
postgresql://hostname:5432/database
snowflake://account.snowflakecomputing.com/database
```

---

## 🎯 Optional Parameters

### **Source Type** (parameter 2)
Specifies the type of data source:
- `csv` - CSV files
- `parquet` - Parquet files
- `excel` - Excel files
- `json` - JSON files
- `azure_blob` - Azure Blob Storage
- `aws_s3` - AWS S3
- `mysql` - MySQL database
- `postgresql` - PostgreSQL database
- `snowflake` - Snowflake database
- `auto` - Auto-detect (default)

**Example:**
```bash
nuvyn-executor metadata_extraction /dbfs/mnt/data/ csv
```

### **Tenant ID** (parameter 3)
Identifies the client/tenant:
- Default: `default`
- Format: Any alphanumeric string

**Example:**
```bash
nuvyn-executor metadata_extraction /dbfs/mnt/data/ csv client-123
```

---

## 🌍 Environment Variables

### **Required for Executor Functionality:**

```bash
# Databricks workspace (optional but recommended)
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"

# Your platform API (required for api_transmission job)
export NUVYN_API_ENDPOINT="https://your-platform-api.com"
export NUVYN_API_KEY="your-api-key"
export NUVYN_TENANT_ID="default"
```

### **Optional Configuration:**

```bash
# Executor behavior
export EXECUTOR_LOG_LEVEL="INFO"              # DEBUG, INFO, WARNING, ERROR
export EXECUTOR_VERBOSE="false"               # true/false
export EXECUTOR_MAX_FILE_SIZE_MB="100"        # Max file size to process
export EXECUTOR_MAX_FILES_PER_JOB="1000"      # Max files per job
export EXECUTOR_SAMPLE_SIZE_ROWS="10000"      # Sample rows for analysis
export EXECUTOR_SCHEMA_NAME="_executor_metadata"  # Schema name
```

---

## 📊 Output Requirements

### **Metadata Schema Structure**

The executor creates and populates the following schema:

**Schema:** `_executor_metadata`

**Tables:**

#### **1. sources**
```sql
CREATE TABLE _executor_metadata.sources (
    source_id STRING PRIMARY KEY,
    source_name STRING,
    source_type STRING,
    connection_details STRING,  -- Masked for security
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

#### **2. tables**
```sql
CREATE TABLE _executor_metadata.tables (
    table_id STRING PRIMARY KEY,
    source_id STRING,  -- FK to sources
    table_name STRING,
    schema_name STRING,
    row_count BIGINT,
    last_refreshed TIMESTAMP
);
```

#### **3. columns**
```sql
CREATE TABLE _executor_metadata.columns (
    column_id STRING PRIMARY KEY,
    table_id STRING,  -- FK to tables
    column_name STRING,
    data_type STRING,
    is_nullable BOOLEAN,
    is_primary_key BOOLEAN,
    sample_value STRING,
    distinct_count BIGINT
);
```

#### **4. executor_runs**
```sql
CREATE TABLE _executor_metadata.executor_runs (
    run_id STRING PRIMARY KEY,
    executor_version STRING,
    source_id STRING,  -- FK to sources
    run_mode STRING,
    status STRING,
    error_message STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);
```

#### **5. logs**
```sql
CREATE TABLE _executor_metadata.logs (
    log_id STRING PRIMARY KEY,
    run_id STRING,  -- FK to executor_runs
    log_level STRING,
    log_message STRING,
    log_timestamp TIMESTAMP
);
```

---

## ✅ Pre-Execution Checklist

Before running metadata extraction, ensure:

- [ ] Executor script is installed (`%pip install git+...`)
- [ ] Cluster is running and accessible
- [ ] Data source path is valid and accessible
- [ ] Required credentials are set (for cloud storage/databases)
- [ ] Schema permissions are granted
- [ ] Network access to data sources is available
- [ ] Platform API endpoint is configured (if using api_transmission)
- [ ] Sufficient cluster resources (memory, CPU)

---

## 🚨 Common Issues and Solutions

### **Issue 1: "No suitable connector found"**

**Error:**
```
❌ No suitable connector found for path: /path/to/data
```

**Solution:**
- Verify the data source path is correct
- Check if the path format matches supported types
- Ensure credentials are set for cloud storage paths
- Use the correct source_type parameter

---

### **Issue 2: "Schema does not exist"**

**Error:**
```
❌ Schema does not exist: _executor_metadata
```

**Solution:**
- Run schema validation job first: `nuvyn-executor schema_validation`
- Check cluster has CREATE SCHEMA permissions
- Verify you're in a Databricks environment

---

### **Issue 3: "Failed to connect to data source"**

**Error:**
```
❌ Failed to connect to data source
```

**Solution:**
- Verify credentials are set correctly
- Check network connectivity to the data source
- Ensure firewall rules allow access
- Test connection using Databricks Secrets

---

### **Issue 4: "Permission denied"**

**Error:**
```
❌ Permission denied accessing /path/to/data
```

**Solution:**
- Check cluster service principal has access to the path
- Verify DBFS mount permissions
- For Unity Catalog, check GRANT permissions

---

## 📈 Expected Performance

### **Metadata Extraction Times:**

| Data Source Size | File Count | Expected Duration | Cluster Size |
|-----------------|------------|-------------------|--------------|
| < 1 GB | 1-10 files | 30-60 seconds | Single node |
| 1-10 GB | 10-100 files | 2-5 minutes | 2-4 workers |
| 10-100 GB | 100-1000 files | 5-15 minutes | 4-8 workers |
| > 100 GB | 1000+ files | 15-60 minutes | 8+ workers |

**Factors Affecting Performance:**
- Number of files to analyze
- File sizes
- Data source type (cloud storage vs database)
- Network latency
- Cluster size and configuration
- Sample size configuration

---

## 💡 Optimization Tips

### **1. Limit Sample Size**
```bash
export EXECUTOR_SAMPLE_SIZE_ROWS="1000"  # Analyze fewer rows for faster execution
```

### **2. Limit File Count**
```bash
export EXECUTOR_MAX_FILES_PER_JOB="100"  # Process fewer files per job
```

### **3. Use Appropriate Cluster Size**
- Small datasets (< 10 GB): Single node or 2 workers
- Medium datasets (10-100 GB): 4-8 workers
- Large datasets (> 100 GB): 8+ workers with autoscaling

### **4. Optimize Data Source**
- Use indexed tables for databases
- Partition large files
- Use columnar formats (parquet) instead of CSV when possible

---

## 📝 Minimum Example

Here's the absolute minimum needed to run metadata extraction:

```bash
# 1. Install (one-time)
curl -X POST https://<workspace>/api/2.0/commands/execute \
  -H "Authorization: Bearer <token>" \
  -d '{"language":"python","clusterId":"<cluster-id>","command":"%pip install git+https://github.com/nuvyn-bldr/executor-script.git@main"}'

# 2. Run schema validation (one-time setup)
curl -X POST https://<workspace>/api/2.1/jobs/create \
  -H "Authorization: Bearer <token>" \
  -d '{"name":"Schema-Setup","existing_cluster_id":"<cluster-id>","spark_python_task":{"python_file":"dbfs:/executor/main.py","parameters":["schema_validation"]}}'

# Get job_id from response, then run:
curl -X POST https://<workspace>/api/2.1/jobs/run-now \
  -H "Authorization: Bearer <token>" \
  -d '{"job_id":<schema-job-id>}'

# 3. Run metadata extraction
curl -X POST https://<workspace>/api/2.1/jobs/create \
  -H "Authorization: Bearer <token>" \
  -d '{"name":"Metadata-Extraction","existing_cluster_id":"<cluster-id>","spark_python_task":{"python_file":"dbfs:/executor/main.py","parameters":["metadata_extraction","/dbfs/mnt/data/"]}}'

# Get job_id from response, then run:
curl -X POST https://<workspace>/api/2.1/jobs/run-now \
  -H "Authorization: Bearer <token>" \
  -d '{"job_id":<metadata-job-id>}'
```

That's it! The executor will handle the rest.

---

## 🎯 Summary

**Mandatory Requirements:**
1. ✅ Databricks cluster (running)
2. ✅ Executor script installed
3. ✅ Data source path (valid and accessible)
4. ✅ Schema permissions (CREATE, INSERT, SELECT)

**Optional but Recommended:**
1. Environment variables for configuration
2. Credentials for cloud storage/databases
3. API endpoint for result transmission
4. Appropriate cluster sizing

**Minimum Command:**
```bash
nuvyn-executor metadata_extraction /path/to/data
```

That's all you need to get started! 🚀

