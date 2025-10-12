# Write Metadata to Database - Guide

## 📋 Overview

The executor script can now write extracted metadata directly to Databricks SQL tables in the `_executor_metadata` schema.

---

## 🚀 Quick Start

### **Without Database Writing (Default)**
```bash
nuvyn-executor metadata_extraction /path/to/data
```
**Result:** Metadata printed to stdout only

### **With Database Writing**
```bash
nuvyn-executor metadata_extraction /path/to/data csv tenant123 --write-to-db
```
**Result:** Metadata extracted AND written to `_executor_metadata` schema

---

## 🔧 Setup Requirements

### **1. Environment Variables**

Set these environment variables before running:

```bash
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_ACCESS_TOKEN="dapi1234567890abcdef..."
```

### **2. Alternative: Use Flag in Environment**

```bash
export EXECUTOR_WRITE_TO_DB="true"
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_ACCESS_TOKEN="dapi1234567890abcdef..."

nuvyn-executor metadata_extraction /path/to/data
```

---

## 📊 Schema Structure

The executor creates and writes to these tables:

### **1. _executor_metadata.sources**
```sql
CREATE TABLE _executor_metadata.sources (
    source_id STRING,
    source_path STRING,
    source_type STRING,
    extraction_timestamp TIMESTAMP,
    files_found INT,
    total_size_bytes BIGINT
);
```

**Example Data:**
| source_id | source_path | source_type | extraction_timestamp | files_found | total_size_bytes |
|-----------|-------------|-------------|---------------------|-------------|------------------|
| uuid-123 | https://... | azure_blob | 2025-10-12 10:30:00 | 1 | 4744481 |

---

### **2. _executor_metadata.tables**
```sql
CREATE TABLE _executor_metadata.tables (
    source_id STRING,
    table_name STRING,
    file_path STRING,
    file_type STRING,
    row_count BIGINT,
    column_count INT,
    size_bytes BIGINT
);
```

**Example Data:**
| source_id | table_name | file_path | file_type | row_count | column_count | size_bytes |
|-----------|------------|-----------|-----------|-----------|--------------|------------|
| uuid-123 | amazon.csv | Amazon-Sales-Data/amazon.csv | csv | 1500 | 16 | 4744481 |

---

### **3. _executor_metadata.columns**
```sql
CREATE TABLE _executor_metadata.columns (
    source_id STRING,
    table_name STRING,
    column_name STRING,
    data_type STRING,
    position INT,
    is_nullable BOOLEAN,
    sample_values STRING
);
```

**Example Data:**
| source_id | table_name | column_name | data_type | position | is_nullable | sample_values |
|-----------|------------|-------------|-----------|----------|-------------|---------------|
| uuid-123 | amazon.csv | product_id | string | 0 | false | ['B07JW9H4J1', 'B098NS6PVG', ...] |
| uuid-123 | amazon.csv | product_name | string | 1 | false | ['Wayona Nylon...', 'Ambrane...', ...] |
| uuid-123 | amazon.csv | category | string | 2 | false | ['Computers&Accessories...', ...] |

---

## 💻 Usage Examples

### **Example 1: Basic Metadata Extraction with DB Write**

```bash
export DATABRICKS_SERVER_HOSTNAME="adb-123456789.12.azuredatabricks.net"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abcd1234efgh5678"
export DATABRICKS_ACCESS_TOKEN="dapi..."

nuvyn-executor metadata_extraction \
  "https://storageaccount.blob.core.windows.net/container/file.csv?sp=r&sig=..." \
  azure_blob \
  client-123 \
  --write-to-db
```

**Output:**
```
✅ Databricks SQL writer initialized
✅ Schema created: _executor_metadata
✅ Table created: _executor_metadata.sources
✅ Table created: _executor_metadata.tables
✅ Table created: _executor_metadata.columns
💾 Writing metadata to Databricks SQL...
✅ Source metadata written: uuid-123
✅ Table metadata written: file.csv
✅ Column metadata written: product_id
✅ Column metadata written: product_name
...
✅ Metadata written to database successfully
✅ Metadata extraction completed: 1 files analyzed
```

---

### **Example 2: Via Databricks Jobs API**

```bash
curl -X POST https://<workspace-url>/api/2.1/jobs/create \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Metadata-Extraction-With-DB-Write",
    "existing_cluster_id": "<cluster-id>",
    "spark_python_task": {
      "python_file": "dbfs:/executor/main.py",
      "parameters": [
        "metadata_extraction",
        "/dbfs/mnt/client-data/",
        "csv",
        "client-123",
        "--write-to-db"
      ]
    },
    "timeout_seconds": 3600,
    "spark_conf": {
      "DATABRICKS_SERVER_HOSTNAME": "your-workspace.cloud.databricks.com",
      "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/warehouse-id",
      "DATABRICKS_ACCESS_TOKEN": "{{secrets/nuvyn-executor/db-token}}"
    }
  }'
```

---

### **Example 3: Using Environment Variable Instead of Flag**

```bash
export EXECUTOR_WRITE_TO_DB="true"
export DATABRICKS_SERVER_HOSTNAME="..."
export DATABRICKS_HTTP_PATH="..."
export DATABRICKS_ACCESS_TOKEN="..."

# No need for --write-to-db flag, it's enabled via environment
nuvyn-executor metadata_extraction /path/to/data csv tenant123
```

---

## 🔍 Querying the Metadata

After writing metadata to the database, you can query it:

### **Query All Sources**
```sql
SELECT * FROM _executor_metadata.sources
ORDER BY extraction_timestamp DESC;
```

### **Query Tables for a Source**
```sql
SELECT 
    t.table_name,
    t.file_type,
    t.row_count,
    t.column_count,
    t.size_bytes
FROM _executor_metadata.tables t
JOIN _executor_metadata.sources s ON t.source_id = s.source_id
WHERE s.source_path LIKE '%amazon%'
ORDER BY t.table_name;
```

### **Query Columns for a Table**
```sql
SELECT 
    c.column_name,
    c.data_type,
    c.position,
    c.is_nullable,
    c.sample_values
FROM _executor_metadata.columns c
JOIN _executor_metadata.tables t ON c.source_id = t.source_id AND c.table_name = t.table_name
WHERE t.table_name = 'amazon.csv'
ORDER BY c.position;
```

### **Get Complete Metadata for a Source**
```sql
SELECT 
    s.source_id,
    s.source_path,
    s.source_type,
    s.extraction_timestamp,
    s.files_found,
    s.total_size_bytes,
    t.table_name,
    t.file_type,
    t.row_count,
    t.column_count,
    c.column_name,
    c.data_type,
    c.position,
    c.is_nullable
FROM _executor_metadata.sources s
JOIN _executor_metadata.tables t ON s.source_id = t.source_id
JOIN _executor_metadata.columns c ON t.source_id = c.source_id AND t.table_name = c.table_name
WHERE s.source_id = 'your-source-id'
ORDER BY t.table_name, c.position;
```

---

## 🎯 Backend Integration

### **Python Service Example**

```python
from databricks import sql

class MetadataQueryService:
    """Service to query executor metadata from Databricks"""
    
    def __init__(self, server_hostname, http_path, access_token):
        self.connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
    
    def get_latest_metadata(self, tenant_id: str = None):
        """Get latest metadata extraction"""
        cursor = self.connection.cursor()
        
        query = """
            SELECT 
                s.source_id,
                s.source_path,
                s.extraction_timestamp,
                s.files_found,
                COUNT(DISTINCT t.table_name) as table_count,
                COUNT(c.column_name) as total_columns
            FROM _executor_metadata.sources s
            LEFT JOIN _executor_metadata.tables t ON s.source_id = t.source_id
            LEFT JOIN _executor_metadata.columns c ON t.source_id = c.source_id
            GROUP BY s.source_id, s.source_path, s.extraction_timestamp, s.files_found
            ORDER BY s.extraction_timestamp DESC
            LIMIT 1
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        return {
            "source_id": result[0],
            "source_path": result[1],
            "extraction_timestamp": result[2],
            "files_found": result[3],
            "table_count": result[4],
            "total_columns": result[5]
        }
    
    def get_source_columns(self, source_id: str):
        """Get all columns for a source"""
        cursor = self.connection.cursor()
        
        query = """
            SELECT 
                table_name,
                column_name,
                data_type,
                position,
                is_nullable,
                sample_values
            FROM _executor_metadata.columns
            WHERE source_id = ?
            ORDER BY table_name, position
        """
        
        cursor.execute(query, (source_id,))
        results = cursor.fetchall()
        cursor.close()
        
        return [
            {
                "table_name": row[0],
                "column_name": row[1],
                "data_type": row[2],
                "position": row[3],
                "is_nullable": row[4],
                "sample_values": row[5]
            }
            for row in results
        ]
```

---

## ✅ Complete Workflow

```bash
# Step 1: Set environment variables
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/warehouse-id"
export DATABRICKS_ACCESS_TOKEN="dapi..."

# Step 2: Run metadata extraction with DB write
nuvyn-executor metadata_extraction \
  "https://storageaccount.blob.core.windows.net/container/data.csv?sig=..." \
  azure_blob \
  client-123 \
  --write-to-db

# Step 3: Query the metadata
databricks-sql-cli -e "SELECT * FROM _executor_metadata.sources;"
databricks-sql-cli -e "SELECT * FROM _executor_metadata.columns WHERE table_name = 'data.csv';"
```

---

## 🔐 Security - Using Databricks Secrets

### **Store Credentials in Secrets**

```bash
# Create secret scope
databricks secrets create-scope --scope nuvyn-executor

# Add secrets
databricks secrets put --scope nuvyn-executor --key db-token
databricks secrets put --scope nuvyn-executor --key db-hostname
databricks secrets put --scope nuvyn-executor --key db-http-path
```

### **Use in Databricks Job**

```python
# In Databricks notebook/job
import os

os.environ['DATABRICKS_SERVER_HOSTNAME'] = dbutils.secrets.get(scope="nuvyn-executor", key="db-hostname")
os.environ['DATABRICKS_HTTP_PATH'] = dbutils.secrets.get(scope="nuvyn-executor", key="db-http-path")
os.environ['DATABRICKS_ACCESS_TOKEN'] = dbutils.secrets.get(scope="nuvyn-executor", key="db-token")
os.environ['EXECUTOR_WRITE_TO_DB'] = "true"

# Then run the executor
!nuvyn-executor metadata_extraction /dbfs/mnt/data/ csv client-123
```

---

## 📈 Benefits

### **Before (stdout only):**
- ❌ Metadata only in job logs
- ❌ Not queryable
- ❌ Lost after job completes
- ❌ No historical tracking

### **After (with --write-to-db):**
- ✅ Metadata stored in Databricks SQL
- ✅ Fully queryable
- ✅ Persistent storage
- ✅ Historical tracking
- ✅ Audit trail
- ✅ Integration with BI tools

---

## 🎯 Summary

**Command Structure:**
```bash
nuvyn-executor metadata_extraction <path> <type> <tenant> --write-to-db
```

**Required Environment Variables:**
```bash
DATABRICKS_SERVER_HOSTNAME
DATABRICKS_HTTP_PATH
DATABRICKS_ACCESS_TOKEN
```

**What Gets Written:**
- ✅ Source information → `_executor_metadata.sources`
- ✅ File/table information → `_executor_metadata.tables`
- ✅ Column information → `_executor_metadata.columns`

The metadata is now **persistent, queryable, and ready for your backend to consume**! 🚀

