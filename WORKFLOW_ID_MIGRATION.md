# Workflow ID Migration Documentation

## 📋 Overview

This document describes the migration from `source_id`-based metadata storage to `workflow_id`-based storage. The executor now uses `workflow_id` as the **primary identifier** for extracting and querying metadata, while `source_id` is retained as an **optional filter** for additional querying capabilities.

**Version:** 2.0  
**Date:** 2025  
**Status:** ✅ Implemented

---

## 🎯 Key Changes

### Before (v1.0)
- `source_id` was the primary identifier for all metadata operations
- All queries and storage operations used `source_id`
- `source_id` was required and auto-generated if not provided

### After (v2.0)
- `workflow_id` is now the **primary identifier** for all metadata operations
- `source_id` is now **optional** and used for filtering purposes
- Both `workflow_id` and `source_id` are stored in all tables
- `workflow_id` is **required** and must be provided by the backend

---

## 📊 Schema Changes

### Updated Tables

All tables in the `_executor_metadata` schema now include both `workflow_id` and `source_id` columns:

#### 1. `sources` Table
```sql
CREATE TABLE IF NOT EXISTS hive_metastore._executor_metadata.sources (
    id BIGINT GENERATED ALWAYS AS IDENTITY,        -- Auto-increment PK
    workflow_id STRING,                            -- Primary identifier (REQUIRED)
    source_id STRING,                              -- Filter identifier (OPTIONAL)
    source_path STRING,
    source_type STRING,
    extraction_timestamp TIMESTAMP,
    files_found INT,
    total_size_bytes BIGINT
)
```

#### 2. `tables` Table
```sql
CREATE TABLE IF NOT EXISTS hive_metastore._executor_metadata.tables (
    id BIGINT GENERATED ALWAYS AS IDENTITY,        -- Auto-increment PK
    workflow_id STRING,                            -- Primary identifier (REQUIRED)
    source_id STRING,                              -- Filter identifier (OPTIONAL)
    table_name STRING,
    file_path STRING,
    file_type STRING,
    row_count BIGINT,
    column_count INT,
    size_bytes BIGINT
)
```

#### 3. `columns` Table
```sql
CREATE TABLE IF NOT EXISTS hive_metastore._executor_metadata.columns (
    id BIGINT GENERATED ALWAYS AS IDENTITY,        -- Auto-increment PK
    workflow_id STRING,                            -- Primary identifier (REQUIRED)
    source_id STRING,                              -- Filter identifier (OPTIONAL)
    table_name STRING,
    column_name STRING,
    data_type STRING,
    position INT,
    is_nullable BOOLEAN,
    sample_values STRING
)
```

#### 4. `executor_runs` Table
```sql
CREATE TABLE IF NOT EXISTS hive_metastore._executor_metadata.executor_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY,        -- Auto-increment PK
    run_id STRING,
    executor_version STRING,
    workflow_id STRING,                            -- Primary identifier (REQUIRED)
    source_id STRING,                              -- Filter identifier (OPTIONAL)
    run_mode STRING,
    status STRING,
    error_message STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
)
```

---

## 🔧 API Changes

### REST API Endpoint: `POST /jobs/create`

#### Request Body (v2.0)

**Required Fields:**
- `job_type` - Type of job (e.g., "metadata_extraction")
- `data_source_path` - Path to the data source
- `workflow_id` - **Primary identifier** (REQUIRED)

**Optional Fields:**
- `source_id` - Filter identifier (OPTIONAL)
- `data_source_type` - Type of data source
- `tenant_id` - Tenant identifier
- `job_metadata` - Additional metadata

#### Example Request

```json
{
  "job_type": "metadata_extraction",
  "data_source_path": "https://storageaccount.blob.core.windows.net/container/path",
  "workflow_id": "workflow_abc123",
  "source_id": "source_xyz789",
  "data_source_type": "azure_blob",
  "tenant_id": "default",
  "job_metadata": {
    "workflow_id": "workflow_abc123",
    "source_id": "source_xyz789"
  }
}
```

#### Response

```json
{
  "job_id": "job_123456",
  "status": "created",
  "message": "Job created successfully"
}
```

---

## 🌐 Environment Variable Support

The executor supports reading `workflow_id` and `source_id` from the `NUVYN_JOB_PAYLOAD` environment variable, which is useful for Databricks Jobs.

### Environment Variable Format

```bash
export NUVYN_JOB_PAYLOAD='{
  "job_type": "metadata_extraction",
  "data_source_path": "https://...",
  "workflow_id": "workflow_abc123",
  "source_id": "source_xyz789",
  "job_metadata": {
    "workflow_id": "workflow_abc123",
    "source_id": "source_xyz789"
  }
}'
```

### Priority Order

1. **Priority 1:** `job_metadata.workflow_id` (from API request or environment)
2. **Priority 2:** Top-level `workflow_id` (from environment variable)
3. **Error:** If `workflow_id` is not found, the job will fail

---

## 📝 Code Changes

### 1. DatabricksWriter (`executor/storage/databricks_writer.py`)

#### Updated Method Signature

```python
def write_metadata(self, metadata: Dict[str, Any], workflow_id: str = None, source_id: str = None) -> bool:
    """
    Write extracted metadata to Databricks SQL tables
    
    Args:
        metadata: Metadata dict
        workflow_id: Required backend-provided workflow_id (primary identifier)
        source_id: Optional source_id for filtering purposes
    """
```

#### Updated Query Method

```python
def query_metadata(self, workflow_id: str = None, source_id: str = None) -> Dict[str, Any]:
    """
    Query metadata from Databricks SQL
    
    Args:
        workflow_id: Primary identifier for querying metadata (required if source_id not provided)
        source_id: Optional filter for source_id
    """
```

### 2. MetadataExtractor (`executor/metadata/extractor.py`)

#### New Methods

```python
def _get_workflow_id_from_environment(self):
    """Extract workflow_id from NUVYN_JOB_PAYLOAD environment variable"""

def _get_source_id_from_environment(self):
    """Extract source_id from NUVYN_JOB_PAYLOAD environment variable (for filtering)"""
```

#### Updated Extraction Logic

- Extracts `workflow_id` as primary identifier
- Extracts `source_id` as optional filter
- Validates `workflow_id` is required
- Passes both to `write_metadata()`

### 3. Main Entry Point (`executor/main.py`)

#### New Functions

```python
def get_workflow_id_from_environment():
    """Extract workflow_id from NUVYN_JOB_PAYLOAD environment variable"""

def get_source_id_from_environment():
    """Extract source_id from NUVYN_JOB_PAYLOAD environment variable"""
```

### 4. API Server (`executor/api_server.py`)

#### Updated Endpoint

- Changed required field from `source_id` to `workflow_id`
- Accepts optional `source_id` for filtering
- Passes both to `job_metadata`

---

## 🔍 Query Examples

### Query by workflow_id (Primary Identifier)

```sql
-- Get all metadata for a specific workflow
SELECT 
    s.workflow_id,
    s.source_id,
    s.source_path,
    s.extraction_timestamp,
    t.table_name,
    t.row_count,
    c.column_name,
    c.data_type
FROM hive_metastore._executor_metadata.sources s
JOIN hive_metastore._executor_metadata.tables t 
    ON s.workflow_id = t.workflow_id
JOIN hive_metastore._executor_metadata.columns c 
    ON t.workflow_id = c.workflow_id 
    AND t.table_name = c.table_name
WHERE s.workflow_id = 'workflow_abc123'
ORDER BY t.table_name, c.position;
```

### Query by workflow_id AND filter by source_id

```sql
-- Get metadata for a specific workflow and source
SELECT 
    c.workflow_id,
    c.source_id,
    c.table_name,
    c.column_name,
    c.data_type,
    c.position,
    c.is_nullable
FROM hive_metastore._executor_metadata.columns c
WHERE c.workflow_id = 'workflow_abc123'
  AND c.source_id = 'source_xyz789'
ORDER BY c.table_name, c.position;
```

### Filter by source_id only (across all workflows)

```sql
-- Get all metadata for a specific source across all workflows
SELECT 
    c.workflow_id,
    c.source_id,
    c.table_name,
    c.column_name,
    c.data_type
FROM hive_metastore._executor_metadata.columns c
WHERE c.source_id = 'source_xyz789'
ORDER BY c.workflow_id, c.table_name, c.position;
```

### Get latest metadata for a workflow

```sql
-- Get the most recent metadata extraction for a workflow
SELECT 
    s.workflow_id,
    s.source_id,
    s.source_path,
    s.extraction_timestamp,
    s.files_found,
    COUNT(DISTINCT t.table_name) as table_count,
    COUNT(c.column_name) as total_columns
FROM hive_metastore._executor_metadata.sources s
LEFT JOIN hive_metastore._executor_metadata.tables t 
    ON s.workflow_id = t.workflow_id
LEFT JOIN hive_metastore._executor_metadata.columns c 
    ON t.workflow_id = c.workflow_id 
    AND t.table_name = c.table_name
WHERE s.workflow_id = 'workflow_abc123'
GROUP BY s.workflow_id, s.source_id, s.source_path, s.extraction_timestamp, s.files_found
ORDER BY s.extraction_timestamp DESC
LIMIT 1;
```

---

## 🚀 Migration Guide

### For Backend Developers

1. **Update API Requests:**
   - Change `source_id` from required to optional
   - Add `workflow_id` as required field
   - Update payload structure to include both fields

2. **Update Environment Variables:**
   - Include `workflow_id` in `NUVYN_JOB_PAYLOAD`
   - Keep `source_id` for filtering if needed

3. **Update Database Queries:**
   - Use `workflow_id` for primary queries
   - Use `source_id` for additional filtering

### For Database Administrators

1. **Schema Migration:**
   ```sql
   -- Add workflow_id column to existing tables
   ALTER TABLE hive_metastore._executor_metadata.sources 
   ADD COLUMN workflow_id STRING;
   
   ALTER TABLE hive_metastore._executor_metadata.tables 
   ADD COLUMN workflow_id STRING;
   
   ALTER TABLE hive_metastore._executor_metadata.columns 
   ADD COLUMN workflow_id STRING;
   
   ALTER TABLE hive_metastore._executor_metadata.executor_runs 
   ADD COLUMN workflow_id STRING;
   ```

2. **Data Migration (if needed):**
   ```sql
   -- Migrate existing source_id values to workflow_id
   -- Note: This is only needed if you want to preserve existing data
   UPDATE hive_metastore._executor_metadata.sources 
   SET workflow_id = source_id 
   WHERE workflow_id IS NULL;
   
   -- Repeat for other tables...
   ```

3. **Drop Old Tables (if starting fresh):**
   ```sql
   -- Drop and recreate tables with new schema
   DROP TABLE IF EXISTS hive_metastore._executor_metadata.sources;
   DROP TABLE IF EXISTS hive_metastore._executor_metadata.tables;
   DROP TABLE IF EXISTS hive_metastore._executor_metadata.columns;
   DROP TABLE IF EXISTS hive_metastore._executor_metadata.executor_runs;
   -- Tables will be recreated automatically on next executor run
   ```

---

## ✅ Validation & Testing

### Test Cases

1. **Test with workflow_id only:**
   ```bash
   curl -X POST http://localhost:8080/jobs/create \
     -H "Content-Type: application/json" \
     -d '{
       "job_type": "metadata_extraction",
       "data_source_path": "https://...",
       "workflow_id": "test_workflow_123"
     }'
   ```

2. **Test with workflow_id and source_id:**
   ```bash
   curl -X POST http://localhost:8080/jobs/create \
     -H "Content-Type: application/json" \
     -d '{
       "job_type": "metadata_extraction",
       "data_source_path": "https://...",
       "workflow_id": "test_workflow_123",
       "source_id": "test_source_456"
     }'
   ```

3. **Test with environment variable:**
   ```bash
   export NUVYN_JOB_PAYLOAD='{"workflow_id":"env_workflow_123","source_id":"env_source_456"}'
   python3 executor/main.py metadata_extraction /path/to/data csv default --write-to-db
   ```

4. **Test error handling (missing workflow_id):**
   ```bash
   curl -X POST http://localhost:8080/jobs/create \
     -H "Content-Type: application/json" \
     -d '{
       "job_type": "metadata_extraction",
       "data_source_path": "https://..."
     }'
   # Should return 400 error: "Missing required field: workflow_id"
   ```

---

## 📚 Backward Compatibility

### Breaking Changes

- ❌ `source_id` is no longer required in API requests
- ❌ Queries using only `source_id` may return multiple workflows
- ❌ Old code expecting `source_id` as primary identifier will break

### Migration Path

1. **Phase 1:** Deploy new code with both `workflow_id` and `source_id` support
2. **Phase 2:** Update backend to send `workflow_id` in all requests
3. **Phase 3:** Update all queries to use `workflow_id` as primary identifier
4. **Phase 4:** Remove `source_id` requirement from old code paths

---

## 🔐 Security Considerations

- `workflow_id` must be provided by the backend (never auto-generated)
- `source_id` is optional and used only for filtering
- Both values are stored in plain text in the database
- Consider adding indexes on `workflow_id` for better query performance

---

## 📊 Performance Considerations

### Recommended Indexes

```sql
-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_sources_workflow_id 
ON hive_metastore._executor_metadata.sources(workflow_id);

CREATE INDEX IF NOT EXISTS idx_tables_workflow_id 
ON hive_metastore._executor_metadata.tables(workflow_id);

CREATE INDEX IF NOT EXISTS idx_columns_workflow_id 
ON hive_metastore._executor_metadata.columns(workflow_id);

-- Composite index for workflow_id + source_id queries
CREATE INDEX IF NOT EXISTS idx_columns_workflow_source 
ON hive_metastore._executor_metadata.columns(workflow_id, source_id);
```

---

## 📞 Support & Troubleshooting

### Common Issues

1. **Error: "workflow_id is required"**
   - **Solution:** Ensure backend sends `workflow_id` in request or environment variable

2. **Queries returning no results**
   - **Solution:** Check that you're using `workflow_id` instead of `source_id` for primary queries

3. **Schema errors on table creation**
   - **Solution:** Drop existing tables and let executor recreate them with new schema

### Debug Logging

Enable debug logging to see workflow_id extraction:

```python
# In executor code
logger.debug(f"Extracted workflow_id: {workflow_id}")
logger.debug(f"Extracted source_id: {source_id}")
```

---

## 📝 Changelog

### Version 2.0 (Current)
- ✅ Added `workflow_id` as primary identifier
- ✅ Made `source_id` optional for filtering
- ✅ Updated all tables to include both columns
- ✅ Updated API to require `workflow_id`
- ✅ Added environment variable support for both fields
- ✅ Updated query methods to support workflow_id-based queries

### Version 1.0 (Deprecated)
- Used `source_id` as primary identifier
- `source_id` was required and auto-generated

---

## 🔗 Related Documentation

- [WRITE_TO_DATABASE_GUIDE.md](./WRITE_TO_DATABASE_GUIDE.md) - Database writing guide
- [BACKEND_INTEGRATION_GUIDE.md](./BACKEND_INTEGRATION_GUIDE.md) - Backend integration guide
- [README.md](./README.md) - Main project documentation

---

**Document Version:** 1.0  
**Last Updated:** 2025  
**Maintained By:** Nuvyn Development Team

