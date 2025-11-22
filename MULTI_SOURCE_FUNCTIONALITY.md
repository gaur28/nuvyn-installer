# Multi-Source Metadata Extraction Documentation

## 📋 Overview

The executor now supports processing **multiple data sources** in a single job, allowing you to extract metadata from multiple sources simultaneously while maintaining clear differentiation between sources using `source_id`. All sources share the same `workflow_id` but have unique `source_id` values for filtering and identification.

**Version:** 2.0  
**Date:** 2025  
**Status:** ✅ Implemented

---

## 🎯 Key Features

### ✅ Multi-Source Processing
- Process multiple data sources in a single job execution
- Each source is processed individually with its own metadata extraction
- All sources share the same `workflow_id` (workflow-level identifier)
- Each source has its own `source_id` (source-level identifier)

### ✅ Source Differentiation
- Metadata is stored with both `workflow_id` and `source_id`
- Query by `workflow_id` to get all sources in a workflow
- Filter by `source_id` to get specific source data
- Each source's metadata is written separately to the database

### ✅ Backward Compatibility
- Single source processing still works (backward compatible)
- Existing API calls continue to function
- No breaking changes for single-source workflows

---

## 📊 How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Single Job                           │
│                  (workflow_id)                          │
└─────────────────────────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
   ┌────▼────┐     ┌────▼────┐     ┌────▼────┐
   │ Source 1│     │ Source 2│     │ Source 3│
   │source_id│     │source_id│     │source_id│
   └────┬────┘     └────┬────┘     └────┬────┘
        │               │               │
        └───────────────┼───────────────┘
                        │
            ┌───────────▼───────────┐
            │  Metadata Storage    │
            │  (Databricks SQL)    │
            │  - workflow_id       │
            │  - source_id         │
            └──────────────────────┘
```

### Processing Flow

1. **Job Creation**: Backend sends job with `workflow_id` and `sources` array
2. **Source Iteration**: Executor processes each source individually
3. **Metadata Extraction**: Each source gets its own metadata extraction
4. **Database Storage**: Each source's metadata is written with:
   - Same `workflow_id` (links all sources together)
   - Unique `source_id` (differentiates each source)
5. **Result Aggregation**: Results are aggregated and returned

---

## 🔧 API Usage

### Single Source (Backward Compatible)

```json
{
  "job_type": "metadata_extraction",
  "data_source_path": "https://storage1.blob.core.windows.net/container1",
  "workflow_id": "workflow_123",
  "source_id": "source_1",
  "data_source_type": "azure_blob"
}
```

### Multiple Sources (New Feature)

```json
{
  "job_type": "metadata_extraction",
  "workflow_id": "workflow_123",
  "sources": [
    {
      "source_id": "source_1",
      "data_source_path": "https://storage1.blob.core.windows.net/container1",
      "data_source_type": "azure_blob"
    },
    {
      "source_id": "source_2",
      "data_source_path": "https://storage2.blob.core.windows.net/container2",
      "data_source_type": "azure_blob"
    },
    {
      "source_id": "source_3",
      "data_source_path": "https://storage3.blob.core.windows.net/container3",
      "data_source_type": "azure_blob"
    }
  ]
}
```

### API Request Example

```bash
curl -X POST http://localhost:8080/jobs/create \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "metadata_extraction",
    "workflow_id": "workflow_123",
    "sources": [
      {
        "source_id": "source_1",
        "data_source_path": "https://storage1.blob.core.windows.net/container1",
        "data_source_type": "azure_blob"
      },
      {
        "source_id": "source_2",
        "data_source_path": "https://storage2.blob.core.windows.net/container2",
        "data_source_type": "azure_blob"
      }
    ]
  }'
```

---

## 🌐 Environment Variable Support

### Multi-Source via Environment Variable

```bash
export NUVYN_JOB_PAYLOAD='{
  "job_type": "metadata_extraction",
  "workflow_id": "workflow_123",
  "sources": [
    {
      "source_id": "source_1",
      "data_source_path": "https://storage1.blob.core.windows.net/container1",
      "data_source_type": "azure_blob"
    },
    {
      "source_id": "source_2",
      "data_source_path": "https://storage2.blob.core.windows.net/container2",
      "data_source_type": "azure_blob"
    }
  ]
}'
```

Then run:
```bash
python3 executor/main.py metadata_extraction "" auto default --write-to-db
```

---

## 📝 Response Format

### Multi-Source Response

```json
{
  "job_id": "job_abc123",
  "status": "completed",
  "success": true,
  "execution_time": 45.2,
  "result_data": {
    "workflow_id": "workflow_123",
    "total_sources": 2,
    "sources_processed": 2,
    "sources_failed": 0,
    "sources": [
      {
        "source_id": "source_1",
        "source_path": "https://storage1.blob.core.windows.net/container1",
        "status": "success",
        "metadata": {
          "files_found": 10,
          "total_size_bytes": 1048576,
          "files": [...],
          "workflow_id": "workflow_123",
          "source_id": "source_1",
          "written_to_db": true
        }
      },
      {
        "source_id": "source_2",
        "source_path": "https://storage2.blob.core.windows.net/container2",
        "status": "success",
        "metadata": {
          "files_found": 15,
          "total_size_bytes": 2097152,
          "files": [...],
          "workflow_id": "workflow_123",
          "source_id": "source_2",
          "written_to_db": true
        }
      }
    ]
  }
}
```

---

## 🔍 Querying Multi-Source Metadata

### Get All Sources for a Workflow

```sql
-- Get all sources in a workflow
SELECT 
    s.workflow_id,
    s.source_id,
    s.source_path,
    s.source_type,
    s.extraction_timestamp,
    s.files_found,
    COUNT(DISTINCT t.table_name) as table_count,
    COUNT(c.column_name) as total_columns
FROM hive_metastore._executor_metadata.sources s
LEFT JOIN hive_metastore._executor_metadata.tables t 
    ON s.workflow_id = t.workflow_id AND s.source_id = t.source_id
LEFT JOIN hive_metastore._executor_metadata.columns c 
    ON t.workflow_id = c.workflow_id 
    AND t.source_id = c.source_id 
    AND t.table_name = c.table_name
WHERE s.workflow_id = 'workflow_123'
GROUP BY s.workflow_id, s.source_id, s.source_path, s.source_type, s.extraction_timestamp, s.files_found
ORDER BY s.source_id;
```

### Get Specific Source Data

```sql
-- Get metadata for a specific source
SELECT 
    c.workflow_id,
    c.source_id,
    c.table_name,
    c.column_name,
    c.data_type,
    c.position,
    c.is_nullable
FROM hive_metastore._executor_metadata.columns c
WHERE c.workflow_id = 'workflow_123'
  AND c.source_id = 'source_1'
ORDER BY c.table_name, c.position;
```

### Compare Sources in a Workflow

```sql
-- Compare table counts across sources
SELECT 
    source_id,
    COUNT(DISTINCT table_name) as table_count,
    COUNT(column_name) as column_count,
    SUM(CASE WHEN is_nullable THEN 1 ELSE 0 END) as nullable_columns
FROM hive_metastore._executor_metadata.columns
WHERE workflow_id = 'workflow_123'
GROUP BY source_id
ORDER BY source_id;
```

---

## 🛠️ Implementation Details

### Code Changes

#### 1. **JobConfig** (`executor/config.py`)
- Added `sources: List[Dict[str, Any]]` field
- Made `data_source_path` optional (defaults to empty string)
- Supports both single and multiple sources

#### 2. **MetadataExtractor** (`executor/metadata/extractor.py`)
- Updated `extract_metadata()` to detect multi-source mode
- Added `_extract_metadata_multiple_sources()` method
- Added `_extract_single_source_metadata()` helper method
- Each source is processed individually with its own `source_id`

#### 3. **API Server** (`executor/api_server.py`)
- Updated `create_job()` endpoint to accept `sources` array
- Validates that each source has `source_id` and `data_source_path`
- Supports both single-source and multi-source modes

#### 4. **Main Entry Point** (`executor/main.py`)
- Added `get_sources_from_environment()` function
- Updated metadata extraction to handle multiple sources
- Detects multi-source mode from environment variable

#### 5. **JobManager** (`executor/job_manager.py`)
- Updated `create_job()` to accept `sources` parameter
- Passes sources to JobConfig

---

## ✅ Validation & Error Handling

### Source Validation

Each source in the `sources` array must have:
- ✅ `source_id` - Unique identifier for the source (required)
- ✅ `data_source_path` - Path to the data source (required)
- ✅ `data_source_type` - Type of data source (optional, defaults to job-level type)

### Error Handling

- **Missing source_id**: Job fails with validation error
- **Missing data_source_path**: Source is skipped with warning
- **Source processing failure**: Failed source is logged, other sources continue
- **Workflow_id missing**: Job fails immediately (required for multi-source)

### Partial Success

If some sources fail:
- Successful sources are still written to database
- Failed sources are logged in results
- Job status reflects overall success/failure
- Individual source status is tracked

---

## 📊 Database Schema

All tables support both `workflow_id` and `source_id`:

### Sources Table
```sql
SELECT workflow_id, source_id, source_path, files_found
FROM hive_metastore._executor_metadata.sources
WHERE workflow_id = 'workflow_123';
```

### Tables Table
```sql
SELECT workflow_id, source_id, table_name, row_count
FROM hive_metastore._executor_metadata.tables
WHERE workflow_id = 'workflow_123';
```

### Columns Table
```sql
SELECT workflow_id, source_id, table_name, column_name, data_type
FROM hive_metastore._executor_metadata.columns
WHERE workflow_id = 'workflow_123' AND source_id = 'source_1';
```

---

## 🧪 Testing Examples

### Test 1: Multi-Source via API

```bash
curl -X POST http://localhost:8080/jobs/create \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "metadata_extraction",
    "workflow_id": "test_workflow_001",
    "sources": [
      {
        "source_id": "amazon_data",
        "data_source_path": "https://storage.blob.core.windows.net/amazon",
        "data_source_type": "azure_blob"
      },
      {
        "source_id": "ebay_data",
        "data_source_path": "https://storage.blob.core.windows.net/ebay",
        "data_source_type": "azure_blob"
      }
    ]
  }'
```

### Test 2: Verify Database Storage

```sql
-- Check all sources were stored
SELECT workflow_id, source_id, source_path, extraction_timestamp
FROM hive_metastore._executor_metadata.sources
WHERE workflow_id = 'test_workflow_001'
ORDER BY source_id;
```

### Test 3: Query Specific Source

```sql
-- Get all columns for amazon_data source
SELECT 
    table_name,
    column_name,
    data_type,
    position
FROM hive_metastore._executor_metadata.columns
WHERE workflow_id = 'test_workflow_001'
  AND source_id = 'amazon_data'
ORDER BY table_name, position;
```

---

## 🎯 Use Cases

### Use Case 1: Multiple Data Sources in One Workflow
**Scenario**: Extract metadata from multiple Azure Blob containers in a single workflow

```json
{
  "workflow_id": "workflow_retail_data",
  "sources": [
    {"source_id": "amazon", "data_source_path": "https://.../amazon"},
    {"source_id": "ebay", "data_source_path": "https://.../ebay"},
    {"source_id": "walmart", "data_source_path": "https://.../walmart"}
  ]
}
```

### Use Case 2: Different Source Types
**Scenario**: Extract from different source types (Azure Blob, S3, Database)

```json
{
  "workflow_id": "workflow_mixed_sources",
  "sources": [
    {
      "source_id": "azure_data",
      "data_source_path": "https://storage.blob.core.windows.net/container",
      "data_source_type": "azure_blob"
    },
    {
      "source_id": "s3_data",
      "data_source_path": "s3://bucket/path",
      "data_source_type": "aws_s3"
    }
  ]
}
```

### Use Case 3: Incremental Source Addition
**Scenario**: Add new sources to existing workflow

```sql
-- Query existing sources
SELECT DISTINCT source_id 
FROM hive_metastore._executor_metadata.sources
WHERE workflow_id = 'workflow_123';

-- Add new source with same workflow_id
-- (Backend creates new job with additional source)
```

---

## 🔄 Migration from Single Source

### Before (Single Source)
```json
{
  "job_type": "metadata_extraction",
  "data_source_path": "https://...",
  "workflow_id": "workflow_123",
  "source_id": "source_1"
}
```

### After (Multi-Source)
```json
{
  "job_type": "metadata_extraction",
  "workflow_id": "workflow_123",
  "sources": [
    {
      "source_id": "source_1",
      "data_source_path": "https://..."
    }
  ]
}
```

**Note**: Single source format still works for backward compatibility!

---

## 📈 Performance Considerations

### Parallel Processing
- Currently processes sources **sequentially** (one after another)
- Future enhancement: Parallel processing for better performance
- Each source is independent, making parallelization straightforward

### Database Writes
- Each source writes its metadata separately
- All writes use the same database connection
- Transactions are per-source (not atomic across all sources)

### Resource Usage
- Memory: Scales with number of sources
- Network: Each source requires separate connection
- Time: Total time = sum of individual source processing times

---

## 🚨 Limitations & Future Enhancements

### Current Limitations
- ⚠️ Sources are processed sequentially (not in parallel)
- ⚠️ If one source fails, others continue (no rollback)
- ⚠️ No built-in retry mechanism for failed sources

### Future Enhancements
- 🔮 Parallel source processing
- 🔮 Atomic transactions across sources
- 🔮 Automatic retry for failed sources
- 🔮 Source-level progress tracking
- 🔮 Streaming results as sources complete

---

## 📞 Support

### Common Issues

1. **"Missing required field: source_id"**
   - Ensure each source in the array has a `source_id` field

2. **"workflow_id is required"**
   - Multi-source mode requires `workflow_id` to be provided

3. **"Source X failed but others succeeded"**
   - This is expected behavior - check individual source errors in results

### Debug Logging

Enable debug logging to see source processing:
```python
# Logs will show:
# 🔍 Extracting metadata from 3 sources
# 📊 Processing source 1/3: source_1
# ✅ Source source_1 processed successfully
# 📊 Processing source 2/3: source_2
# ...
```

---

## 📚 Related Documentation

- [WORKFLOW_ID_MIGRATION.md](./WORKFLOW_ID_MIGRATION.md) - Workflow ID migration guide
- [WRITE_TO_DATABASE_GUIDE.md](./WRITE_TO_DATABASE_GUIDE.md) - Database writing guide
- [BACKEND_INTEGRATION_GUIDE.md](./BACKEND_INTEGRATION_GUIDE.md) - Backend integration

---

**Document Version:** 1.0  
**Last Updated:** 2025  
**Status:** ✅ Production Ready

