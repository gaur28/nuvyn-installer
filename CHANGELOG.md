# Changelog - Recent Updates

## Latest Changes (Current Session)

### 🔧 Database Schema Updates

#### 1. Added Auto-Incrementing ID Columns

All tables now have an `id` field that auto-increments as the primary key:

```sql
CREATE TABLE hive_metastore._executor_metadata.sources (
    id BIGINT GENERATED ALWAYS AS IDENTITY,  -- ✅ Auto-increment PK
    source_id STRING,                        -- Backend-provided (non-unique)
    source_path STRING,
    source_type STRING,
    extraction_timestamp TIMESTAMP,
    files_found INT,
    total_size_bytes BIGINT
);
```

**Affected Tables:**
- `sources` - Added `id` column
- `tables` - Added `id` column
- `columns` - Added `id` column
- `executor_runs` - Added `id` column
- `logs` - Added `id` column

**Key Changes:**
- `id`: Unique, auto-incrementing primary key
- `source_id`: Non-unique, provided by your backend
- Multiple entries can share the same `source_id`

### 🔄 source_id Handling

#### 2. Backend-Provided source_id Support

The executor now accepts `source_id` from your backend instead of generating it:

**Before:**
```python
# Executor generated source_id
source_id = str(uuid.uuid4())
```

**After:**
```python
# Executor accepts source_id from backend
source_id = job_config.job_metadata.get('source_id')
```

**How to Provide source_id:**

```python
# Via HTTP API
curl -X POST http://executor:8080/jobs/create \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "metadata_extraction",
    "data_source_path": "https://...",
    "job_metadata": {
      "source_id": "your-backend-id"  # ✅ Your identifier
    },
    "tenant_id": "client-123"
  }'
```

### 🏗️ Catalog Context Fix

#### 3. Proper Unity Catalog Integration

The executor now uses the correct catalog context:

**Before:**
```python
cursor.execute("CREATE SCHEMA IF NOT EXISTS _executor_metadata")
# Created in wrong catalog
```

**After:**
```python
cursor.execute("USE CATALOG hive_metastore")
cursor.execute("CREATE SCHEMA IF NOT EXISTS hive_metastore._executor_metadata")
cursor.execute("INSERT INTO hive_metastore._executor_metadata.sources ...")
```

**Location:** `hive_metastore._executor_metadata`

### 📦 Updated Files

1. **executor/storage/databricks_writer.py**
   - Added `id` columns to all table creation statements
   - Updated `write_metadata()` to accept `source_id` parameter
   - Fixed catalog context to use `hive_metastore`

2. **executor/metadata/extractor.py**
   - Reads `source_id` from `job_config.job_metadata`
   - Passes `source_id` to `db_writer.write_metadata()`

### 🎯 Impact

**Breaking Changes:**
- Existing databases will need schema migration to add `id` columns
- Old queries without catalog prefix may not work

**Migration Required:**
```sql
-- Add id columns to existing tables
ALTER TABLE hive_metastore._executor_metadata.sources 
ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY;

ALTER TABLE hive_metastore._executor_metadata.tables 
ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY;

ALTER TABLE hive_metastore._executor_metadata.columns 
ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY;
```

### 📊 Query Changes

**New Query Pattern:**

```python
# Query by source_id (non-unique, can have multiple rows)
query = """
SELECT * FROM hive_metastore._executor_metadata.sources
WHERE source_id = 'your-backend-id'
ORDER BY id DESC  # Latest entry first
"""

# Get specific entry by id (unique)
query = """
SELECT * FROM hive_metastore._executor_metadata.sources
WHERE id = 123
"""
```

### 🔍 How to Use

**From Your Backend:**

1. **Trigger Job with source_id:**
```python
POST /jobs/create
{
  "job_type": "metadata_extraction",
  "data_source_path": "https://...",
  "job_metadata": {
    "source_id": "backend-source-123"
  }
}
```

2. **Query Results by source_id:**
```python
SELECT * FROM hive_metastore._executor_metadata.sources
WHERE source_id = 'backend-source-123'
```

3. **Get Latest Entry:**
```python
SELECT * FROM hive_metastore._executor_metadata.sources
WHERE source_id = 'backend-source-123'
ORDER BY id DESC
LIMIT 1
```

### ✅ Benefits

1. **Reusable source_id**: Same backend identifier can track multiple extractions
2. **Query Flexibility**: Query by your backend's ID or unique database ID
3. **Audit Trail**: Auto-incrementing `id` provides execution history
4. **Proper Relationships**: Unique `id` enables proper foreign key constraints (future)

### 📝 Documentation Updates

- `doc/BACKEND_DATA_FLOW.md` - Complete data flow documentation
- `doc/SOURCE_ID_USAGE.md` - How to use source_id
- `doc/SETUP_DATABRICKS_CREDENTIALS.md` - Setup guide

### 🚨 Breaking Changes Summary

| Feature | Before | After |
|---------|--------|-------|
| source_id | Auto-generated UUID | Provided by backend |
| Database ID | None | Auto-incrementing `id` column |
| Catalog | Not specified | `hive_metastore` |
| Primary Key | None | `id` (auto-increment) |

### 🔄 Migration Path

1. **Drop existing tables** (if created without `id` columns)
2. **Re-run executor** with `--write-to-db` flag
3. **Tables will be recreated** with `id` columns automatically
4. **Update your queries** to use `hive_metastore` catalog prefix

