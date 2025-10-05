# Executor Script – Technical Design Document

## 🔹 Overview

The Executor Script is a Python-based module that will be installed and executed in the client's Databricks environment. Its primary role is to:

- Connect to client-provided data sources (configured via your platform)
- Extract metadata about those sources (schema, table definitions, data types, row counts, freshness, etc.)
- Store metadata inside the client's Databricks (for audit/traceability)
- Send metadata back to your platform for further processing (e.g., automated data warehouse generation)

The Executor Script acts as a bridge between the client's environment and your central platform.

## 🔹 Key Responsibilities

### Metadata Extraction
- Detect schema, tables, and columns from configured sources (e.g., MySQL, PostgreSQL, Snowflake, APIs, flat files, etc.)
- Read actual data from client data sources (Azure Blob Storage, Amazon S3, databases, data lakes, etc.)
- Collect metadata attributes:
  - Table name
  - Column names & data types
  - Primary keys, foreign keys
  - Row counts, sample values
  - Last updated timestamp
  - Data quality metrics from actual data reads

### Schema Management
- Check and validate required `_executor_metadata` schema in client's Databricks environment
- Ensure proper schema structure for storing extracted information
- Create and manage 5 core tables: `sources`, `tables`, `columns`, `executor_runs`, and `logs`

### Metadata Storage
- Store a copy of extracted metadata inside the client's Databricks workspace (under `_executor_metadata` schema)
- This ensures transparency and client-side ownership of metadata

### Metadata Transmission
- Push metadata back to your platform through secure REST API calls
- Format: JSON or Parquet (depending on use case)

### Audit Logging
- Maintain logs inside client environment for:
  - When metadata was collected
  - Which data sources were scanned
  - Executor version used
  - Schema validation results

## 🔹 Expected Flow

1. **Installation**
   - Executor is packaged as a pip-installable module (executor-script)
   - Installed into client's Databricks cluster:
     ```bash
     %pip install git+https://github.com/<org>/executor-script.git@main
     ```

2. **Execution**
   - Triggered by your platform through a Databricks Job or API call
   - Runs main entrypoint:
     ```bash
     executor --mode metadata --datasource mysql --config config.json
     ```

3. **Schema Validation**
   - Executor checks for required audit and metadata schemas
   - Creates or validates schema structure if needed

4. **Metadata Extraction**
   - Executor connects to configured data sources using credentials passed from your platform (stored securely as secrets)
   - Reads schema, tables, and columns
   - Performs actual data reads from client data sources (Azure Blob Storage, Amazon S3, databases, data lakes, etc.)
   - Extracts data quality metrics and sample data for analysis

5. **Metadata Storage in Client Environment**
   - Writes metadata to validated `_executor_metadata` schema:
     - `_executor_metadata.sources` - Data source registry
     - `_executor_metadata.tables` - Table metadata per source
     - `_executor_metadata.columns` - Column metadata per table
     - `_executor_metadata.executor_runs` - Execution audit trail
     - `_executor_metadata.logs` - Detailed execution logs

6. **Metadata Sharing with Platform**
   - Sends metadata to your platform's REST API:
     ```http
     POST https://<your-platform>/api/metadata
     Authorization: Bearer <client-api-key>
     Body: { ... metadata JSON ... }
     ```

7. **Logging**
   - Logs stored in Databricks DBFS under:
     - `dbfs:/executor/logs/<date>/<run_id>.log`

## 🔹 Internal Design

### A. Executor Structure
```
executor/
│── __init__.py
│── main.py        # Entry point
│── config.py      # Handles client configs
│── schema/        # Schema validation and management
│── datasource/    # Connectors for MySQL, Postgres, APIs, etc.
│── data_reader/   # Data reading from Azure Blob, S3, databases, data lakes
│── metadata/      # Functions for metadata extraction
│── storage/       # Write to Databricks
│── transport/     # Push metadata to platform via REST APIs
│── logger.py      # Logging utils
```

### B. Main Entry Point
```python
def run():
    config = load_config()
    validate_schemas(config)
    data = read_client_data(config)
    metadata = extract_metadata(config, data)
    store_metadata(metadata, config)
    send_metadata(metadata, config)
    log_run(config)
```

## 🔹 Schema Design for Client Databricks

### Schema: `_executor_metadata`

The executor creates and manages a dedicated schema with 5 core tables:

**1. `sources`** - Data source registry
- Stores configured data sources (MySQL, PostgreSQL, Snowflake, APIs, etc.)
- Includes source details, connection info (masked), and timestamps

**2. `tables`** - Table metadata
- Stores table information per source
- Includes table names, schema names, row counts, and refresh timestamps

**3. `columns`** - Column metadata
- Stores detailed column information per table
- Includes data types, nullability, primary keys, sample values, and distinct counts

**4. `executor_runs`** - Execution audit trail
- Tracks each executor run with version, status, timestamps, and error details

**5. `logs`** - Detailed logging
- Stores detailed logs for each execution run with log levels and messages

### Relationships
- One source → many tables
- One table → many columns
- One run → many logs

### Example Query
```sql
-- Get all columns for a given source
SELECT s.source_name, t.table_name, c.column_name, c.data_type
FROM _executor_metadata.sources s
JOIN _executor_metadata.tables t ON s.source_id = t.source_id
JOIN _executor_metadata.columns c ON t.table_id = c.table_id
WHERE s.source_name = 'MySQL_Orders';
```

## 🔹 Security Considerations

- Credentials are never hardcoded → always retrieved via Databricks Secrets
- Metadata only (not raw data) is shared with your platform
- Executor version is tracked to ensure upgrade compatibility
- Schema validation ensures data integrity and proper structure
- Connection details are masked for security in the sources table

## 🔹 Data Reading Capabilities

The executor can read data from various client data sources:

- **Azure Blob Storage** - Read files and data from Azure storage accounts
- **Amazon S3** - Access data from S3 buckets and objects
- **Databases** - Direct database connections (MySQL, PostgreSQL, Snowflake, etc.)
- **Data Lakes** - Read from data lake storage systems
- **APIs** - Fetch data from REST APIs and web services
- **File Systems** - Access local and network file systems

This enables the executor to:
- Extract actual data samples for quality analysis
- Perform data profiling and statistics
- Validate data integrity and consistency
- Generate comprehensive metadata including data quality metrics

## 🔹 Future Extensions

- Enhanced data quality checks (null % per column, outliers, patterns)
- Advanced data profiling and statistics
- Integration with data governance rules
- Enhanced schema management and validation
- Real-time data monitoring and change detection

## ✅ Summary

The Executor Script is a lightweight Python agent that lives in the client's Databricks environment. Its job is to:

1. Install via pip
2. Validate and manage required schemas
3. Connect to client sources
4. Extract metadata
5. Store metadata locally + send back to platform via REST APIs
6. Log activity for audit

It is the core foundation for enabling automated metadata-driven warehouse creation with proper schema management and REST API integration.
