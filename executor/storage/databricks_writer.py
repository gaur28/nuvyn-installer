"""
Databricks SQL Writer for storing metadata in _executor_metadata schema
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from databricks import sql
from logger import get_logger

logger = get_logger(__name__)


class DatabricksWriter:
    """Writes metadata to Databricks SQL tables"""
    
    def __init__(self, 
                 server_hostname: str,
                 http_path: str,
                 access_token: str):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.connection = None
        self.schema_name = "_executor_metadata"
    
    def connect(self) -> bool:
        """Connect to Databricks SQL Warehouse"""
        try:
            logger.info("🔗 Connecting to Databricks SQL Warehouse...")
            
            self.connection = sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            
            logger.info("✅ Databricks SQL connection established")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Databricks SQL: {e}")
            return False
    
    def disconnect(self):
        """Close Databricks SQL connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("🔌 Databricks SQL connection closed")
    
    def create_schema_and_tables(self) -> bool:
        """Create the _executor_metadata schema and tables if they don't exist"""
        try:
            logger.info(f"🏗️ Creating schema and tables...")
            
            cursor = self.connection.cursor()
            
            # Set catalog context (use hive_metastore for compatibility)
            logger.info("Setting catalog context to hive_metastore...")
            cursor.execute("USE CATALOG hive_metastore")
            
            # Create schema in hive_metastore catalog
            logger.info(f"Creating schema in hive_metastore catalog...")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{self.schema_name}")
            logger.info(f"✅ Schema created: hive_metastore.{self.schema_name}")
            
            # Switch to the schema
            cursor.execute(f"USE SCHEMA {self.schema_name}")
            
            # Create sources table (id is auto-increment PK, source_id is backend-provided, non-unique)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.sources (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    source_id STRING,
                    source_path STRING,
                    source_type STRING,
                    extraction_timestamp TIMESTAMP,
                    files_found INT,
                    total_size_bytes BIGINT
                )
            """)
            logger.info(f"✅ Table created: {self.schema_name}.sources")
            
            # Create tables table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.tables (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    source_id STRING,
                    table_name STRING,
                    file_path STRING,
                    file_type STRING,
                    row_count BIGINT,
                    column_count INT,
                    size_bytes BIGINT
                )
            """)
            logger.info(f"✅ Table created: {self.schema_name}.tables")
            
            # Create columns table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.columns (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    source_id STRING,
                    table_name STRING,
                    column_name STRING,
                    data_type STRING,
                    position INT,
                    is_nullable BOOLEAN,
                    sample_values STRING
                )
            """)
            logger.info(f"✅ Table created: {self.schema_name}.columns")
            
            # Create executor_runs table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.executor_runs (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    run_id STRING,
                    executor_version STRING,
                    source_id STRING,
                    run_mode STRING,
                    status STRING,
                    error_message STRING,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP
                )
            """)
            logger.info(f"✅ Table created: {self.schema_name}.executor_runs")
            
            # Create logs table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.logs (
                    id BIGINT GENERATED ALWAYS AS IDENTITY,
                    log_id STRING,
                    run_id STRING,
                    log_level STRING,
                    log_message STRING,
                    log_timestamp TIMESTAMP
                )
            """)
            logger.info(f"✅ Table created: {self.schema_name}.logs")
            
            cursor.close()
            logger.info(f"✅ Schema and all tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create schema and tables: {e}")
            return False
    
    def write_metadata(self, metadata: Dict[str, Any], source_id: str = None) -> bool:
        """Write extracted metadata to Databricks SQL tables
        
        Args:
            metadata: Metadata dict
            source_id: Required backend-provided source_id (non-unique). Must be provided by backend.
        """
        try:
            logger.info("💾 Writing metadata to Databricks SQL...")
            
            # source_id must be provided by backend - do not auto-generate
            if not source_id:
                error_msg = "source_id is required and must be provided by the backend server"
                logger.error(f"❌ {error_msg}")
                raise ValueError(error_msg)
            
            logger.info(f"Using backend-provided source_id: {source_id}")
            
            # Write to sources table
            self._write_source(source_id, metadata)
            
            # Write to tables table
            for file_info in metadata.get('files', []):
                self._write_table(source_id, file_info)
                
                # Write to columns table
                if 'columns' in file_info:
                    for column in file_info['columns']:
                        self._write_column(source_id, file_info['name'], column)
            
            # Store source_id in metadata for reference
            metadata['source_id'] = source_id
            
            logger.info(f"✅ Metadata written successfully (source_id: {source_id})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to write metadata: {e}")
            return False
    
    def _write_source(self, source_id: str, metadata: Dict[str, Any]):
        """Write to sources table"""
        try:
            cursor = self.connection.cursor()
            
            # Convert timestamp
            extraction_timestamp = datetime.now(timezone.utc)
            
            cursor.execute(f"""
                INSERT INTO hive_metastore.{self.schema_name}.sources
                (source_id, source_path, source_type, extraction_timestamp, files_found, total_size_bytes)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                source_id,
                metadata.get('source_path', ''),
                metadata.get('source_type', ''),
                extraction_timestamp,
                metadata.get('files_found', 0),
                metadata.get('total_size_bytes', 0)
            ))
            
            cursor.close()
            logger.info(f"✅ Source metadata written: {source_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to write source metadata: {e}")
            raise
    
    def _write_table(self, source_id: str, file_info: Dict[str, Any]):
        """Write to tables table"""
        try:
            cursor = self.connection.cursor()
            
            cursor.execute(f"""
                INSERT INTO hive_metastore.{self.schema_name}.tables
                (source_id, table_name, file_path, file_type, row_count, column_count, size_bytes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                source_id,
                file_info.get('name', ''),
                file_info.get('path', ''),
                file_info.get('file_type', ''),
                file_info.get('row_count', 0),
                file_info.get('column_count', 0),
                file_info.get('size_bytes', 0)
            ))
            
            cursor.close()
            logger.info(f"✅ Table metadata written: {file_info.get('name', '')}")
            
        except Exception as e:
            logger.error(f"❌ Failed to write table metadata: {e}")
            raise
    
    def _write_column(self, source_id: str, table_name: str, column: Dict[str, Any]):
        """Write to columns table"""
        try:
            cursor = self.connection.cursor()
            
            # Convert sample_values array to string (since Databricks might not support ARRAY in all modes)
            sample_values = str(column.get('sample_values', []))
            
            cursor.execute(f"""
                INSERT INTO hive_metastore.{self.schema_name}.columns
                (source_id, table_name, column_name, data_type, position, is_nullable, sample_values)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                source_id,
                table_name,
                column.get('column_name', ''),
                column.get('data_type', ''),
                column.get('position', 0),
                column.get('is_nullable', True),
                sample_values
            ))
            
            cursor.close()
            logger.debug(f"✅ Column metadata written: {column.get('column_name', '')}")
            
        except Exception as e:
            logger.error(f"❌ Failed to write column metadata: {e}")
            raise
    
    def query_metadata(self, source_id: str = None) -> Dict[str, Any]:
        """Query metadata from Databricks SQL"""
        try:
            cursor = self.connection.cursor()
            
            if source_id:
                # Query specific source
                cursor.execute(f"""
                    SELECT * FROM hive_metastore.{self.schema_name}.sources
                    WHERE source_id = ?
                """, (source_id,))
            else:
                # Query all sources
                cursor.execute(f"""
                    SELECT * FROM hive_metastore.{self.schema_name}.sources
                    ORDER BY extraction_timestamp DESC
                    LIMIT 10
                """)
            
            results = cursor.fetchall()
            cursor.close()
            
            return {"sources": results}
            
        except Exception as e:
            logger.error(f"❌ Failed to query metadata: {e}")
            return {"sources": []}
    
    def get_source_statistics(self) -> Dict[str, Any]:
        """Get statistics about stored metadata"""
        try:
            cursor = self.connection.cursor()
            
            # Count sources
            cursor.execute(f"SELECT COUNT(*) FROM hive_metastore.{self.schema_name}.sources")
            source_count = cursor.fetchone()[0]
            
            # Count tables
            cursor.execute(f"SELECT COUNT(*) FROM hive_metastore.{self.schema_name}.tables")
            table_count = cursor.fetchone()[0]
            
            # Count columns
            cursor.execute(f"SELECT COUNT(*) FROM hive_metastore.{self.schema_name}.columns")
            column_count = cursor.fetchone()[0]
            
            cursor.close()
            
            return {
                "total_sources": source_count,
                "total_tables": table_count,
                "total_columns": column_count
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to get statistics: {e}")
            return {
                "total_sources": 0,
                "total_tables": 0,
                "total_columns": 0
            }
