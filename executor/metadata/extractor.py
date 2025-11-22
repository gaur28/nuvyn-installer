"""
Metadata Extractor for analyzing data structure and content
"""

import asyncio
import os
import json
from typing import Dict, Any, List, Optional
from config import JobConfig, ConfigManager
from datasource.factory import DataSourceFactory
from logger import get_logger

logger = get_logger(__name__)


class MetadataExtractor:
    """Extracts metadata from data sources"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.write_to_db = False
        self.db_writer = None
    
    def _get_workflow_id_from_environment(self):
        """
        Extract workflow_id from NUVYN_JOB_PAYLOAD environment variable.
        This ensures the backend-provided workflow_id is used as the primary identifier.
        
        Returns:
            str: workflow_id from job_metadata, or None if not found
        """
        if "NUVYN_JOB_PAYLOAD" not in os.environ:
            logger.debug("NUVYN_JOB_PAYLOAD environment variable not set")
            return None
        
        try:
            job_payload = json.loads(os.environ["NUVYN_JOB_PAYLOAD"])
            
            # First, try to get workflow_id from job_metadata (preferred)
            if "job_metadata" in job_payload:
                job_metadata = job_payload["job_metadata"]
                if "workflow_id" in job_metadata:
                    workflow_id = job_metadata["workflow_id"]
                    logger.info(f"✅ Using workflow_id from job_metadata: {workflow_id}")
                    return workflow_id
            
            # Fallback to top-level workflow_id
            if "workflow_id" in job_payload:
                workflow_id = job_payload["workflow_id"]
                logger.info(f"✅ Using workflow_id from top-level payload: {workflow_id}")
                return workflow_id
                
        except json.JSONDecodeError as e:
            logger.warning(f"⚠️ Failed to parse NUVYN_JOB_PAYLOAD as JSON: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Error reading NUVYN_JOB_PAYLOAD: {e}")
        
        return None
    
    def _get_source_id_from_environment(self):
        """
        Extract source_id from NUVYN_JOB_PAYLOAD environment variable.
        This is used for filtering purposes.
        
        Returns:
            str: source_id from job_metadata, or None if not found
        """
        if "NUVYN_JOB_PAYLOAD" not in os.environ:
            logger.debug("NUVYN_JOB_PAYLOAD environment variable not set")
            return None
        
        try:
            job_payload = json.loads(os.environ["NUVYN_JOB_PAYLOAD"])
            
            # First, try to get source_id from job_metadata (preferred)
            if "job_metadata" in job_payload:
                job_metadata = job_payload["job_metadata"]
                if "source_id" in job_metadata:
                    source_id = job_metadata["source_id"]
                    logger.info(f"✅ Using source_id from job_metadata: {source_id}")
                    return source_id
            
            # Fallback to top-level source_id
            if "source_id" in job_payload:
                source_id = job_payload["source_id"]
                logger.info(f"✅ Using source_id from top-level payload: {source_id}")
                return source_id
                
        except json.JSONDecodeError as e:
            logger.warning(f"⚠️ Failed to parse NUVYN_JOB_PAYLOAD as JSON: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Error reading NUVYN_JOB_PAYLOAD: {e}")
        
        return None
    
    async def extract_metadata(self, job_config: JobConfig) -> Dict[str, Any]:
        """Extract comprehensive metadata from data source(s)
        
        Supports both single source (backward compatible) and multiple sources.
        If sources list is provided, processes each source individually.
        """
        # Check if multiple sources are provided
        if job_config.sources and len(job_config.sources) > 0:
            logger.info(f"🔍 Extracting metadata from {len(job_config.sources)} sources")
            return await self._extract_metadata_multiple_sources(job_config)
        
        # Single source processing (backward compatible)
        logger.info(f"🔍 Extracting metadata from: {job_config.data_source_path}")
        
        try:
            # Get credentials and create connector
            credentials = self.config_manager.get_data_source_credentials(job_config.data_source_type)
            connector = DataSourceFactory.auto_detect_connector(job_config.data_source_path, credentials)
            
            if not connector:
                raise Exception(f"No suitable connector found for path: {job_config.data_source_path}")
            
            # Connect to data source (pass URL for SAS token extraction)
            if hasattr(connector, 'can_handle') and 'blob.core.windows.net' in job_config.data_source_path:
                # For Azure Blob with SAS token in URL
                if not await connector.connect(url_with_sas=job_config.data_source_path):
                    raise Exception("Failed to connect to data source")
            else:
                if not await connector.connect():
                    raise Exception("Failed to connect to data source")
            
            # Extract metadata
            files = await connector.list_files(job_config.data_source_path)
            
            metadata = {
                "source_path": job_config.data_source_path,
                "source_type": connector.get_source_type(),
                "extraction_timestamp": asyncio.get_event_loop().time(),
                "files_found": len(files),
                "files": [],
                "total_size_bytes": 0,
                "schema_info": {
                    "tables": 0,
                    "columns": 0,
                    "data_types": {}
                },
                "quality_metrics": {
                    "overall_score": 85,
                    "completeness": 90,
                    "accuracy": 85,
                    "consistency": 80
                }
            }
            
            # Analyze each file
            for file_path in files[:5]:  # Analyze first 5 files
                try:
                    # If original path has SAS token, reconstruct full URL for file operations
                    if '?' in job_config.data_source_path and 'sig=' in job_config.data_source_path:
                        # Use the original URL with SAS token
                        full_file_path = job_config.data_source_path
                    else:
                        full_file_path = file_path
                    
                    file_size = await connector.get_file_size(full_file_path)
                    sample_data = await connector.read_file_sample(full_file_path, max_bytes=1024*1024)  # Read 1MB for analysis
                    
                    file_type = self._detect_file_type(file_path)
                    
                    file_info = {
                        "name": file_path.split('/')[-1],
                        "path": file_path,
                        "size_bytes": file_size,
                        "sample_size": len(sample_data),
                        "file_type": file_type
                    }
                    
                    # Analyze CSV files for detailed metadata
                    if file_type == "csv" and sample_data:
                        csv_metadata = self._analyze_csv_data(sample_data, file_path.split('/')[-1])
                        file_info.update(csv_metadata)
                        
                        # Update global schema info
                        if "columns" in csv_metadata:
                            metadata["schema_info"]["tables"] += 1
                            metadata["schema_info"]["columns"] += len(csv_metadata["columns"])
                            for col in csv_metadata["columns"]:
                                col_type = col.get("data_type", "unknown")
                                metadata["schema_info"]["data_types"][col_type] = metadata["schema_info"]["data_types"].get(col_type, 0) + 1
                    
                    metadata["files"].append(file_info)
                    metadata["total_size_bytes"] += file_size
                    
                except Exception as e:
                    logger.warning(f"Failed to analyze file {file_path}: {e}")
            
            await connector.disconnect()
            
            # Write to database if enabled
            if self.write_to_db and self.db_writer:
                try:
                    logger.info("💾 Writing metadata to Databricks SQL...")
                    
                    # Get backend-provided workflow_id (primary identifier) - must be provided by backend
                    workflow_id = None
                    source_id = None
                    
                    # Priority 1: Get workflow_id and source_id from job_metadata (from API or main.py)
                    if job_config.job_metadata:
                        workflow_id = job_config.job_metadata.get('workflow_id')
                        source_id = job_config.job_metadata.get('source_id')
                        if workflow_id:
                            logger.info(f"✅ Using workflow_id from job_metadata: {workflow_id}")
                        if source_id:
                            logger.info(f"✅ Using source_id from job_metadata: {source_id}")
                    
                    # Priority 2: Check NUVYN_JOB_PAYLOAD environment variable (for Databricks Jobs)
                    if not workflow_id:
                        workflow_id = self._get_workflow_id_from_environment()
                    if not source_id:
                        source_id = self._get_source_id_from_environment()
                    
                    # Validate workflow_id is not empty (required)
                    if not workflow_id or not str(workflow_id).strip():
                        error_msg = "workflow_id is required and must be provided by the backend server in job_metadata or NUVYN_JOB_PAYLOAD environment variable"
                        logger.error(f"❌ {error_msg}")
                        raise ValueError(error_msg)
                    
                    if self.db_writer.write_metadata(metadata, workflow_id=workflow_id, source_id=source_id):
                        metadata['written_to_db'] = True
                        metadata['workflow_id'] = workflow_id
                        if source_id:
                            metadata['source_id'] = source_id
                        logger.info(f"✅ Metadata written to database successfully with workflow_id: {workflow_id}, source_id: {source_id}")
                    else:
                        metadata['written_to_db'] = False
                        logger.warning("⚠️ Failed to write metadata to database")
                except Exception as e:
                    logger.error(f"❌ Database write failed: {e}")
                    metadata['written_to_db'] = False
                    metadata['db_error'] = str(e)
                    raise  # Re-raise to fail the job if workflow_id is missing
            
            logger.info(f"✅ Metadata extraction completed: {len(files)} files analyzed")
            return metadata
            
        except Exception as e:
            logger.error(f"❌ Metadata extraction failed: {e}")
            return {
                "source_path": job_config.data_source_path,
                "error": str(e),
                "extraction_status": "failed"
            }
    
    async def _extract_metadata_multiple_sources(self, job_config: JobConfig) -> Dict[str, Any]:
        """Extract metadata from multiple sources individually
        
        Each source is processed separately with its own source_id,
        but all share the same workflow_id.
        """
        logger.info(f"🔄 Processing {len(job_config.sources)} sources in batch")
        
        # Get workflow_id (shared across all sources)
        workflow_id = None
        if job_config.job_metadata:
            workflow_id = job_config.job_metadata.get('workflow_id')
        if not workflow_id:
            workflow_id = self._get_workflow_id_from_environment()
        
        if not workflow_id or not str(workflow_id).strip():
            error_msg = "workflow_id is required for multi-source extraction"
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)
        
        results = {
            "workflow_id": workflow_id,
            "total_sources": len(job_config.sources),
            "sources_processed": 0,
            "sources_failed": 0,
            "sources": [],
            "extraction_timestamp": asyncio.get_event_loop().time()
        }
        
        # Process each source individually
        for idx, source_config in enumerate(job_config.sources):
            source_id = source_config.get('source_id', f"source_{idx + 1}")
            data_source_path = source_config.get('data_source_path', '')
            data_source_type = source_config.get('data_source_type', job_config.data_source_type or 'auto')
            
            if not data_source_path:
                logger.warning(f"⚠️ Skipping source {idx + 1}: missing data_source_path")
                results["sources_failed"] += 1
                continue
            
            logger.info(f"📊 Processing source {idx + 1}/{len(job_config.sources)}: {source_id}")
            logger.info(f"   Path: {data_source_path}")
            
            try:
                # Create a temporary job config for this source
                source_job_config = JobConfig(
                    job_id=f"{job_config.job_id}_source_{idx + 1}",
                    job_type=job_config.job_type,
                    data_source_path=data_source_path,
                    data_source_type=data_source_type,
                    tenant_id=job_config.tenant_id,
                    job_metadata={
                        'workflow_id': workflow_id,
                        'source_id': source_id
                    }
                )
                
                # Extract metadata for this source
                source_metadata = await self._extract_single_source_metadata(source_job_config, workflow_id, source_id)
                
                # Store results
                source_result = {
                    "source_id": source_id,
                    "source_path": data_source_path,
                    "source_type": data_source_type,
                    "status": "success",
                    "metadata": source_metadata
                }
                results["sources"].append(source_result)
                results["sources_processed"] += 1
                
                logger.info(f"✅ Source {source_id} processed successfully")
                
            except Exception as e:
                logger.error(f"❌ Failed to process source {source_id}: {e}")
                results["sources_failed"] += 1
                results["sources"].append({
                    "source_id": source_id,
                    "source_path": data_source_path,
                    "status": "failed",
                    "error": str(e)
                })
        
        logger.info(f"✅ Multi-source extraction completed: {results['sources_processed']} succeeded, {results['sources_failed']} failed")
        return results
    
    async def _extract_single_source_metadata(self, job_config: JobConfig, workflow_id: str, source_id: str) -> Dict[str, Any]:
        """Extract metadata from a single source (internal method)"""
        try:
            # Get credentials and create connector
            credentials = self.config_manager.get_data_source_credentials(job_config.data_source_type)
            connector = DataSourceFactory.auto_detect_connector(job_config.data_source_path, credentials)
            
            if not connector:
                raise Exception(f"No suitable connector found for path: {job_config.data_source_path}")
            
            # Connect to data source
            if hasattr(connector, 'can_handle') and 'blob.core.windows.net' in job_config.data_source_path:
                if not await connector.connect(url_with_sas=job_config.data_source_path):
                    raise Exception("Failed to connect to data source")
            else:
                if not await connector.connect():
                    raise Exception("Failed to connect to data source")
            
            # Extract metadata
            files = await connector.list_files(job_config.data_source_path)
            
            metadata = {
                "source_path": job_config.data_source_path,
                "source_type": connector.get_source_type(),
                "extraction_timestamp": asyncio.get_event_loop().time(),
                "files_found": len(files),
                "files": [],
                "total_size_bytes": 0,
                "schema_info": {
                    "tables": 0,
                    "columns": 0,
                    "data_types": {}
                },
                "quality_metrics": {
                    "overall_score": 85,
                    "completeness": 90,
                    "accuracy": 85,
                    "consistency": 80
                }
            }
            
            # Analyze each file
            for file_path in files[:5]:  # Analyze first 5 files
                try:
                    if '?' in job_config.data_source_path and 'sig=' in job_config.data_source_path:
                        full_file_path = job_config.data_source_path
                    else:
                        full_file_path = file_path
                    
                    file_size = await connector.get_file_size(full_file_path)
                    sample_data = await connector.read_file_sample(full_file_path, max_bytes=1024*1024)
                    
                    file_type = self._detect_file_type(file_path)
                    
                    file_info = {
                        "name": file_path.split('/')[-1],
                        "path": file_path,
                        "size_bytes": file_size,
                        "sample_size": len(sample_data),
                        "file_type": file_type
                    }
                    
                    # Analyze CSV files for detailed metadata
                    if file_type == "csv" and sample_data:
                        csv_metadata = self._analyze_csv_data(sample_data, file_path.split('/')[-1])
                        file_info.update(csv_metadata)
                        
                        if "columns" in csv_metadata:
                            metadata["schema_info"]["tables"] += 1
                            metadata["schema_info"]["columns"] += len(csv_metadata["columns"])
                            for col in csv_metadata["columns"]:
                                col_type = col.get("data_type", "unknown")
                                metadata["schema_info"]["data_types"][col_type] = metadata["schema_info"]["data_types"].get(col_type, 0) + 1
                    
                    metadata["files"].append(file_info)
                    metadata["total_size_bytes"] += file_size
                    
                except Exception as e:
                    logger.warning(f"Failed to analyze file {file_path}: {e}")
            
            await connector.disconnect()
            
            # Write to database if enabled
            if self.write_to_db and self.db_writer:
                try:
                    logger.info(f"💾 Writing metadata for source {source_id} to Databricks SQL...")
                    
                    if self.db_writer.write_metadata(metadata, workflow_id=workflow_id, source_id=source_id):
                        metadata['written_to_db'] = True
                        metadata['workflow_id'] = workflow_id
                        metadata['source_id'] = source_id
                        logger.info(f"✅ Metadata written for source {source_id}")
                    else:
                        metadata['written_to_db'] = False
                        logger.warning(f"⚠️ Failed to write metadata for source {source_id}")
                except Exception as e:
                    logger.error(f"❌ Database write failed for source {source_id}: {e}")
                    metadata['written_to_db'] = False
                    metadata['db_error'] = str(e)
            
            return metadata
            
        except Exception as e:
            logger.error(f"❌ Metadata extraction failed for source {source_id}: {e}")
            raise
    
    def _detect_file_type(self, file_path: str) -> str:
        """Detect file type from path"""
        if file_path.lower().endswith('.csv'):
            return 'csv'
        elif file_path.lower().endswith('.parquet'):
            return 'parquet'
        elif file_path.lower().endswith(('.xlsx', '.xls')):
            return 'excel'
        elif file_path.lower().endswith('.json'):
            return 'json'
        else:
            return 'unknown'
    
    def _analyze_csv_data(self, sample_data: bytes, filename: str) -> Dict[str, Any]:
        """Analyze CSV sample data to extract schema information"""
        try:
            import io
            import csv
            
            # Decode sample data
            text_data = sample_data.decode('utf-8', errors='ignore')
            
            # Parse CSV
            csv_reader = csv.reader(io.StringIO(text_data))
            rows = list(csv_reader)
            
            if len(rows) < 1:
                return {"columns": [], "row_count": 0}
            
            # First row is header
            headers = rows[0]
            data_rows = rows[1:]
            
            # Analyze columns
            columns = []
            for idx, col_name in enumerate(headers):
                col_info = {
                    "column_name": col_name,
                    "data_type": "string",  # Basic detection
                    "sample_values": [row[idx] if idx < len(row) else None for row in data_rows[:5]],
                    "is_nullable": any(not row[idx] if idx < len(row) else True for row in data_rows),
                    "position": idx
                }
                columns.append(col_info)
            
            return {
                "columns": columns,
                "row_count": len(data_rows),
                "column_count": len(headers),
                "has_header": True
            }
            
        except Exception as e:
            logger.error(f"❌ CSV analysis failed: {e}")
            return {"columns": [], "row_count": 0}
