"""
Schema Manager for Nuvyn Executor Script
Manages schema operations and data storage in the _executor_metadata schema
"""

import asyncio
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from executor.config import JobConfig, ConfigManager
from executor.logger import get_logger

logger = get_logger(__name__)


class SchemaManager:
    """Manages data storage operations in the executor metadata schema"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.schema_name = config_manager.executor_config.schema_name
    
    async def store_source_metadata(self, 
                                  source_data: Dict[str, Any],
                                  job_config: JobConfig) -> Dict[str, Any]:
        """Store data source metadata in the sources table"""
        logger.info(f"💾 Storing source metadata: {source_data.get('source_name', 'unknown')}")
        
        result = {
            'operation': 'store_source_metadata',
            'success': False,
            'source_id': None,
            'error': None
        }
        
        try:
            # Generate source ID
            source_id = str(uuid.uuid4())
            
            # Prepare source data
            source_record = {
                'source_id': source_id,
                'source_name': source_data.get('source_name', 'unknown'),
                'source_type': source_data.get('source_type', 'unknown'),
                'connection_details': self._mask_connection_details(
                    source_data.get('connection_details', {})
                ),
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Store in database
            success = await self._insert_record('sources', source_record)
            
            if success:
                result['success'] = True
                result['source_id'] = source_id
                logger.info(f"✅ Source metadata stored: {source_id}")
            else:
                result['error'] = 'Failed to insert source record'
                logger.error(f"❌ Failed to store source metadata: {source_data.get('source_name')}")
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Error storing source metadata: {e}")
            return result
    
    async def store_table_metadata(self, 
                                 table_data: Dict[str, Any],
                                 source_id: str,
                                 job_config: JobConfig) -> Dict[str, Any]:
        """Store table metadata in the tables table"""
        logger.info(f"💾 Storing table metadata: {table_data.get('table_name', 'unknown')}")
        
        result = {
            'operation': 'store_table_metadata',
            'success': False,
            'table_id': None,
            'error': None
        }
        
        try:
            # Generate table ID
            table_id = str(uuid.uuid4())
            
            # Prepare table data
            table_record = {
                'table_id': table_id,
                'source_id': source_id,
                'table_name': table_data.get('table_name', 'unknown'),
                'schema_name': table_data.get('schema_name', 'unknown'),
                'row_count': table_data.get('row_count', 0),
                'last_refreshed': datetime.now(timezone.utc).isoformat()
            }
            
            # Store in database
            success = await self._insert_record('tables', table_record)
            
            if success:
                result['success'] = True
                result['table_id'] = table_id
                logger.info(f"✅ Table metadata stored: {table_id}")
            else:
                result['error'] = 'Failed to insert table record'
                logger.error(f"❌ Failed to store table metadata: {table_data.get('table_name')}")
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Error storing table metadata: {e}")
            return result
    
    async def store_column_metadata(self, 
                                  column_data: Dict[str, Any],
                                  table_id: str,
                                  job_config: JobConfig) -> Dict[str, Any]:
        """Store column metadata in the columns table"""
        logger.info(f"💾 Storing column metadata: {column_data.get('column_name', 'unknown')}")
        
        result = {
            'operation': 'store_column_metadata',
            'success': False,
            'column_id': None,
            'error': None
        }
        
        try:
            # Generate column ID
            column_id = str(uuid.uuid4())
            
            # Prepare column data
            column_record = {
                'column_id': column_id,
                'table_id': table_id,
                'column_name': column_data.get('column_name', 'unknown'),
                'data_type': column_data.get('data_type', 'unknown'),
                'is_nullable': column_data.get('is_nullable', True),
                'is_primary_key': column_data.get('is_primary_key', False),
                'sample_value': str(column_data.get('sample_value', ''))[:1000],  # Limit length
                'distinct_count': column_data.get('distinct_count', 0)
            }
            
            # Store in database
            success = await self._insert_record('columns', column_record)
            
            if success:
                result['success'] = True
                result['column_id'] = column_id
                logger.info(f"✅ Column metadata stored: {column_id}")
            else:
                result['error'] = 'Failed to insert column record'
                logger.error(f"❌ Failed to store column metadata: {column_data.get('column_name')}")
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Error storing column metadata: {e}")
            return result
    
    async def store_executor_run(self, 
                               run_data: Dict[str, Any],
                               job_config: JobConfig) -> Dict[str, Any]:
        """Store executor run information in the executor_runs table"""
        logger.info(f"💾 Storing executor run: {job_config.job_id}")
        
        result = {
            'operation': 'store_executor_run',
            'success': False,
            'run_id': None,
            'error': None
        }
        
        try:
            # Use job_id as run_id
            run_id = job_config.job_id
            
            # Prepare run data
            run_record = {
                'run_id': run_id,
                'executor_version': '1.0.0',
                'source_id': run_data.get('source_id', ''),
                'run_mode': job_config.job_type.value,
                'status': run_data.get('status', 'running'),
                'error_message': run_data.get('error_message', ''),
                'started_at': run_data.get('started_at', datetime.now(timezone.utc).isoformat()),
                'finished_at': run_data.get('finished_at', '')
            }
            
            # Store in database
            success = await self._insert_record('executor_runs', run_record)
            
            if success:
                result['success'] = True
                result['run_id'] = run_id
                logger.info(f"✅ Executor run stored: {run_id}")
            else:
                result['error'] = 'Failed to insert executor run record'
                logger.error(f"❌ Failed to store executor run: {job_config.job_id}")
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Error storing executor run: {e}")
            return result
    
    async def store_log_entry(self, 
                            log_data: Dict[str, Any],
                            run_id: str,
                            job_config: JobConfig) -> Dict[str, Any]:
        """Store log entry in the logs table"""
        logger.info(f"💾 Storing log entry for run: {run_id}")
        
        result = {
            'operation': 'store_log_entry',
            'success': False,
            'log_id': None,
            'error': None
        }
        
        try:
            # Generate log ID
            log_id = str(uuid.uuid4())
            
            # Prepare log data
            log_record = {
                'log_id': log_id,
                'run_id': run_id,
                'log_level': log_data.get('log_level', 'INFO'),
                'log_message': str(log_data.get('log_message', ''))[:4000],  # Limit length
                'log_timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            # Store in database
            success = await self._insert_record('logs', log_record)
            
            if success:
                result['success'] = True
                result['log_id'] = log_id
                logger.debug(f"✅ Log entry stored: {log_id}")
            else:
                result['error'] = 'Failed to insert log record'
                logger.error(f"❌ Failed to store log entry: {run_id}")
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Error storing log entry: {e}")
            return result
    
    async def get_source_metadata(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve source metadata by ID"""
        try:
            # In a real implementation, this would query the database
            # SELECT * FROM {self.schema_name}.sources WHERE source_id = '{source_id}'
            
            logger.debug(f"🔍 Retrieving source metadata: {source_id}")
            return None  # Placeholder
            
        except Exception as e:
            logger.error(f"❌ Error retrieving source metadata: {e}")
            return None
    
    async def get_table_metadata(self, source_id: str) -> List[Dict[str, Any]]:
        """Retrieve all table metadata for a source"""
        try:
            # In a real implementation, this would query the database
            # SELECT * FROM {self.schema_name}.tables WHERE source_id = '{source_id}'
            
            logger.debug(f"🔍 Retrieving table metadata for source: {source_id}")
            return []  # Placeholder
            
        except Exception as e:
            logger.error(f"❌ Error retrieving table metadata: {e}")
            return []
    
    async def get_column_metadata(self, table_id: str) -> List[Dict[str, Any]]:
        """Retrieve all column metadata for a table"""
        try:
            # In a real implementation, this would query the database
            # SELECT * FROM {self.schema_name}.columns WHERE table_id = '{table_id}'
            
            logger.debug(f"🔍 Retrieving column metadata for table: {table_id}")
            return []  # Placeholder
            
        except Exception as e:
            logger.error(f"❌ Error retrieving column metadata: {e}")
            return []
    
    async def get_executor_runs(self, 
                              source_id: Optional[str] = None,
                              status: Optional[str] = None,
                              limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve executor run history"""
        try:
            # In a real implementation, this would query the database with filters
            
            logger.debug(f"🔍 Retrieving executor runs - source: {source_id}, status: {status}")
            return []  # Placeholder
            
        except Exception as e:
            logger.error(f"❌ Error retrieving executor runs: {e}")
            return []
    
    async def update_executor_run_status(self, 
                                       run_id: str, 
                                       status: str, 
                                       error_message: str = None) -> bool:
        """Update executor run status"""
        try:
            # In a real implementation, this would execute:
            # UPDATE {self.schema_name}.executor_runs 
            # SET status = '{status}', finished_at = NOW()
            # WHERE run_id = '{run_id}'
            
            logger.info(f"📊 Updating executor run status: {run_id} -> {status}")
            return True  # Placeholder
            
        except Exception as e:
            logger.error(f"❌ Error updating executor run status: {e}")
            return False
    
    async def cleanup_old_data(self, days_to_keep: int = 30) -> Dict[str, Any]:
        """Clean up old data from the schema"""
        logger.info(f"🧹 Cleaning up data older than {days_to_keep} days")
        
        result = {
            'operation': 'cleanup_old_data',
            'success': False,
            'records_deleted': 0,
            'error': None
        }
        
        try:
            # In a real implementation, this would delete old records
            # DELETE FROM {self.schema_name}.logs WHERE log_timestamp < DATE_SUB(NOW(), INTERVAL {days_to_keep} DAY)
            # DELETE FROM {self.schema_name}.executor_runs WHERE finished_at < DATE_SUB(NOW(), INTERVAL {days_to_keep} DAY)
            
            result['success'] = True
            result['records_deleted'] = 0  # Placeholder
            logger.info(f"✅ Data cleanup completed: {result['records_deleted']} records deleted")
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Error during data cleanup: {e}")
            return result
    
    async def _insert_record(self, table_name: str, record: Dict[str, Any]) -> bool:
        """Insert a record into the specified table"""
        try:
            # In a real implementation, this would execute the INSERT statement
            # INSERT INTO {self.schema_name}.{table_name} (...) VALUES (...)
            
            logger.debug(f"💾 Inserting record into {table_name}: {list(record.keys())}")
            return True  # Placeholder - assume success
            
        except Exception as e:
            logger.error(f"❌ Error inserting record into {table_name}: {e}")
            return False
    
    def _mask_connection_details(self, connection_details: Dict[str, Any]) -> str:
        """Mask sensitive connection details"""
        if not connection_details:
            return ""
        
        # Create a copy and mask sensitive fields
        masked_details = {}
        for key, value in connection_details.items():
            if any(sensitive in key.lower() for sensitive in ['password', 'key', 'token', 'secret']):
                masked_details[key] = '***MASKED***'
            else:
                masked_details[key] = str(value)
        
        # Convert to string representation
        return str(masked_details)
    
    def get_schema_statistics(self) -> Dict[str, Any]:
        """Get statistics about the schema data"""
        # In a real implementation, this would query the database for counts
        return {
            'schema_name': self.schema_name,
            'total_sources': 0,
            'total_tables': 0,
            'total_columns': 0,
            'total_runs': 0,
            'total_logs': 0,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
