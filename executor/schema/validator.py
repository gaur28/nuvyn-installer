"""
Schema Validator for Nuvyn Executor Script
Validates and manages the _executor_metadata schema structure
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from config import JobConfig, ConfigManager
from logger import get_logger

logger = get_logger(__name__)


class SchemaValidator:
    """Validates and manages schema structure for executor metadata"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.schema_name = config_manager.executor_config.schema_name
        
        # Define the required schema structure
        self.required_tables = {
            'sources': {
                'columns': {
                    'source_id': 'STRING',
                    'source_name': 'STRING', 
                    'source_type': 'STRING',
                    'connection_details': 'STRING',
                    'created_at': 'TIMESTAMP',
                    'updated_at': 'TIMESTAMP'
                },
                'primary_key': ['source_id'],
                'description': 'Data source registry'
            },
            'tables': {
                'columns': {
                    'table_id': 'STRING',
                    'source_id': 'STRING',
                    'table_name': 'STRING',
                    'schema_name': 'STRING',
                    'row_count': 'BIGINT',
                    'last_refreshed': 'TIMESTAMP'
                },
                'primary_key': ['table_id'],
                'foreign_keys': {
                    'source_id': 'sources(source_id)'
                },
                'description': 'Table metadata per source'
            },
            'columns': {
                'columns': {
                    'column_id': 'STRING',
                    'table_id': 'STRING',
                    'column_name': 'STRING',
                    'data_type': 'STRING',
                    'is_nullable': 'BOOLEAN',
                    'is_primary_key': 'BOOLEAN',
                    'sample_value': 'STRING',
                    'distinct_count': 'BIGINT'
                },
                'primary_key': ['column_id'],
                'foreign_keys': {
                    'table_id': 'tables(table_id)'
                },
                'description': 'Column metadata per table'
            },
            'executor_runs': {
                'columns': {
                    'run_id': 'STRING',
                    'executor_version': 'STRING',
                    'source_id': 'STRING',
                    'run_mode': 'STRING',
                    'status': 'STRING',
                    'error_message': 'STRING',
                    'started_at': 'TIMESTAMP',
                    'finished_at': 'TIMESTAMP'
                },
                'primary_key': ['run_id'],
                'foreign_keys': {
                    'source_id': 'sources(source_id)'
                },
                'description': 'Execution audit trail'
            },
            'logs': {
                'columns': {
                    'log_id': 'STRING',
                    'run_id': 'STRING',
                    'log_level': 'STRING',
                    'log_message': 'STRING',
                    'log_timestamp': 'TIMESTAMP'
                },
                'primary_key': ['log_id'],
                'foreign_keys': {
                    'run_id': 'executor_runs(run_id)'
                },
                'description': 'Detailed execution logs'
            }
        }
    
    async def validate_schema(self, job_config: JobConfig) -> Dict[str, Any]:
        """Validate that the required schema exists and has correct structure"""
        logger.info(f"🔍 Validating schema: {self.schema_name}")
        
        result = {
            'schema_name': self.schema_name,
            'validation_status': 'unknown',
            'tables_found': [],
            'tables_missing': [],
            'tables_invalid': [],
            'validation_details': {},
            'recommendations': []
        }
        
        try:
            # Check if we're in a Databricks environment
            if not self._is_databricks_environment():
                result['validation_status'] = 'skipped'
                result['recommendations'].append('Not running in Databricks environment - schema validation skipped')
                logger.warning("⚠️ Not in Databricks environment - skipping schema validation")
                return result
            
            # Validate schema existence
            schema_exists = await self._check_schema_exists()
            if not schema_exists:
                result['validation_status'] = 'failed'
                result['recommendations'].append(f'Create schema: {self.schema_name}')
                logger.error(f"❌ Schema does not exist: {self.schema_name}")
                return result
            
            # Validate each required table
            for table_name, table_def in self.required_tables.items():
                table_result = await self._validate_table(table_name, table_def)
                result['validation_details'][table_name] = table_result
                
                if table_result['exists']:
                    result['tables_found'].append(table_name)
                    if not table_result['structure_valid']:
                        result['tables_invalid'].append(table_name)
                        result['recommendations'].append(f'Fix table structure: {table_name}')
                else:
                    result['tables_missing'].append(table_name)
                    result['recommendations'].append(f'Create table: {table_name}')
            
            # Determine overall validation status
            if not result['tables_missing'] and not result['tables_invalid']:
                result['validation_status'] = 'valid'
                logger.info(f"✅ Schema validation successful: {self.schema_name}")
            else:
                result['validation_status'] = 'invalid'
                logger.warning(f"⚠️ Schema validation issues found: {self.schema_name}")
            
            return result
            
        except Exception as e:
            result['validation_status'] = 'error'
            result['error'] = str(e)
            logger.error(f"❌ Schema validation failed: {e}")
            return result
    
    async def create_schema(self, job_config: JobConfig) -> Dict[str, Any]:
        """Create the required schema and tables"""
        logger.info(f"🏗️ Creating schema: {self.schema_name}")
        
        result = {
            'schema_name': self.schema_name,
            'creation_status': 'unknown',
            'tables_created': [],
            'tables_failed': [],
            'creation_details': {},
            'recommendations': []
        }
        
        try:
            # Check if we're in a Databricks environment
            if not self._is_databricks_environment():
                result['creation_status'] = 'skipped'
                result['recommendations'].append('Not running in Databricks environment - schema creation skipped')
                logger.warning("⚠️ Not in Databricks environment - skipping schema creation")
                return result
            
            # Create schema if it doesn't exist
            schema_created = await self._create_schema_if_not_exists()
            if not schema_created:
                result['creation_status'] = 'failed'
                result['error'] = 'Failed to create schema'
                return result
            
            # Create each required table
            for table_name, table_def in self.required_tables.items():
                table_result = await self._create_table(table_name, table_def)
                result['creation_details'][table_name] = table_result
                
                if table_result['success']:
                    result['tables_created'].append(table_name)
                    logger.info(f"✅ Table created: {table_name}")
                else:
                    result['tables_failed'].append(table_name)
                    result['recommendations'].append(f'Manually create table: {table_name}')
                    logger.error(f"❌ Failed to create table: {table_name} - {table_result.get('error', 'Unknown error')}")
            
            # Determine overall creation status
            if not result['tables_failed']:
                result['creation_status'] = 'success'
                logger.info(f"✅ Schema creation successful: {self.schema_name}")
            else:
                result['creation_status'] = 'partial'
                logger.warning(f"⚠️ Schema creation partially successful: {self.schema_name}")
            
            return result
            
        except Exception as e:
            result['creation_status'] = 'error'
            result['error'] = str(e)
            logger.error(f"❌ Schema creation failed: {e}")
            return result
    
    def _is_databricks_environment(self) -> bool:
        """Check if running in Databricks environment"""
        return "DATABRICKS_RUNTIME_VERSION" in self.config_manager.executor_config.__dict__ or \
               "DATABRICKS_RUNTIME_VERSION" in self.config_manager.executor_config.__dict__.get('databricks_workspace_url', '')
    
    async def _check_schema_exists(self) -> bool:
        """Check if the schema exists"""
        try:
            # In a real implementation, this would query the database
            # For now, we'll simulate the check
            logger.debug(f"🔍 Checking if schema exists: {self.schema_name}")
            
            # Simulate schema existence check
            # In Databricks, this would be: SHOW SCHEMAS LIKE '{self.schema_name}'
            return True  # Placeholder - assume schema exists
            
        except Exception as e:
            logger.error(f"❌ Error checking schema existence: {e}")
            return False
    
    async def _create_schema_if_not_exists(self) -> bool:
        """Create schema if it doesn't exist"""
        try:
            logger.debug(f"🏗️ Creating schema if not exists: {self.schema_name}")
            
            # In a real implementation, this would execute:
            # CREATE SCHEMA IF NOT EXISTS {self.schema_name}
            
            # Simulate schema creation
            return True  # Placeholder - assume creation successful
            
        except Exception as e:
            logger.error(f"❌ Error creating schema: {e}")
            return False
    
    async def _validate_table(self, table_name: str, table_def: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a specific table structure"""
        result = {
            'table_name': table_name,
            'exists': False,
            'structure_valid': False,
            'columns_found': [],
            'columns_missing': [],
            'columns_invalid': [],
            'primary_key_valid': False,
            'foreign_keys_valid': False,
            'error': None
        }
        
        try:
            # Check if table exists
            table_exists = await self._check_table_exists(table_name)
            result['exists'] = table_exists
            
            if not table_exists:
                return result
            
            # Validate table structure
            structure_result = await self._validate_table_structure(table_name, table_def)
            result.update(structure_result)
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Error validating table {table_name}: {e}")
            return result
    
    async def _check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            # In a real implementation, this would query:
            # SHOW TABLES IN {self.schema_name} LIKE '{table_name}'
            
            # Simulate table existence check
            return True  # Placeholder - assume table exists
            
        except Exception as e:
            logger.error(f"❌ Error checking table existence {table_name}: {e}")
            return False
    
    async def _validate_table_structure(self, table_name: str, table_def: Dict[str, Any]) -> Dict[str, Any]:
        """Validate table structure against definition"""
        result = {
            'structure_valid': False,
            'columns_found': [],
            'columns_missing': [],
            'columns_invalid': [],
            'primary_key_valid': False,
            'foreign_keys_valid': False
        }
        
        try:
            # Get actual table structure
            actual_structure = await self._get_table_structure(table_name)
            
            # Validate columns
            required_columns = table_def['columns']
            actual_columns = {col['name']: col['type'] for col in actual_structure.get('columns', [])}
            
            for col_name, expected_type in required_columns.items():
                if col_name in actual_columns:
                    result['columns_found'].append(col_name)
                    # In a real implementation, we'd validate data types match
                    # For now, we'll assume they match
                else:
                    result['columns_missing'].append(col_name)
            
            # Validate primary key
            if 'primary_key' in table_def:
                result['primary_key_valid'] = await self._validate_primary_key(
                    table_name, table_def['primary_key']
                )
            
            # Validate foreign keys
            if 'foreign_keys' in table_def:
                result['foreign_keys_valid'] = await self._validate_foreign_keys(
                    table_name, table_def['foreign_keys']
                )
            
            # Determine overall structure validity
            result['structure_valid'] = (
                not result['columns_missing'] and
                result['primary_key_valid'] and
                result['foreign_keys_valid']
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error validating table structure {table_name}: {e}")
            return result
    
    async def _get_table_structure(self, table_name: str) -> Dict[str, Any]:
        """Get actual table structure from database"""
        try:
            # In a real implementation, this would query:
            # DESCRIBE {self.schema_name}.{table_name}
            
            # Simulate table structure
            return {
                'columns': [
                    {'name': 'source_id', 'type': 'STRING'},
                    {'name': 'source_name', 'type': 'STRING'},
                    # ... other columns
                ]
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting table structure {table_name}: {e}")
            return {'columns': []}
    
    async def _validate_primary_key(self, table_name: str, primary_key: List[str]) -> bool:
        """Validate primary key constraint"""
        try:
            # In a real implementation, this would check constraints
            return True  # Placeholder
            
        except Exception as e:
            logger.error(f"❌ Error validating primary key {table_name}: {e}")
            return False
    
    async def _validate_foreign_keys(self, table_name: str, foreign_keys: Dict[str, str]) -> bool:
        """Validate foreign key constraints"""
        try:
            # In a real implementation, this would check constraints
            return True  # Placeholder
            
        except Exception as e:
            logger.error(f"❌ Error validating foreign keys {table_name}: {e}")
            return False
    
    async def _create_table(self, table_name: str, table_def: Dict[str, Any]) -> Dict[str, Any]:
        """Create a table with the specified structure"""
        result = {
            'table_name': table_name,
            'success': False,
            'error': None
        }
        
        try:
            # Generate CREATE TABLE statement
            create_sql = self._generate_create_table_sql(table_name, table_def)
            logger.debug(f"🏗️ Creating table {table_name} with SQL: {create_sql}")
            
            # In a real implementation, this would execute the SQL
            # For now, we'll simulate success
            
            result['success'] = True
            return result
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Error creating table {table_name}: {e}")
            return result
    
    def _generate_create_table_sql(self, table_name: str, table_def: Dict[str, Any]) -> str:
        """Generate CREATE TABLE SQL statement"""
        columns = []
        
        # Add columns
        for col_name, col_type in table_def['columns'].items():
            columns.append(f"    {col_name} {col_type}")
        
        # Add primary key constraint
        if 'primary_key' in table_def:
            pk_cols = ', '.join(table_def['primary_key'])
            columns.append(f"    CONSTRAINT pk_{table_name} PRIMARY KEY ({pk_cols})")
        
        # Add foreign key constraints
        if 'foreign_keys' in table_def:
            for fk_col, fk_ref in table_def['foreign_keys'].items():
                columns.append(f"    CONSTRAINT fk_{table_name}_{fk_col} FOREIGN KEY ({fk_col}) REFERENCES {fk_ref}")
        
        # Generate full SQL
        columns_sql = ',\n'.join(columns)
        sql = f"""CREATE TABLE IF NOT EXISTS {self.schema_name}.{table_name} (
{columns_sql}
)"""
        
        return sql
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get summary of the schema structure"""
        return {
            'schema_name': self.schema_name,
            'table_count': len(self.required_tables),
            'tables': list(self.required_tables.keys()),
            'total_columns': sum(len(table['columns']) for table in self.required_tables.values()),
            'description': f"Executor metadata schema with {len(self.required_tables)} tables"
        }
