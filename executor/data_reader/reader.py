"""
Data Reader for reading actual data from various sources
"""

import asyncio
from typing import Dict, Any, List, Optional
from config import JobConfig, ConfigManager
from datasource.factory import DataSourceFactory
from logger import get_logger

logger = get_logger(__name__)


class DataReader:
    """Reads actual data from various data sources"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
    
    async def read_data(self, job_config: JobConfig) -> Dict[str, Any]:
        """Read data from the specified source"""
        logger.info(f"📖 Reading data from: {job_config.data_source_path}")
        
        try:
            # Get credentials for the data source
            credentials = self.config_manager.get_data_source_credentials(job_config.data_source_type)
            
            # Auto-detect and create connector
            connector = DataSourceFactory.auto_detect_connector(
                job_config.data_source_path, 
                credentials
            )
            
            if not connector:
                raise Exception(f"No suitable connector found for path: {job_config.data_source_path}")
            
            # Connect to data source
            if not await connector.connect():
                raise Exception("Failed to connect to data source")
            
            # Read data
            files = await connector.list_files(job_config.data_source_path)
            
            result = {
                "source_path": job_config.data_source_path,
                "source_type": connector.get_source_type(),
                "files_found": len(files),
                "files": files[:10],  # Limit to first 10 files
                "total_size": 0,
                "sample_data": {},
                "connection_status": "success"
            }
            
            # Get sample data from first few files
            for file_path in files[:3]:
                try:
                    sample_data = await connector.read_file_sample(file_path, max_bytes=1024)
                    result["sample_data"][file_path] = {
                        "size": len(sample_data),
                        "preview": sample_data[:200].decode('utf-8', errors='ignore')
                    }
                except Exception as e:
                    logger.warning(f"Failed to read sample from {file_path}: {e}")
            
            await connector.disconnect()
            
            logger.info(f"✅ Data reading completed: {len(files)} files found")
            return result
            
        except Exception as e:
            logger.error(f"❌ Data reading failed: {e}")
            return {
                "source_path": job_config.data_source_path,
                "error": str(e),
                "connection_status": "failed"
            }
