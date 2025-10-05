"""
Data source factory for creating appropriate connectors
"""

from typing import Dict, Any, Optional
from datasource.base import DataSourceBase
from datasource.azure_blob import AzureBlobDataSource
from datasource.aws_s3 import AWSS3DataSource
from datasource.database import DatabaseDataSource
from logger import get_logger

logger = get_logger(__name__)


class DataSourceFactory:
    """Factory for creating data source connectors"""
    
    _connectors = {
        'azure_blob': AzureBlobDataSource,
        'aws_s3': AWSS3DataSource,
        'database': DatabaseDataSource,
    }
    
    @classmethod
    def create_connector(cls, source_type: str, credentials: Dict[str, str]) -> Optional[DataSourceBase]:
        """Create a data source connector based on type"""
        try:
            if source_type in cls._connectors:
                connector_class = cls._connectors[source_type]
                connector = connector_class(credentials)
                
                # Validate credentials
                if connector.validate_credentials():
                    logger.info(f"✅ Created {source_type} connector")
                    return connector
                else:
                    logger.error(f"❌ Invalid credentials for {source_type} connector")
                    return None
            else:
                logger.error(f"❌ Unknown data source type: {source_type}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error creating {source_type} connector: {e}")
            return None
    
    @classmethod
    def auto_detect_connector(cls, path: str, credentials: Dict[str, str]) -> Optional[DataSourceBase]:
        """Auto-detect and create appropriate connector based on path"""
        try:
            logger.info(f"🔍 Auto-detecting connector for path: {path}")
            
            # Try each connector to see which one can handle the path
            for source_type, connector_class in cls._connectors.items():
                connector = connector_class(credentials)
                if connector.can_handle(path):
                    if connector.validate_credentials():
                        logger.info(f"✅ Auto-detected {source_type} connector for path: {path}")
                        return connector
                    else:
                        logger.warning(f"⚠️ {source_type} can handle path but has invalid credentials")
            
            # If no connector can handle the path, return None
            logger.warning(f"⚠️ No suitable connector found for path: {path}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error auto-detecting connector: {e}")
            return None
    
    @classmethod
    def get_supported_types(cls) -> list:
        """Get list of supported data source types"""
        return list(cls._connectors.keys())
    
    @classmethod
    def register_connector(cls, source_type: str, connector_class):
        """Register a new connector type"""
        cls._connectors[source_type] = connector_class
        logger.info(f"📝 Registered new connector type: {source_type}")
    
    @classmethod
    def get_connector_info(cls, source_type: str) -> Dict[str, Any]:
        """Get information about a specific connector type"""
        if source_type in cls._connectors:
            connector_class = cls._connectors[source_type]
            # Create a temporary instance to get info
            temp_connector = connector_class({})
            
            return {
                "source_type": source_type,
                "class_name": connector_class.__name__,
                "can_handle_example": temp_connector.can_handle.__doc__ or "Auto-detection available",
                "required_credentials": cls._get_required_credentials(source_type)
            }
        else:
            return {"error": f"Unknown source type: {source_type}"}
    
    @classmethod
    def _get_required_credentials(cls, source_type: str) -> list:
        """Get required credentials for a source type"""
        credential_requirements = {
            'azure_blob': [
                'connection_string (or account_name + account_key, or sas_token)'
            ],
            'aws_s3': [
                'access_key_id',
                'secret_access_key',
                'region (optional, defaults to us-east-1)'
            ],
            'database': [
                'host',
                'username', 
                'password',
                'database',
                'port (optional)',
                'type (mysql, postgresql, snowflake, mssql, oracle)'
            ]
        }
        
        return credential_requirements.get(source_type, [])
    
    @classmethod
    def validate_credentials(cls, source_type: str, credentials: Dict[str, str]) -> Dict[str, Any]:
        """Validate credentials for a specific source type"""
        try:
            connector = cls.create_connector(source_type, credentials)
            if connector:
                return {
                    "valid": True,
                    "message": "Credentials are valid"
                }
            else:
                return {
                    "valid": False,
                    "message": "Invalid credentials or connection failed"
                }
        except Exception as e:
            return {
                "valid": False,
                "message": f"Error validating credentials: {str(e)}"
            }
    
    @classmethod
    def test_connection(cls, source_type: str, credentials: Dict[str, str]) -> Dict[str, Any]:
        """Test connection to a data source"""
        import asyncio
        
        try:
            connector = cls.create_connector(source_type, credentials)
            if not connector:
                return {
                    "success": False,
                    "message": "Failed to create connector"
                }
            
            # Run async test
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(connector.test_connection())
            loop.close()
            
            return result
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": "Connection test failed"
            }
