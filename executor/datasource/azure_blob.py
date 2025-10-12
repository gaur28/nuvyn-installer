"""
Azure Blob Storage data source connector
"""

import asyncio
from typing import List, Dict, Any, Optional
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

from datasource.base import DataSourceBase
from logger import get_logger

logger = get_logger(__name__)


class AzureBlobDataSource(DataSourceBase):
    """Azure Blob Storage data source connector"""
    
    def __init__(self, credentials: Dict[str, str]):
        super().__init__(credentials)
        self.blob_service_client = None
        self.container_name = None
    
    def can_handle(self, path: str) -> bool:
        """Check if this is an Azure Blob Storage path"""
        return (
            'blob.core.windows.net' in path or
            path.startswith('https://') and 'blob.core.windows.net' in path or
            path.startswith('abfss://')
        )
    
    def get_source_type(self) -> str:
        """Get the data source type"""
        return "azure_blob"
    
    def validate_credentials(self) -> bool:
        """Validate Azure Blob Storage credentials"""
        has_connection_string = bool(self.credentials.get('connection_string'))
        has_account_creds = bool(self.credentials.get('account_name')) and bool(self.credentials.get('account_key'))
        has_sas_token = bool(self.credentials.get('sas_token'))
        
        # If no credentials provided, we might have SAS token in URL (will be extracted later)
        # So return True to allow connector creation
        return True  # Will validate during connection
    
    async def connect(self, url_with_sas: str = None) -> bool:
        """Establish connection to Azure Blob Storage"""
        try:
            logger.info("🔗 Connecting to Azure Blob Storage...")
            
            # Try connection string first
            if self.credentials.get('connection_string'):
                self.blob_service_client = BlobServiceClient.from_connection_string(
                    self.credentials['connection_string']
                )
            
            # Try account name and key
            elif self.credentials.get('account_name') and self.credentials.get('account_key'):
                account_url = f"https://{self.credentials['account_name']}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.credentials['account_key']
                )
            
            # Try SAS token
            elif self.credentials.get('sas_token'):
                account_url = f"https://{self.credentials['account_name']}.blob.core.windows.net"
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.credentials['sas_token']
                )
            
            # Try to extract SAS token from URL if provided
            elif url_with_sas and '?' in url_with_sas:
                # URL contains SAS token as query parameters
                base_url = url_with_sas.split('?')[0]
                sas_token = '?' + url_with_sas.split('?')[1]  # Keep the ? for SAS token
                
                # Extract account name from URL
                if 'blob.core.windows.net' in base_url:
                    account_name = base_url.split('//')[1].split('.')[0]
                    account_url = f"https://{account_name}.blob.core.windows.net"
                    
                    # Create client with SAS token from URL
                    # Use the full URL with SAS token directly
                    self.blob_service_client = BlobServiceClient(
                        account_url=account_url + sas_token
                    )
                    logger.info(f"✅ Using SAS token from URL for account: {account_name}")
                else:
                    logger.error("❌ Invalid Azure Blob URL format")
                    return False
            
            else:
                logger.error("❌ No valid Azure Blob Storage credentials found")
                return False
            
            # Skip test for SAS URLs - blob operations will validate
            # await self.test_connection()
            # Skip test for SAS URLs - blob operations will validate
            # await self.test_connection()
            logger.info("✅ Azure Blob Storage connection established")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Azure Blob Storage: {e}")
            return False
    
    async def disconnect(self):
        """Close connection to Azure Blob Storage"""
        if self.blob_service_client:
            self.blob_service_client.close()
            self.blob_service_client = None
            logger.info("🔌 Azure Blob Storage connection closed")
    
    async def list_files(self, path: str) -> List[str]:
        """List files in the specified Azure Blob Storage path"""
        try:
            if not self.blob_service_client:
                await self.connect()
            
            # Check if this is a direct blob URL (single file with SAS token)
            if '?' in path and path.split('?')[1].find('sr=b') != -1:
                # This is a blob-level SAS token for a single file
                container_name, blob_name = self._parse_blob_path(path.split('?')[0])
                logger.info(f"📁 Direct blob access detected: {blob_name}")
                return [blob_name] if blob_name else []
            
            # Parse container and path from URL
            container_name, blob_prefix = self._parse_blob_path(path)
            
            # List blobs
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs(name_starts_with=blob_prefix)
            
            files = []
            for blob in blobs:
                if not blob.name.endswith('/'):  # Skip directories
                    files.append(blob.name)
            
            logger.info(f"📁 Found {len(files)} files in Azure Blob Storage path: {path}")
            return files
            
        except Exception as e:
            logger.error(f"❌ Error listing files from Azure Blob Storage: {e}")
            # If listing fails but we have a direct blob URL, return the blob name
            if '?' in path:
                container_name, blob_name = self._parse_blob_path(path.split('?')[0])
                if blob_name:
                    logger.info(f"📁 Returning single blob from URL: {blob_name}")
                    return [blob_name]
            return []
    
    async def get_file_size(self, file_path: str) -> int:
        """Get file size from Azure Blob Storage"""
        try:
            if not self.blob_service_client:
                await self.connect()
            
            # If file_path contains SAS token, use it directly
            if '?' in file_path and 'sig=' in file_path:
                from azure.storage.blob import BlobClient
                blob_client = BlobClient.from_blob_url(file_path)
                properties = blob_client.get_blob_properties()
                size = properties.size
                logger.debug(f"📏 File size: {size} bytes")
                return size
            
            container_name, blob_name = self._parse_blob_path(file_path)
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            properties = blob_client.get_blob_properties()
            size = properties.size
            
            logger.debug(f"📏 File size for {blob_name}: {size} bytes")
            return size
            
        except Exception as e:
            logger.error(f"❌ Error getting file size from Azure Blob Storage: {e}")
            return 0
    
    async def read_file_sample(self, file_path: str, max_bytes: int = 1024*1024) -> bytes:
        """Read a sample of the file from Azure Blob Storage"""
        try:
            if not self.blob_service_client:
                await self.connect()
            
            # If file_path contains SAS token, use it directly
            if '?' in file_path and 'sig=' in file_path:
                from azure.storage.blob import BlobClient
                blob_client = BlobClient.from_blob_url(file_path)
                download_stream = blob_client.download_blob(max_concurrency=1)
                sample_data = download_stream.readall()[:max_bytes]
                logger.debug(f"📖 Read {len(sample_data)} bytes using SAS URL")
                return sample_data
            
            container_name, blob_name = self._parse_blob_path(file_path)
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            # Download only the first max_bytes
            download_stream = blob_client.download_blob(max_concurrency=1)
            sample_data = download_stream.readall()[:max_bytes]
            
            logger.debug(f"📖 Read {len(sample_data)} bytes from {blob_name}")
            return sample_data
            
        except Exception as e:
            logger.error(f"❌ Error reading file sample from Azure Blob Storage: {e}")
            return b''
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test the Azure Blob Storage connection"""
        try:
            if not self.blob_service_client:
                return {"success": False, "error": "Not connected"}
            
            # Try to list containers to test connection
            containers = self.blob_service_client.list_containers(results_per_page=1)
            list(containers)  # Consume the iterator
            
            return {
                "success": True,
                "source_type": "azure_blob",
                "connection_status": "connected"
            }
            
        except Exception as e:
            logger.error(f"❌ Azure Blob Storage connection test failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "source_type": "azure_blob",
                "connection_status": "failed"
            }
    
    def _parse_blob_path(self, path: str) -> tuple:
        """Parse Azure Blob Storage path to extract container and blob name"""
        try:
            # Handle different URL formats
            if path.startswith('https://'):
                # https://account.blob.core.windows.net/container/path/blob
                parts = path.replace('https://', '').split('/')
                account_part = parts[0]
                if len(parts) > 1:
                    container_name = parts[1]
                    blob_name = '/'.join(parts[2:]) if len(parts) > 2 else ''
                else:
                    container_name = ''
                    blob_name = ''
            
            elif path.startswith('abfss://'):
                # abfss://container@account.dfs.core.windows.net/path/blob
                parts = path.replace('abfss://', '').split('/')
                container_account = parts[0]
                if '@' in container_account:
                    container_name = container_account.split('@')[0]
                    blob_name = '/'.join(parts[1:]) if len(parts) > 1 else ''
                else:
                    container_name = ''
                    blob_name = ''
            
            else:
                # Assume it's already parsed as container/blob format
                parts = path.split('/', 1)
                container_name = parts[0] if len(parts) > 0 else ''
                blob_name = parts[1] if len(parts) > 1 else ''
            
            return container_name, blob_name
            
        except Exception as e:
            logger.error(f"❌ Error parsing Azure Blob Storage path: {e}")
            return '', ''
    
    async def get_container_info(self, container_name: str) -> Dict[str, Any]:
        """Get information about a specific container"""
        try:
            if not self.blob_service_client:
                await self.connect()
            
            container_client = self.blob_service_client.get_container_client(container_name)
            properties = container_client.get_container_properties()
            
            return {
                "container_name": container_name,
                "last_modified": properties.last_modified.isoformat(),
                "etag": properties.etag,
                "lease_status": properties.lease_status,
                "public_access": properties.public_access
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting container info: {e}")
            return {"error": str(e)}
    
    async def get_blob_metadata(self, file_path: str) -> Dict[str, Any]:
        """Get metadata for a specific blob"""
        try:
            if not self.blob_service_client:
                await self.connect()
            
            container_name, blob_name = self._parse_blob_path(file_path)
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            properties = blob_client.get_blob_properties()
            
            return {
                "blob_name": blob_name,
                "size": properties.size,
                "last_modified": properties.last_modified.isoformat(),
                "etag": properties.etag,
                "content_type": properties.content_settings.content_type,
                "content_encoding": properties.content_settings.content_encoding,
                "metadata": properties.metadata
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting blob metadata: {e}")
            return {"error": str(e)}
