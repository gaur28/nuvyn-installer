"""
AWS S3 data source connector
"""

import asyncio
from typing import List, Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from executor.datasource.base import DataSourceBase
from executor.logger import get_logger

logger = get_logger(__name__)


class AWSS3DataSource(DataSourceBase):
    """AWS S3 data source connector"""
    
    def __init__(self, credentials: Dict[str, str]):
        super().__init__(credentials)
        self.s3_client = None
        self.bucket_name = None
    
    def can_handle(self, path: str) -> bool:
        """Check if this is an AWS S3 path"""
        return (
            's3.amazonaws.com' in path or
            path.startswith('s3://') or
            path.startswith('https://s3') or
            's3.' in path and '.amazonaws.com' in path
        )
    
    def get_source_type(self) -> str:
        """Get the data source type"""
        return "aws_s3"
    
    def validate_credentials(self) -> bool:
        """Validate AWS S3 credentials"""
        has_access_key = bool(self.credentials.get('access_key_id'))
        has_secret_key = bool(self.credentials.get('secret_access_key'))
        return has_access_key and has_secret_key
    
    async def connect(self) -> bool:
        """Establish connection to AWS S3"""
        try:
            logger.info("🔗 Connecting to AWS S3...")
            
            # Create S3 client
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.credentials.get('access_key_id'),
                aws_secret_access_key=self.credentials.get('secret_access_key'),
                region_name=self.credentials.get('region', 'us-east-1')
            )
            
            # Test the connection
            await self.test_connection()
            logger.info("✅ AWS S3 connection established")
            return True
            
        except NoCredentialsError:
            logger.error("❌ AWS credentials not found")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to connect to AWS S3: {e}")
            return False
    
    async def disconnect(self):
        """Close connection to AWS S3"""
        if self.s3_client:
            self.s3_client = None
            logger.info("🔌 AWS S3 connection closed")
    
    async def list_files(self, path: str) -> List[str]:
        """List files in the specified S3 path"""
        try:
            if not self.s3_client:
                await self.connect()
            
            # Parse bucket and key from path
            bucket_name, key_prefix = self._parse_s3_path(path)
            
            # List objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=key_prefix)
            
            files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if not obj['Key'].endswith('/'):  # Skip directories
                            files.append(obj['Key'])
            
            logger.info(f"📁 Found {len(files)} files in S3 path: {path}")
            return files
            
        except ClientError as e:
            logger.error(f"❌ AWS S3 error listing files: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Error listing files from S3: {e}")
            return []
    
    async def get_file_size(self, file_path: str) -> int:
        """Get file size from S3"""
        try:
            if not self.s3_client:
                await self.connect()
            
            bucket_name, key = self._parse_s3_path(file_path)
            
            response = self.s3_client.head_object(Bucket=bucket_name, Key=key)
            size = response['ContentLength']
            
            logger.debug(f"📏 File size for {key}: {size} bytes")
            return size
            
        except ClientError as e:
            logger.error(f"❌ AWS S3 error getting file size: {e}")
            return 0
        except Exception as e:
            logger.error(f"❌ Error getting file size from S3: {e}")
            return 0
    
    async def read_file_sample(self, file_path: str, max_bytes: int = 1024*1024) -> bytes:
        """Read a sample of the file from S3"""
        try:
            if not self.s3_client:
                await self.connect()
            
            bucket_name, key = self._parse_s3_path(file_path)
            
            # Download only the first max_bytes
            response = self.s3_client.get_object(
                Bucket=bucket_name,
                Key=key,
                Range=f'bytes=0-{max_bytes-1}'
            )
            
            sample_data = response['Body'].read()
            
            logger.debug(f"📖 Read {len(sample_data)} bytes from {key}")
            return sample_data
            
        except ClientError as e:
            logger.error(f"❌ AWS S3 error reading file sample: {e}")
            return b''
        except Exception as e:
            logger.error(f"❌ Error reading file sample from S3: {e}")
            return b''
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test the AWS S3 connection"""
        try:
            if not self.s3_client:
                return {"success": False, "error": "Not connected"}
            
            # Try to list buckets to test connection
            response = self.s3_client.list_buckets()
            
            return {
                "success": True,
                "source_type": "aws_s3",
                "connection_status": "connected",
                "bucket_count": len(response.get('Buckets', []))
            }
            
        except ClientError as e:
            logger.error(f"❌ AWS S3 connection test failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "source_type": "aws_s3",
                "connection_status": "failed"
            }
        except Exception as e:
            logger.error(f"❌ AWS S3 connection test failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "source_type": "aws_s3",
                "connection_status": "failed"
            }
    
    def _parse_s3_path(self, path: str) -> tuple:
        """Parse S3 path to extract bucket and key"""
        try:
            if path.startswith('s3://'):
                # s3://bucket/path/key
                path = path[5:]  # Remove s3:// prefix
                parts = path.split('/', 1)
                bucket_name = parts[0] if len(parts) > 0 else ''
                key = parts[1] if len(parts) > 1 else ''
            
            elif path.startswith('https://s3'):
                # https://bucket.s3.region.amazonaws.com/path/key
                # or https://s3.region.amazonaws.com/bucket/path/key
                if '.s3.' in path:
                    # Format: https://bucket.s3.region.amazonaws.com/path/key
                    parts = path.replace('https://', '').split('.s3.')
                    bucket_name = parts[0]
                    key = '/'.join(parts[1].split('/')[1:])  # Remove region.amazonaws.com
                else:
                    # Format: https://s3.region.amazonaws.com/bucket/path/key
                    parts = path.replace('https://', '').split('/')
                    bucket_name = parts[1] if len(parts) > 1 else ''
                    key = '/'.join(parts[2:]) if len(parts) > 2 else ''
            
            else:
                # Assume it's already parsed as bucket/key format
                parts = path.split('/', 1)
                bucket_name = parts[0] if len(parts) > 0 else ''
                key = parts[1] if len(parts) > 1 else ''
            
            return bucket_name, key
            
        except Exception as e:
            logger.error(f"❌ Error parsing S3 path: {e}")
            return '', ''
    
    async def get_bucket_info(self, bucket_name: str) -> Dict[str, Any]:
        """Get information about a specific S3 bucket"""
        try:
            if not self.s3_client:
                await self.connect()
            
            response = self.s3_client.head_bucket(Bucket=bucket_name)
            
            return {
                "bucket_name": bucket_name,
                "region": response.get('ResponseMetadata', {}).get('HTTPHeaders', {}).get('x-amz-bucket-region'),
                "status": "exists"
            }
            
        except ClientError as e:
            logger.error(f"❌ Error getting bucket info: {e}")
            return {"error": str(e)}
    
    async def get_object_metadata(self, file_path: str) -> Dict[str, Any]:
        """Get metadata for a specific S3 object"""
        try:
            if not self.s3_client:
                await self.connect()
            
            bucket_name, key = self._parse_s3_path(file_path)
            
            response = self.s3_client.head_object(Bucket=bucket_name, Key=key)
            
            return {
                "key": key,
                "size": response['ContentLength'],
                "last_modified": response['LastModified'].isoformat(),
                "etag": response['ETag'].strip('"'),
                "content_type": response.get('ContentType'),
                "content_encoding": response.get('ContentEncoding'),
                "metadata": response.get('Metadata', {})
            }
            
        except ClientError as e:
            logger.error(f"❌ Error getting object metadata: {e}")
            return {"error": str(e)}
