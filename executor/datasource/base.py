"""
Base data source connector class
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import os


class DataSourceBase(ABC):
    """Base class for all data source connectors"""
    
    def __init__(self, credentials: Dict[str, str]):
        self.credentials = credentials
        self.connection = None
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the data source"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Close connection to the data source"""
        pass
    
    @abstractmethod
    async def list_files(self, path: str) -> List[str]:
        """List files in the specified path"""
        pass
    
    @abstractmethod
    async def get_file_size(self, file_path: str) -> int:
        """Get file size in bytes"""
        pass
    
    @abstractmethod
    async def read_file_sample(self, file_path: str, max_bytes: int = 1024*1024) -> bytes:
        """Read a sample of the file (first max_bytes)"""
        pass
    
    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """Test the connection and return status"""
        pass
    
    def can_handle(self, path: str) -> bool:
        """Check if this connector can handle the given path"""
        return False
    
    def get_source_type(self) -> str:
        """Get the data source type"""
        return "unknown"
    
    def validate_credentials(self) -> bool:
        """Validate that required credentials are present"""
        return True
    
    def mask_credentials(self) -> Dict[str, str]:
        """Return credentials with sensitive data masked"""
        masked = {}
        for key, value in self.credentials.items():
            if any(sensitive in key.lower() for sensitive in ['password', 'key', 'token', 'secret']):
                masked[key] = '***MASKED***' if value else ''
            else:
                masked[key] = str(value)
        return masked
