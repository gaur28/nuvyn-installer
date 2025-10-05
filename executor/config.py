"""
Configuration management for Nuvyn Executor Script
Handles job configuration, environment variables, and settings
"""

import os
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class JobType(Enum):
    """Available job types"""
    METADATA_EXTRACTION = "metadata_extraction"
    SCHEMA_VALIDATION = "schema_validation"
    DATA_READING = "data_reading"
    QUALITY_ASSESSMENT = "quality_assessment"
    API_TRANSMISSION = "api_transmission"
    FULL_PIPELINE = "full_pipeline"


class JobStatus(Enum):
    """Job execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class JobConfig:
    """Configuration for a specific job"""
    job_id: str
    job_type: JobType
    data_source_path: str
    data_source_type: str = "auto"
    tenant_id: str = "default"
    api_endpoint: str = ""
    api_key: str = ""
    priority: int = 1
    timeout_minutes: int = 60
    created_at: datetime = None
    created_by: str = "system"
    job_metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.job_metadata is None:
            self.job_metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['job_type'] = self.job_type.value
        data['created_at'] = self.created_at.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobConfig':
        """Create from dictionary"""
        data['job_type'] = JobType(data['job_type'])
        data['created_at'] = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00'))
        return cls(**data)


@dataclass
class ExecutorConfig:
    """Global executor configuration"""
    # Database/Storage
    databricks_workspace_url: str = ""
    schema_name: str = "_executor_metadata"
    
    # API Configuration
    api_base_url: str = ""
    api_timeout: int = 30
    api_retry_attempts: int = 3
    
    # Data Source Configuration
    max_file_size_mb: int = 100
    max_files_per_job: int = 1000
    sample_size_rows: int = 10000
    
    # Job Configuration
    max_concurrent_jobs: int = 5
    job_timeout_minutes: int = 60
    cleanup_completed_jobs_days: int = 30
    
    # Logging
    log_level: str = "INFO"
    log_file_path: str = ""
    verbose: bool = False
    
    # Security
    enable_credential_validation: bool = True
    mask_sensitive_data: bool = True
    
    @classmethod
    def from_environment(cls) -> 'ExecutorConfig':
        """Create configuration from environment variables"""
        return cls(
            databricks_workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL", ""),
            schema_name=os.getenv("EXECUTOR_SCHEMA_NAME", "_executor_metadata"),
            api_base_url=os.getenv("NUVYN_API_ENDPOINT", ""),
            api_timeout=int(os.getenv("NUVYN_API_TIMEOUT", "30")),
            api_retry_attempts=int(os.getenv("NUVYN_API_RETRY_ATTEMPTS", "3")),
            max_file_size_mb=int(os.getenv("EXECUTOR_MAX_FILE_SIZE_MB", "100")),
            max_files_per_job=int(os.getenv("EXECUTOR_MAX_FILES_PER_JOB", "1000")),
            sample_size_rows=int(os.getenv("EXECUTOR_SAMPLE_SIZE_ROWS", "10000")),
            max_concurrent_jobs=int(os.getenv("EXECUTOR_MAX_CONCURRENT_JOBS", "5")),
            job_timeout_minutes=int(os.getenv("EXECUTOR_JOB_TIMEOUT_MINUTES", "60")),
            cleanup_completed_jobs_days=int(os.getenv("EXECUTOR_CLEANUP_DAYS", "30")),
            log_level=os.getenv("EXECUTOR_LOG_LEVEL", "INFO"),
            log_file_path=os.getenv("EXECUTOR_LOG_FILE_PATH", ""),
            verbose=os.getenv("EXECUTOR_VERBOSE", "false").lower() == "true",
            enable_credential_validation=os.getenv("EXECUTOR_ENABLE_CREDENTIAL_VALIDATION", "true").lower() == "true",
            mask_sensitive_data=os.getenv("EXECUTOR_MASK_SENSITIVE_DATA", "true").lower() == "true"
        )


class ConfigManager:
    """Manages configuration loading and validation"""
    
    def __init__(self):
        self.executor_config = ExecutorConfig.from_environment()
        self.job_configs: Dict[str, JobConfig] = {}
        self._validate_config()
    
    def _validate_config(self):
        """Validate configuration settings"""
        if not self.executor_config.databricks_workspace_url:
            print("⚠️ Warning: DATABRICKS_WORKSPACE_URL not set")
        
        if not self.executor_config.api_base_url:
            print("⚠️ Warning: NUVYN_API_ENDPOINT not set")
        
        if self.executor_config.max_file_size_mb <= 0:
            raise ValueError("max_file_size_mb must be positive")
        
        if self.executor_config.max_concurrent_jobs <= 0:
            raise ValueError("max_concurrent_jobs must be positive")
    
    def load_job_config(self, job_id: str) -> Optional[JobConfig]:
        """Load job configuration by ID"""
        try:
            # In a real implementation, this would load from database
            # For now, we'll create a basic config
            return JobConfig(
                job_id=job_id,
                job_type=JobType.METADATA_EXTRACTION,
                data_source_path="",  # Will be set by job execution
                tenant_id=self.executor_config.schema_name.split('_')[0] if '_' in self.executor_config.schema_name else "default"
            )
        except Exception as e:
            print(f"❌ Failed to load job config for {job_id}: {e}")
            return None
    
    def save_job_config(self, job_config: JobConfig) -> bool:
        """Save job configuration"""
        try:
            # Store job config in memory
            self.job_configs[job_config.job_id] = job_config
            print(f"💾 Job config saved for {job_config.job_id}")
            return True
        except Exception as e:
            print(f"❌ Failed to save job config: {e}")
            return False
    
    def get_job_config(self, job_id: str) -> Optional[JobConfig]:
        """Get job configuration by job ID"""
        return self.job_configs.get(job_id)
    
    def get_data_source_credentials(self, data_source_type: str) -> Dict[str, str]:
        """Get credentials for data source type"""
        credentials = {}
        
        if data_source_type.lower() in ['azure_blob', 'azure']:
            credentials.update({
                'connection_string': os.getenv('AZURE_STORAGE_CONNECTION_STRING', ''),
                'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME', ''),
                'account_key': os.getenv('AZURE_STORAGE_ACCOUNT_KEY', ''),
                'sas_token': os.getenv('AZURE_STORAGE_SAS_TOKEN', '')
            })
        elif data_source_type.lower() in ['aws_s3', 's3']:
            credentials.update({
                'access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
                'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
                'region': os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
            })
        elif data_source_type.lower() in ['mysql', 'postgresql', 'postgres', 'snowflake']:
            credentials.update({
                'host': os.getenv(f'{data_source_type.upper()}_HOST', ''),
                'port': os.getenv(f'{data_source_type.upper()}_PORT', ''),
                'username': os.getenv(f'{data_source_type.upper()}_USERNAME', ''),
                'password': os.getenv(f'{data_source_type.upper()}_PASSWORD', ''),
                'database': os.getenv(f'{data_source_type.upper()}_DATABASE', '')
            })
        
        # Mask sensitive data if enabled
        if self.executor_config.mask_sensitive_data:
            masked_credentials = {}
            for key, value in credentials.items():
                if 'password' in key.lower() or 'key' in key.lower() or 'token' in key.lower():
                    masked_credentials[key] = '***MASKED***' if value else ''
                else:
                    masked_credentials[key] = value
            return masked_credentials
        
        return credentials
    
    def get_api_credentials(self) -> Dict[str, str]:
        """Get API credentials"""
        credentials = {
            'api_key': os.getenv('NUVYN_API_KEY', ''),
            'api_endpoint': os.getenv('NUVYN_API_ENDPOINT', ''),
            'tenant_id': os.getenv('NUVYN_TENANT_ID', 'default')
        }
        
        # Mask API key if enabled
        if self.executor_config.mask_sensitive_data and credentials['api_key']:
            credentials['api_key'] = '***MASKED***'
        
        return credentials
