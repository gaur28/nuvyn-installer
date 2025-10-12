"""
Job Manager for Nuvyn Executor Script
Handles job lifecycle, status tracking, and execution coordination
"""

import asyncio
import uuid
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from config import JobConfig, JobType, JobStatus, ConfigManager
from logger import get_logger

logger = get_logger(__name__)


@dataclass
class JobResult:
    """Result of a job execution"""
    job_id: str
    status: JobStatus
    result_data: Dict[str, Any] = None
    error_message: str = None
    execution_time_seconds: float = 0
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.result_data is None:
            self.result_data = {}
        if self.metadata is None:
            self.metadata = {}


class JobManager:
    """Manages job lifecycle and execution"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.active_jobs: Dict[str, asyncio.Task] = {}
        self.job_results: Dict[str, JobResult] = {}
        self.max_concurrent_jobs = config_manager.executor_config.max_concurrent_jobs
    
    async def create_job(self, 
                        job_type: JobType,
                        data_source_path: str,
                        data_source_type: str = "auto",
                        tenant_id: str = "default",
                        job_metadata: Dict[str, Any] = None) -> str:
        """Create a new job and return job ID"""
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        
        job_config = JobConfig(
            job_id=job_id,
            job_type=job_type,
            data_source_path=data_source_path,
            data_source_type=data_source_type,
            tenant_id=tenant_id,
            job_metadata=job_metadata or {}
        )
        
        # Save job configuration
        if self.config_manager.save_job_config(job_config):
            logger.info(f"✅ Job created: {job_id} ({job_type.value})")
            return job_id
        else:
            raise Exception(f"Failed to create job {job_id}")
    
    async def execute_job(self, job_id: str) -> JobResult:
        """Execute a specific job"""
        logger.info(f"🚀 Starting job execution: {job_id}")
        
        # Load job configuration
        job_config = self.config_manager.get_job_config(job_id)
        if not job_config:
            raise Exception(f"Job configuration not found: {job_id}")
        
        # Check concurrent job limit
        if len(self.active_jobs) >= self.max_concurrent_jobs:
            raise Exception(f"Maximum concurrent jobs ({self.max_concurrent_jobs}) reached")
        
        # Update job status to RUNNING
        await self.update_job_status(job_id, JobStatus.RUNNING)
        
        # Create and track the job task
        task = asyncio.create_task(self._execute_job_task(job_config))
        self.active_jobs[job_id] = task
        
        try:
            # Wait for job completion with timeout
            timeout_seconds = job_config.timeout_minutes * 60
            result = await asyncio.wait_for(task, timeout=timeout_seconds)
            
            # Store result
            self.job_results[job_id] = result
            logger.info(f"✅ Job completed: {job_id} ({result.status.value})")
            
            return result
            
        except asyncio.TimeoutError:
            logger.error(f"⏰ Job timeout: {job_id}")
            await self.update_job_status(job_id, JobStatus.FAILED, "Job timeout")
            result = JobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message="Job execution timeout"
            )
            self.job_results[job_id] = result
            return result
            
        except Exception as e:
            logger.error(f"❌ Job execution failed: {job_id} - {str(e)}")
            await self.update_job_status(job_id, JobStatus.FAILED, str(e))
            result = JobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=str(e)
            )
            self.job_results[job_id] = result
            return result
            
        finally:
            # Clean up active job tracking
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]
    
    async def _execute_job_task(self, job_config: JobConfig) -> JobResult:
        """Execute the actual job task"""
        start_time = datetime.now(timezone.utc)
        
        try:
            logger.info(f"🔧 Executing {job_config.job_type.value} job: {job_config.job_id}")
            logger.debug(f"🔍 Job type enum: {job_config.job_type}")
            logger.debug(f"🔍 Job type name: {job_config.job_type.name}")
            logger.debug(f"🔍 Job type value: {job_config.job_type.value}")
            
            # Import job executors dynamically
            if job_config.job_type == JobType.METADATA_EXTRACTION:
                from metadata.extractor import MetadataExtractor
                executor = MetadataExtractor(self.config_manager)
                
                # Check if db_writer is provided in job_metadata
                if 'db_writer' in job_config.job_metadata:
                    executor.write_to_db = True
                    executor.db_writer = job_config.job_metadata['db_writer']
                
                result_data = await executor.extract_metadata(job_config)
                
            elif job_config.job_type == JobType.SCHEMA_VALIDATION:
                from schema.validator import SchemaValidator
                executor = SchemaValidator(self.config_manager)
                result_data = await executor.validate_schema(job_config)
                
            elif job_config.job_type == JobType.DATA_READING:
                from data_reader.reader import DataReader
                executor = DataReader(self.config_manager)
                result_data = await executor.read_data(job_config)
                
            elif job_config.job_type == JobType.QUALITY_ASSESSMENT:
                from metadata.quality_assessor import QualityAssessor
                executor = QualityAssessor(self.config_manager)
                result_data = await executor.assess_quality(job_config)
                
            elif job_config.job_type == JobType.API_TRANSMISSION:
                from transport.api_client import APIClient
                executor = APIClient(self.config_manager)
                result_data = await executor.transmit_data(job_config)
                
            elif job_config.job_type == JobType.FULL_PIPELINE:
                # Execute full pipeline (multiple job types in sequence)
                result_data = await self._execute_full_pipeline(job_config)
                
            else:
                raise Exception(f"Unknown job type: {job_config.job_type}")
            
            # Calculate execution time
            end_time = datetime.now(timezone.utc)
            execution_time = (end_time - start_time).total_seconds()
            
            # Update job status to COMPLETED
            await self.update_job_status(job_config.job_id, JobStatus.COMPLETED)
            
            return JobResult(
                job_id=job_config.job_id,
                status=JobStatus.COMPLETED,
                result_data=result_data,
                execution_time_seconds=execution_time,
                metadata={
                    "job_type": job_config.job_type.value,
                    "data_source_path": job_config.data_source_path,
                    "tenant_id": job_config.tenant_id,
                    "started_at": start_time.isoformat(),
                    "completed_at": end_time.isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"❌ Job task failed: {job_config.job_id} - {str(e)}")
            
            # Calculate execution time
            end_time = datetime.now(timezone.utc)
            execution_time = (end_time - start_time).total_seconds()
            
            return JobResult(
                job_id=job_config.job_id,
                status=JobStatus.FAILED,
                error_message=str(e),
                execution_time_seconds=execution_time,
                metadata={
                    "job_type": job_config.job_type.value,
                    "data_source_path": job_config.data_source_path,
                    "tenant_id": job_config.tenant_id,
                    "started_at": start_time.isoformat(),
                    "failed_at": end_time.isoformat()
                }
            )
    
    async def _execute_full_pipeline(self, job_config: JobConfig) -> Dict[str, Any]:
        """Execute full pipeline (multiple job types in sequence)"""
        logger.info(f"🔄 Executing full pipeline: {job_config.job_id}")
        
        pipeline_results = {}
        
        # Step 1: Schema Validation
        try:
            schema_job_config = JobConfig(
                job_id=f"{job_config.job_id}_schema",
                job_type=JobType.SCHEMA_VALIDATION,
                data_source_path=job_config.data_source_path,
                data_source_type=job_config.data_source_type,
                tenant_id=job_config.tenant_id
            )
            
            from .schema.validator import SchemaValidator
            schema_validator = SchemaValidator(self.config_manager)
            schema_result = await schema_validator.validate_schema(schema_job_config)
            pipeline_results["schema_validation"] = schema_result
            
        except Exception as e:
            logger.error(f"❌ Schema validation failed: {e}")
            pipeline_results["schema_validation"] = {"error": str(e)}
        
        # Step 2: Metadata Extraction
        try:
            metadata_job_config = JobConfig(
                job_id=f"{job_config.job_id}_metadata",
                job_type=JobType.METADATA_EXTRACTION,
                data_source_path=job_config.data_source_path,
                data_source_type=job_config.data_source_type,
                tenant_id=job_config.tenant_id
            )
            
            from .metadata.extractor import MetadataExtractor
            metadata_extractor = MetadataExtractor(self.config_manager)
            metadata_result = await metadata_extractor.extract_metadata(metadata_job_config)
            pipeline_results["metadata_extraction"] = metadata_result
            
        except Exception as e:
            logger.error(f"❌ Metadata extraction failed: {e}")
            pipeline_results["metadata_extraction"] = {"error": str(e)}
        
        # Step 3: Quality Assessment
        try:
            quality_job_config = JobConfig(
                job_id=f"{job_config.job_id}_quality",
                job_type=JobType.QUALITY_ASSESSMENT,
                data_source_path=job_config.data_source_path,
                data_source_type=job_config.data_source_type,
                tenant_id=job_config.tenant_id
            )
            
            from .metadata.quality_assessor import QualityAssessor
            quality_assessor = QualityAssessor(self.config_manager)
            quality_result = await quality_assessor.assess_quality(quality_job_config)
            pipeline_results["quality_assessment"] = quality_result
            
        except Exception as e:
            logger.error(f"❌ Quality assessment failed: {e}")
            pipeline_results["quality_assessment"] = {"error": str(e)}
        
        # Step 4: API Transmission
        try:
            api_job_config = JobConfig(
                job_id=f"{job_config.job_id}_api",
                job_type=JobType.API_TRANSMISSION,
                data_source_path=job_config.data_source_path,
                data_source_type=job_config.data_source_type,
                tenant_id=job_config.tenant_id
            )
            
            from .transport.api_client import APIClient
            api_client = APIClient(self.config_manager)
            api_result = await api_client.transmit_data(api_job_config)
            pipeline_results["api_transmission"] = api_result
            
        except Exception as e:
            logger.error(f"❌ API transmission failed: {e}")
            pipeline_results["api_transmission"] = {"error": str(e)}
        
        return pipeline_results
    
    async def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        """Get current job status"""
        # Check if job is currently running
        if job_id in self.active_jobs:
            return JobStatus.RUNNING
        
        # Check if job has completed
        if job_id in self.job_results:
            return self.job_results[job_id].status
        
        # In a real implementation, this would query the database
        return None
    
    async def get_job_result(self, job_id: str) -> Optional[JobResult]:
        """Get job result if available"""
        return self.job_results.get(job_id)
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job"""
        if job_id in self.active_jobs:
            task = self.active_jobs[job_id]
            task.cancel()
            del self.active_jobs[job_id]
            
            await self.update_job_status(job_id, JobStatus.CANCELLED)
            logger.info(f"🚫 Job cancelled: {job_id}")
            return True
        
        return False
    
    async def list_jobs(self, 
                       status_filter: Optional[JobStatus] = None,
                       tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List jobs with optional filtering"""
        jobs = []
        
        # Add active jobs
        for job_id, task in self.active_jobs.items():
            job_config = self.config_manager.load_job_config(job_id)
            if job_config and (not tenant_id or job_config.tenant_id == tenant_id):
                jobs.append({
                    "job_id": job_id,
                    "status": JobStatus.RUNNING.value,
                    "job_type": job_config.job_type.value,
                    "data_source_path": job_config.data_source_path,
                    "tenant_id": job_config.tenant_id,
                    "created_at": job_config.created_at.isoformat()
                })
        
        # Add completed jobs
        for job_id, result in self.job_results.items():
            if not status_filter or result.status == status_filter:
                job_config = self.config_manager.load_job_config(job_id)
                if job_config and (not tenant_id or job_config.tenant_id == tenant_id):
                    jobs.append({
                        "job_id": job_id,
                        "status": result.status.value,
                        "job_type": job_config.job_type.value,
                        "data_source_path": job_config.data_source_path,
                        "tenant_id": job_config.tenant_id,
                        "created_at": job_config.created_at.isoformat(),
                        "execution_time": result.execution_time_seconds,
                        "error_message": result.error_message
                    })
        
        return jobs
    
    async def update_job_status(self, 
                               job_id: str, 
                               status: JobStatus, 
                               error_message: str = None):
        """Update job status in storage"""
        try:
            # In a real implementation, this would update the database
            logger.info(f"📊 Job status updated: {job_id} -> {status.value}")
            
            if error_message:
                logger.error(f"❌ Job error: {job_id} - {error_message}")
                
        except Exception as e:
            logger.error(f"❌ Failed to update job status: {e}")
    
    async def cleanup_old_jobs(self):
        """Clean up old completed jobs"""
        cutoff_date = datetime.now(timezone.utc) - timedelta(
            days=self.config_manager.executor_config.cleanup_completed_jobs_days
        )
        
        jobs_to_remove = []
        for job_id, result in self.job_results.items():
            job_config = self.config_manager.load_job_config(job_id)
            if (job_config and 
                job_config.created_at < cutoff_date and 
                result.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]):
                jobs_to_remove.append(job_id)
        
        for job_id in jobs_to_remove:
            del self.job_results[job_id]
            logger.info(f"🧹 Cleaned up old job: {job_id}")
    
    def get_active_job_count(self) -> int:
        """Get number of currently active jobs"""
        return len(self.active_jobs)
    
    def get_job_statistics(self) -> Dict[str, Any]:
        """Get job execution statistics"""
        total_jobs = len(self.job_results)
        completed_jobs = sum(1 for result in self.job_results.values() 
                           if result.status == JobStatus.COMPLETED)
        failed_jobs = sum(1 for result in self.job_results.values() 
                         if result.status == JobStatus.FAILED)
        
        avg_execution_time = 0
        if completed_jobs > 0:
            total_time = sum(result.execution_time_seconds for result in self.job_results.values()
                           if result.status == JobStatus.COMPLETED)
            avg_execution_time = total_time / completed_jobs
        
        return {
            "total_jobs": total_jobs,
            "active_jobs": len(self.active_jobs),
            "completed_jobs": completed_jobs,
            "failed_jobs": failed_jobs,
            "success_rate": (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0,
            "average_execution_time_seconds": avg_execution_time,
            "max_concurrent_jobs": self.max_concurrent_jobs
        }
