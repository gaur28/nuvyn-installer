#!/usr/bin/env python3
"""
Nuvyn Executor Script - Databricks Job Integration
Executes specific jobs triggered via Databricks Jobs API
"""

import sys
import os
import asyncio
import argparse
import json
from datetime import datetime, timezone

# Add executor directory to path
sys.path.insert(0, os.path.dirname(__file__))

from config import ConfigManager, JobType
from job_manager import JobManager
from logger import initialize_logger, get_logger

logger = get_logger(__name__)


async def execute_job_by_id(job_id: str, config_manager: ConfigManager = None) -> dict:
    """Execute a specific job by ID"""
    logger.info(f"🚀 Starting job execution: {job_id}")
    
    # Create job manager
    job_manager = JobManager(config_manager)
    
    try:
        # Execute the job
        result = await job_manager.execute_job(job_id)
        
        # Log results
        if result.status.value == "completed":
            logger.info(f"✅ Job completed successfully: {job_id}")
            logger.info(f"📊 Execution time: {result.execution_time_seconds:.2f}s")
        else:
            logger.error(f"❌ Job failed: {job_id} - {result.error_message}")
        
        return {
            "job_id": job_id,
            "status": result.status.value,
            "success": result.status.value == "completed",
            "execution_time": result.execution_time_seconds,
            "error": result.error_message,
            "result_data": result.result_data,
            "metadata": result.metadata
        }
        
    except Exception as e:
        logger.error(f"❌ Job execution error: {job_id} - {str(e)}")
        return {
            "job_id": job_id,
            "status": "error",
            "success": False,
            "error": str(e),
            "execution_time": 0,
            "result_data": {},
            "metadata": {}
        }


async def create_and_execute_job(job_type: str, 
                                data_source_path: str,
                                data_source_type: str = "auto",
                                tenant_id: str = "default",
                                config_manager: ConfigManager = None) -> dict:
    """Create a new job and execute it"""
    logger.info(f"🆕 Creating new job: {job_type} for {data_source_path}")
    
    # Create config manager if not provided
    if config_manager is None:
        config_manager = ConfigManager()
    
    # Create job manager
    job_manager = JobManager(config_manager)
    
    try:
        # Create the job
        job_id = await job_manager.create_job(
            job_type=JobType(job_type),
            data_source_path=data_source_path,
            data_source_type=data_source_type,
            tenant_id=tenant_id
        )
        
        logger.info(f"📋 Job created: {job_id}")
        
        # Execute the job
        result = await job_manager.execute_job(job_id)
        
        # Log results
        if result.status.value == "completed":
            logger.info(f"✅ Job completed successfully: {job_id}")
            logger.info(f"📊 Execution time: {result.execution_time_seconds:.2f}s")
        else:
            logger.error(f"❌ Job failed: {job_id} - {result.error_message}")
        
        return {
            "job_id": job_id,
            "status": result.status.value,
            "success": result.status.value == "completed",
            "execution_time": result.execution_time_seconds,
            "error": result.error_message,
            "result_data": result.result_data,
            "metadata": result.metadata
        }
        
    except Exception as e:
        logger.error(f"❌ Job creation/execution error: {str(e)}")
        return {
            "job_id": "unknown",
            "status": "error",
            "success": False,
            "error": str(e),
            "execution_time": 0,
            "result_data": {},
            "metadata": {}
        }


async def get_job_status(job_id: str, config_manager: ConfigManager = None) -> dict:
    """Get status of a specific job"""
    logger.info(f"📊 Getting job status: {job_id}")
    
    job_manager = JobManager(config_manager)
    
    try:
        status = await job_manager.get_job_status(job_id)
        result = await job_manager.get_job_result(job_id)
        
        return {
            "job_id": job_id,
            "status": status.value if status else "unknown",
            "result": result.result_data if result else None,
            "execution_time": result.execution_time_seconds if result else 0,
            "error": result.error_message if result else None,
            "metadata": result.metadata if result else {}
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting job status: {str(e)}")
        return {
            "job_id": job_id,
            "status": "error",
            "error": str(e),
            "result": None,
            "execution_time": 0,
            "metadata": {}
        }


async def list_jobs(config_manager: ConfigManager, 
                   status_filter: str = None,
                   tenant_id: str = None) -> dict:
    """List jobs with optional filtering"""
    logger.info("📋 Listing jobs")
    
    job_manager = JobManager(config_manager)
    
    try:
        jobs = await job_manager.list_jobs(
            status_filter=status_filter,
            tenant_id=tenant_id
        )
        
        return {
            "total_jobs": len(jobs),
            "jobs": jobs,
            "filters": {
                "status": status_filter,
                "tenant_id": tenant_id
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error listing jobs: {str(e)}")
        return {
            "total_jobs": 0,
            "jobs": [],
            "error": str(e),
            "filters": {
                "status": status_filter,
                "tenant_id": tenant_id
            }
        }


async def get_job_statistics(config_manager: ConfigManager) -> dict:
    """Get job execution statistics"""
    logger.info("📊 Getting job statistics")
    
    job_manager = JobManager(config_manager)
    
    try:
        stats = job_manager.get_job_statistics()
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error getting job statistics: {str(e)}")
        return {
            "error": str(e),
            "total_jobs": 0,
            "active_jobs": 0,
            "completed_jobs": 0,
            "failed_jobs": 0,
            "success_rate": 0
        }


def print_usage():
    """Print usage information for Databricks Jobs"""
    print("""
    Nuvyn Executor Script - Databricks Jobs Integration

    Usage:
        python main.py <job_type> [data_source_path] [source_type] [tenant_id]

    Job Types:
        metadata_extraction                 Extract metadata from data sources
        schema_validation                   Validate/create executor metadata schema
        data_reading                        Read actual data from sources
        quality_assessment                  Assess data quality
        api_transmission                    Send results to backend API
        full_pipeline                       Execute complete pipeline

    Examples:
        python main.py metadata_extraction /path/to/data csv default
        python main.py schema_validation
        python main.py full_pipeline /Volumes/data/sales parquet tenant123

    Environment Variables:
        DATABRICKS_WORKSPACE_URL           Databricks workspace URL
        NUVYN_API_ENDPOINT                 Backend API endpoint
        NUVYN_API_KEY                      API authentication key
        EXECUTOR_LOG_LEVEL                 Log level (DEBUG, INFO, WARNING, ERROR)
    """)


async def main():
    """Main entry point for Databricks Jobs"""
    # Initialize configuration
    config_manager = ConfigManager()
    
    # Initialize logger
    initialize_logger(
        log_level="INFO",
        enable_console=True,
        enable_colors=True
    )
    
    logger.info("🚀 Nuvyn Executor Script starting via Databricks Jobs...")
    
    try:
        # Parse command line arguments (from Databricks Jobs API)
        if len(sys.argv) < 2:
            logger.error("❌ Error: No job type specified")
            print_usage()
            sys.exit(1)
        
        job_type = sys.argv[1]
        data_source_path = sys.argv[2] if len(sys.argv) > 2 else None
        data_source_type = sys.argv[3] if len(sys.argv) > 3 else "auto"
        tenant_id = sys.argv[4] if len(sys.argv) > 4 else "default"
        
        logger.info(f"📋 Job Type: {job_type}")
        logger.info(f"📁 Data Source: {data_source_path}")
        logger.info(f"🔍 Source Type: {data_source_type}")
        logger.info(f"🏢 Tenant ID: {tenant_id}")
        
        # Execute based on job type
        if job_type == "metadata_extraction":
            if not data_source_path:
                logger.error("❌ Error: Data source path required for metadata extraction")
                sys.exit(1)
            
            result = await create_and_execute_job(
                job_type="metadata_extraction",
                data_source_path=data_source_path,
                data_source_type=data_source_type,
                tenant_id=tenant_id,
                config_manager=config_manager
            )
            
        elif job_type == "schema_validation":
            result = await create_and_execute_job(
                job_type="schema_validation",
                data_source_path="/tmp",  # Schema validation doesn't need specific path
                data_source_type="auto",
                tenant_id=tenant_id,
                config_manager=config_manager
            )
            
        elif job_type == "data_reading":
            if not data_source_path:
                logger.error("❌ Error: Data source path required for data reading")
                sys.exit(1)
            
            result = await create_and_execute_job(
                job_type="data_reading",
                data_source_path=data_source_path,
                data_source_type=data_source_type,
                tenant_id=tenant_id,
                config_manager=config_manager
            )
            
        elif job_type == "quality_assessment":
            if not data_source_path:
                logger.error("❌ Error: Data source path required for quality assessment")
                sys.exit(1)
            
            result = await create_and_execute_job(
                job_type="quality_assessment",
                data_source_path=data_source_path,
                data_source_type=data_source_type,
                tenant_id=tenant_id,
                config_manager=config_manager
            )
            
        elif job_type == "api_transmission":
            result = await create_and_execute_job(
                job_type="api_transmission",
                data_source_path=data_source_path or "/tmp",
                data_source_type=data_source_type,
                tenant_id=tenant_id,
                config_manager=config_manager
            )
            
        elif job_type == "full_pipeline":
            if not data_source_path:
                logger.error("❌ Error: Data source path required for full pipeline")
                sys.exit(1)
            
            result = await create_and_execute_job(
                job_type="full_pipeline",
                data_source_path=data_source_path,
                data_source_type=data_source_type,
                tenant_id=tenant_id,
                config_manager=config_manager
            )
            
        else:
            logger.error(f"❌ Error: Unknown job type '{job_type}'")
            print_usage()
            sys.exit(1)
        
        # Print results for Databricks Jobs
        logger.info("📊 Execution completed")
        
        # Simplified output for Databricks Jobs
        if "success" in result:
            if result["success"]:
                logger.info(f"✅ Success: {result.get('job_id', 'N/A')} - {result.get('status', 'N/A')}")
                if "execution_time" in result:
                    logger.info(f"⏱️  Execution time: {result['execution_time']:.2f}s")
            else:
                logger.error(f"❌ Failed: {result.get('job_id', 'N/A')} - {result.get('error', 'Unknown error')}")
        
        # Exit with appropriate code for Databricks Jobs
        if "success" in result and not result["success"]:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except KeyboardInterrupt:
        logger.info("⏹️  Execution interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Execution error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def cli_main():
    """CLI entry point for pip-installed package"""
    asyncio.run(main())

if __name__ == "__main__":
    asyncio.run(main())
