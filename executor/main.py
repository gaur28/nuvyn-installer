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
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

# Add executor directory to path
sys.path.insert(0, os.path.dirname(__file__))

from executor.config import ConfigManager, JobType
from executor.job_manager import JobManager
from executor.logger import initialize_logger, get_logger

logger = get_logger(__name__)


def get_workflow_id_from_environment():
    """
    Extract workflow_id from NUVYN_JOB_PAYLOAD environment variable.
    This ensures the backend-provided workflow_id is used as the primary identifier.
    
    Returns:
        str: workflow_id from job_metadata, or None if not found
    """
    if "NUVYN_JOB_PAYLOAD" not in os.environ:
        logger.debug("NUVYN_JOB_PAYLOAD environment variable not set")
        return None
    
    try:
        job_payload = json.loads(os.environ["NUVYN_JOB_PAYLOAD"])
        
        # First, try to get workflow_id from job_metadata (preferred)
        if "job_metadata" in job_payload:
            job_metadata = job_payload["job_metadata"]
            if "workflow_id" in job_metadata:
                workflow_id = job_metadata["workflow_id"]
                logger.info(f"✅ Using workflow_id from job_metadata: {workflow_id}")
                return workflow_id
        
        # Fallback to top-level workflow_id
        if "workflow_id" in job_payload:
            workflow_id = job_payload["workflow_id"]
            logger.info(f"✅ Using workflow_id from top-level payload: {workflow_id}")
            return workflow_id
            
    except json.JSONDecodeError as e:
        logger.warning(f"⚠️ Failed to parse NUVYN_JOB_PAYLOAD as JSON: {e}")
    except Exception as e:
        logger.warning(f"⚠️ Error reading NUVYN_JOB_PAYLOAD: {e}")
    
    return None


def get_source_id_from_environment():
    """
    Extract source_id from NUVYN_JOB_PAYLOAD environment variable.
    This is used for filtering purposes.
    
    Returns:
        str: source_id from job_metadata, or None if not found
    """
    if "NUVYN_JOB_PAYLOAD" not in os.environ:
        logger.debug("NUVYN_JOB_PAYLOAD environment variable not set")
        return None
    
    try:
        job_payload = json.loads(os.environ["NUVYN_JOB_PAYLOAD"])
        
        # First, try to get source_id from job_metadata (preferred)
        if "job_metadata" in job_payload:
            job_metadata = job_payload["job_metadata"]
            if "source_id" in job_metadata:
                source_id = job_metadata["source_id"]
                logger.info(f"✅ Using source_id from job_metadata: {source_id}")
                return source_id
        
        # Fallback to top-level source_id
        if "source_id" in job_payload:
            source_id = job_payload["source_id"]
            logger.info(f"✅ Using source_id from top-level payload: {source_id}")
            return source_id
            
    except json.JSONDecodeError as e:
        logger.warning(f"⚠️ Failed to parse NUVYN_JOB_PAYLOAD as JSON: {e}")
    except Exception as e:
        logger.warning(f"⚠️ Error reading NUVYN_JOB_PAYLOAD: {e}")
    
    return None


def get_sources_from_environment() -> Optional[List[Dict[str, Any]]]:
    """
    Extract sources list from NUVYN_JOB_PAYLOAD environment variable.
    Used for multi-source processing.
    
    Returns:
        List[Dict[str, Any]]: List of source configurations, or None if not found
    """
    if "NUVYN_JOB_PAYLOAD" not in os.environ:
        logger.debug("NUVYN_JOB_PAYLOAD environment variable not set")
        return None
    
    try:
        job_payload = json.loads(os.environ["NUVYN_JOB_PAYLOAD"])
        
        # Check for sources array
        if "sources" in job_payload and isinstance(job_payload["sources"], list):
            sources = job_payload["sources"]
            logger.info(f"✅ Found {len(sources)} sources in NUVYN_JOB_PAYLOAD")
            return sources
        
        # Also check in job_metadata
        if "job_metadata" in job_payload and "sources" in job_payload["job_metadata"]:
            sources = job_payload["job_metadata"]["sources"]
            if isinstance(sources, list):
                logger.info(f"✅ Found {len(sources)} sources in job_metadata")
                return sources
            
    except json.JSONDecodeError as e:
        logger.warning(f"⚠️ Failed to parse NUVYN_JOB_PAYLOAD as JSON: {e}")
    except Exception as e:
        logger.warning(f"⚠️ Error reading NUVYN_JOB_PAYLOAD: {e}")
    
    return None


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
                                data_source_path: str = "",
                                data_source_type: str = "auto",
                                tenant_id: str = "default",
                                config_manager: ConfigManager = None,
                                job_metadata: Dict[str, Any] = None,
                                sources: List[Dict[str, Any]] = None) -> dict:
    """Create a new job and execute it
    
    Supports both single source and multiple sources:
    - Single source: Provide data_source_path
    - Multiple sources: Provide sources list
    """
    if sources and len(sources) > 0:
        logger.info(f"🆕 Creating new job: {job_type} for {len(sources)} sources")
    else:
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
            tenant_id=tenant_id,
            job_metadata=job_metadata or {},
            sources=sources or []
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
        python main.py <job_type> [data_source_path] [source_type] [tenant_id] [--write-to-db]

    Job Types:
        metadata_extraction                 Extract metadata from data sources
        schema_validation                   Validate/create executor metadata schema
        data_reading                        Read actual data from sources
        quality_assessment                  Assess data quality
        api_transmission                    Send results to backend API
        full_pipeline                       Execute complete pipeline

    Flags:
        --write-to-db                      Write metadata to Databricks SQL tables
    
    Examples:
        python main.py metadata_extraction /path/to/data csv default
        python main.py metadata_extraction /path/to/data csv default --write-to-db
        python main.py schema_validation
        python main.py full_pipeline /Volumes/data/sales parquet tenant123

    Environment Variables:
        DATABRICKS_WORKSPACE_URL           Databricks workspace URL
        NUVYN_API_ENDPOINT                 Backend API endpoint
        NUVYN_API_KEY                      API authentication key
        EXECUTOR_LOG_LEVEL                 Log level (DEBUG, INFO, WARNING, ERROR)
        
        # For --write-to-db flag:
        DATABRICKS_SERVER_HOSTNAME         Databricks SQL Warehouse hostname
        DATABRICKS_HTTP_PATH               Databricks SQL Warehouse HTTP path
        DATABRICKS_ACCESS_TOKEN            Databricks access token
        EXECUTOR_WRITE_TO_DB               Set to 'true' to enable DB writing
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
        
        # Check for sources in environment first (for multi-source mode)
        env_sources = get_sources_from_environment()
        
        # Parse arguments - if sources are in environment, data_source_path is optional
        if env_sources and len(env_sources) > 0:
            # Multi-source mode: data_source_path not required
            # Arguments can be: metadata_extraction <workflow_id> [--write-to-db]
            # or: metadata_extraction --write-to-db (workflow_id from env)
            data_source_path = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("--") else None
            data_source_type = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith("--") else "auto"
            tenant_id = sys.argv[4] if len(sys.argv) > 4 and not sys.argv[4].startswith("--") else "default"
        else:
            # Single source mode: data_source_path required
            data_source_path = sys.argv[2] if len(sys.argv) > 2 else None
            data_source_type = sys.argv[3] if len(sys.argv) > 3 else "auto"
            tenant_id = sys.argv[4] if len(sys.argv) > 4 else "default"
        
        # Check for --write-to-db flag
        write_to_db = "--write-to-db" in sys.argv or os.getenv("EXECUTOR_WRITE_TO_DB", "false").lower() == "true"
        
        # Databricks SQL connection parameters (from environment)
        db_server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
        db_http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
        db_access_token = os.getenv("DATABRICKS_ACCESS_TOKEN", "")
        
        logger.info(f"📋 Job Type: {job_type}")
        logger.info(f"📁 Data Source: {data_source_path or 'N/A (multi-source mode)'}")
        logger.info(f"🔍 Source Type: {data_source_type}")
        logger.info(f"🏢 Tenant ID: {tenant_id}")
        logger.info(f"💾 Write to DB: {write_to_db}")
        
        # Initialize Databricks writer if write-to-db is enabled
        db_writer = None
        if write_to_db:
            if db_server_hostname and db_http_path and db_access_token:
                from executor.storage.databricks_writer import DatabricksWriter
                db_writer = DatabricksWriter(db_server_hostname, db_http_path, db_access_token)
                if db_writer.connect():
                    db_writer.create_schema_and_tables()
                    logger.info("✅ Databricks SQL writer initialized")
                else:
                    logger.error("❌ Failed to initialize Databricks SQL writer")
                    db_writer = None
            else:
                logger.warning("⚠️ --write-to-db flag set but Databricks SQL credentials not provided")
                logger.warning("   Set: DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_ACCESS_TOKEN")
        
        # Execute based on job type
        if job_type == "metadata_extraction":
            # Pass db_writer in job_metadata if available
            job_metadata = {}
            if db_writer:
                job_metadata['db_writer'] = db_writer
            
            # Check for workflow_id, source_id in NUVYN_JOB_PAYLOAD environment variable
            # (sources already retrieved at the top of main() function)
            env_workflow_id = get_workflow_id_from_environment()
            env_source_id = get_source_id_from_environment()
            
            if env_workflow_id:
                if "workflow_id" not in job_metadata:
                    job_metadata["workflow_id"] = env_workflow_id
                    logger.info(f"✅ Using workflow_id from NUVYN_JOB_PAYLOAD: {env_workflow_id}")
            
            if env_source_id:
                if "source_id" not in job_metadata:
                    job_metadata["source_id"] = env_source_id
                    logger.info(f"✅ Using source_id from NUVYN_JOB_PAYLOAD: {env_source_id}")
            
            # Use sources already retrieved at the top of main() function
            sources = env_sources if env_sources else []
            
            if sources and len(sources) > 0:
                # Multi-source mode
                logger.info(f"🔄 Multi-source mode: Processing {len(sources)} sources")
                
                # Validate workflow_id is present
                if not env_workflow_id:
                    logger.error("❌ Error: workflow_id is required for multi-source extraction")
                    sys.exit(1)
                
                result = await create_and_execute_job(
                    job_type="metadata_extraction",
                    data_source_path="",  # Not used in multi-source mode
                    data_source_type=data_source_type,
                    tenant_id=tenant_id,
                    config_manager=config_manager,
                    job_metadata=job_metadata,
                    sources=sources
                )
            else:
                # Single source mode (backward compatible)
                if not data_source_path:
                    logger.error("❌ Error: Data source path required for metadata extraction")
                    sys.exit(1)
                
                result = await create_and_execute_job(
                    job_type="metadata_extraction",
                    data_source_path=data_source_path,
                    data_source_type=data_source_type,
                    tenant_id=tenant_id,
                    config_manager=config_manager,
                    job_metadata=job_metadata
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
        
        # Print result data
        if "result_data" in result and result["result_data"]:
            print("\n" + "="*60)
            print("📊 METADATA EXTRACTION RESULTS")
            print("="*60)
            import json
            print(json.dumps(result["result_data"], indent=2, default=str))
            print("="*60 + "\n")
        
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
