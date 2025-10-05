#!/usr/bin/env python3
"""
HTTP API Server for Nuvyn Executor Script
Provides curl-friendly endpoints for job execution and management
"""

import asyncio
import json
import sys
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional

# Add executor directory to path
sys.path.insert(0, os.path.dirname(__file__))

from aiohttp import web, web_request
from aiohttp.web import Request, Response, json_response
import aiohttp_cors

from config import ConfigManager, JobType
from job_manager import JobManager
from logger import initialize_logger, get_logger

logger = get_logger(__name__)


class ExecutorAPIServer:
    """HTTP API Server for the executor script"""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8080):
        self.host = host
        self.port = port
        self.config_manager = ConfigManager()
        self.job_manager = JobManager(self.config_manager)
        self.app = web.Application()
        self._setup_routes()
        self._setup_cors()
    
    def _setup_routes(self):
        """Setup API routes"""
        
        # Health check
        self.app.router.add_get('/health', self.health_check)
        
        # Job execution endpoints
        self.app.router.add_post('/jobs/create', self.create_job)
        self.app.router.add_post('/jobs/{job_id}/execute', self.execute_job)
        self.app.router.add_get('/jobs/{job_id}/status', self.get_job_status)
        self.app.router.add_get('/jobs/{job_id}/result', self.get_job_result)
        self.app.router.add_delete('/jobs/{job_id}/cancel', self.cancel_job)
        
        # Job management endpoints
        self.app.router.add_get('/jobs', self.list_jobs)
        self.app.router.add_get('/jobs/stats', self.get_job_statistics)
        
        # Data source endpoints
        self.app.router.add_post('/datasources/test', self.test_data_source)
        self.app.router.add_get('/datasources/types', self.get_data_source_types)
        
        # Schema endpoints
        self.app.router.add_post('/schema/validate', self.validate_schema)
        self.app.router.add_post('/schema/create', self.create_schema)
        
        # Utility endpoints
        self.app.router.add_get('/info', self.get_info)
        self.app.router.add_post('/ping', self.ping)
    
    def _setup_cors(self):
        """Setup CORS for cross-origin requests"""
        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods="*"
            )
        })
        
        # Add CORS to all routes
        for route in list(self.app.router.routes()):
            cors.add(route)
    
    async def health_check(self, request: Request) -> Response:
        """Health check endpoint"""
        return json_response({
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0",
            "service": "nuvyn-executor-api"
        })
    
    async def create_job(self, request: Request) -> Response:
        """Create a new job"""
        try:
            data = await request.json()
            
            # Validate required fields
            required_fields = ['job_type', 'data_source_path']
            for field in required_fields:
                if field not in data:
                    return json_response({
                        "error": f"Missing required field: {field}",
                        "status": "error"
                    }, status=400)
            
            # Create job
            job_id = await self.job_manager.create_job(
                job_type=JobType(data['job_type']),
                data_source_path=data['data_source_path'],
                data_source_type=data.get('data_source_type', 'auto'),
                tenant_id=data.get('tenant_id', 'default'),
                job_metadata=data.get('job_metadata', {})
            )
            
            return json_response({
                "job_id": job_id,
                "status": "created",
                "message": "Job created successfully"
            })
            
        except Exception as e:
            logger.error(f"❌ Error creating job: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def execute_job(self, request: Request) -> Response:
        """Execute a specific job"""
        try:
            job_id = request.match_info['job_id']
            
            # Execute the job
            result = await self.job_manager.execute_job(job_id)
            
            return json_response({
                "job_id": job_id,
                "status": result.status.value,
                "success": result.status.value == "completed",
                "execution_time": result.execution_time_seconds,
                "error": result.error_message,
                "result_data": result.result_data,
                "metadata": result.metadata
            })
            
        except Exception as e:
            logger.error(f"❌ Error executing job: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def get_job_status(self, request: Request) -> Response:
        """Get job status"""
        try:
            job_id = request.match_info['job_id']
            
            status = await self.job_manager.get_job_status(job_id)
            result = await self.job_manager.get_job_result(job_id)
            
            return json_response({
                "job_id": job_id,
                "status": status.value if status else "unknown",
                "result": result.result_data if result else None,
                "execution_time": result.execution_time_seconds if result else 0,
                "error": result.error_message if result else None,
                "metadata": result.metadata if result else {}
            })
            
        except Exception as e:
            logger.error(f"❌ Error getting job status: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def get_job_result(self, request: Request) -> Response:
        """Get job result"""
        try:
            job_id = request.match_info['job_id']
            
            result = await self.job_manager.get_job_result(job_id)
            
            if not result:
                return json_response({
                    "error": "Job result not found",
                    "status": "not_found"
                }, status=404)
            
            return json_response({
                "job_id": job_id,
                "status": result.status.value,
                "result_data": result.result_data,
                "execution_time": result.execution_time_seconds,
                "error": result.error_message,
                "metadata": result.metadata
            })
            
        except Exception as e:
            logger.error(f"❌ Error getting job result: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def cancel_job(self, request: Request) -> Response:
        """Cancel a running job"""
        try:
            job_id = request.match_info['job_id']
            
            success = await self.job_manager.cancel_job(job_id)
            
            return json_response({
                "job_id": job_id,
                "cancelled": success,
                "status": "cancelled" if success else "not_found"
            })
            
        except Exception as e:
            logger.error(f"❌ Error cancelling job: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def list_jobs(self, request: Request) -> Response:
        """List jobs with optional filtering"""
        try:
            # Get query parameters
            status_filter = request.query.get('status')
            tenant_id = request.query.get('tenant_id')
            
            jobs = await self.job_manager.list_jobs(
                status_filter=status_filter,
                tenant_id=tenant_id
            )
            
            return json_response({
                "total_jobs": len(jobs),
                "jobs": jobs,
                "filters": {
                    "status": status_filter,
                    "tenant_id": tenant_id
                }
            })
            
        except Exception as e:
            logger.error(f"❌ Error listing jobs: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def get_job_statistics(self, request: Request) -> Response:
        """Get job execution statistics"""
        try:
            stats = self.job_manager.get_job_statistics()
            return json_response(stats)
            
        except Exception as e:
            logger.error(f"❌ Error getting job statistics: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def test_data_source(self, request: Request) -> Response:
        """Test data source connection"""
        try:
            data = await request.json()
            
            source_type = data.get('source_type')
            credentials = data.get('credentials', {})
            path = data.get('path', '')
            
            if not source_type:
                return json_response({
                    "error": "source_type is required",
                    "status": "error"
                }, status=400)
            
            # Import here to avoid circular imports
            from datasource.factory import DataSourceFactory
            
            # Test connection
            result = DataSourceFactory.test_connection(source_type, credentials)
            
            return json_response(result)
            
        except Exception as e:
            logger.error(f"❌ Error testing data source: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def get_data_source_types(self, request: Request) -> Response:
        """Get supported data source types"""
        try:
            from datasource.factory import DataSourceFactory
            
            types = DataSourceFactory.get_supported_types()
            type_info = {}
            
            for source_type in types:
                type_info[source_type] = DataSourceFactory.get_connector_info(source_type)
            
            return json_response({
                "supported_types": types,
                "type_info": type_info
            })
            
        except Exception as e:
            logger.error(f"❌ Error getting data source types: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def validate_schema(self, request: Request) -> Response:
        """Validate executor metadata schema"""
        try:
            from schema.validator import SchemaValidator
            
            validator = SchemaValidator(self.config_manager)
            
            # Create a dummy job config for validation
            from config import JobConfig
            job_config = JobConfig(
                job_id="schema_validation",
                job_type=JobType.SCHEMA_VALIDATION,
                data_source_path="",
                tenant_id=request.query.get('tenant_id', 'default')
            )
            
            result = await validator.validate_schema(job_config)
            
            return json_response(result)
            
        except Exception as e:
            logger.error(f"❌ Error validating schema: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def create_schema(self, request: Request) -> Response:
        """Create executor metadata schema"""
        try:
            from schema.validator import SchemaValidator
            
            validator = SchemaValidator(self.config_manager)
            
            # Create a dummy job config for schema creation
            from config import JobConfig
            job_config = JobConfig(
                job_id="schema_creation",
                job_type=JobType.SCHEMA_VALIDATION,
                data_source_path="",
                tenant_id=request.query.get('tenant_id', 'default')
            )
            
            result = await validator.create_schema(job_config)
            
            return json_response(result)
            
        except Exception as e:
            logger.error(f"❌ Error creating schema: {e}")
            return json_response({
                "error": str(e),
                "status": "error"
            }, status=500)
    
    async def get_info(self, request: Request) -> Response:
        """Get API information"""
        return json_response({
            "service": "nuvyn-executor-api",
            "version": "1.0.0",
            "description": "Job-based metadata extraction and processing system",
            "endpoints": {
                "health": "GET /health",
                "create_job": "POST /jobs/create",
                "execute_job": "POST /jobs/{job_id}/execute",
                "get_status": "GET /jobs/{job_id}/status",
                "get_result": "GET /jobs/{job_id}/result",
                "cancel_job": "DELETE /jobs/{job_id}/cancel",
                "list_jobs": "GET /jobs",
                "job_stats": "GET /jobs/stats",
                "test_datasource": "POST /datasources/test",
                "datasource_types": "GET /datasources/types",
                "validate_schema": "POST /schema/validate",
                "create_schema": "POST /schema/create"
            },
            "job_types": [job_type.value for job_type in JobType],
            "supported_data_sources": [
                "azure_blob",
                "aws_s3", 
                "database",
                "local_filesystem"
            ]
        })
    
    async def ping(self, request: Request) -> Response:
        """Ping endpoint for connectivity testing"""
        data = await request.json() if request.content_type == 'application/json' else {}
        
        return json_response({
            "pong": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "received_data": data,
            "message": "Pong! API is responding"
        })
    
    async def start_server(self):
        """Start the API server"""
        logger.info(f"🚀 Starting Nuvyn Executor API Server on {self.host}:{self.port}")
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        
        logger.info(f"✅ API Server started successfully")
        logger.info(f"📋 Available endpoints:")
        logger.info(f"   Health: http://{self.host}:{self.port}/health")
        logger.info(f"   Info: http://{self.host}:{self.port}/info")
        logger.info(f"   Create Job: POST http://{self.host}:{self.port}/jobs/create")
        logger.info(f"   Execute Job: POST http://{self.host}:{self.port}/jobs/{{job_id}}/execute")
        
        # Keep the server running
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("⏹️  Server shutdown requested")
        finally:
            await runner.cleanup()


async def main():
    """Main entry point for API server"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Nuvyn Executor API Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind to")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    # Initialize logger
    initialize_logger(
        log_level=args.log_level,
        enable_console=True,
        enable_colors=True
    )
    
    # Create and start server
    server = ExecutorAPIServer(host=args.host, port=args.port)
    await server.start_server()


if __name__ == "__main__":
    asyncio.run(main())
