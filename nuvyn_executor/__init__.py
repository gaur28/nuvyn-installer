"""
Nuvyn Executor - Namespace package for backward compatibility
This allows both 'import executor' and 'import nuvyn_executor' to work
"""

__version__ = "1.0.0"

# Re-export everything from executor for compatibility
try:
    from executor.config import ConfigManager, JobConfig, JobType, JobStatus
    from executor.job_manager import JobManager
    from executor.logger import get_logger, initialize_logger
    from executor.main import create_and_execute_job, execute_job_by_id
    
    # Import executor module itself
    import executor
    
    __all__ = [
        "ConfigManager",
        "JobConfig",
        "JobType", 
        "JobStatus",
        "JobManager",
        "get_logger",
        "initialize_logger",
        "create_and_execute_job",
        "execute_job_by_id",
        "executor",
    ]
except ImportError as e:
    # If executor module is not available, provide helpful error
    import sys
    raise ImportError(
        f"Failed to import executor module: {e}\n"
        f"Please ensure 'nuvyn-executor-script' package is installed.\n"
        f"Install with: pip install git+https://github.com/nuvyn-bldr/executor-script.git@main"
    ) from e

