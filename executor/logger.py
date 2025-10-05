"""
Logging utilities for Nuvyn Executor Script
Provides structured logging with different output formats and levels
"""

import logging
import sys
import os
from datetime import datetime
from typing import Optional
from pathlib import Path


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    # Emojis for different log levels
    EMOJIS = {
        'DEBUG': '🔍',
        'INFO': 'ℹ️',
        'WARNING': '⚠️',
        'ERROR': '❌',
        'CRITICAL': '🚨'
    }
    
    def format(self, record):
        # Add emoji and color
        level_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        emoji = self.EMOJIS.get(record.levelname, '📝')
        reset_color = self.COLORS['RESET']
        
        # Format the message
        record.levelname = f"{level_color}{record.levelname}{reset_color}"
        record.msg = f"{emoji} {record.msg}"
        
        return super().format(record)


class JobFormatter(logging.Formatter):
    """Formatter specifically for job-related logs"""
    
    def format(self, record):
        # Add job context if available
        job_id = getattr(record, 'job_id', None)
        if job_id:
            record.msg = f"[{job_id}] {record.msg}"
        
        return super().format(record)


class ExecutorLogger:
    """Main logger class for the executor"""
    
    def __init__(self, 
                 name: str = "nuvyn_executor",
                 log_level: str = "INFO",
                 log_file: Optional[str] = None,
                 enable_console: bool = True,
                 enable_colors: bool = True):
        self.name = name
        self.log_level = log_level.upper()
        self.log_file = log_file
        self.enable_console = enable_console
        self.enable_colors = enable_colors
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, self.log_level))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Setup formatters
        self._setup_formatters()
        
        # Setup handlers
        if self.enable_console:
            self._setup_console_handler()
        
        if self.log_file:
            self._setup_file_handler()
    
    def _setup_formatters(self):
        """Setup log formatters"""
        # Console formatter with colors and emojis
        if self.enable_colors:
            self.console_formatter = ColoredFormatter(
                fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%H:%M:%S'
            )
        else:
            self.console_formatter = logging.Formatter(
                fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%H:%M:%S'
            )
        
        # File formatter (no colors)
        self.file_formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Job formatter
        self.job_formatter = JobFormatter(
            fmt='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
    
    def _setup_console_handler(self):
        """Setup console handler"""
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, self.log_level))
        console_handler.setFormatter(self.console_formatter)
        self.logger.addHandler(console_handler)
    
    def _setup_file_handler(self):
        """Setup file handler"""
        # Create log directory if it doesn't exist
        log_path = Path(self.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(getattr(logging, self.log_level))
        file_handler.setFormatter(self.file_formatter)
        self.logger.addHandler(file_handler)
    
    def get_job_logger(self, job_id: str) -> logging.Logger:
        """Get a logger specifically for a job"""
        job_logger = logging.getLogger(f"{self.name}.job.{job_id}")
        job_logger.setLevel(getattr(logging, self.log_level))
        
        # Clear existing handlers
        job_logger.handlers.clear()
        
        # Add console handler with job formatter
        if self.enable_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, self.log_level))
            console_handler.setFormatter(self.job_formatter)
            job_logger.addHandler(console_handler)
        
        # Add file handler if log file is specified
        if self.log_file:
            # Create job-specific log file
            job_log_file = str(Path(self.log_file).parent / f"job_{job_id}.log")
            file_handler = logging.FileHandler(job_log_file)
            file_handler.setLevel(getattr(logging, self.log_level))
            file_handler.setFormatter(self.file_formatter)
            job_logger.addHandler(file_handler)
        
        # Prevent propagation to parent logger
        job_logger.propagate = False
        
        return job_logger
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self.logger.debug(message, extra=kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self.logger.info(message, extra=kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.logger.warning(message, extra=kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self.logger.error(message, extra=kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self.logger.critical(message, extra=kwargs)
    
    def job_start(self, job_id: str, job_type: str, data_source: str):
        """Log job start"""
        self.info(f"🚀 Starting job: {job_type} for {data_source}", 
                 job_id=job_id, job_type=job_type, data_source=data_source)
    
    def job_complete(self, job_id: str, execution_time: float):
        """Log job completion"""
        self.info(f"✅ Job completed in {execution_time:.2f}s", 
                 job_id=job_id, execution_time=execution_time)
    
    def job_failed(self, job_id: str, error: str):
        """Log job failure"""
        self.error(f"❌ Job failed: {error}", job_id=job_id, error=error)
    
    def job_progress(self, job_id: str, step: str, progress: float):
        """Log job progress"""
        self.info(f"📊 Progress: {step} ({progress:.1f}%)", 
                 job_id=job_id, step=step, progress=progress)


# Global logger instance
_global_logger: Optional[ExecutorLogger] = None


def initialize_logger(log_level: str = "INFO", 
                     log_file: Optional[str] = None,
                     enable_console: bool = True,
                     enable_colors: bool = True) -> ExecutorLogger:
    """Initialize the global logger"""
    global _global_logger
    
    # Default log file location
    if not log_file:
        log_dir = Path.home() / ".nuvyn" / "logs"
        log_file = str(log_dir / f"executor_{datetime.now().strftime('%Y%m%d')}.log")
    
    _global_logger = ExecutorLogger(
        name="nuvyn_executor",
        log_level=log_level,
        log_file=log_file,
        enable_console=enable_console,
        enable_colors=enable_colors
    )
    
    return _global_logger


def get_logger(name: str = None) -> logging.Logger:
    """Get a logger instance"""
    global _global_logger
    
    if _global_logger is None:
        # Initialize with defaults
        initialize_logger()
    
    if name:
        return logging.getLogger(f"nuvyn_executor.{name}")
    else:
        return _global_logger.logger


def get_job_logger(job_id: str) -> logging.Logger:
    """Get a job-specific logger"""
    global _global_logger
    
    if _global_logger is None:
        # Initialize with defaults
        initialize_logger()
    
    return _global_logger.get_job_logger(job_id)


def log_job_start(job_id: str, job_type: str, data_source: str):
    """Log job start (convenience function)"""
    if _global_logger:
        _global_logger.job_start(job_id, job_type, data_source)


def log_job_complete(job_id: str, execution_time: float):
    """Log job completion (convenience function)"""
    if _global_logger:
        _global_logger.job_complete(job_id, execution_time)


def log_job_failed(job_id: str, error: str):
    """Log job failure (convenience function)"""
    if _global_logger:
        _global_logger.job_failed(job_id, error)


def log_job_progress(job_id: str, step: str, progress: float):
    """Log job progress (convenience function)"""
    if _global_logger:
        _global_logger.job_progress(job_id, step, progress)


# Utility functions for structured logging
def log_data_source_connection(source_type: str, source_path: str, success: bool):
    """Log data source connection attempt"""
    logger = get_logger("datasource")
    if success:
        logger.info(f"✅ Connected to {source_type}: {source_path}")
    else:
        logger.error(f"❌ Failed to connect to {source_type}: {source_path}")


def log_schema_operation(operation: str, schema_name: str, success: bool):
    """Log schema operation"""
    logger = get_logger("schema")
    if success:
        logger.info(f"✅ Schema {operation}: {schema_name}")
    else:
        logger.error(f"❌ Schema {operation} failed: {schema_name}")


def log_metadata_extraction(file_count: int, total_size: str, quality_score: float):
    """Log metadata extraction results"""
    logger = get_logger("metadata")
    logger.info(f"📊 Metadata extracted: {file_count} files, {total_size}, quality: {quality_score}/100")


def log_api_transmission(endpoint: str, payload_size: int, success: bool):
    """Log API transmission"""
    logger = get_logger("api")
    if success:
        logger.info(f"✅ API transmission successful: {endpoint} ({payload_size} bytes)")
    else:
        logger.error(f"❌ API transmission failed: {endpoint}")


def log_performance_metric(metric_name: str, value: float, unit: str = ""):
    """Log performance metric"""
    logger = get_logger("performance")
    logger.info(f"📈 {metric_name}: {value} {unit}")


def log_security_event(event_type: str, details: str):
    """Log security-related event"""
    logger = get_logger("security")
    logger.warning(f"🔒 Security event: {event_type} - {details}")
