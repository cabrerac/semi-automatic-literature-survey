#!/usr/bin/env python3
"""
Logging Standards for SaLS Project.

This module defines consistent logging patterns, message formats,
and logging configuration across the entire SaLS codebase.

Standards ensure:
1. Consistent log levels and their usage
2. Uniform message formatting
3. Standardized logging configuration
4. Consistent progress reporting
5. Unified debug information
"""

import logging
import logging.handlers
import os
import sys
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum


class LogLevel(Enum):
    """Standard log levels for consistent usage across SaLS."""
    CRITICAL = logging.CRITICAL    # Pipeline cannot continue
    ERROR = logging.ERROR          # Operation failed, but pipeline can continue
    WARNING = logging.WARNING      # Issue detected, operation continues with defaults
    INFO = logging.INFO            # Informational message, user-facing
    DEBUG = logging.DEBUG          # Debug information, developer-facing
    NOTSET = logging.NOTSET        # Not set


class LogCategory(Enum):
    """Standard log categories for consistent classification."""
    PIPELINE = "PIPELINE"         # Main pipeline operations
    CONFIGURATION = "CONFIG"      # Configuration operations
    DATABASE = "DATABASE"         # Database operations
    API = "API"                   # External API calls
    DATA = "DATA"                 # Data processing operations
    FILE = "FILE"                 # File I/O operations
    VALIDATION = "VALIDATION"     # Data validation operations
    USER = "USER"                 # User interaction operations
    SYSTEM = "SYSTEM"             # System-level operations


class LogFormatter:
    """Standard log formatter for consistent message formatting."""
    
    # Standard format for different log levels
    STANDARD_FORMATS = {
        LogLevel.CRITICAL: "[CRITICAL] {asctime} | {category} | {module}.{function} | {message}",
        LogLevel.ERROR: "[ERROR] {asctime} | {category} | {module}.{function} | {message}",
        LogLevel.WARNING: "[WARNING] {asctime} | {category} | {module}.{function} | {message}",
        LogLevel.INFO: "[INFO] {asctime} | {category} | {module}.{function} | {message}",
        LogLevel.DEBUG: "[DEBUG] {asctime} | {category} | {module}.{function} | {message}"
    }
    
    # User-friendly format for console output
    USER_FORMAT = "[{levelname}] {message}"
    
    # Detailed format for file logging
    DETAILED_FORMAT = "[{levelname}] {asctime} | {category} | {module}.{function} | {message} | {extra_info}"
    
    @staticmethod
    def format_message(level: LogLevel, 
                      category: LogCategory,
                      module: str,
                      function: str,
                      message: str,
                      extra_info: Optional[Dict[str, Any]] = None) -> str:
        """Format a log message according to SaLS standards."""
        
        # Get base format
        base_format = LogFormatter.STANDARD_FORMATS.get(level, LogFormatter.STANDARD_FORMATS[LogLevel.INFO])
        
        # Format extra info if provided
        extra_str = ""
        if extra_info:
            extra_parts = []
            for key, value in extra_info.items():
                if isinstance(value, (dict, list)):
                    extra_parts.append(f"{key}: {str(value)[:100]}...")
                else:
                    extra_parts.append(f"{key}: {value}")
            extra_str = " | " + " | ".join(extra_parts)
        
        # Format the message
        formatted = base_format.format(
            asctime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            category=category.value,
            module=module,
            function=function,
            message=message,
            extra_info=extra_str
        )
        
        return formatted


class SaLSLogger:
    """Standard SaLS logger with consistent configuration and methods."""
    
    def __init__(self, 
                 name: str,
                 log_file: Optional[str] = None,
                 console_level: LogLevel = LogLevel.INFO,
                 file_level: LogLevel = LogLevel.DEBUG,
                 max_file_size: int = 10 * 1024 * 1024,  # 10MB
                 backup_count: int = 5):
        self.name = name
        self.log_file = log_file
        self.console_level = console_level
        self.file_level = file_level
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)  # Set to lowest level, handlers will filter
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Setup handlers
        self._setup_console_handler()
        if log_file:
            self._setup_file_handler()
    
    def _setup_console_handler(self) -> None:
        """Setup console handler with user-friendly formatting."""
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.console_level.value)
        
        # Create formatter for console
        console_formatter = logging.Formatter(LogFormatter.USER_FORMAT)
        console_handler.setFormatter(console_formatter)
        
        self.logger.addHandler(console_handler)
    
    def _setup_file_handler(self) -> None:
        """Setup file handler with detailed formatting."""
        # Ensure log directory exists
        log_dir = os.path.dirname(self.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # Create rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            self.log_file,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(self.file_level.value)
        
        # Create formatter for file
        file_formatter = logging.Formatter(LogFormatter.DETAILED_FORMAT)
        file_handler.setFormatter(file_formatter)
        
        self.logger.addHandler(file_handler)
    
    def log(self, 
            level: LogLevel,
            category: LogCategory,
            module: str,
            function: str,
            message: str,
            extra_info: Optional[Dict[str, Any]] = None,
            print_to_console: bool = False) -> None:
        """Log a message with consistent formatting."""
        
        # Format the message
        formatted_message = LogFormatter.format_message(
            level=level,
            category=category,
            module=module,
            function=function,
            message=message,
            extra_info=extra_info
        )
        
        # Log according to level
        if level == LogLevel.CRITICAL:
            self.logger.critical(formatted_message)
        elif level == LogLevel.ERROR:
            self.logger.error(formatted_message)
        elif level == LogLevel.WARNING:
            self.logger.warning(formatted_message)
        elif level == LogLevel.INFO:
            self.logger.info(formatted_message)
        else:  # DEBUG
            self.logger.debug(formatted_message)
        
        # Optionally print to console for user-facing messages
        if print_to_console and level in [LogLevel.CRITICAL, LogLevel.ERROR, LogLevel.WARNING, LogLevel.INFO]:
            print(formatted_message)
    
    def critical(self, category: LogCategory, module: str, function: str, message: str, 
                extra_info: Optional[Dict[str, Any]] = None, print_to_console: bool = True) -> None:
        """Log a critical message."""
        self.log(LogLevel.CRITICAL, category, module, function, message, extra_info, print_to_console)
    
    def error(self, category: LogCategory, module: str, function: str, message: str,
              extra_info: Optional[Dict[str, Any]] = None, print_to_console: bool = True) -> None:
        """Log an error message."""
        self.log(LogLevel.ERROR, category, module, function, message, extra_info, print_to_console)
    
    def warning(self, category: LogCategory, module: str, function: str, message: str,
                extra_info: Optional[Dict[str, Any]] = None, print_to_console: bool = False) -> None:
        """Log a warning message."""
        self.log(LogLevel.WARNING, category, module, function, message, extra_info, print_to_console)
    
    def info(self, category: LogCategory, module: str, function: str, message: str,
             extra_info: Optional[Dict[str, Any]] = None, print_to_console: bool = False) -> None:
        """Log an info message."""
        self.log(LogLevel.INFO, category, module, function, message, extra_info, print_to_console)
    
    def debug(self, category: LogCategory, module: str, function: str, message: str,
              extra_info: Optional[Dict[str, Any]] = None, print_to_console: bool = False) -> None:
        """Log a debug message."""
        self.log(LogLevel.DEBUG, category, module, function, message, extra_info, print_to_console)
    
    def progress(self, current: int, total: int, operation: str, 
                extra_info: Optional[Dict[str, Any]] = None) -> None:
        """Log progress information with consistent formatting."""
        percentage = (current / total) * 100 if total > 0 else 0
        progress_message = f"Progress: {current}/{total} ({percentage:.1f}%) - {operation}"
        
        self.info(
            category=LogCategory.PIPELINE,
            module=self.name,
            function="progress",
            message=progress_message,
            extra_info=extra_info,
            print_to_console=False
        )
    
    def operation_start(self, operation: str, extra_info: Optional[Dict[str, Any]] = None) -> None:
        """Log the start of an operation."""
        self.info(
            category=LogCategory.PIPELINE,
            module=self.name,
            function="operation_start",
            message=f"Starting: {operation}",
            extra_info=extra_info,
            print_to_console=False
        )
    
    def operation_complete(self, operation: str, result: str, 
                          extra_info: Optional[Dict[str, Any]] = None) -> None:
        """Log the completion of an operation."""
        self.info(
            category=LogCategory.PIPELINE,
            module=self.name,
            function="operation_complete",
            message=f"Completed: {operation} - Result: {result}",
            extra_info=extra_info,
            print_to_console=False
        )
    
    def operation_failed(self, operation: str, error: str,
                        extra_info: Optional[Dict[str, Any]] = None) -> None:
        """Log the failure of an operation."""
        self.error(
            category=LogCategory.PIPELINE,
            module=self.name,
            function="operation_failed",
            message=f"Failed: {operation} - Error: {error}",
            extra_info=extra_info,
            print_to_console=True
        )


def setup_sals_logger(name: str,
                     log_file: Optional[str] = None,
                     console_level: LogLevel = LogLevel.INFO,
                     file_level: LogLevel = LogLevel.DEBUG) -> SaLSLogger:
    """Setup a standardized SaLS logger."""
    return SaLSLogger(
        name=name,
        log_file=log_file,
        console_level=console_level,
        file_level=file_level
    )


# Standard logging configuration
def get_standard_logging_config() -> Dict[str, Any]:
    """Get standard logging configuration for SaLS."""
    return {
        "console_level": LogLevel.INFO,
        "file_level": LogLevel.DEBUG,
        "max_file_size": 10 * 1024 * 1024,  # 10MB
        "backup_count": 5,
        "log_format": "detailed",
        "encoding": "utf-8"
    }


# Example usage:
"""
# In your module:
from util.logging_standards import (
    setup_sals_logger, LogCategory, LogLevel
)

# Setup logger
logger = setup_sals_logger(
    name="my_module",
    log_file="logs/my_module.log"
)

# Log operations
logger.operation_start("data_processing")
logger.info(LogCategory.DATA, "my_module", "process_data", "Processing 100 records")
logger.progress(50, 100, "data_processing")
logger.operation_complete("data_processing", "100 records processed")
"""
