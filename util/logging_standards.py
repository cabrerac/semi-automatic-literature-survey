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
        
        # Create formatter for console - use a simpler format since we handle custom fields in our log method
        console_formatter = logging.Formatter("[%(levelname)s] %(message)s")
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
        
        # Create formatter for file - use a simpler format since we handle custom fields in our log method
        file_formatter = logging.Formatter("[%(levelname)s] %(asctime)s | %(message)s")
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
        
        # Format the message with our custom formatter for console output
        formatted_message = LogFormatter.format_message(
            level=level,
            category=category,
            module=module,
            function=function,
            message=message,
            extra_info=extra_info
        )
        
        # Log according to level - pass the original message to Python's logger
        # Python's logger will format it with its own formatter
        if level == LogLevel.CRITICAL:
            self.logger.critical(message)
        elif level == LogLevel.ERROR:
            self.logger.error(message)
        elif level == LogLevel.WARNING:
            self.logger.warning(message)
        elif level == LogLevel.INFO:
            self.logger.info(message)
        else:  # DEBUG
            self.logger.debug(message)
        
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
    new_logger = SaLSLogger(
        name=name,
        log_file=log_file,
        console_level=console_level,
        file_level=file_level
    )

    # Attach the same handlers to legacy 'logger' so existing modules print to console and files
    try:
        legacy_logger = logging.getLogger('logger')
        legacy_logger.setLevel(logging.DEBUG)
        legacy_logger.handlers.clear()
        for handler in new_logger.logger.handlers:
            legacy_logger.addHandler(handler)
        legacy_logger.propagate = False
    except Exception:
        pass

    # Keep a reference to the current SaLS logger
    global _CURRENT_SALS_LOGGER
    _CURRENT_SALS_LOGGER = new_logger
    return new_logger


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


# Keep reference to the most recently configured SaLS logger
_CURRENT_SALS_LOGGER: Optional[SaLSLogger] = None

def get_current_sals_logger() -> Optional[SaLSLogger]:
    """Return the last configured SaLSLogger, if any."""
    return _CURRENT_SALS_LOGGER

# Compatibility logger that accepts both SaLSLogger-style and std logging-style calls
class CompatLogger:
    def __init__(self, sals_logger: Optional[SaLSLogger], std_logger: logging.Logger):
        self._sals = sals_logger
        self._std = std_logger
        try:
            self._std.setLevel(logging.INFO)
        except Exception:
            pass

    def _route(self, level: LogLevel, *args, **kwargs):
        # SaLS-style: (category, module, function, message, extra_info=None, print_to_console=False)
        if len(args) >= 4 and isinstance(args[0], LogCategory):
            category, module, function, message = args[:4]
            extra_info = args[4] if len(args) >= 5 else kwargs.get('extra_info')
            print_to_console = args[5] if len(args) >= 6 else kwargs.get('print_to_console', False)
            if self._sals:
                self._sals.log(level, category, module, function, message, extra_info, print_to_console)
            else:
                rendered = LogFormatter.format_message(level, category, module, function, message, extra_info)
                if level == LogLevel.CRITICAL:
                    self._std.critical(rendered)
                elif level == LogLevel.ERROR:
                    self._std.error(rendered)
                elif level == LogLevel.WARNING:
                    self._std.warning(rendered)
                elif level == LogLevel.INFO:
                    self._std.info(rendered)
                else:
                    self._std.debug(rendered)
                if print_to_console and level in [LogLevel.CRITICAL, LogLevel.ERROR, LogLevel.WARNING, LogLevel.INFO]:
                    try:
                        print(rendered)
                    except Exception:
                        pass
            return

        # Std-style: (msg, *fmt_args)
        if len(args) >= 1:
            msg = args[0]
            fmt_args = args[1:] if len(args) > 1 else ()
            try:
                if level == LogLevel.CRITICAL:
                    self._std.critical(msg, *fmt_args)
                elif level == LogLevel.ERROR:
                    self._std.error(msg, *fmt_args)
                elif level == LogLevel.WARNING:
                    self._std.warning(msg, *fmt_args)
                elif level == LogLevel.INFO:
                    self._std.info(msg, *fmt_args)
                else:
                    self._std.debug(msg, *fmt_args)
            except Exception:
                safe_msg = str(msg)
                if level == LogLevel.CRITICAL:
                    self._std.critical(safe_msg)
                elif level == LogLevel.ERROR:
                    self._std.error(safe_msg)
                elif level == LogLevel.WARNING:
                    self._std.warning(safe_msg)
                elif level == LogLevel.INFO:
                    self._std.info(safe_msg)
                else:
                    self._std.debug(safe_msg)

    def info(self, *args, **kwargs):
        self._route(LogLevel.INFO, *args, **kwargs)

    def warning(self, *args, **kwargs):
        self._route(LogLevel.WARNING, *args, **kwargs)

    def error(self, *args, **kwargs):
        self._route(LogLevel.ERROR, *args, **kwargs)

    def debug(self, *args, **kwargs):
        self._route(LogLevel.DEBUG, *args, **kwargs)

    def critical(self, *args, **kwargs):
        self._route(LogLevel.CRITICAL, *args, **kwargs)


def get_compat_logger() -> CompatLogger:
    """Return a logger that accepts both SaLSLogger-style and std logging-style calls."""
    return CompatLogger(_CURRENT_SALS_LOGGER, logging.getLogger('sals_pipeline'))

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
