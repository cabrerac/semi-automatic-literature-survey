#!/usr/bin/env python3
"""
Error Handling Standards for SaLS Project.

This module defines consistent error handling patterns, logging standards,
and user experience guidelines across the entire SaLS codebase.

Standards ensure:
1. Consistent error categorization (CRITICAL, ERROR, WARNING, INFO)
2. Uniform logging message formats
3. Standardized user-facing error messages
4. Consistent exception handling patterns
5. Unified error recovery suggestions
"""

import logging
from typing import Dict, List, Optional, Tuple, Union
from enum import Enum


class ErrorSeverity(Enum):
    """Standard error severity levels for consistent categorization."""
    CRITICAL = "CRITICAL"      # Pipeline cannot continue
    ERROR = "ERROR"            # Operation failed, but pipeline can continue
    WARNING = "WARNING"        # Issue detected, operation continues with defaults
    INFO = "INFO"              # Informational message
    DEBUG = "DEBUG"            # Debug information


class ErrorCategory(Enum):
    """Standard error categories for consistent classification."""
    CONFIGURATION = "CONFIGURATION"           # Configuration file issues
    API = "API"                              # External API failures
    NETWORK = "NETWORK"                      # Network/connection issues
    DATA = "DATA"                            # Data processing issues
    FILE = "FILE"                            # File I/O operations
    VALIDATION = "VALIDATION"                # Data validation failures
    SYSTEM = "SYSTEM"                        # System-level issues
    USER_INPUT = "USER_INPUT"                # User input validation
    RESOURCE = "RESOURCE"                    # Resource limitations (quotas, etc.)


class ErrorContext:
    """Standard error context information for consistent error reporting."""
    
    def __init__(self, 
                 module: str,
                 function: str,
                 operation: str,
                 severity: ErrorSeverity,
                 category: ErrorCategory,
                 user_facing: bool = True):
        self.module = module
        self.function = function
        self.operation = operation
        self.severity = severity
        self.category = category
        self.user_facing = user_facing
        self.timestamp = None  # Will be set by error handler
        self.additional_context: Dict = {}
    
    def add_context(self, key: str, value: str) -> None:
        """Add additional context information."""
        self.additional_context[key] = value
    
    def get_formatted_context(self) -> str:
        """Get formatted context string for logging."""
        context_parts = [
            f"Module: {self.module}",
            f"Function: {self.function}",
            f"Operation: {self.operation}",
            f"Category: {self.category.value}",
            f"Severity: {self.severity.value}"
        ]
        
        for key, value in self.additional_context.items():
            context_parts.append(f"{key}: {value}")
        
        return " | ".join(context_parts)


class ErrorMessage:
    """Standard error message format for consistent error reporting."""
    
    def __init__(self, 
                 context: ErrorContext,
                 error_type: str,
                 error_description: str,
                 recovery_suggestion: Optional[str] = None,
                 next_steps: Optional[List[str]] = None):
        self.context = context
        self.error_type = error_type
        self.error_description = error_description
        self.recovery_suggestion = recovery_suggestion
        self.next_steps = next_steps or []
    
    def get_log_message(self) -> str:
        """Get formatted message for logging."""
        message_parts = [
            f"[{self.context.severity.value}] {self.error_type}",
            f"Description: {self.error_description}",
            f"Context: {self.context.get_formatted_context()}"
        ]
        
        if self.recovery_suggestion:
            message_parts.append(f"Recovery: {self.recovery_suggestion}")
        
        return " | ".join(message_parts)
    
    def get_user_message(self) -> str:
        """Get user-friendly error message."""
        if not self.context.user_facing:
            return f"An error occurred: {self.error_description}"
        
        message_parts = []
        
        # Add severity indicator
        if self.context.severity == ErrorSeverity.CRITICAL:
            message_parts.append("🔴 CRITICAL ERROR")
        elif self.context.severity == ErrorSeverity.ERROR:
            message_parts.append("❌ ERROR")
        elif self.context.severity == ErrorSeverity.WARNING:
            message_parts.append("⚠️  WARNING")
        else:
            message_parts.append("ℹ️  INFO")
        
        # Add main message
        message_parts.append(self.error_description)
        
        # Add recovery suggestion
        if self.recovery_suggestion:
            message_parts.append(f"\n💡 {self.recovery_suggestion}")
        
        # Add next steps
        if self.next_steps:
            message_parts.append("\n📋 Next steps:")
            for i, step in enumerate(self.next_steps, 1):
                message_parts.append(f"   {i}. {step}")
        
        return "\n".join(message_parts)


class ErrorHandler:
    """Standard error handler for consistent error processing."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def handle_error(self, 
                    error: Exception,
                    context: ErrorContext,
                    error_type: str,
                    error_description: str,
                    recovery_suggestion: Optional[str] = None,
                    next_steps: Optional[List[str]] = None) -> ErrorMessage:
        """Handle an error according to SaLS standards."""
        
        # Create error message
        error_msg = ErrorMessage(
            context=context,
            error_type=error_type,
            error_description=error_description,
            recovery_suggestion=recovery_suggestion,
            next_steps=next_steps
        )
        
        # Log according to severity
        if context.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(error_msg.get_log_message())
        elif context.severity == ErrorSeverity.ERROR:
            self.logger.error(error_msg.get_log_message())
        elif context.severity == ErrorSeverity.WARNING:
            self.logger.warning(error_msg.get_log_message())
        elif context.severity == ErrorSeverity.INFO:
            self.logger.info(error_msg.get_log_message())
        else:  # DEBUG
            self.logger.debug(error_msg.get_log_message())
        
        # Log exception details if available
        if error:
            self.logger.debug(f"Exception details: {type(error).__name__}: {str(error)}")
        
        return error_msg
    
    def log_and_print(self, 
                      error_msg: ErrorMessage,
                      print_to_console: bool = True) -> None:
        """Log error and optionally print to console for user-facing errors."""
        
        # Always log
        if error_msg.context.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(error_msg.get_log_message())
        elif error_msg.context.severity == ErrorSeverity.ERROR:
            self.logger.error(error_msg.get_log_message())
        elif error_msg.context.severity == ErrorSeverity.WARNING:
            self.logger.warning(error_msg.get_log_message())
        elif error_msg.context.severity == ErrorSeverity.INFO:
            self.logger.info(error_msg.get_log_message())
        else:  # DEBUG
            self.logger.debug(error_msg.get_log_message())
        
        # Print to console for user-facing errors
        if print_to_console and error_msg.context.user_facing:
            print(error_msg.get_user_message())


# Standard error messages for common scenarios
STANDARD_ERROR_MESSAGES = {
    "file_not_found": {
        "description": "File not found",
        "recovery": "Check file path and ensure file exists",
        "next_steps": [
            "Verify the file path is correct",
            "Check file permissions",
            "Ensure the file exists in the specified location"
        ]
    },
    "invalid_configuration": {
        "description": "Invalid configuration detected",
        "recovery": "Review configuration file and fix validation errors",
        "next_steps": [
            "Check the configuration file format",
            "Verify all required fields are present",
            "Ensure field values are in correct format"
        ]
    },
    "api_quota_exceeded": {
        "description": "API quota exceeded",
        "recovery": "Wait for quota reset or use alternative search strategies",
        "next_steps": [
            "Wait for daily quota reset",
            "Reduce search scope using filters",
            "Use date ranges to limit results"
        ]
    },
    "network_timeout": {
        "description": "Network request timed out",
        "recovery": "Check network connection and retry",
        "next_steps": [
            "Verify internet connection",
            "Check firewall settings",
            "Retry the operation"
        ]
    },
    "data_validation_failed": {
        "description": "Data validation failed",
        "recovery": "Review input data and fix validation issues",
        "next_steps": [
            "Check data format and content",
            "Verify required fields are present",
            "Ensure data meets validation criteria"
        ]
    },
    "pipeline_step_failed": {
        "description": "Pipeline step execution failed",
        "recovery": "Review the step that failed and check for configuration or data issues",
        "next_steps": [
            "Check the logs for detailed error information",
            "Verify input data for the failed step",
            "Review step configuration parameters",
            "Consider running the pipeline from the failed step"
        ]
    },
    "pipeline_execution_failed": {
        "description": "Pipeline execution failed",
        "recovery": "Review the pipeline execution and check for critical errors",
        "next_steps": [
            "Check the logs for detailed error information",
            "Verify all configuration parameters",
            "Check system resources and permissions",
            "Review input data quality and format"
        ]
    },
    "configuration_fallback_failed": {
        "description": "Configuration fallback application failed",
        "recovery": "Review configuration parameters and apply fallbacks manually",
        "next_steps": [
            "Check configuration file format and content",
            "Verify parameter types and values",
            "Apply missing parameters manually",
            "Restart the pipeline with corrected configuration"
        ]
    }
}


def create_error_context(module: str,
                        function: str,
                        operation: str,
                        severity: ErrorSeverity,
                        category: ErrorCategory,
                        user_facing: bool = True) -> ErrorContext:
    """Create a standardized error context."""
    return ErrorContext(
        module=module,
        function=function,
        operation=operation,
        severity=severity,
        category=category,
        user_facing=user_facing
    )


def get_standard_error_info(error_key: str) -> Dict:
    """Get standard error information for common error types."""
    return STANDARD_ERROR_MESSAGES.get(error_key, {
        "description": "An error occurred",
        "recovery": "Review the error details and take appropriate action",
        "next_steps": ["Check the logs for detailed information", "Review the operation that failed"]
    })


# Example usage:
"""
# In your module:
from util.error_standards import (
    ErrorHandler, create_error_context, ErrorSeverity, ErrorCategory,
    get_standard_error_info
)

# Create error handler
error_handler = ErrorHandler(logger)

# Handle an error
try:
    # Your operation here
    pass
except FileNotFoundError as e:
    context = create_error_context(
        module="my_module",
        function="my_function",
        operation="file_reading",
        severity=ErrorSeverity.ERROR,
        category=ErrorCategory.FILE
    )
    
    error_info = get_standard_error_info("file_not_found")
    error_msg = error_handler.handle_error(
        error=e,
        context=context,
        error_type="FileNotFoundError",
        error_description=error_info["description"],
        recovery_suggestion=error_info["recovery"],
        next_steps=error_info["next_steps"]
    )
    
    # Log and print to console
    error_handler.log_and_print(error_msg, print_to_console=True)
"""
