#!/usr/bin/env python3
"""
Main Pipeline for Semi-automated Literature Survey (SaLS).

This module orchestrates the complete literature review pipeline using
standardized error handling and logging patterns.
"""

from util import util
from util.error_standards import (
    ErrorHandler, create_error_context, ErrorSeverity, ErrorCategory,
    get_standard_error_info
)
from util.logging_standards import (
    setup_sals_logger, LogCategory, LogLevel
)
from analysis import retrieve
from analysis import semantic_analyser
from analysis import manual
import sys
import pandas as pd
import os
from datetime import datetime


def setup_pipeline_logging(parameters_file: str) -> tuple:
    """Setup standardized logging for the pipeline.
    
    Args:
        parameters_file: Name of the parameters file for log naming.
        
    Returns:
        Tuple of (logger, log_file_path) or (None, None) if setup fails.
    """
    try:
        # Create logs directory if it doesn't exist
        if not os.path.exists('./logs/'):
            os.makedirs('./logs/', exist_ok=True)
        
        # Generate log file name
        timestamp = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
        log_file = f'./logs/{parameters_file.replace(".yaml", "")}_{timestamp}.log'
        
        # Setup standardized SaLS logger
        logger = setup_sals_logger(
            name="sals_pipeline",
            log_file=log_file,
            console_level=LogLevel.INFO,
            file_level=LogLevel.DEBUG
        )
        
        logger.info(
            LogCategory.SYSTEM,
            "main",
            "setup_pipeline_logging",
            f"Pipeline logging initialized - Console: INFO, File: DEBUG",
            extra_info={"log_file": log_file}
        )
        
        return logger, log_file
        
    except Exception as ex:
        # Fallback to basic error reporting if logging setup fails
        print(f"🔴 CRITICAL ERROR: Failed to setup pipeline logging")
        print(f"Error: {type(ex).__name__}: {str(ex)}")
        print("Pipeline cannot start without logging. Check system permissions.")
        return None, None


def validate_parameters_file(parameters_file: str) -> bool:
    """Validate the parameters file before pipeline execution.
    
    Args:
        parameters_file: Path to the parameters file.
        
    Returns:
        True if validation passes, False otherwise.
    """
    try:
        # Check if file exists
        if not os.path.exists(parameters_file):
            print(f"❌ ERROR: Parameters file '{parameters_file}' not found.")
            print("💡 Please provide a valid parameters file path.")
            return False
        
        # Check file extension
        if not parameters_file.endswith(('.yaml', '.yml')):
            print(f"⚠️  WARNING: Parameters file '{parameters_file}' doesn't have .yaml or .yml extension.")
            print("💡 The file will be processed, but ensure it contains valid YAML content.")
        
        return True
        
    except Exception as ex:
        print(f"❌ ERROR: Failed to validate parameters file")
        print(f"Error: {type(ex).__name__}: {str(ex)}")
        return False


def execute_pipeline_step(logger, step: int, operation: str, step_function, *args, **kwargs):
    """Execute a pipeline step with standardized error handling and logging.
    
    Args:
        logger: Standardized SaLS logger.
        step: Current pipeline step number.
        operation: Description of the operation being performed.
        step_function: Function to execute for this step.
        *args, **kwargs: Arguments to pass to the step function.
        
    Returns:
        Result from step function or None if step failed.
    """
    try:
        # Log step start
        logger.operation_start(f"Step {step}: {operation}")
        
        # Execute step
        result = step_function(*args, **kwargs)
        
        # Log step completion
        logger.operation_complete(f"Step {step}: {operation}", "completed successfully")
        
        return result
        
    except Exception as ex:
        # Create error context
        context = create_error_context(
            module="main",
            function="execute_pipeline_step",
            operation=f"step_{step}_{operation.lower().replace(' ', '_')}",
            severity=ErrorSeverity.ERROR,
            category=ErrorCategory.PIPELINE
        )
        
        # Get standard error info
        error_info = get_standard_error_info("pipeline_step_failed")
        
        # Handle error
        error_handler = ErrorHandler(logger.logger)
        error_msg = error_handler.handle_error(
            error=ex,
            context=context,
            error_type=f"Step{step}Error",
            error_description=f"Step {step} ({operation}) failed: {str(ex)}",
            recovery_suggestion=error_info["recovery"],
            next_steps=error_info["next_steps"]
        )
        
        # Log and print error
        error_handler.log_and_print(error_msg, print_to_console=True)
        
        return None


def main(parameters_file: str) -> None:
    """Main pipeline execution function with standardized error handling.
    
    Args:
        parameters_file: Path to the parameters file.
    """
    logger = None
    log_file = None
    
    try:
        # Setup logging
        logger, log_file = setup_pipeline_logging(parameters_file)
        if not logger:
            return
        
        logger.info(
            LogCategory.PIPELINE,
            "main",
            "main",
            f"Starting SaLS pipeline with parameters file: {parameters_file}",
            extra_info={"parameters_file": parameters_file, "log_file": log_file}
        )
        
        # Read search parameters
        try:
            queries, syntactic_filters, semantic_filters, fields, types, synonyms, databases, dates, start_date, end_date, \
                search_date, folder_name = util.read_parameters(parameters_file)
            
            logger.info(
                LogCategory.CONFIGURATION,
                "main",
                "main",
                "Parameters loaded successfully",
                extra_info={
                    "queries_count": len(queries),
                    "databases_count": len(databases),
                    "semantic_filters_count": len(semantic_filters)
                }
            )
            
        except Exception as ex:
            context = create_error_context(
                module="main",
                function="main",
                operation="parameter_loading",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION
            )
            
            error_info = get_standard_error_info("invalid_configuration")
            error_handler = ErrorHandler(logger.logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="ParameterLoadingError",
                error_description=f"Failed to read parameters from {parameters_file}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            error_handler.log_and_print(error_msg, print_to_console=True)
            return
        
        # Validate parsed queries
        try:
            parsed_queries, valid = util.parse_queries(queries)
            if not valid:
                logger.error(
                    LogCategory.VALIDATION,
                    "main",
                    "main",
                    "Invalid queries detected in parameters file",
                    extra_info={"queries": queries},
                    print_to_console=True
                )
                print("💡 Please check your query syntax and restart the pipeline.")
                return
                
        except Exception as ex:
            context = create_error_context(
                module="main",
                function="main",
                operation="query_validation",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.VALIDATION
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger.logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="QueryValidationError",
                error_description="Failed to validate search queries",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            error_handler.log_and_print(error_msg, print_to_console=True)
            return
        
        # Pipeline execution with standardized error handling
        try:
            step = 0
            next_file = None
            
            # Step 0: Retrieve papers
            step = 0
            logger.info(
                LogCategory.PIPELINE,
                "main",
                "main",
                f"Step {step}: Retrieving papers from databases"
            )
            
            result = execute_pipeline_step(
                logger, step, "Retrieving papers from databases",
                retrieve.get_papers,
                queries, syntactic_filters, synonyms, databases, fields, types, 
                folder_name, dates, start_date, end_date, search_date
            )
            
            if result is None:
                logger.error(
                    LogCategory.PIPELINE,
                    "main",
                    "main",
                    f"Step {step} failed - Pipeline cannot continue without retrieved papers",
                    print_to_console=True
                )
                return
            
            # Step 1: Preprocess papers
            step = 1
            logger.info(
                LogCategory.PIPELINE,
                "main",
                "main",
                f"Step {step}: Preprocessing papers"
            )
            
            file_name = execute_pipeline_step(
                logger, step, "Preprocessing papers",
                retrieve.preprocess,
                queries, databases, folder_name, search_date, dates, start_date, end_date, step
            )
            
            if not file_name or file_name == "":
                logger.error(
                    LogCategory.PIPELINE,
                    "main",
                    "main",
                    f"Step {step} failed - No output file generated",
                    print_to_console=True
                )
                return
            
            next_file = f"{step}_preprocessed_papers.csv"
            logger.info(
                LogCategory.FILE,
                "main",
                "main",
                f"Preprocessing results saved to: {file_name}"
            )
            
            # Step 2: Semantic filtering (if enabled)
            if len(semantic_filters) > 0:
                step = 2
                logger.info(
                    LogCategory.PIPELINE,
                    "main",
                    "main",
                    f"Step {step}: Semantic filtering by abstract"
                )
                
                file_name = execute_pipeline_step(
                    logger, step, "Semantic filtering by abstract",
                    semantic_analyser.search,
                    semantic_filters, folder_name, next_file, search_date, step
                )
                
                if file_name and file_name != f"{step-1}_preprocessed_papers.csv":
                    next_file = f"{step}_semantic_filtered_papers.csv"
                    logger.info(
                        LogCategory.FILE,
                        "main",
                        "main",
                        f"Semantic filtering results saved to: {file_name}"
                    )
                else:
                    step = step - 1
                    next_file = f"{step}_preprocessed_papers.csv"
                    logger.warning(
                        LogCategory.PIPELINE,
                        "main",
                        "main",
                        "Semantic filtering failed, continuing with preprocessed papers"
                    )
            
            # Step 3: Manual filtering by abstract
            step = step + 1
            logger.info(
                LogCategory.PIPELINE,
                "main",
                "main",
                f"Step {step}: Manual filtering by abstract"
            )
            
            result = execute_pipeline_step(
                logger, step, "Manual filtering by abstract",
                manual.manual_filter_by_abstract,
                folder_name, next_file, search_date, step
            )
            
            if result is None:
                logger.error(
                    LogCategory.PIPELINE,
                    "main",
                    "main",
                    f"Step {step} failed - Pipeline cannot continue without manual filtering",
                    print_to_console=True
                )
                return
            
            next_file, removed_papers_abstract = result
            
            # Step 4: Manual filtering by full text
            step = step + 1
            logger.info(
                LogCategory.PIPELINE,
                "main",
                "main",
                f"Step {step}: Manual filtering by full text"
            )
            
            result = execute_pipeline_step(
                logger, step, "Manual filtering by full text",
                manual.manual_filter_by_full_text,
                folder_name, next_file, search_date, step
            )
            
            if result is None:
                logger.error(
                    LogCategory.PIPELINE,
                    "main",
                    "main",
                    f"Step {step} failed - Pipeline cannot continue without manual filtering",
                    print_to_console=True
                )
                return
            
            next_file, removed_papers_full = result
            merge_step_1 = step
            
            # Step 5: Snowballing process
            step = step + 1
            logger.info(
                LogCategory.PIPELINE,
                "main",
                "main",
                f"Step {step}: Snowballing process"
            )
            
            # Validate removed papers before concatenation
            if removed_papers_abstract is None or removed_papers_full is None:
                logger.warning(
                    LogCategory.DATA,
                    "main",
                    "main",
                    "Some removed papers are None, using empty DataFrames for snowballing"
                )
                removed_papers_abstract = pd.DataFrame() if removed_papers_abstract is None else removed_papers_abstract
                removed_papers_full = pd.DataFrame() if removed_papers_full is None else removed_papers_full
            
            removed_papers = pd.concat([removed_papers_abstract, removed_papers_full])
            
            file_name = execute_pipeline_step(
                logger, step, "Snowballing process",
                retrieve.snowballing,
                folder_name, search_date, step, dates, start_date, end_date, semantic_filters, removed_papers
            )
            
            if file_name:
                next_file = f"{step}_snowballing_papers.csv"
                logger.info(
                    LogCategory.FILE,
                    "main",
                    "main",
                    f"Snowballing results saved to: {file_name}"
                )
            else:
                logger.warning(
                    LogCategory.PIPELINE,
                    "main",
                    "main",
                    "Snowballing failed, continuing without snowballing papers"
                )
                next_file = f"{step-1}_manually_filtered_by_full_text_papers.csv"
            
            # Step 6: Manual filtering for snowballing papers (if available)
            merge_step_2 = -1
            if file_name and len(file_name) > 0:
                step = step + 1
                logger.info(
                    LogCategory.PIPELINE,
                    "main",
                    "main",
                    f"Step {step}: Manual filtering by abstract for snowballing papers"
                )
                
                result = execute_pipeline_step(
                    logger, step, "Manual filtering by abstract for snowballing papers",
                    manual.manual_filter_by_abstract,
                    folder_name, next_file, search_date, step
                )
                
                if result:
                    next_file, removed_papers_abstract_snowballing = result
                    
                    # Step 7: Manual filtering by full text for snowballing
                    step = step + 1
                    logger.info(
                        LogCategory.PIPELINE,
                        "main",
                        "main",
                        f"Step {step}: Manual filtering by full text for snowballing papers"
                    )
                    
                    result = execute_pipeline_step(
                        logger, step, "Manual filtering by full text for snowballing papers",
                        manual.manual_filter_by_full_text,
                        folder_name, next_file, search_date, step
                    )
                    
                    if result:
                        merge_step_2 = step
                    else:
                        merge_step_2 = -1
                else:
                    merge_step_2 = -1
            
            # Final Step: Merge papers
            step = step + 1
            logger.info(
                LogCategory.PIPELINE,
                "main",
                "main",
                f"Step {step}: Merging papers"
            )
            
            file_name = execute_pipeline_step(
                logger, step, "Merging papers",
                util.merge_papers,
                step, merge_step_1, merge_step_2, folder_name, search_date
            )
            
            if not file_name or file_name == "":
                logger.error(
                    LogCategory.PIPELINE,
                    "main",
                    "main",
                    f"Step {step} failed - Final merge failed",
                    print_to_console=True
                )
                print("💡 Pipeline completed but final merge failed. Check output files manually.")
                return
            
            # Pipeline completed successfully
            logger.info(
                LogCategory.PIPELINE,
                "main",
                "main",
                "Pipeline completed successfully!",
                extra_info={"final_output": file_name}
            )
            print(f"✅ Pipeline completed successfully! Merged papers saved to: {file_name}")
            
        except Exception as ex:
            context = create_error_context(
                module="main",
                function="main",
                operation="pipeline_execution",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.PIPELINE
            )
            
            error_info = get_standard_error_info("pipeline_execution_failed")
            error_handler = ErrorHandler(logger.logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="PipelineExecutionError",
                error_description="Critical error during pipeline execution",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            error_handler.log_and_print(error_msg, print_to_console=True)
            return
            
    except Exception as ex:
        # Fallback error handling if main function fails
        print(f"🔴 CRITICAL ERROR: Main pipeline function failed")
        print(f"Error: {type(ex).__name__}: {str(ex)}")
        print("💡 Pipeline cannot start. Check your configuration and try again.")
        return


if __name__ == "__main__":
    try:
        if len(sys.argv) == 2:
            parameters_file = sys.argv[1]
            
            # Validate parameters file
            if not validate_parameters_file(parameters_file):
                sys.exit(1)
            
            # Execute main pipeline
            main(parameters_file)
        else:
            print('❌ ERROR: Incorrect number of arguments.')
            print('💡 Usage: python main.py <parameters_file.yaml>')
            print('📋 Example: python main.py parameters_ar.yaml')
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline execution interrupted by user.")
        sys.exit(1)
    except Exception as ex:
        print(f"🔴 CRITICAL ERROR: Main entry point failed")
        print(f"Error: {type(ex).__name__}: {str(ex)}")
        print("💡 Pipeline cannot start. Check your configuration and try again.")
        sys.exit(1)
