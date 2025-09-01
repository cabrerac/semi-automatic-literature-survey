# Standard library imports
import logging
import os
import random
import re
from datetime import datetime
from os.path import exists

# Third-party imports
import numpy as np
import pandas as pd
import spacy
import yaml
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from tqdm import tqdm

# Local imports
from . import parser as par
from .error_standards import (
    ErrorHandler, create_error_context, ErrorSeverity, ErrorCategory,
    get_standard_error_info
)
from .logging_standards import LogCategory

# Configure logging
logger = logging.getLogger('logger')

# Constants
DEFAULT_ENCODING = 'utf-8'
DEFAULT_BASE_DELAY = 1.0
DEFAULT_MAX_DELAY = 64.0
VALID_DATABASES = [
    'arxiv', 'springer', 'ieeexplore', 'scopus', 'core', 'semantic_scholar',
    'crossref', 'europe_pmc', 'pubmed', 'openalex'
]


def _apply_word_replacements_outside_quotes(text: str) -> str:
    """Normalize boolean operators outside quoted phrases to internal tokens <AND>/<OR>.
    
    This function processes boolean expressions, converting various operator formats
    to standardized internal tokens while preserving quoted phrases. It handles
    both textual operators (AND/OR) and symbolic operators (&, |, &&, ||, ¦).
    
    The function uses a state machine approach to track quote boundaries and only
    applies replacements outside of quoted content. This ensures that operators
    within phrases like 'a & b' are preserved literally.
    
    Args:
        text: String containing boolean expressions with potential quoted phrases.
            
    Returns:
        String with normalized boolean operators outside quotes, preserving quoted content.
        
    Example:
        >>> _apply_word_replacements_outside_quotes("'a & b' AND c OR d")
        "'a & b' <AND> c <OR> d"
        
    Note:
        Supported operator formats:
        - Textual: AND, and, OR, or (case-insensitive)
        - Symbolic: &, |, &&, ||, ¦ (legacy OR character)
        
        The function processes operators in order of specificity (doubles first)
        to avoid conflicts with single character operators.
    """
    def apply_replacements(segment):
        # Textual operators (word boundaries, case-insensitive)
        segment = re.sub(r"\bAND\b", " <AND> ", segment, flags=re.IGNORECASE)
        segment = re.sub(r"\bOR\b", " <OR> ", segment, flags=re.IGNORECASE)
        # Symbol operators – replace doubles first, then singles
        segment = segment.replace('&&', ' <AND> ')
        segment = segment.replace('||', ' <OR> ')
        # Handle legacy/alternate symbols
        segment = segment.replace('¦', ' <OR> ')
        # Replace single symbols after doubles are handled
        segment = re.sub(r"(?<!<)\&(?!>)", " <AND> ", segment)  # avoid touching existing <AND>
        segment = re.sub(r"(?<!<)\|(?!>)", " <OR> ", segment)   # avoid touching existing <OR>
        # Collapse multiple spaces
        segment = re.sub(r"\s+", " ", segment)
        return segment

    result_chars = []
    i = 0
    n = len(text)
    in_quote = False
    quote_char = ''
    buffer_outside = []
    while i < n:
        ch = text[i]
        if in_quote:
            result_chars.append(ch)
            if ch == quote_char:
                in_quote = False
            i += 1
        else:
            if ch == '"' or ch == '\'':
                # flush buffer with replacements
                if buffer_outside:
                    segment = ''.join(buffer_outside)
                    result_chars.append(apply_replacements(segment))
                    buffer_outside = []
                in_quote = True
                quote_char = ch
                result_chars.append(ch)
                i += 1
            else:
                buffer_outside.append(ch)
                i += 1
    if buffer_outside:
        segment = ''.join(buffer_outside)
        result_chars.append(apply_replacements(segment))

    normalized = ''.join(result_chars)
    return normalized.strip()


def normalize_query_expression(expression: str) -> str:
    """Normalize boolean expressions for consistent processing.
    
    This function standardizes boolean query expressions by removing encoding
    artifacts, normalizing operators, and ensuring consistent spacing. It's
    designed to handle various input formats and produce standardized output
    that can be reliably processed by the query parser.
    
    The normalization process includes:
    1. Removing common encoding artifacts (e.g., 'Â' characters)
    2. Converting boolean operators to internal tokens outside quotes
    3. Normalizing whitespace for consistent processing
    
    Args:
        expression: Raw boolean expression string that may contain various formats.
            
    Returns:
        Normalized boolean expression string ready for parsing.
        
    Example:
        >>> normalize_query_expression("'machine learning' & 'edge computing'")
        "'machine learning' <AND> 'edge computing'"
        
    Note:
        This function preserves the semantic meaning of the expression while
        standardizing the format. It's safe to apply multiple times without
        changing the result.
        
        The function handles:
        - Various boolean operator formats (AND, &, &&, etc.)
        - Quoted phrases (single and double quotes)
        - Encoding artifacts from different sources
        - Inconsistent whitespace patterns
    """
    # Remove common stray encoding artifacts
    expression = expression.replace('Â', '')
    # Normalize operators outside of quotes
    expression = _apply_word_replacements_outside_quotes(expression)
    return expression.strip()




# Legacy variable - kept for backward compatibility
fr = DEFAULT_ENCODING

# Initialize spaCy for language detection
nlp = spacy.load('en_core_web_sm')


# =============================================================================
# CONFIGURATION VALIDATION FUNCTIONS
# =============================================================================
# These functions handle validation of YAML configuration files, providing
# detailed error messages and recovery suggestions for various configuration issues.

def _validate_queries(parameters: dict, parameters_file_name: str) -> tuple[list, list]:
    """Validate queries section of configuration (critical validation).
    
    Args:
        parameters: Configuration parameters dictionary.
        parameters_file_name: Name of the configuration file.
            
    Returns:
        Tuple of (critical_errors, recovery_suggestions).
    """
    critical_errors = []
    recovery_suggestions = []
    
    if 'queries' not in parameters:
        critical_errors.append(f"Configuration error: 'queries' section is missing in {parameters_file_name}")
        recovery_suggestions.append({
            'issue': 'Missing queries section',
            'fix': 'Add a queries section with your search terms',
            'example': '''queries:
  - augmented reality: "'augmented reality' & 'edge'"
  - machine learning: "'machine learning' & 'systems'"
''',
            'severity': 'critical'
        })
    elif not isinstance(parameters['queries'], list):
        critical_errors.append(f"Configuration error: 'queries' must be a list in {parameters_file_name}")
        recovery_suggestions.append({
            'issue': f'Queries is {type(parameters["queries"]).__name__}, not a list',
            'fix': 'Ensure queries is formatted as a list with "-" items',
            'example': '''queries:
  - topic1: "'term1' & 'term2'"
  - topic2: "'term3' | 'term4'"
''',
            'severity': 'critical'
        })
    elif len(parameters['queries']) == 0:
        critical_errors.append(f"Configuration error: 'queries' list is empty in {parameters_file_name}")
        recovery_suggestions.append({
            'issue': 'Empty queries list',
            'fix': 'Add at least one search query to proceed',
            'example': '''queries:
  - your_topic: "'your research topic' & 'key concept'"
''',
            'severity': 'critical'
        })
    else:
        # Validate individual queries
        for i, query in enumerate(parameters['queries']):
            if not isinstance(query, dict):
                critical_errors.append(f"Configuration error: Query {i+1} must be a dictionary in {parameters_file_name}")
                recovery_suggestions.append({
                    'issue': f'Query {i+1} is {type(query).__name__}, not a dictionary',
                    'fix': 'Format each query as: query_name: "query_value"',
                    'example': f'  - query_name: "term1 & term2"',
                    'severity': 'critical'
                })
                break
            elif len(query) != 1:
                critical_errors.append(f"Configuration error: Query {i+1} must have exactly one key-value pair in {parameters_file_name}")
                recovery_suggestions.append({
                    'issue': f'Query {i+1} has {len(query)} keys: {list(query.keys())}',
                    'fix': 'Each query should have exactly one key-value pair',
                    'example': f'  - query_name: "term1 & term2"',
                    'severity': 'critical'
                })
                break
            else:
                query_name = list(query.keys())[0]
                query_value = query[query_name]
                
                if not isinstance(query_name, str) or not query_name.strip():
                    critical_errors.append(f"Configuration error: Query {i+1} name must be a non-empty string in {parameters_file_name}")
                    recovery_suggestions.append({
                        'issue': f'Query name is {repr(query_name)}',
                        'fix': 'Use a descriptive, non-empty string for the query name',
                        'example': f'  - machine_learning: "term1 & term2"',
                        'severity': 'critical'
                    })
                    break
                
                if not isinstance(query_value, str) or not query_value.strip():
                    critical_errors.append(f"Configuration error: Query '{query_name}' value must be a non-empty string in {parameters_file_name}")
                    recovery_suggestions.append({
                        'issue': f'Query value is {repr(query_name)}',
                        'fix': 'Use a non-empty string for the query value',
                        'example': f'  - {query_name}: "term1 & term2"',
                        'severity': 'critical'
                    })
                    break
    
    return critical_errors, recovery_suggestions


def _validate_databases(parameters: dict, parameters_file_name: str) -> tuple[list, list]:
    """Validate databases section of configuration (warning validation).
    
    Args:
        parameters: Configuration parameters dictionary.
        parameters_file_name: Name of the configuration file.
            
    Returns:
        Tuple of (warnings, recovery_suggestions).
    """
    warnings = []
    recovery_suggestions = []
    
    if 'databases' not in parameters:
        warnings.append("Configuration warning: 'databases' section is missing")
        recovery_suggestions.append({
            'issue': 'Missing databases section',
            'fix': 'Add databases section or use default open databases',
            'example': '''databases:
  - arxiv                    # Open access, no API key needed
  - semantic_scholar        # Open access, no API key needed
''',
            'severity': 'warning',
            'default': ['arxiv', 'semantic_scholar']
        })
    elif not isinstance(parameters['databases'], list):
        warnings.append(f"Configuration warning: 'databases' must be a list in {parameters_file_name}")
        recovery_suggestions.append({
            'issue': f'Databases is {type(parameters["databases"]).__name__}, not a list',
            'fix': 'Format databases as a list with "-" items',
            'example': '''databases:
  - arxiv
  - semantic_scholar
''',
            'severity': 'warning',
            'default': ['arxiv', 'semantic_scholar']
        })
    else:
        invalid_dbs = []
        for db in parameters['databases']:
            if not isinstance(db, str):
                invalid_dbs.append(f"{repr(db)} (not a string)")
            elif db not in VALID_DATABASES:
                invalid_dbs.append(f"'{db}' (unknown database)")
        
        if invalid_dbs:
            warnings.append(f"Configuration warning: Invalid database(s) found: {', '.join(invalid_dbs)}")
            recovery_suggestions.append({
                'issue': f'Invalid databases: {", ".join(invalid_dbs)}',
                'fix': 'Use only valid database names',
                'example': f'Valid databases: {", ".join(VALID_DATABASES)}',
                'severity': 'warning',
                'note': 'Some databases require API keys in config.json'
            })
    
    return warnings, recovery_suggestions


def _validate_dates(parameters: dict, parameters_file_name: str) -> tuple[list, list]:
    """Validate date fields in configuration (warning validation).
    
    Args:
        parameters: Configuration parameters dictionary.
        parameters_file_name: Name of the configuration file.
            
    Returns:
        Tuple of (warnings, recovery_suggestions).
    """
    warnings = []
    recovery_suggestions = []
    
    # Validate start_date
    if 'start_date' in parameters:
        try:
            if isinstance(parameters['start_date'], str):
                datetime.strptime(parameters['start_date'], '%Y-%m-%d')
            elif hasattr(parameters['start_date'], 'strftime'):
                if parameters['start_date'].year < 1900 or parameters['start_date'].year > 2100:
                    warnings.append(f"Configuration warning: 'start_date' year seems unreasonable: {parameters['start_date'].year}")
                    recovery_suggestions.append({
                        'issue': f'Unreasonable start_date year: {parameters["start_date"].year}',
                        'fix': 'Use a year between 1900 and 2100',
                        'example': 'start_date: 2020-01-01',
                        'severity': 'warning',
                        'default': '1950-01-01'
                    })
            else:
                warnings.append(f"Configuration warning: 'start_date' must be a string or date in {parameters_file_name}")
                recovery_suggestions.append({
                    'issue': f'start_date is {type(parameters["start_date"]).__name__}',
                    'fix': 'Use YYYY-MM-DD format',
                    'example': 'start_date: 2020-01-01',
                    'severity': 'warning',
                    'default': '1950-01-01'
                })
        except ValueError:
            warnings.append(f"Configuration warning: Invalid 'start_date' format in {parameters_file_name}")
            recovery_suggestions.append({
                'issue': f'Invalid start_date format: {parameters["start_date"]}',
                'fix': 'Use YYYY-MM-DD format',
                'example': 'start_date: 2020-01-01',
                'severity': 'warning',
                'default': '1950-01-01'
            })
    
    # Validate end_date
    if 'end_date' in parameters:
        try:
            if isinstance(parameters['end_date'], str):
                datetime.strptime(parameters['end_date'], '%Y-%m-%d')
            elif hasattr(parameters['end_date'], 'strftime'):
                if parameters['end_date'].year < 1900 or parameters['end_date'].year > 2100:
                    warnings.append(f"Configuration warning: 'end_date' year seems unreasonable: {parameters['end_date'].year}")
                    recovery_suggestions.append({
                        'issue': f'Unreasonable end_date year: {parameters["end_date"].year}',
                        'fix': 'Use a year between 1900 and 2100',
                        'example': 'end_date: 2024-12-31',
                        'severity': 'warning',
                        'default': 'current date'
                    })
            else:
                warnings.append(f"Configuration warning: 'end_date' must be a string or date in {parameters_file_name}")
                recovery_suggestions.append({
                    'issue': f'end_date is {type(parameters["end_date"]).__name__}',
                    'fix': 'Use YYYY-MM-DD format',
                    'example': 'end_date: 2024-12-31',
                    'severity': 'warning',
                    'default': 'current date'
                })
        except ValueError:
            warnings.append(f"Configuration warning: Invalid 'end_date' format in {parameters_file_name}")
            recovery_suggestions.append({
                'issue': f'Invalid end_date format: {parameters["end_date"]}',
                'fix': 'Use YYYY-MM-DD format',
                'example': 'end_date: 2024-12-31',
                'severity': 'warning',
                'default': 'current date'
            })
    
    # Validate search_date
    if 'search_date' not in parameters:
        warnings.append("Configuration warning: 'search_date' is missing")
        recovery_suggestions.append({
            'issue': 'Missing search_date',
            'fix': 'Add search_date or use current date',
            'example': f'search_date: {datetime.today().strftime("%Y-%m-%d")}',
            'severity': 'warning',
            'default': 'current date'
        })
    else:
        try:
            if isinstance(parameters['search_date'], str):
                datetime.strptime(parameters['search_date'], '%Y-%m-%d')
            elif hasattr(parameters['search_date'], 'strftime'):
                if parameters['search_date'].year < 1900 or parameters['search_date'].year > 2100:
                    warnings.append(f"Configuration warning: 'search_date' year seems unreasonable: {parameters['search_date'].year}")
                    recovery_suggestions.append({
                        'issue': f'Unreasonable search_date year: {parameters["search_date"].year}',
                        'fix': 'Use a year between 1900 and 2100',
                        'example': f'search_date: {datetime.today().strftime("%Y-%m-%d")}',
                        'severity': 'warning',
                        'default': 'current date'
                    })
            else:
                warnings.append(f"Configuration warning: 'search_date' must be a string or date in {parameters_file_name}")
                recovery_suggestions.append({
                    'issue': f'search_date is {type(parameters["search_date"]).__name__}',
                    'fix': 'Use YYYY-MM-DD format',
                    'example': f'search_date: {datetime.today().strftime("%Y-%m-%d")}',
                    'severity': 'warning',
                    'default': 'current date'
                })
        except ValueError:
            warnings.append(f"Configuration warning: Invalid 'search_date' format in {parameters_file_name}")
            recovery_suggestions.append({
                'issue': f'Invalid search_date format: {parameters["search_date"]}',
                'fix': 'Use YYYY-MM-DD format',
                'example': f'search_date: {datetime.today().strftime("%Y-%m-%d")}',
                'severity': 'warning',
                'default': 'current date'
            })
    
    return warnings, recovery_suggestions


# Main validation function that orchestrates all validation steps
def _validate_configuration(parameters: dict, parameters_file_name: str) -> tuple[bool, str, list]:
    """Validate configuration parameters and provide user-friendly error messages with recovery suggestions.
    
    This function performs comprehensive validation of SaLS configuration files, categorizing issues
    into critical errors (which prevent pipeline execution) and warnings (which allow execution
    with automatic fallbacks). It provides detailed recovery suggestions for each issue found.
    
    Args:
        parameters: Dictionary containing the loaded configuration parameters.
        parameters_file_name: Name of the configuration file being validated (for error messages).
            
    Returns:
        A tuple containing:
            - is_valid: Boolean indicating if the configuration can proceed (True for warnings only, False for critical errors)
            - error_message: Detailed error message explaining all issues found
            - recovery_suggestions: List of dictionaries with recovery guidance for each issue
            
    Raises:
        Exception: If an unexpected error occurs during validation.
        
    Example:
        >>> config = {'queries': [{'test': 'test'}]}
        >>> is_valid, msg, suggestions = _validate_configuration(config, 'test.yaml')
        >>> print(f"Valid: {is_valid}, Issues: {len(suggestions)}")
        Valid: True, Issues: 1
        
    Note:
        Critical errors (missing queries) prevent pipeline execution, while warnings
        (missing databases, dates) allow execution with sensible defaults.
    """
    try:
        recovery_suggestions = []
        critical_errors = []
        warnings = []
        
        # Validate queries (CRITICAL - pipeline cannot continue without)
        query_errors, query_suggestions = _validate_queries(parameters, parameters_file_name)
        critical_errors.extend(query_errors)
        recovery_suggestions.extend(query_suggestions)
        
        # Validate databases (WARNING - can provide defaults)
        db_warnings, db_suggestions = _validate_databases(parameters, parameters_file_name)
        warnings.extend(db_warnings)
        recovery_suggestions.extend(db_suggestions)
        
        # Validate dates (WARNING - can provide defaults)
        date_warnings, date_suggestions = _validate_dates(parameters, parameters_file_name)
        warnings.extend(date_warnings)
        recovery_suggestions.extend(date_suggestions)
        
        # Validate folder_name (WARNING - can provide default)
        if 'folder_name' not in parameters:
            warnings.append("Configuration warning: 'folder_name' is missing")
            recovery_suggestions.append({
                'issue': 'Missing folder_name',
                'fix': 'Add folder_name or use filename-based default',
                'example': f'folder_name: {parameters_file_name.replace(".yaml", "")}',
                'severity': 'warning',
                'default': 'filename-based'
            })
        elif not isinstance(parameters['folder_name'], str) or not parameters['folder_name'].strip():
            warnings.append(f"Configuration warning: 'folder_name' must be a non-empty string in {parameters_file_name}")
            recovery_suggestions.append({
                'issue': f'folder_name is {repr(parameters["folder_name"])}',
                'fix': 'Use a non-empty string for folder_name',
                'example': 'folder_name: my_literature_search',
                'severity': 'warning',
                'default': 'filename-based'
            })
        
        # Validate filters (WARNING - can provide defaults)
        if 'syntactic_filters' in parameters and not isinstance(parameters['syntactic_filters'], list):
            warnings.append(f"Configuration warning: 'syntactic_filters' must be a list in {parameters_file_name}")
            recovery_suggestions.append({
                'issue': f'syntactic_filters is {type(parameters["syntactic_filters"]).__name__}',
                'fix': 'Format as a list with "-" items',
                'example': '''syntactic_filters:
  - edge
  - orchestration
''',
                'severity': 'warning',
                'default': 'empty list'
            })
        
        if 'semantic_filters' in parameters and not isinstance(parameters['semantic_filters'], list):
            warnings.append(f"Configuration warning: 'semantic_filters' must be a list in {parameters_file_name}")
            recovery_suggestions.append({
                'issue': f'semantic_filters is {type(parameters["semantic_filters"]).__name__}',
                'fix': 'Format as a list with "-" items',
                'example': '''semantic_filters:
  - edge computing: "Edge computing and fog computing technologies"
  - orchestration: "Service orchestration and composition"
''',
                'severity': 'warning',
                'default': 'empty list'
            })
        
        # Determine overall validation result
        if critical_errors:
            # Critical errors prevent pipeline execution
            error_message = "\n".join(critical_errors)
            error_message += "\n\n🔴 CRITICAL ERRORS - Pipeline cannot continue:"
            for suggestion in recovery_suggestions:
                if suggestion['severity'] == 'critical':
                    error_message += f"\n\n❌ {suggestion['issue']}"
                    error_message += f"\n   Fix: {suggestion['fix']}"
                    error_message += f"\n   Example:\n{suggestion['example']}"
            
            return False, error_message, recovery_suggestions
        else:
            # Only warnings - pipeline can continue with defaults
            if warnings:
                warning_message = "Configuration validation completed with warnings:\n"
                warning_message += "\n".join(warnings)
                warning_message += "\n\n🟡 WARNINGS - Pipeline will continue with defaults where possible:"
                
                for suggestion in recovery_suggestions:
                    if suggestion['severity'] == 'warning':
                        warning_message += f"\n\n⚠️  {suggestion['issue']}"
                        warning_message += f"\n   Fix: {suggestion['fix']}"
                        if 'default' in suggestion:
                            warning_message += f"\n   Default: {suggestion['default']}"
                        warning_message += f"\n   Example:\n{suggestion['example']}"
                
                return True, warning_message, recovery_suggestions
            else:
                return True, "Configuration validation passed successfully!", []
        
    except Exception as ex:
        return False, f"Configuration validation error: {type(ex).__name__}: {str(ex)}\n" \
                     f"Please check your configuration file format and try again.", []


# =============================================================================
# CONFIGURATION FALLBACK FUNCTIONS
# =============================================================================
# These functions apply automatic fallbacks for missing or invalid configuration
# values, allowing the pipeline to continue with sensible defaults.

def _apply_configuration_fallbacks(parameters: dict, parameters_file_name: str, recovery_suggestions: list) -> dict:
    """Apply graceful fallbacks for missing or invalid configuration values.
    
    This function automatically applies sensible default values for configuration parameters
    that are missing or invalid, based on the recovery suggestions from validation.
    It ensures the pipeline can continue with reasonable defaults while logging what
    was applied for transparency.
    
    Args:
        parameters: Dictionary containing the current configuration parameters.
        parameters_file_name: Name of the configuration file (used for generating default folder names).
        recovery_suggestions: List of recovery suggestion dictionaries from validation.
            
    Returns:
        Updated parameters dictionary with fallback values applied.
        
    Raises:
        Exception: If an error occurs while applying fallbacks (logged as warning).
        
    Example:
        >>> config = {'queries': [{'test': 'test'}]}
        >>> suggestions = [{'issue': 'Missing databases', 'severity': 'warning', 'default': ['arxiv']}]
        >>> updated = _apply_configuration_fallbacks(config, 'test.yaml', suggestions)
        >>> print(f"Databases: {updated.get('databases', 'NOT SET')}")
        Databases: ['arxiv']
        
    Note:
        Only applies fallbacks for warning-level issues. Critical errors should be
        resolved before calling this function.
    """
    try:
        logger.info(
            LogCategory.CONFIGURATION,
            "util",
            "_apply_configuration_fallbacks",
            "Applying configuration fallbacks..."
        )
        
        # Apply fallbacks based on recovery suggestions
        for suggestion in recovery_suggestions:
            if suggestion['severity'] == 'warning' and 'default' in suggestion:
                if 'databases' in suggestion['issue']:
                    if 'databases' not in parameters or not isinstance(parameters['databases'], list):
                        parameters['databases'] = ['arxiv', 'semantic_scholar']
                        logger.info(
                            LogCategory.CONFIGURATION,
                            "util",
                            "_apply_configuration_fallbacks",
                            "Applied default databases: arxiv, semantic_scholar"
                        )
                
                elif 'search_date' in suggestion['issue']:
                    if 'search_date' not in parameters:
                        parameters['search_date'] = datetime.today().strftime('%Y-%m-%d')
                        logger.info(
                            LogCategory.CONFIGURATION,
                            "util",
                            "_apply_configuration_fallbacks",
                            f"Applied default search_date: {parameters['search_date']}"
                        )
                
                elif 'folder_name' in suggestion['issue']:
                    if 'folder_name' not in parameters or not isinstance(parameters['folder_name'], str) or not parameters['folder_name'].strip():
                        parameters['folder_name'] = parameters_file_name.replace('.yaml', '')
                        logger.info(
                            LogCategory.CONFIGURATION,
                            "util",
                            "_apply_configuration_fallbacks",
                            f"Applied default folder_name: {parameters['folder_name']}"
                        )
                
                elif 'syntactic_filters' in suggestion['issue']:
                    if 'syntactic_filters' not in parameters or not isinstance(parameters['syntactic_filters'], list):
                        parameters['syntactic_filters'] = []
                        logger.info(
                            LogCategory.CONFIGURATION,
                            "util",
                            "_apply_configuration_fallbacks",
                            "Applied default syntactic_filters: empty list"
                        )
                
                elif 'semantic_filters' in suggestion['issue']:
                    if 'semantic_filters' not in parameters or not isinstance(parameters['semantic_filters'], list):
                        parameters['semantic_filters'] = []
                        logger.info(
                            LogCategory.CONFIGURATION,
                            "util",
                            "_apply_configuration_fallbacks",
                            "Applied default semantic_filters: empty list"
                        )
        
        logger.info(
            LogCategory.CONFIGURATION,
            "util",
            "_apply_configuration_fallbacks",
            "Configuration fallbacks applied successfully!"
        )
        return parameters
        
    except Exception as ex:
        # Create error context
        context = create_error_context(
            module="util",
            function="_apply_configuration_fallbacks",
            operation="applying_fallbacks",
            severity=ErrorSeverity.WARNING,
            category=ErrorCategory.CONFIGURATION
        )
        
        # Get standard error info
        error_info = get_standard_error_info("configuration_fallback_failed")
        
        # Handle error
        error_handler = ErrorHandler(logger)
        error_msg = error_handler.handle_error(
            error=ex,
            context=context,
            error_type="ConfigurationFallbackError",
            error_description="Error applying configuration fallbacks",
            recovery_suggestion=error_info["recovery"],
            next_steps=error_info["next_steps"]
        )
        
        return parameters


# =============================================================================
# MAIN CONFIGURATION FUNCTIONS
# =============================================================================
# These functions handle the main configuration loading and processing workflow.

def read_parameters(parameters_file_name: str) -> tuple[list, list, list, list, list, dict, list, bool, datetime, datetime, str, str]:
    """Read and validate configuration parameters from a YAML file.
    
    This is the main entry point for loading SaLS configuration files. It reads the YAML file,
    validates all parameters, applies automatic fallbacks for missing optional values, and
    returns a comprehensive tuple of all configuration parameters needed for the pipeline.
    
    The function provides robust error handling with user-friendly messages and automatic
    recovery for common configuration issues. It ensures the pipeline can continue with
    sensible defaults when optional parameters are missing.
    
    Args:
        parameters_file_name: Path to the YAML configuration file to load.
            
    Returns:
        A tuple containing all configuration parameters in the following order:
            - queries: List of search query dictionaries
            - syntactic_filters: List of syntactic filter terms
            - semantic_filters: List of semantic filter descriptions
            - fields: List of search fields to use
            - types: List of document types to include
            - synonyms: Dictionary of synonym mappings for query expansion
            - databases: List of database names to search
            - dates: Boolean indicating if date filtering is enabled
            - start_date: Start date for search range (datetime object)
            - end_date: End date for search range (datetime object)
            - search_date: Date when search was performed (string)
            - folder_name: Name of output folder for results
            
    Raises:
        FileNotFoundError: If the parameters file doesn't exist.
        yaml.YAMLError: If the YAML file has syntax errors.
        ValueError: If configuration validation fails with critical errors.
        Exception: For any other unexpected errors during loading.
        
    Example:
        >>> queries, syn_filters, sem_filters, fields, types, synonyms, databases, dates, start, end, search, folder = read_parameters('config.yaml')
        >>> print(f"Loaded {len(queries)} queries, {len(databases)} databases")
        Loaded 2 queries, 3 databases
        
    Note:
        This function automatically applies fallbacks for missing optional parameters:
        - Missing databases → defaults to ['arxiv', 'semantic_scholar']
        - Missing search_date → defaults to current date
        - Missing folder_name → defaults to filename-based name
        - Missing filters → defaults to empty lists
    """
    try:
        # Read and parse YAML file with error handling
        try:
            with open(parameters_file_name) as file:
                parameters = yaml.load(file, Loader=yaml.FullLoader)
        except FileNotFoundError:
            context = create_error_context(
                module="util",
                function="read_parameters",
                operation="file_reading",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=FileNotFoundError(f"Parameters file not found: {parameters_file_name}"),
                context=context,
                error_type="FileNotFoundError",
                error_description=f"Parameters file not found: {parameters_file_name}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            raise FileNotFoundError(f"Parameters file not found: {parameters_file_name}")
        except yaml.YAMLError as e:
            context = create_error_context(
                module="util",
                function="read_parameters",
                operation="yaml_parsing",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION
            )
            
            error_info = get_standard_error_info("invalid_configuration")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="YAMLError",
                error_description=f"Error parsing YAML file {parameters_file_name}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            raise
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="read_parameters",
                operation="file_reading",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="UnexpectedError",
                error_description=f"Unexpected error reading parameters file {parameters_file_name}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            raise

        # NEW: Proactive configuration validation with error recovery and graceful fallbacks
        try:
            is_valid, validation_message, recovery_suggestions = _validate_configuration(parameters, parameters_file_name)
            if not is_valid:
                context = create_error_context(
                    module="util",
                    function="read_parameters",
                    operation="configuration_validation",
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.CONFIGURATION
                )
                
                error_info = get_standard_error_info("invalid_configuration")
                error_handler = ErrorHandler(logger)
                error_msg = error_handler.handle_error(
                    error=ValueError("Configuration validation failed"),
                    context=context,
                    error_type="ConfigurationValidationError",
                    error_description=f"Configuration validation failed:\n{validation_message}",
                    recovery_suggestion=error_info["recovery"],
                    next_steps=error_info["next_steps"]
                )
                
                raise ValueError(f"Configuration validation failed. Please fix the critical errors above and restart the pipeline.")
            elif validation_message != "Configuration validation passed successfully!":
                # Show warnings but continue with defaults
                logger.warning(
                    LogCategory.CONFIGURATION,
                    "util",
                    "read_parameters",
                    f"Configuration validation completed with warnings:\n{validation_message}"
                )
                logger.info(
                    LogCategory.CONFIGURATION,
                    "util",
                    "read_parameters",
                    "Pipeline will continue with default values where possible."
                )
                
                # Apply graceful fallbacks for missing values
                parameters = _apply_configuration_fallbacks(parameters, parameters_file_name, recovery_suggestions)
            else:
                logger.info(
                    LogCategory.CONFIGURATION,
                    "util",
                    "read_parameters",
                    "Configuration validation passed successfully!"
                )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="read_parameters",
                operation="configuration_validation",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION
            )
            
            error_info = get_standard_error_info("invalid_configuration")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="ConfigurationValidationError",
                error_description="Error during configuration validation",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            raise

        # Safe extraction of queries with error handling
        try:
            if 'queries' in parameters:
                queries = parameters['queries']
            else:
                queries = []

            for query in queries:
                try:
                    keys = query.keys()
                    for key in keys:
                        # Normalize to make the syntax user-friendly while keeping internal tokens
                        query[key] = normalize_query_expression(query[key])
                except (KeyError, AttributeError, TypeError) as e:
                    logger.warning(f"Error processing query {query}: {type(e).__name__}: {str(e)}")
                    continue
                except Exception as ex:
                    logger.warning(f"Unexpected error processing query {query}: {type(ex).__name__}: {str(ex)}")
                    continue
        except Exception as ex:
            logger.warning(f"Error processing queries: {type(ex).__name__}: {str(ex)}")
            queries = []

        # Safe extraction of filters
        try:
            if 'syntactic_filters' in parameters:
                syntactic_filters = parameters['syntactic_filters']
            else:
                syntactic_filters = []

            if 'semantic_filters' in parameters:
                semantic_filters = parameters['semantic_filters']
            else:
                semantic_filters = []
        except Exception as ex:
            logger.warning(f"Error processing filters: {type(ex).__name__}: {str(ex)}")
            syntactic_filters = []
            semantic_filters = []

        # Default fields and types
        fields = ['title', 'abstract']
        types = ['conferences', 'journals']

        # Safe extraction of synonyms
        try:
            synonyms = {}
            for query in queries:
                try:
                    query_name = list(query.keys())[0]
                    words = query[query_name].replace("'", '*').split('*')
                    for word in words:
                        if word in parameters and word not in synonyms.keys():
                            synonyms[word] = parameters[word]
                except (KeyError, AttributeError, TypeError, IndexError) as e:
                    logger.debug(f"Error processing query for synonyms: {type(e).__name__}: {str(e)}")
                    continue
                except Exception as ex:
                    logger.debug(f"Unexpected error processing query for synonyms: {type(ex).__name__}: {str(ex)}")
                    continue
                    
            for syntactic_filter in syntactic_filters:
                try:
                    if syntactic_filter in parameters and syntactic_filter not in synonyms.keys():
                        synonyms[syntactic_filter] = parameters[syntactic_filter]
                except (KeyError, AttributeError, TypeError) as e:
                    logger.debug(f"Error processing syntactic filter for synonyms: {type(e).__name__}: {str(e)}")
                    continue
                except Exception as ex:
                    logger.debug(f"Unexpected error processing syntactic filter for synonyms: {type(ex).__name__}: {str(ex)}")
                    continue
        except Exception as ex:
            logger.warning(f"Error processing synonyms: {type(ex).__name__}: {str(ex)}")
            synonyms = {}

        # Safe extraction of databases
        try:
            if 'databases' in parameters:
                databases = parameters['databases']
            else:
                logger.debug('Databases missing in parameters file. Using default values: arxiv, springer, ieeexplore, '
                            'scopus, core, semantic_scholar')
                databases = ['arxiv', 'springer', 'ieeexplore', 'scopus', 'core', 'semantic_scholar']
        except Exception as ex:
            logger.warning(f"Error processing databases: {type(ex).__name__}: {str(ex)}")
            databases = ['arxiv', 'springer', 'ieeexplore', 'scopus', 'core', 'semantic_scholar']

        # Safe extraction of dates
        try:
            dates = False
            if 'start_date' in parameters:
                start_date = parameters['start_date']
                dates = True
            else:
                start_date = datetime.strptime('1950-01-01', '%Y-%m-%d')
                
            if 'end_date' in parameters:
                end_date = parameters['end_date']
                dates = True
            else:
                end_date = datetime.today()

            if not dates:
                logger.debug('Search dates missing in parameters file. Searching without considering dates...')
                logger.debug('Including dates can reduce the searching time...')
        except (ValueError, TypeError) as e:
            logger.warning(f"Error processing dates: {type(e).__name__}: {str(e)}")
            start_date = datetime.strptime('1950-01-01', '%Y-%m-%d')
            end_date = datetime.today()
            dates = False
        except Exception as ex:
            logger.warning(f"Unexpected error processing dates: {type(ex).__name__}: {str(ex)}")
            start_date = datetime.strptime('1950-01-01', '%Y-%m-%d')
            end_date = datetime.today()
            dates = False

        # Safe extraction of search date
        try:
            if 'search_date' in parameters:
                search_date = str(parameters['search_date'])
            else:
                logger.debug('Search date missing in parameters file. Using current date: '
                            + datetime.today().strftime('%Y-%m-%d'))
                search_date = datetime.today().strftime('%Y-%m-%d')
        except Exception as ex:
            logger.warning(f"Error processing search date: {type(ex).__name__}: {str(ex)}")
            search_date = datetime.today().strftime('%Y-%m-%d')

        # Safe extraction of folder name
        try:
            if 'folder_name' in parameters:
                folder_name = parameters['folder_name']
            else:
                folder_name = parameters_file_name.replace('.yaml', '')
        except Exception as ex:
            logger.warning(f"Error processing folder name: {type(ex).__name__}: {str(ex)}")
            folder_name = parameters_file_name.replace('.yaml', '')

        return queries, syntactic_filters, semantic_filters, fields, types, synonyms, databases, dates, start_date, \
            end_date, search_date, folder_name
            
    except Exception as ex:
        context = create_error_context(
            module="util",
            function="read_parameters",
            operation="parameter_loading",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.CONFIGURATION
        )
        
        error_info = get_standard_error_info("invalid_configuration")
        error_handler = ErrorHandler(logger)
        error_msg = error_handler.handle_error(
            error=ex,
            context=context,
            error_type="ParameterLoadingError",
            error_description="Critical error in read_parameters",
            recovery_suggestion=error_info["recovery"],
            next_steps=error_info["next_steps"]
        )
        
        raise


# =============================================================================
# FILE OPERATION FUNCTIONS
# =============================================================================
# These functions handle file I/O operations including saving, loading,
# and managing CSV files for research papers data.

def save(file_name: str, papers: pd.DataFrame, fmt: str, option: str) -> None:
    """Save papers DataFrame to a CSV file with comprehensive error handling.
    
    This function safely saves research papers data to CSV files, handling common
    file I/O errors and providing detailed error messages for troubleshooting.
    It automatically creates directories if they don't exist and handles various
    file writing scenarios.
    
    Args:
        file_name: Path where the CSV file should be saved.
        papers: Pandas DataFrame containing the papers data to save.
        fmt: Encoding format for the file (e.g., 'utf-8').
        option: File writing mode ('w' for overwrite, 'a+' for append).
            
    Returns:
        None
        
    Raises:
        OSError: If there are file system errors (permissions, disk space, etc.).
        PermissionError: If the file cannot be written due to permission issues.
        ValueError: If there are issues with the DataFrame data.
        TypeError: If the DataFrame cannot be converted to CSV format.
        Exception: For any other unexpected errors during saving.
        
    Example:
        >>> df = pd.DataFrame({'title': ['Paper 1'], 'abstract': ['Abstract 1']})
        >>> save('papers/results.csv', df, 'utf-8', 'w')
        >>> # File saved successfully
        
    Note:
        The function automatically creates parent directories if they don't exist.
        It uses pandas' to_csv method with the specified encoding and mode.
        Error messages are logged for debugging purposes.
    """
    try:
        # Create directory if it doesn't exist
        try:
            os.makedirs(os.path.dirname(file_name), exist_ok=True)
        except (OSError, PermissionError) as e:
            context = create_error_context(
                module="util",
                function="save",
                operation="directory_creation",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="DirectoryCreationError",
                error_description=f"Error creating directory for {file_name}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            raise
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="save",
                operation="directory_creation",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="DirectoryCreationError",
                error_description=f"Unexpected error creating directory for {file_name}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            raise

        # Save the file with error handling
        try:
            with open(file_name, option, newline='', encoding=fmt) as f:
                papers.to_csv(f, encoding=fmt, index=False, header=f.tell() == 0)
        except (OSError, PermissionError) as e:
            context = create_error_context(
                module="util",
                function="save",
                operation="file_writing",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="FileWritingError",
                error_description=f"Error writing to file {file_name}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            raise
        except (ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="save",
                operation="csv_conversion",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="CSVConversionError",
                error_description=f"Error converting papers to CSV for {file_name}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            raise
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="save",
                operation="file_saving",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="FileSavingError",
                error_description=f"Unexpected error saving file {file_name}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            raise
            
    except Exception as ex:
        context = create_error_context(
            module="util",
            function="save",
            operation="file_saving",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.FILE
        )
        
        error_info = get_standard_error_info("file_not_found")
        error_handler = ErrorHandler(logger)
        error_msg = error_handler.handle_error(
            error=ex,
            context=context,
            error_type="FileSavingError",
            error_description=f"Critical error in save function for {file_name}",
            recovery_suggestion=error_info["recovery"],
            next_steps=error_info["next_steps"]
            )
        
        raise


def merge_papers(step: int, merge_step_1: int, merge_step_2: int, folder_name: str, search_date: str) -> str:
    """Merge papers from two different pipeline steps into a single result file.
    
    This function combines papers from two different pipeline stages (typically from
    manual filtering steps) into a single consolidated file. It handles deduplication
    based on title and DOI, and ensures the final result maintains proper paper IDs.
    
    The function constructs file paths based on the pipeline step numbers and search
    metadata, then merges the data while removing duplicates and maintaining data integrity.
    
    Args:
        step: Current pipeline step number for naming the output file.
        merge_step_1: First pipeline step number to merge from.
        merge_step_2: Second pipeline step number to merge from.
        folder_name: Name of the output folder for organizing results.
        search_date: Date when the search was performed (used in file path construction).
            
    Returns:
        Path to the merged result file, regardless of success or failure.
        
    Raises:
        Exception: Various exceptions are caught and logged, but the function always
                  returns the result file path to allow pipeline continuation.
        
    Example:
        >>> result_file = merge_papers(5, 3, 4, 'my_search', '2024-12-15')
        >>> print(f"Merged papers saved to: {result_file}")
        Merged papers saved to: ./papers/my_search/2024_12_15/5_final_list_papers.csv
        
    Note:
        The function handles cases where one or both input files might not exist.
        It performs deduplication based on title (case-insensitive) and DOI.
        If only one file exists, it copies that file as the result.
        All errors are logged but don't prevent the function from returning a result path.
    """
    try:
        file1 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(merge_step_1) + \
                '_manually_filtered_by_full_text_papers.csv'
        file2 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(merge_step_2) + \
                '_manually_filtered_by_full_text_papers.csv'
        result = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) + \
                 '_final_list_papers.csv'
        
        if not exists(result):
            try:
                if exists(file1) and exists(file2):
                    # Read both files with error handling
                    try:
                        df1 = pd.read_csv(file1)
                        df2 = pd.read_csv(file2)
                    except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
                        context = create_error_context(
                            module="util",
                            function="merge_papers",
                            operation="file_reading",
                            severity=ErrorSeverity.ERROR,
                            category=ErrorCategory.FILE
                        )
                        
                        error_info = get_standard_error_info("file_not_found")
                        error_handler = ErrorHandler(logger)
                        error_msg = error_handler.handle_error(
                            error=e,
                            context=context,
                            error_type="FileReadingError",
                            error_description=f"Error reading input files for merge: {type(e).__name__}: {str(e)}",
                            recovery_suggestion=error_info["recovery"],
                            next_steps=error_info["next_steps"]
                        )
                        
                        return result
                    except Exception as ex:
                        context = create_error_context(
                            module="util",
                            function="merge_papers",
                            operation="file_reading",
                            severity=ErrorSeverity.ERROR,
                            category=ErrorCategory.FILE
                        )
                        
                        error_info = get_standard_error_info("file_not_found")
                        error_handler = ErrorHandler(logger)
                        error_msg = error_handler.handle_error(
                            error=ex,
                            context=context,
                            error_type="FileReadingError",
                            error_description=f"Unexpected error reading input files for merge: {type(ex).__name__}: {str(ex)}",
                            recovery_suggestion=error_info["recovery"],
                            next_steps=error_info["next_steps"]
                        )
                        
                        return result

                    try:
                        # Merge dataframes
                        df_result = pd.concat([df1, df2])
                        df_result['title'] = df_result['title'].str.lower()
                        df_result = df_result.drop_duplicates('title')
                        df_result['doi'] = df_result['doi'].str.lower()
                        df_result['doi'].replace(r'\s+', 'nan', regex=True)
                        nan_doi = df_result.loc[df_result['doi'] == 'nan']
                        df_result = df_result.drop_duplicates('doi')
                        df_result = pd.concat([df_result, nan_doi])
                        df_result['id'] = list(range(1, len(df_result) + 1))
                    except (KeyError, ValueError, TypeError) as e:
                        context = create_error_context(
                            module="util",
                            function="merge_papers",
                            operation="data_processing",
                            severity=ErrorSeverity.ERROR,
                            category=ErrorCategory.DATA
                        )
                        
                        error_info = get_standard_error_info("data_validation_failed")
                        error_handler = ErrorHandler(logger)
                        error_msg = error_handler.handle_error(
                            error=e,
                            context=context,
                            error_type="DataProcessingError",
                            error_description=f"Error processing merged data: {type(e).__name__}: {str(e)}",
                            recovery_suggestion=error_info["recovery"],
                            next_steps=error_info["next_steps"]
                        )
                        
                        return result
                    except Exception as ex:
                        context = create_error_context(
                            module="util",
                            function="merge_papers",
                            operation="data_processing",
                            severity=ErrorSeverity.ERROR,
                            category=ErrorCategory.DATA
                        )
                        
                        error_info = get_standard_error_info("data_validation_failed")
                        error_handler = ErrorHandler(logger)
                        error_msg = error_handler.handle_error(
                            error=ex,
                            context=context,
                            error_type="DataProcessingError",
                            error_description=f"Unexpected error processing merged data: {type(ex).__name__}: {str(ex)}",
                            recovery_suggestion=error_info["recovery"],
                            next_steps=error_info["next_steps"]
                        )
                        
                        return result

                    try:
                        save(result, df_result, fr, 'a+')
                        remove_repeated(result)
                    except Exception as save_ex:
                        context = create_error_context(
                            module="util",
                            function="merge_papers",
                            operation="file_saving",
                            severity=ErrorSeverity.ERROR,
                            category=ErrorCategory.FILE
                        )
                        
                        error_info = get_standard_error_info("file_not_found")
                        error_handler = ErrorHandler(logger)
                        error_msg = error_handler.handle_error(
                            error=save_ex,
                            context=context,
                            error_type="FileSavingError",
                            error_description=f"Error saving merged result: {type(save_ex).__name__}: {str(save_ex)}",
                            recovery_suggestion=error_info["recovery"],
                            next_steps=error_info["next_steps"]
                        )
                        
                        return result
                        
                elif exists(file1):
                    try:
                        df_result = pd.read_csv(file1)
                        save(result, df_result, fr, 'a+')
                        remove_repeated(result)
                    except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
                        context = create_error_context(
                            module="util",
                            function="merge_papers",
                            operation="file_reading",
                            severity=ErrorSeverity.ERROR,
                            category=ErrorCategory.FILE
                        )
                        
                        error_info = get_standard_error_info("file_not_found")
                        error_handler = ErrorHandler(logger)
                        error_msg = error_handler.handle_error(
                            error=e,
                            context=context,
                            error_type="FileReadingError",
                            error_description=f"Error reading single file for merge: {type(e).__name__}: {str(e)}",
                            recovery_suggestion=error_info["recovery"],
                            next_steps=error_info["next_steps"]
                        )
                        
                        return result
                    except Exception as ex:
                        context = create_error_context(
                            module="util",
                            function="merge_papers",
                            operation="file_reading",
                            severity=ErrorSeverity.ERROR,
                            category=ErrorCategory.FILE
                        )
                        
                        error_info = get_standard_error_info("file_not_found")
                        error_handler = ErrorHandler(logger)
                        error_msg = error_handler.handle_error(
                            error=ex,
                            context=context,
                            error_type="FileReadingError",
                            error_description=f"Unexpected error reading single file for merge: {type(ex).__name__}: {str(ex)}",
                            recovery_suggestion=error_info["recovery"],
                            next_steps=error_info["next_steps"]
                        )
                        
                        return result
                        
            except Exception as ex:
                context = create_error_context(
                    module="util",
                    function="merge_papers",
                    operation="merge_operation",
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.DATA
                )
                
                error_info = get_standard_error_info("data_validation_failed")
                error_handler = ErrorHandler(logger)
                error_msg = error_handler.handle_error(
                    error=ex,
                    context=context,
                    error_type="MergeOperationError",
                    error_description=f"Error during merge operation: {type(ex).__name__}: {str(ex)}",
                    recovery_suggestion=error_info["recovery"],
                    next_steps=error_info["next_steps"]
                )
                
                return result
                
        return result
        
    except Exception as ex:
        context = create_error_context(
            module="util",
            function="merge_papers",
            operation="merge_papers",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.DATA
        )
        
        error_info = get_standard_error_info("data_validation_failed")
        error_handler = ErrorHandler(logger)
        error_msg = error_handler.handle_error(
            error=ex,
            context=context,
            error_type="MergePapersError",
            error_description=f"Critical error in merge_papers: {type(ex).__name__}: {str(ex)}",
            recovery_suggestion=error_info["recovery"],
            next_steps=error_info["next_steps"]
        )
        
        return result


# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================
# These functions handle data cleaning, deduplication, and quality filtering
# for research papers data.

def remove_repeated(file: str) -> None:
    """Remove duplicate papers from a CSV file based on multiple criteria.
    
    This function performs comprehensive deduplication of research papers based on
    DOI, title, and abstract content. It handles various edge cases including
    missing DOIs, empty abstracts, and different text formats. The deduplication
    process is designed to maintain data quality while removing obvious duplicates.
    
    The function processes the file in-place, reading the CSV, performing deduplication,
    and then saving the cleaned data back to the same file.
    
    Args:
        file: Path to the CSV file containing papers to deduplicate.
            
    Returns:
        None
        
    Raises:
        Exception: Various exceptions are caught and logged, but the function
                  continues processing to handle partial failures gracefully.
        
    Example:
        >>> remove_repeated('papers/results.csv')
        >>> # File deduplicated in-place
        
    Note:
        Deduplication is performed in the following order:
        1. DOI-based deduplication (case-insensitive)
        2. Title-based deduplication (normalized, case-insensitive)
        3. Abstract-based deduplication (normalized, case-insensitive)
        
        Empty abstracts and titles are filtered out during processing.
        The function handles various text normalization (hyphens, newlines, spaces).
        All operations are logged for transparency and debugging.
    """
    try:
        # Read the file with error handling
        try:
            df = pd.read_csv(file)
        except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="file_reading",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="FileReadingError",
                error_description=f"Error reading file {file} for deduplication: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            return
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="file_reading",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="FileReadingError",
                error_description=f"Unexpected error reading file {file} for deduplication: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            return

        try:
            # Process DOI deduplication
            df['doi'] = df['doi'].str.lower()
            df['doi'].replace(r'\s+', np.nan, regex=True)
            nan_doi = df.loc[df['doi'] == np.nan]
            df = df.drop_duplicates('doi')
            df = pd.concat([df, nan_doi])
        except (KeyError, ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="doi_deduplication",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="DOIDeduplicationError",
                error_description=f"Error processing DOI deduplication: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="doi_deduplication",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="DOIDeduplicationError",
                error_description=f"Unexpected error processing DOI deduplication: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            # Process title deduplication
            df['title_lower'] = df['title'].str.lower()
            df['title_lower'] = df['title_lower'].str.replace('-', ' ')
            df['title_lower'] = df['title_lower'].str.replace('\n', '')
            df['title_lower'] = df['title_lower'].str.replace(' ', '')
            df = df.drop_duplicates('title_lower')
        except (KeyError, ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="title_deduplication",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="TitleDeduplicationError",
                error_description=f"Error processing title deduplication: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="title_deduplication",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="TitleDeduplicationError",
                error_description=f"Unexpected error processing title deduplication: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            # Clean empty abstracts and titles
            df.loc[:, 'abstract'] = df['abstract'].replace('', float("NaN"))
            df.dropna(subset=['abstract'], inplace=True)
            df.loc[:, 'title'] = df['title'].replace('', float("NaN"))
            df.dropna(subset=['title'], inplace=True)
        except (KeyError, ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="empty_values_cleaning",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="EmptyValuesCleaningError",
                error_description=f"Error cleaning empty values: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="empty_values_cleaning",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="EmptyValuesCleaningError",
                error_description=f"Unexpected error cleaning empty values: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            # Process abstract deduplication
            df['abstract_lower'] = df['abstract'].str.lower()
            df['abstract_lower'] = df['abstract_lower'].str.replace('-', ' ')
            df['abstract_lower'] = df['abstract_lower'].str.replace('\n', '')
            df['abstract_lower'] = df['abstract_lower'].str.replace(' ', '')
            df = df.drop_duplicates('abstract_lower')
        except (KeyError, ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="abstract_deduplication",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="AbstractDeduplicationError",
                error_description=f"Error processing abstract deduplication: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="abstract_deduplication",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="AbstractDeduplicationError",
                error_description=f"Unexpected error processing abstract deduplication: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            # Clean up temporary columns
            df = df.drop(['abstract_lower', 'title_lower'], axis=1)
        except (KeyError, ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="temporary_columns_cleanup",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="TemporaryColumnsCleanupError",
                error_description=f"Error cleaning up temporary columns: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="temporary_columns_cleanup",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="TemporaryColumnsCleanupError",
                error_description=f"Unexpected error cleaning up temporary columns: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            logger.info(
                LogCategory.DATA,
                "util",
                "remove_repeated",
                f"Number of papers: {len(df)}"
            )
            save(file, df, fr, 'w')
        except Exception as save_ex:
            context = create_error_context(
                module="util",
                function="remove_repeated",
                operation="file_saving",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=save_ex,
                context=context,
                error_type="FileSavingError",
                error_description=f"Error saving deduplicated file: {type(save_ex).__name__}: {str(save_ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
    except Exception as ex:
        context = create_error_context(
            module="util",
            function="remove_repeated",
            operation="remove_repeated",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.DATA
        )
        
        error_info = get_standard_error_info("data_validation_failed")
        error_handler = ErrorHandler(logger)
        error_msg = error_handler.handle_error(
            error=ex,
            context=context,
            error_type="RemoveRepeatedError",
            error_description=f"Critical error in remove_repeated for {file}: {type(ex).__name__}: {str(ex)}",
            recovery_suggestion=error_info["recovery"],
            next_steps=error_info["next_steps"]
        )


def clean_papers(file: str) -> None:
    """Clean and filter research papers based on content quality and language.
    
    This function performs comprehensive cleaning of research papers, including:
    - Removing papers with empty abstracts or titles
    - Filtering out survey/review papers based on title keywords
    - Removing thesis papers based on abstract content
    - Language detection to keep only English papers
    - Quality scoring for language confidence
    
    The function processes papers in batches with progress tracking and handles
    various edge cases gracefully. It's designed to improve the overall quality
    of the paper collection for research analysis.
    
    Args:
        file: Path to the CSV file containing papers to clean.
            
    Returns:
        None
        
    Raises:
        Exception: Various exceptions are caught and logged, but the function
                  continues processing to handle partial failures gracefully.
        
    Example:
        >>> clean_papers('papers/results.csv')
        >>> # File cleaned in-place with progress bar
        
    Note:
        The cleaning process includes:
        1. Content validation (non-empty abstracts/titles)
        2. Survey/review paper filtering (title-based)
        3. Thesis paper filtering (abstract-based)
        4. Language detection using spaCy (English only)
        5. Language confidence scoring (threshold: 0.99)
        
        Progress is displayed using tqdm for long operations.
        Non-English papers are marked and then filtered out.
        All operations are logged for transparency and debugging.
    """
    try:
        # Read the file with error handling
        try:
            df = pd.read_csv(file)
        except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="file_reading",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="FileReadingError",
                error_description=f"Error reading file {file} for cleaning: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            return
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="file_reading",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="FileReadingError",
                error_description=f"Unexpected error reading file {file} for cleaning: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
            return

        try:
            # Clean empty abstracts and titles
            df['abstract'].replace('', np.nan, inplace=True)
            df.dropna(subset=['abstract'], inplace=True)
            df['title'].replace('', np.nan, inplace=True)
            df.dropna(subset=['title'], inplace=True)
        except (KeyError, ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="empty_values_cleaning",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="EmptyValuesCleaningError",
                error_description=f"Error cleaning empty values: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="empty_values_cleaning",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="EmptyValuesCleaningError",
                error_description=f"Unexpected error cleaning empty values: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            # Remove survey/review papers
            values_to_remove = ['survey', 'review', 'progress']
            pattern = '|'.join(values_to_remove)
            df = df.loc[~df['title'].str.contains(pattern, case=False)]
        except (KeyError, ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="survey_review_removal",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="SurveyReviewRemovalError",
                error_description=f"Error removing survey/review papers: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="survey_review_removal",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="SurveyReviewRemovalError",
                error_description=f"Unexpected error removing survey/review papers: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            # Remove thesis papers
            pattern = '(?<!\w)thesis(?!\w)'
            df = df.loc[~df['abstract'].str.contains(pattern, case=False)]
        except (KeyError, ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="thesis_removal",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="ThesisRemovalError",
                error_description=f"Error removing thesis papers: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="thesis_removal",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="ThesisRemovalError",
                error_description=f"Unexpected error removing thesis papers: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            # Initialize language detection
            not_included = 0
            df.loc[:, 'language'] = 'english'
            total_papers = len(df.index)
            current_paper = 0
            
            if total_papers == 0:
                logger.warning(
                    LogCategory.DATA,
                    "util",
                    "clean_papers",
                    "No papers to process for language detection"
                )
                return
                
            pbar = tqdm(total=len(df.index))
            
            for index, row in df.iterrows():
                try:
                    current_paper = current_paper + 1
                    
                    # Language detection with error handling
                    try:
                        doc = nlp(row['abstract'])
                        detect_language = doc._.language
                        
                        if detect_language['language'] != 'en':
                            row['language'] = 'not english'
                            not_included = not_included + 1
                        else:
                            if detect_language['score'] < 0.99:
                                row['language'] = 'not english'
                                not_included = not_included + 1
                                
                    except (AttributeError, KeyError, TypeError) as e:
                        logger.debug(
                            LogCategory.DATA,
                            "util",
                            "clean_papers",
                            f"Error in language detection for paper {index}: {type(e).__name__}: {str(e)}"
                        )
                        row['language'] = 'not english'
                        not_included = not_included + 1
                    except Exception as ex:
                        logger.debug(
                            LogCategory.DATA,
                            "util",
                            "clean_papers",
                            f"Unexpected error in language detection for paper {index}: {type(ex).__name__}: {str(ex)}"
                        )
                        row['language'] = 'not english'
                        not_included = not_included + 1
                        
                    df.loc[index] = row
                    pbar.update(1)
                    
                except (KeyError, ValueError, TypeError, IndexError) as e:
                    logger.debug(
                        LogCategory.DATA,
                        "util",
                        "clean_papers",
                        f"Error processing paper at index {index}: {type(e).__name__}: {str(e)}"
                    )
                    continue
                except Exception as ex:
                    logger.debug(
                        LogCategory.DATA,
                        "util",
                        "clean_papers",
                        f"Unexpected error processing paper at index {index}: {type(ex).__name__}: {str(ex)}"
                    )
                    continue
                    
            pbar.close()
            print('', end="\r")
            
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="language_detection",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="LanguageDetectionError",
                error_description=f"Error during language detection: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            # Filter out non-English papers
            df = df[df['language'] != 'not english']
            df = df.drop(columns=['language'])
        except (KeyError, ValueError, TypeError) as e:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="non_english_filtering",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="NonEnglishFilteringError",
                error_description=f"Error filtering non-English papers: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
        except Exception as ex:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="non_english_filtering",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="NonEnglishFilteringError",
                error_description=f"Unexpected error filtering non-English papers: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )

        try:
            logger.info(
                LogCategory.DATA,
                "util",
                "clean_papers",
                f"Number of papers: {len(df)}"
            )
            save(file, df, fr, 'w')
        except Exception as save_ex:
            context = create_error_context(
                module="util",
                function="clean_papers",
                operation="file_saving",
                severity=ErrorSeverity.ERROR,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=save_ex,
                context=context,
                error_type="FileSavingError",
                error_description=f"Error saving cleaned file: {type(save_ex).__name__}: {str(save_ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            
    except Exception as ex:
        context = create_error_context(
            module="util",
            function="clean_papers",
            operation="clean_papers",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.DATA
        )
        
        error_info = get_standard_error_info("data_validation_failed")
        error_handler = ErrorHandler(logger)
        error_msg = error_handler.handle_error(
            error=ex,
            context=context,
            error_type="CleanPapersError",
            error_description=f"Critical error in clean_papers for {file}: {type(ex).__name__}: {str(ex)}",
            recovery_suggestion=error_info["recovery"],
            next_steps=error_info["next_steps"]
        )


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
# These functions provide general utility operations including retry mechanisms,
# query processing, and other helper functionality.

def exponential_backoff(attempt: int, base_delay: float = DEFAULT_BASE_DELAY, max_delay: float = DEFAULT_MAX_DELAY) -> float:
    """Calculate exponential backoff delay with jitter for retry mechanisms.
    
    This function implements exponential backoff with jitter, which is a common
    pattern for handling transient failures in distributed systems. The jitter
    helps prevent thundering herd problems when multiple clients retry simultaneously.
    
    The delay follows the formula: min(base_delay * 2^attempt, max_delay) with
    additional random jitter to spread out retry attempts.
    
    Args:
        attempt: Current retry attempt number (0-based).
        base_delay: Base delay in seconds (default: 1.0).
        max_delay: Maximum delay cap in seconds (default: 64.0).
            
    Returns:
        Calculated delay in seconds with jitter applied.
        
    Example:
        >>> delays = [exponential_backoff(i) for i in range(5)]
        >>> print(f"Delays: {[f'{d:.1f}s' for d in delays]}")
        Delays: ['0.8s', '1.2s', '2.1s', '4.3s', '8.7s']
        
    Note:
        The function uses exponential growth: 1s, 2s, 4s, 8s, 16s, 32s, 64s...
        Jitter adds randomness: delay * (0.5 + random_factor) where random_factor is 0-1
        This prevents synchronized retry attempts from multiple clients.
        
        Common use cases:
        - API rate limiting
        - Network connection failures
        - Database connection retries
        - File I/O retry operations
    """
    delay = min(base_delay * (2 ** attempt), max_delay)
    delay_with_jitter = delay * (random.random() + 0.5)
    return delay_with_jitter


def parse_queries(queries: list) -> tuple[list, bool]:
    """Parse and validate boolean expressions in search queries.
    
    This function processes a list of search queries, parsing the boolean expressions
    in each query to ensure they are syntactically valid. It uses the parser module
    to validate boolean syntax and provides detailed feedback on any parsing errors.
    
    The function handles various edge cases including None queries, empty query lists,
    and individual query parsing failures. It's designed to catch configuration errors
    early in the pipeline to prevent downstream issues.
    
    Args:
        queries: List of query dictionaries, each containing a query name and boolean expression.
            
    Returns:
        A tuple containing:
            - parsed_queries: List of successfully parsed queries with validated expressions
            - valid: Boolean indicating if all queries were parsed successfully
            
    Raises:
        Exception: Various exceptions are caught and logged, but the function
                  continues processing to handle partial failures gracefully.
        
    Example:
        >>> queries = [{'ml': "'machine learning' & 'edge computing'"}]
        >>> parsed, is_valid = parse_queries(queries)
        >>> print(f"Valid: {is_valid}, Parsed: {len(parsed)}")
        Valid: True, Parsed: 1
        
    Note:
        The function validates each query individually and continues processing
        even if some queries fail. It logs warnings for failed queries but
        returns the overall validation status.
        
        Boolean expressions support:
        - AND/OR operators (&, |, &&, ||)
        - Parentheses for grouping
        - Quoted phrases
        - Legacy '¦' character for OR operations
    """
    try:
        parsed_queries = []
        valid = True
        
        if queries is None:
            logger.warning(
                LogCategory.VALIDATION,
                "util",
                "parse_queries",
                "Queries parameter is None"
            )
            return [], False
            
        if len(queries) > 0:
            for query in queries:
                try:
                    if query is None:
                        logger.warning(
                            LogCategory.VALIDATION,
                            "util",
                            "parse_queries",
                            "Individual query is None, skipping"
                        )
                        continue
                        
                    key = list(query.keys())[0]
                    value = query[key]
                    
                    try:
                        parsed_query, query_valid = par.parse_boolean_expression(value)
                        if not query_valid:
                            valid = False
                            logger.warning(
                                LogCategory.VALIDATION,
                                "util",
                                "parse_queries",
                                f"Invalid boolean expression in query {key}: {value}"
                            )
                            break
                        parsed_queries.append({key: parsed_query})
                    except Exception as parse_ex:
                        context = create_error_context(
                            module="util",
                            function="parse_queries",
                            operation="boolean_expression_parsing",
                            severity=ErrorSeverity.WARNING,
                            category=ErrorCategory.VALIDATION
                        )
                        
                        error_info = get_standard_error_info("data_validation_failed")
                        error_handler = ErrorHandler(logger)
                        error_msg = error_handler.handle_error(
                            error=parse_ex,
                            context=context,
                            error_type="BooleanExpressionParsingError",
                            error_description=f"Error parsing boolean expression for query {key}: {type(parse_ex).__name__}: {str(parse_ex)}",
                            recovery_suggestion=error_info["recovery"],
                            next_steps=error_info["next_steps"]
                        )
                        valid = False
                        break
                        
                except (KeyError, AttributeError, TypeError, IndexError) as e:
                    context = create_error_context(
                        module="util",
                        function="parse_queries",
                        operation="query_processing",
                        severity=ErrorSeverity.WARNING,
                        category=ErrorCategory.VALIDATION
                    )
                    
                    error_info = get_standard_error_info("data_validation_failed")
                    error_handler = ErrorHandler(logger)
                    error_msg = error_handler.handle_error(
                        error=e,
                        context=context,
                        error_type="QueryProcessingError",
                        error_description=f"Error processing query {query}: {type(e).__name__}: {str(e)}",
                        recovery_suggestion=error_info["recovery"],
                        next_steps=error_info["next_steps"]
                    )
                    continue
                except Exception as ex:
                    context = create_error_context(
                        module="util",
                        function="parse_queries",
                        operation="query_processing",
                        severity=ErrorSeverity.WARNING,
                        category=ErrorCategory.VALIDATION
                    )
                    
                    error_info = get_standard_error_info("data_validation_failed")
                    error_handler = ErrorHandler(logger)
                    error_msg = error_handler.handle_error(
                        error=ex,
                        context=context,
                        error_type="QueryProcessingError",
                        error_description=f"Unexpected error processing query {query}: {type(ex).__name__}: {str(ex)}",
                        recovery_suggestion=error_info["recovery"],
                        next_steps=error_info["next_steps"]
                    )
                    continue
        else:
            valid = False
            logger.debug(
                LogCategory.VALIDATION,
                "util",
                "parse_queries",
                "No queries provided"
            )
            
        return parsed_queries, valid
        
    except Exception as ex:
        context = create_error_context(
            module="util",
            function="parse_queries",
            operation="parse_queries",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.VALIDATION
        )
        
        error_info = get_standard_error_info("data_validation_failed")
        error_handler = ErrorHandler(logger)
        error_msg = error_handler.handle_error(
            error=ex,
            context=context,
            error_type="ParseQueriesError",
            error_description=f"Critical error in parse_queries: {type(ex).__name__}: {str(ex)}",
            recovery_suggestion=error_info["recovery"],
            next_steps=error_info["next_steps"]
        )
        return [], False
