import yaml
import pandas as pd
import numpy as np
import os
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from datetime import datetime
import spacy
from os.path import exists
import logging
import random
from . import parser as par
from tqdm import tqdm


logger = logging.getLogger('logger')


def _apply_word_replacements_outside_quotes(text):
    """Normalize boolean operators outside quoted phrases to internal tokens <AND>/<OR>.

    - Supports: AND/and, OR/or, &&, ||, single & and |, and the '¦' character
    - Preserves anything inside single or double quotes
    """
    import re

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


def normalize_query_expression(expression):
    """Return a normalized boolean expression string.

    - Removes stray encoding artifacts
    - Normalizes boolean operators to <AND>/<OR> outside quotes
    - Ensures consistent spacing
    """
    # Remove common stray encoding artifacts
    expression = expression.replace('Â', '')
    # Normalize operators outside of quotes
    expression = _apply_word_replacements_outside_quotes(expression)
    return expression.strip()


@Language.factory('language_detector')
def language_detector(nlp, name):
    return LanguageDetector()


nlp = spacy.load('en_core_web_sm')

nlp.add_pipe('language_detector', last=True)

fr = 'utf-8'


def _validate_configuration(parameters, parameters_file_name):
    """
    Validate configuration parameters and provide user-friendly error messages with recovery suggestions.
    Returns (is_valid, error_message, recovery_suggestions) tuple.
    """
    try:
        recovery_suggestions = []
        critical_errors = []
        warnings = []
        
        # Validate queries (CRITICAL - pipeline cannot continue without)
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
                            'issue': f'Query value is {repr(query_value)}',
                            'fix': 'Use a non-empty string for the query value',
                            'example': f'  - {query_name}: "term1 & term2"',
                            'severity': 'critical'
                        })
                        break
        
        # Validate databases (WARNING - can provide defaults)
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
            valid_databases = ['arxiv', 'springer', 'ieeexplore', 'scopus', 'core', 'semantic_scholar', 
                             'crossref', 'europe_pmc', 'pubmed', 'openalex']
            
            invalid_dbs = []
            for db in parameters['databases']:
                if not isinstance(db, str):
                    invalid_dbs.append(f"{repr(db)} (not a string)")
                elif db not in valid_databases:
                    invalid_dbs.append(f"'{db}' (unknown database)")
            
            if invalid_dbs:
                warnings.append(f"Configuration warning: Invalid database(s) found: {', '.join(invalid_dbs)}")
                recovery_suggestions.append({
                    'issue': f'Invalid databases: {", ".join(invalid_dbs)}',
                    'fix': 'Use only valid database names',
                    'example': f'Valid databases: {", ".join(valid_databases)}',
                    'severity': 'warning',
                    'note': 'Some databases require API keys in config.json'
                })
        
        # Validate dates (WARNING - can provide defaults)
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
        
        # Validate search_date (WARNING - can provide default)
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


def _apply_configuration_fallbacks(parameters, parameters_file_name, recovery_suggestions):
    """
    Apply graceful fallbacks for missing or invalid configuration values.
    Returns updated parameters with defaults applied.
    """
    try:
        logger.info("Applying configuration fallbacks...")
        
        # Apply fallbacks based on recovery suggestions
        for suggestion in recovery_suggestions:
            if suggestion['severity'] == 'warning' and 'default' in suggestion:
                if 'databases' in suggestion['issue']:
                    if 'databases' not in parameters or not isinstance(parameters['databases'], list):
                        parameters['databases'] = ['arxiv', 'semantic_scholar']
                        logger.info("Applied default databases: arxiv, semantic_scholar")
                
                elif 'search_date' in suggestion['issue']:
                    if 'search_date' not in parameters:
                        parameters['search_date'] = datetime.today().strftime('%Y-%m-%d')
                        logger.info(f"Applied default search_date: {parameters['search_date']}")
                
                elif 'folder_name' in suggestion['issue']:
                    if 'folder_name' not in parameters or not isinstance(parameters['folder_name'], str) or not parameters['folder_name'].strip():
                        parameters['folder_name'] = parameters_file_name.replace('.yaml', '')
                        logger.info(f"Applied default folder_name: {parameters['folder_name']}")
                
                elif 'syntactic_filters' in suggestion['issue']:
                    if 'syntactic_filters' not in parameters or not isinstance(parameters['syntactic_filters'], list):
                        parameters['syntactic_filters'] = []
                        logger.info("Applied default syntactic_filters: empty list")
                
                elif 'semantic_filters' in suggestion['issue']:
                    if 'semantic_filters' not in parameters or not isinstance(parameters['semantic_filters'], list):
                        parameters['semantic_filters'] = []
                        logger.info("Applied default semantic_filters: empty list")
        
        logger.info("Configuration fallbacks applied successfully!")
        return parameters
        
    except Exception as ex:
        logger.warning(f"Error applying configuration fallbacks: {type(ex).__name__}: {str(ex)}")
        return parameters


def read_parameters(parameters_file_name):
    try:
        # Read and parse YAML file with error handling
        try:
            with open(parameters_file_name) as file:
                parameters = yaml.load(file, Loader=yaml.FullLoader)
        except FileNotFoundError:
            logger.error(f"Parameters file not found: {parameters_file_name}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {parameters_file_name}: {str(e)}")
            raise
        except Exception as ex:
            logger.error(f"Unexpected error reading parameters file {parameters_file_name}: {type(ex).__name__}: {str(ex)}")
            raise

        # NEW: Proactive configuration validation with error recovery and graceful fallbacks
        try:
            is_valid, validation_message, recovery_suggestions = _validate_configuration(parameters, parameters_file_name)
            if not is_valid:
                logger.error(f"Configuration validation failed:\n{validation_message}")
                raise ValueError(f"Configuration validation failed. Please fix the critical errors above and restart the pipeline.")
            elif validation_message != "Configuration validation passed successfully!":
                # Show warnings but continue with defaults
                logger.warning(f"Configuration validation completed with warnings:\n{validation_message}")
                logger.info("Pipeline will continue with default values where possible.")
                
                # Apply graceful fallbacks for missing values
                parameters = _apply_configuration_fallbacks(parameters, parameters_file_name, recovery_suggestions)
            else:
                logger.info("Configuration validation passed successfully!")
        except Exception as ex:
            logger.error(f"Error during configuration validation: {type(ex).__name__}: {str(ex)}")
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
        logger.error(f"Critical error in read_parameters: {type(ex).__name__}: {str(ex)}")
        raise


def save(file_name, papers, fmt, option):
    try:
        # Create directory if it doesn't exist
        try:
            os.makedirs(os.path.dirname(file_name), exist_ok=True)
        except (OSError, PermissionError) as e:
            logger.error(f"Error creating directory for {file_name}: {type(e).__name__}: {str(e)}")
            raise
        except Exception as ex:
            logger.error(f"Unexpected error creating directory for {file_name}: {type(ex).__name__}: {str(ex)}")
            raise

        # Save the file with error handling
        try:
            with open(file_name, option, newline='', encoding=fmt) as f:
                papers.to_csv(f, encoding=fmt, index=False, header=f.tell() == 0)
        except (OSError, PermissionError) as e:
            logger.error(f"Error writing to file {file_name}: {type(e).__name__}: {str(e)}")
            raise
        except (ValueError, TypeError) as e:
            logger.error(f"Error converting papers to CSV for {file_name}: {type(e).__name__}: {str(e)}")
            raise
        except Exception as ex:
            logger.error(f"Unexpected error saving file {file_name}: {type(ex).__name__}: {str(ex)}")
            raise
            
    except Exception as ex:
        logger.error(f"Critical error in save function for {file_name}: {type(ex).__name__}: {str(ex)}")
        raise


def merge_papers(step, merge_step_1, merge_step_2, folder_name, search_date):
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
                        logger.error(f"Error reading input files for merge: {type(e).__name__}: {str(e)}")
                        return result
                    except Exception as ex:
                        logger.error(f"Unexpected error reading input files for merge: {type(ex).__name__}: {str(ex)}")
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
                        logger.error(f"Error processing merged data: {type(e).__name__}: {str(e)}")
                        return result
                    except Exception as ex:
                        logger.error(f"Unexpected error processing merged data: {type(ex).__name__}: {str(ex)}")
                        return result

                    try:
                        save(result, df_result, fr, 'a+')
                        remove_repeated(result)
                    except Exception as save_ex:
                        logger.error(f"Error saving merged result: {type(save_ex).__name__}: {str(save_ex)}")
                        return result
                        
                elif exists(file1):
                    try:
                        df_result = pd.read_csv(file1)
                        save(result, df_result, fr, 'a+')
                        remove_repeated(result)
                    except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
                        logger.error(f"Error reading single file for merge: {type(e).__name__}: {str(e)}")
                        return result
                    except Exception as ex:
                        logger.error(f"Unexpected error reading single file for merge: {type(ex).__name__}: {str(ex)}")
                        return result
                        
            except Exception as ex:
                logger.error(f"Error during merge operation: {type(ex).__name__}: {str(ex)}")
                return result
                
        return result
        
    except Exception as ex:
        logger.error(f"Critical error in merge_papers: {type(ex).__name__}: {str(ex)}")
        return result


def remove_repeated(file):
    try:
        # Read the file with error handling
        try:
            df = pd.read_csv(file)
        except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
            logger.error(f"Error reading file {file} for deduplication: {type(e).__name__}: {str(e)}")
            return
        except Exception as ex:
            logger.error(f"Unexpected error reading file {file} for deduplication: {type(ex).__name__}: {str(ex)}")
            return

        try:
            # Process DOI deduplication
            df['doi'] = df['doi'].str.lower()
            df['doi'].replace(r'\s+', np.nan, regex=True)
            nan_doi = df.loc[df['doi'] == np.nan]
            df = df.drop_duplicates('doi')
            df = pd.concat([df, nan_doi])
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error processing DOI deduplication: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error processing DOI deduplication: {type(ex).__name__}: {str(ex)}")

        try:
            # Process title deduplication
            df['title_lower'] = df['title'].str.lower()
            df['title_lower'] = df['title_lower'].str.replace('-', ' ')
            df['title_lower'] = df['title_lower'].str.replace('\n', '')
            df['title_lower'] = df['title_lower'].str.replace(' ', '')
            df = df.drop_duplicates('title_lower')
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error processing title deduplication: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error processing title deduplication: {type(ex).__name__}: {str(ex)}")

        try:
            # Clean empty abstracts and titles
            df.loc[:, 'abstract'] = df['abstract'].replace('', float("NaN"))
            df.dropna(subset=['abstract'], inplace=True)
            df.loc[:, 'title'] = df['title'].replace('', float("NaN"))
            df.dropna(subset=['title'], inplace=True)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error cleaning empty values: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error cleaning empty values: {type(ex).__name__}: {str(ex)}")

        try:
            # Process abstract deduplication
            df['abstract_lower'] = df['abstract'].str.lower()
            df['abstract_lower'] = df['abstract_lower'].str.replace('-', ' ')
            df['abstract_lower'] = df['abstract_lower'].str.replace('\n', '')
            df['abstract_lower'] = df['abstract_lower'].str.replace(' ', '')
            df = df.drop_duplicates('abstract_lower')
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error processing abstract deduplication: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error processing abstract deduplication: {type(ex).__name__}: {str(ex)}")

        try:
            # Clean up temporary columns
            df = df.drop(['abstract_lower', 'title_lower'], axis=1)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error cleaning up temporary columns: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error cleaning up temporary columns: {type(ex).__name__}: {str(ex)}")

        try:
            logger.info('Number of papers: ' + str(len(df)))
            save(file, df, fr, 'w')
        except Exception as save_ex:
            logger.error(f"Error saving deduplicated file: {type(save_ex).__name__}: {str(save_ex)}")
            
    except Exception as ex:
        logger.error(f"Critical error in remove_repeated for {file}: {type(ex).__name__}: {str(ex)}")


def remove_repeated_df(df):
    try:
        if df is None or df.empty:
            logger.warning("DataFrame is None or empty in remove_repeated_df")
            return df
            
        try:
            # Process title deduplication
            df['title_lower'] = df['title'].str.lower()
            df['title_lower'] = df['title_lower'].str.replace('-', ' ')
            df['title_lower'] = df['title_lower'].str.replace('\n', '')
            df['title_lower'] = df['title_lower'].str.replace(' ', '')
            df = df.drop_duplicates('title_lower')
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error processing title deduplication in DataFrame: {type(e).__name__}: {str(e)}")
            return df
        except Exception as ex:
            logger.warning(f"Unexpected error processing title deduplication in DataFrame: {type(ex).__name__}: {str(ex)}")
            return df

        try:
            # Clean up temporary columns
            df = df.drop(columns=['title_lower'], errors='ignore')
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error cleaning up temporary columns in DataFrame: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error cleaning up temporary columns in DataFrame: {type(ex).__name__}: {str(ex)}")

        return df
        
    except Exception as ex:
        logger.error(f"Critical error in remove_repeated_df: {type(ex).__name__}: {str(ex)}")
        return df


def remove_repeated_ieee(file):
    try:
        # Read the file with error handling
        try:
            df = pd.read_csv(file)
        except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
            logger.error(f"Error reading file {file} for IEEE deduplication: {type(e).__name__}: {str(e)}")
            return 0
        except Exception as ex:
            logger.error(f"Unexpected error reading file {file} for IEEE deduplication: {type(ex).__name__}: {str(ex)}")
            return 0

        try:
            # Process DOI deduplication
            df = df.drop_duplicates('doi')
            df.dropna(subset=['abstract'], inplace=True)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error processing DOI deduplication: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error processing DOI deduplication: {type(ex).__name__}: {str(ex)}")

        try:
            # Process title deduplication
            df['title_lower'] = df['title'].str.lower()
            df['title_lower'] = df['title_lower'].str.replace('-', ' ')
            df['title_lower'] = df['title_lower'].str.replace('\n', '')
            df['title_lower'] = df['title_lower'].str.replace(' ', '')
            df = df.drop_duplicates('title_lower')
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error processing title deduplication: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error processing title deduplication: {type(ex).__name__}: {str(ex)}")

        try:
            # Clean empty abstracts and titles
            df['abstract'].replace('', np.nan, inplace=True)
            df.dropna(subset=['abstract'], inplace=True)
            df['title'].replace('', np.nan, inplace=True)
            df.dropna(subset=['title'], inplace=True)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error cleaning empty values: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error cleaning empty values: {type(ex).__name__}: {str(ex)}")

        try:
            # Process abstract deduplication
            df['abstract_lower'] = df['abstract'].str.lower()
            df['abstract_lower'] = df['abstract_lower'].str.replace('-', ' ')
            df['abstract_lower'] = df['abstract_lower'].str.replace('\n', '')
            df['abstract_lower'] = df['abstract_lower'].str.replace(' ', '')
            df = df.drop_duplicates('abstract_lower')
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error processing abstract deduplication: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error processing abstract deduplication: {type(ex).__name__}: {str(ex)}")

        try:
            # Clean up temporary columns
            df = df.drop(['abstract_lower', 'title_lower'], axis=1)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error cleaning up temporary columns: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error cleaning up temporary columns: {type(ex).__name__}: {str(ex)}")

        try:
            logger.info('Number of papers: ' + str(len(df)))
            save(file, df, fr, 'w')
            return len(df.index)
        except Exception as save_ex:
            logger.error(f"Error saving IEEE deduplicated file: {type(save_ex).__name__}: {str(save_ex)}")
            return 0
            
    except Exception as ex:
        logger.error(f"Critical error in remove_repeated_ieee for {file}: {type(ex).__name__}: {str(ex)}")
        return 0


def clean_papers(file):
    try:
        # Read the file with error handling
        try:
            df = pd.read_csv(file)
        except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
            logger.error(f"Error reading file {file} for cleaning: {type(e).__name__}: {str(e)}")
            return
        except Exception as ex:
            logger.error(f"Unexpected error reading file {file} for cleaning: {type(ex).__name__}: {str(ex)}")
            return

        try:
            # Clean empty abstracts and titles
            df['abstract'].replace('', np.nan, inplace=True)
            df.dropna(subset=['abstract'], inplace=True)
            df['title'].replace('', np.nan, inplace=True)
            df.dropna(subset=['title'], inplace=True)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error cleaning empty values: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error cleaning empty values: {type(ex).__name__}: {str(ex)}")

        try:
            # Remove survey/review papers
            values_to_remove = ['survey', 'review', 'progress']
            pattern = '|'.join(values_to_remove)
            df = df.loc[~df['title'].str.contains(pattern, case=False)]
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error removing survey/review papers: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error removing survey/review papers: {type(ex).__name__}: {str(ex)}")

        try:
            # Remove thesis papers
            pattern = '(?<!\w)thesis(?!\w)'
            df = df.loc[~df['abstract'].str.contains(pattern, case=False)]
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error removing thesis papers: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error removing thesis papers: {type(ex).__name__}: {str(ex)}")

        try:
            # Initialize language detection
            not_included = 0
            df.loc[:, 'language'] = 'english'
            total_papers = len(df.index)
            current_paper = 0
            
            if total_papers == 0:
                logger.warning("No papers to process for language detection")
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
                        logger.debug(f"Error in language detection for paper {index}: {type(e).__name__}: {str(e)}")
                        row['language'] = 'not english'
                        not_included = not_included + 1
                    except Exception as ex:
                        logger.debug(f"Unexpected error in language detection for paper {index}: {type(ex).__name__}: {str(ex)}")
                        row['language'] = 'not english'
                        not_included = not_included + 1
                        
                    df.loc[index] = row
                    pbar.update(1)
                    
                except (KeyError, ValueError, TypeError, IndexError) as e:
                    logger.debug(f"Error processing paper at index {index}: {type(e).__name__}: {str(e)}")
                    continue
                except Exception as ex:
                    logger.debug(f"Unexpected error processing paper at index {index}: {type(ex).__name__}: {str(ex)}")
                    continue
                    
            pbar.close()
            print('', end="\r")
            
        except Exception as ex:
            logger.warning(f"Error during language detection: {type(ex).__name__}: {str(ex)}")

        try:
            # Filter out non-English papers
            df = df[df['language'] != 'not english']
            df = df.drop(columns=['language'])
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Error filtering non-English papers: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            logger.warning(f"Unexpected error filtering non-English papers: {type(ex).__name__}: {str(ex)}")

        try:
            logger.info('Number of papers: ' + str(len(df)))
            save(file, df, fr, 'w')
        except Exception as save_ex:
            logger.error(f"Error saving cleaned file: {type(save_ex).__name__}: {str(save_ex)}")
            
    except Exception as ex:
        logger.error(f"Critical error in clean_papers for {file}: {type(ex).__name__}: {str(ex)}")


def exponential_backoff(attempt, base_delay=1, max_delay=64):
    delay = min(base_delay * (2 ** attempt), max_delay)
    delay_with_jitter = delay * (random.random() + 0.5)
    return delay_with_jitter


def parse_queries(queries):
    try:
        parsed_queries = []
        valid = True
        
        if queries is None:
            logger.warning("Queries parameter is None")
            return [], False
            
        if len(queries) > 0:
            for query in queries:
                try:
                    if query is None:
                        logger.warning("Individual query is None, skipping")
                        continue
                        
                    key = list(query.keys())[0]
                    value = query[key]
                    
                    try:
                        parsed_query, query_valid = par.parse_boolean_expression(value)
                        if not query_valid:
                            valid = False
                            logger.warning(f"Invalid boolean expression in query {key}: {value}")
                            break
                        parsed_queries.append({key: parsed_query})
                    except Exception as parse_ex:
                        logger.warning(f"Error parsing boolean expression for query {key}: {type(parse_ex).__name__}: {str(parse_ex)}")
                        valid = False
                        break
                        
                except (KeyError, AttributeError, TypeError, IndexError) as e:
                    logger.warning(f"Error processing query {query}: {type(e).__name__}: {str(e)}")
                    continue
                except Exception as ex:
                    logger.warning(f"Unexpected error processing query {query}: {type(ex).__name__}: {str(ex)}")
                    continue
        else:
            valid = False
            logger.debug("No queries provided")
            
        return parsed_queries, valid
        
    except Exception as ex:
        logger.error(f"Critical error in parse_queries: {type(ex).__name__}: {str(ex)}")
        return [], False
