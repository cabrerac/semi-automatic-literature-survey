import time
from .apis.xploreapi import XPLORE
from .apis.generic import Generic
from .base_client import DatabaseClient
import json
import pandas as pd
from os.path import exists
from util import util
from tqdm import tqdm
import logging
from util.error_standards import (
    ErrorHandler, create_error_context, ErrorSeverity, ErrorCategory,
    get_standard_error_info
)
from util.logging_standards import LogCategory


class IeeeXploreClient(DatabaseClient):
    """
    Refactored IEEE Xplore client using the Template Method pattern.
    """
    
    def __init__(self):
        super().__init__(
            database_name='ieeexplore',
            max_papers=200,
            waiting_time=5,
            max_retries=3,
            client_fields={'abstract': 'abstract', 'title': 'article_title'},
            quota=200
        )
        self.client = Generic()
        
        # Load API access from config
        if exists('./config.json'):
            with open("./config.json", "r") as file:
                config = json.load(file)
            if 'api_access_ieee' in config:
                self.api_access = config['api_access_ieee']
            else:
                self.api_access = ''
        else:
            self.api_access = ''
        
        # Define client-specific fields and types after API access is loaded
        self.client_fields = {'abstract': 'abstract', 'title': 'article_title'}
        self.client_types = {'conferences': 'Conferences', 'early access': 'Early Access', 'journals': 'Journals', 'standards': 'Standards'}

    def _has_api_access(self) -> bool:
        """Check if IEEE Xplore API access is available."""
        return self.api_access != ''

    def _plan_requests(self, query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date) -> pd.DataFrame:
        """Plan the API requests for IEEE Xplore."""
        # Extract query value from the query dictionary
        query_name = list(query.keys())[0]
        query_value = query[query_name]
        
        # Build query parameters
        c_fields = []
        for field in fields:
            if field in self.client_fields:
                c_fields.append(self.client_fields[field])
        
        c_types = []
        for t in types:
            if t in self.client_types:
                c_types.append(self.client_types[t])
        
        parameters = {
            'query': query_value,
            'syntactic_filters': syntactic_filters,
            'synonyms': synonyms,
            'fields': c_fields,
            'types': c_types
        }
        
        # Create initial requests to get total count
        reqs = self._create_request(parameters)
        total_requests = 0
        planning_requests = 0
        
        # Calculate total requests needed
        for req in reqs:
            for field in c_fields:
                for p_type in c_types:
                    raw_papers = self._retry_request(self._request, req, field, p_type, 0)
                    planning_requests = planning_requests + 1
                    total_requests = total_requests + 1
                    expected_papers = self._get_expected_papers(raw_papers)
                    times = int(expected_papers / self.max_papers) - 1
                    mod = int(expected_papers) % self.max_papers
                    if mod > 0:
                        times = times + 1
                    total_requests = total_requests + times
        
        # Check quota constraints
        if total_requests >= self.quota:
            self.logger.info(LogCategory.DATABASE, "ieeexplore", "_plan_requests", f"The number of expected papers requires {total_requests} requests which exceeds the {self.database_name} quota of {self.quota} requests per day.")
            if len(syntactic_filters) > 0:
                self.logger.info(LogCategory.DATABASE, "ieeexplore", "_plan_requests", "Trying to reduce the number of requests using syntactic filters.")
                que = ''
                for word in syntactic_filters:
                    que = que.replace('<AND>last', '<AND> ')
                    que = que + "'" + word + "' <AND>last"
                que = que.replace(' <AND>last', '')
                parameters['query'] = que
                reqs = self._create_request(parameters)
                
                # Recalculate with syntactic filters
                total_requests = 0
                for req in reqs:
                    for field in c_fields:
                        for p_type in c_types:
                            raw_papers = self._retry_request(self._request, req, field, p_type, 0)
                            planning_requests = planning_requests + 1
                            total_requests = total_requests + 1
                            expected_papers = self._get_expected_papers(raw_papers)
                            times = int(expected_papers / self.max_papers) - 1
                            mod = int(expected_papers) % self.max_papers
                            if mod > 0:
                                times = times + 1
                            total_requests = total_requests + times
                
                if total_requests >= self.quota:
                    self.logger.info(LogCategory.DATABASE, "ieeexplore", "_plan_requests", f"The number of expected papers requires {total_requests} requests which exceeds the {self.database_name} quota of {self.quota} requests per day.")
                    self.logger.info(LogCategory.DATABASE, "ieeexplore", "_plan_requests", "Skipping to next repository. Try to redefine your search queries and syntactic filters.")
                    return pd.DataFrame()
            else:
                self.logger.info(LogCategory.DATABASE, "ieeexplore", "_plan_requests", "Skipping to next repository. Please use syntactic filters to avoid this problem.")
                return pd.DataFrame()
        
        # Execute requests
        papers = self._execute_requests(query, parameters, planning_requests)
        return papers

    def _execute_requests(self, query, parameters, planning_requests):
        """Execute the planned requests to retrieve papers."""
        total_requests = planning_requests
        papers = pd.DataFrame()
        reqs = self._create_request(parameters)
        fields = parameters['fields']
        types = parameters['types']
        current_request = 0
        total_queries = len(reqs) * len(fields) * len(types)
        
        self.logger.info(LogCategory.DATABASE, "ieeexplore", "_execute_requests", f"There will be {total_queries} different queries to the {self.database_name} API...")
        pbar = tqdm(total=total_queries)
        for req in reqs:
            for field in fields:
                for p_type in types:
                    current_request = current_request + 1
                    raw_papers = self._retry_request(self._request, req, field, p_type, 0)
                    total_requests = total_requests + 1
                    expected_papers = self._get_expected_papers(raw_papers)
                    times = int(expected_papers / self.max_papers) - 1
                    mod = int(expected_papers) % self.max_papers
                    if mod > 0:
                        times = times + 1
                    
                    if (total_requests + times) < self.quota:
                        for t in range(0, times + 1):
                            time.sleep(self.waiting_time)
                            start = self.max_papers * t
                            raw_papers = self._retry_request(self._request, req, field, p_type, start)
                            total_requests = total_requests + 1
                            papers_request = self._process_raw_papers(query, raw_papers)
                            if len(papers) == 0:
                                papers = papers_request
                            else:
                                papers = pd.concat([papers, papers_request])
                    else:
                        self.logger.info(LogCategory.DATABASE, "ieeexplore", "_execute_requests", f"Query {current_request}...")
                        self.logger.info(LogCategory.DATABASE, "ieeexplore", "_execute_requests", f"The number of requests {total_requests} exceeds the {self.database_name} quota of {self.quota} requests per day.")
                        self.logger.info(LogCategory.DATABASE, "ieeexplore", "_execute_requests", f"If you continue requesting the {self.database_name} API with the current key today, you will get errors from the API.")
                        self.logger.info(LogCategory.DATABASE, "ieeexplore", "_execute_requests", "Skipping to next repository.")
                        break
                    pbar.update(1)
        pbar.close()
        
        return papers

    def _create_request(self, parameters):
        """Create the API requests for IEEE Xplore."""
        return self.client.ieeexplore_query(parameters)

    def _request(self, query, field, p_type, start_record):
        """Make a single API request to IEEE Xplore."""
        client_ieee = XPLORE(self.api_access)
        client_ieee.searchField(field, query)
        client_ieee.resultsFilter("content_type", p_type)
        client_ieee.startingResult(start_record)
        client_ieee.maximumResults(self.max_papers)
        raw_papers = client_ieee.callAPI()
        return raw_papers

    def _get_expected_papers(self, raw_papers):
        """Get the expected number of papers from the API response."""
        total = 0
        try:
            if raw_papers.status_code == 200:
                try:
                    raw_json = json.loads(raw_papers.text)
                    if 'articles' in raw_json:
                        total = raw_json['total_records']
                except (json.JSONDecodeError, KeyError) as e:
                    # User-friendly message explaining what's happening
                    context = create_error_context(
                        "ieeexplore", "_get_expected_papers", 
                        ErrorSeverity.WARNING, 
                        ErrorCategory.DATA,
                        f"Data parsing error in IEEE Xplore response: {type(e).__name__}: {str(e)}"
                    )
                    error_info = get_standard_error_info("data_validation_failed")
                    ErrorHandler.handle_error(e, context, error_info, self.logger)
                except Exception as ex:
                    # User-friendly message explaining what's happening
                    context = create_error_context(
                        "ieeexplore", "_get_expected_papers", 
                        ErrorSeverity.ERROR, 
                        ErrorCategory.DATA,
                        f"Unexpected error parsing IEEE Xplore response: {type(ex).__name__}: {str(ex)}"
                    )
                    error_info = get_standard_error_info("unexpected_error")
                    ErrorHandler.handle_error(ex, context, error_info, self.logger)
            else:
                self._log_api_error(raw_papers, raw_papers.request.url if raw_papers.request else "")
        except (AttributeError, TypeError) as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "ieeexplore", "_get_expected_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Attribute error in IEEE Xplore request: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
        except Exception as ex:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "ieeexplore", "_get_expected_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Unexpected error in IEEE Xplore request: {type(ex).__name__}: {str(ex)}"
            )
            error_info = get_standard_error_info("unexpected_error")
            ErrorHandler.handle_error(ex, context, error_info, self.logger)
        return total

    def _process_raw_papers(self, query, raw_papers):
        """Process the raw API response into a DataFrame."""
        query_name = list(query.keys())[0]
        query_value = query[query_name]
        papers_request = pd.DataFrame()
        
        try:
            if raw_papers.status_code == 200:
                try:
                    raw_json = json.loads(raw_papers.text)
                    temp_papers = pd.json_normalize(raw_json['articles']).copy()
                    papers_request = temp_papers[['doi', 'title', 'publisher', 'content_type', 'abstract', 'html_url',
                                            'publication_title', 'publication_date']].copy()
                    papers_request.loc[:, 'database'] = self.database_name
                    papers_request.loc[:, 'query_name'] = query_name
                    papers_request.loc[:, 'query_value'] = query_value.replace('<AND>', 'AND').replace('<OR>', 'OR')
                except (json.JSONDecodeError, KeyError) as e:
                    # Handle JSON parsing and missing key errors
                    context = create_error_context(
                        "ieeexplore", "_process_raw_papers", 
                        ErrorSeverity.WARNING, 
                        ErrorCategory.DATA,
                        f"Data parsing error in IEEE Xplore response: {type(e).__name__}: {str(e)}"
                    )
                    error_info = get_standard_error_info("data_validation_failed")
                    ErrorHandler.handle_error(e, context, error_info, self.logger)
                except Exception as ex:
                    # Handle unexpected errors
                    context = create_error_context(
                        "ieeexplore", "_process_raw_papers", 
                        ErrorSeverity.ERROR, 
                        ErrorCategory.DATA,
                        f"Unexpected error parsing IEEE Xplore response: {type(ex).__name__}: {str(ex)}"
                    )
                    error_info = get_standard_error_info("unexpected_error")
                    ErrorHandler.handle_error(ex, context, error_info, self.logger)
            else:
                self._log_api_error(raw_papers, raw_papers.request.url if raw_papers.request else "")
        except (AttributeError, TypeError) as e:
            # Handle attribute access errors
            context = create_error_context(
                "ieeexplore", "_process_raw_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Attribute error in IEEE Xplore request: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
        except Exception as ex:
            # Handle unexpected errors
            context = create_error_context(
                "ieeexplore", "_process_raw_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Unexpected error in IEEE Xplore request: {type(ex).__name__}: {str(ex)}"
            )
            error_info = get_standard_error_info("unexpected_error")
            ErrorHandler.handle_error(ex, context, error_info, self.logger)
        
        return papers_request

    def _filter_papers(self, papers: pd.DataFrame, dates, start_date, end_date) -> pd.DataFrame:
        """Filter papers based on criteria."""
        self.logger.info(LogCategory.DATA, "ieeexplore", "_filter_papers", "Filtering papers...")
        try:
            # Filter by title
            papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
            papers = papers.dropna(subset=['title'])
            papers.loc[:, 'title'] = papers['title'].str.lower()
            papers = papers.drop_duplicates('title')
            
            # Filter by abstract
            papers.loc[:, 'abstract'] = papers['abstract'].replace('', float("NaN"))
            papers = papers.dropna(subset=['abstract'])
            papers = papers.drop_duplicates(subset=['doi'])
            
        except (ValueError, TypeError) as e:
            # Handle data type conversion errors (e.g., non-string titles, invalid data)
            context = create_error_context(
                "ieeexplore", "_filter_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Data type error during IEEE Xplore paper filtering: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Continue with unfiltered papers rather than failing completely
        except KeyError as e:
            # Handle missing column errors
            context = create_error_context(
                "ieeexplore", "_filter_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Missing required column during IEEE Xplore paper filtering: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            # Handle unexpected errors
            context = create_error_context(
                "ieeexplore", "_filter_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Unexpected error during IEEE Xplore paper filtering: {type(ex).__name__}: {str(ex)}"
            )
            error_info = get_standard_error_info("unexpected_error")
            ErrorHandler.handle_error(ex, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        
        return papers

    def _clean_papers(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize paper data."""
        self.logger.info(LogCategory.DATA, "ieeexplore", "_clean_papers", "Cleaning papers...")
        try:
            papers.replace('', float("NaN"), inplace=True)
            papers.dropna(how='all', axis=1, inplace=True)
        except (ValueError, TypeError) as e:
            # Handle data type conversion errors
            context = create_error_context(
                "ieeexplore", "_clean_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Data type error during IEEE Xplore paper cleaning: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Continue with uncleaned papers rather than failing completely
        except KeyError as e:
            # Handle missing column errors
            context = create_error_context(
                "ieeexplore", "_clean_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Missing required column during IEEE Xplore paper cleaning: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            # Handle unexpected errors
            context = create_error_context(
                "ieeexplore", "_clean_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Unexpected error during IEEE Xplore paper cleaning: {type(ex).__name__}: {str(ex)}"
            )
            error_info = get_standard_error_info("unexpected_error")
            ErrorHandler.handle_error(ex, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        
        return papers

    def _get_abstracts(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Get abstracts for papers."""
        pass