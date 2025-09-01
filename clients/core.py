import time
import pandas as pd
import json
from .apis.generic import Generic
from .base_client import DatabaseClient
from os.path import exists
from util import util
from tqdm import tqdm
import logging
from util.error_standards import (
    ErrorHandler, create_error_context, ErrorSeverity, ErrorCategory,
    get_standard_error_info
)
from util.logging_standards import LogCategory


class CoreClient(DatabaseClient):
    """
    Refactored CORE client using the Template Method pattern.
    """
    
    def __init__(self):
        super().__init__(
            database_name='core',
            max_papers=1000,
            waiting_time=2,
            max_retries=3,
            quota=1000
        )
        self.api_url = 'https://api.core.ac.uk/v3/search/works'
        self.client_fields = {'title': 'title', 'abstract': 'abstract'}
        self.client = Generic()
        
        # Load API access from config
        if exists('./config.json'):
            with open("./config.json", "r") as file:
                config = json.load(file)
            if 'api_access_core' in config:
                self.api_access = config['api_access_core']
            else:
                self.api_access = ''
        else:
            self.api_access = ''

    def _has_api_access(self) -> bool:
        """Check if CORE API access is available."""
        return self.api_access != ''

    def _plan_requests(self, query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date) -> pd.DataFrame:
        """Plan the API requests for CORE."""
        # Extract query value from the query dictionary
        query_name = list(query.keys())[0]
        query_value = query[query_name]
        
        # Build query parameters
        c_fields = []
        for field in fields:
            if field in self.client_fields:
                c_fields.append(self.client_fields[field])
        
        parameters = {
            'query': query_value,
            'syntactic_filters': syntactic_filters,
            'synonyms': synonyms,
            'fields': c_fields,
            'types': types
        }
        
        # Create initial request to get total count
        request = self._create_request(parameters, dates, start_date, end_date)
        headers = {'Authorization': 'Bearer ' + self.api_access}
        raw_papers = self._retry_request(self.client.request, self.api_url, 'post', request, headers)
        expected_papers = self._get_expected_papers(raw_papers)
        
        self.logger.info(LogCategory.DATABASE, "core", "_plan_requests", f"Expected papers from {self.database_name}: {expected_papers}...")
        
        # Calculate number of requests needed
        times = int(expected_papers / self.max_papers) - 1
        mod = int(expected_papers) % self.max_papers
        if mod > 0:
            times = times + 1
        
        # Check quota constraints
        if times >= self.quota:
            self.logger.info(LogCategory.DATABASE, "core", "_plan_requests", f"The number of expected papers requires {times} requests which exceeds the {self.database_name} quota of {self.quota} requests per day.")
            if len(syntactic_filters) > 0:
                self.logger.info(LogCategory.DATABASE, "core", "_plan_requests", "Trying to reduce the number of requests using syntactic filters.")
                que = ''
                for word in syntactic_filters:
                    que = que.replace('<AND>last', '<AND> ')
                    que = que + "'" + word + "' <AND>last"
                que = que.replace(' <AND>last', '')
                parameters['query'] = que
                request = self._create_request(parameters, dates, start_date, end_date)
                headers = {'Authorization': 'Bearer ' + self.api_access}
                raw_papers = self._retry_request(self.client.request, self.api_url, 'post', request, headers)
                expected_papers = self._get_expected_papers(raw_papers)
                self.logger.info(LogCategory.DATABASE, "core", "_plan_requests", f"Expected papers from {self.database_name} using syntactic filters: {expected_papers}...")
                times = int(expected_papers / self.max_papers) - 1
                mod = int(expected_papers) % self.max_papers
                if mod > 0:
                    times = times + 1
                if times >= self.quota:
                    self.logger.info(LogCategory.DATABASE, "core", "_plan_requests", f"The number of expected papers requires {times} requests which exceeds the {self.database_name} quota of {self.quota} requests per day.")
                    self.logger.info(LogCategory.DATABASE, "core", "_plan_requests", "Skipping to next repository. Try to redefine your search queries and syntactic filters. Using dates to limit your search can help in case you are not.")
                    return pd.DataFrame()
            else:
                self.logger.info(LogCategory.DATABASE, "core", "_plan_requests", "Skipping to next repository. Please use syntactic filters to avoid this problem. Using dates to limit your search can help in case you are not.")
                return pd.DataFrame()
        
        # Execute requests
        parameters['expected_papers'] = expected_papers
        papers = self._execute_requests(query, parameters, dates, start_date, end_date)
        return papers

    def _execute_requests(self, query, parameters, dates, start_date, end_date):
        """Execute the planned requests to retrieve papers."""
        papers = pd.DataFrame()
        times = int(parameters.get('expected_papers', 0) / self.max_papers) - 1
        mod = int(parameters.get('expected_papers', 0) % self.max_papers)
        if mod > 0:
            times = times + 1
        
        for t in tqdm(range(0, times + 1)):
            time.sleep(self.waiting_time)
            start = self.max_papers * t
            request = self._create_request(parameters, dates, start_date, end_date)
            request['from'] = start
            headers = {'Authorization': 'Bearer ' + self.api_access}
            
            raw_papers = self._retry_request(self.client.request, self.api_url, 'post', request, headers)
            
            if raw_papers is None:
                continue
            
            papers_request = self._process_raw_papers(query, raw_papers)
            if len(papers) == 0:
                papers = papers_request
            else:
                papers = pd.concat([papers, papers_request])
        
        return papers

    def _create_request(self, parameters, dates, start_date, end_date):
        """Create the API request for CORE."""
        start_year = start_date.year
        end_year = end_date.year
        query = self.client.core_query(parameters)
        if dates:
            query = '(yearPublished>=' + str(start_year) + ' AND yearPublished<=' + str(end_year) + ') AND ' + query
        request = {
            'q': query,
            'limit': self.max_papers,
            'offset': 0
        }
        
        return request

    def _get_expected_papers(self, raw_papers):
        """Get the expected number of papers from the API response."""
        total = 0
        if raw_papers.status_code == 200:
            try:
                json_results = json.loads(raw_papers.text)
                total = int(json_results['totalHits'])
            except (json.JSONDecodeError, KeyError) as e:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "core", "_get_expected_papers", 
                    ErrorSeverity.WARNING, 
                    ErrorCategory.DATA,
                    f"Data parsing error in CORE response: {type(e).__name__}: {str(e)}"
                )
                error_info = get_standard_error_info("data_validation_failed")
                ErrorHandler.handle_error(e, context, error_info, self.logger)
            except (ValueError, TypeError) as e:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "core", "_get_expected_papers", 
                    ErrorSeverity.WARNING, 
                    ErrorCategory.DATA,
                    f"Data type error in CORE response: {type(e).__name__}: {str(e)}"
                )
                error_info = get_standard_error_info("data_validation_failed")
                ErrorHandler.handle_error(e, context, error_info, self.logger)
            except Exception as ex:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "core", "_get_expected_papers", 
                    ErrorSeverity.ERROR, 
                    ErrorCategory.DATA,
                    f"Unexpected error parsing CORE response: {type(ex).__name__}: {str(ex)}"
                )
                error_info = get_standard_error_info("unexpected_error")
                ErrorHandler.handle_error(ex, context, error_info, self.logger)
        else:
            self._log_api_error(raw_papers, self.api_url)
        return total

    def _process_raw_papers(self, query, raw_papers):
        """Process the raw API response into a DataFrame."""
        query_name = list(query.keys())[0]
        query_value = query[query_name]
        papers_request = pd.DataFrame()
        
        if raw_papers.status_code == 200:
            try:
                json_results = json.loads(raw_papers.text)
                raw_papers = pd.json_normalize(json_results['results'])
                papers_request['id'] = raw_papers['id']
                papers_request['title'] = raw_papers['title']
                papers_request['abstract'] = raw_papers['abstract']
                papers_request['url'] = raw_papers['downloadUrl']
                papers_request['publication'] = raw_papers['publisher']
                papers_request['publisher'] = self.database_name
                papers_request['publication_date'] = raw_papers['publishedDate']
                papers_request['database'] = self.database_name
                papers_request['query_name'] = query_name
                papers_request['query_value'] = query_value.replace('<AND>', 'AND').replace('<OR>', 'OR')
            except (json.JSONDecodeError, KeyError) as e:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "core", "_process_raw_papers", 
                    ErrorSeverity.WARNING, 
                    ErrorCategory.DATA,
                    f"Data parsing error in CORE response: {type(e).__name__}: {str(e)}"
                )
                error_info = get_standard_error_info("data_validation_failed")
                ErrorHandler.handle_error(e, context, error_info, self.logger)
            except Exception as ex:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "core", "_process_raw_papers", 
                    ErrorSeverity.ERROR, 
                    ErrorCategory.DATA,
                    f"Unexpected error parsing CORE response: {type(ex).__name__}: {str(ex)}"
                )
                error_info = get_standard_error_info("unexpected_error")
                ErrorHandler.handle_error(ex, context, error_info, self.logger)
        else:
            self._log_api_error(raw_papers, self.api_url)
        
        return papers_request

    def _filter_papers(self, papers: pd.DataFrame, dates, start_date, end_date) -> pd.DataFrame:
        """Filter papers based on criteria."""
        self.logger.info(LogCategory.DATA, "core", "_filter_papers", "Filtering papers...")
        try:
            # Filter by title
            papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
            papers = papers.dropna(subset=['title'])
            papers.loc[:, 'title'] = papers['title'].str.lower()
            papers = papers.drop_duplicates('title')
            
            # Filter by abstract
            papers.loc[:, 'abstract'] = papers['abstract'].replace('', float("NaN"))
            papers = papers.dropna(subset=['abstract'])
            
        except (ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "core", "_filter_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Data type error during CORE paper filtering: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Continue with unfiltered papers rather than failing completely
        except KeyError as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "core", "_filter_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Missing required column during CORE paper filtering: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "core", "_filter_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Unexpected error during CORE paper filtering: {type(ex).__name__}: {str(ex)}"
            )
            error_info = get_standard_error_info("unexpected_error")
            ErrorHandler.handle_error(ex, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        
        return papers

    def _clean_papers(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize paper data."""
        self.logger.info(LogCategory.DATA, "core", "_clean_papers", "Cleaning papers...")
        try:
            papers.replace('', float("NaN"), inplace=True)
            papers.dropna(how='all', axis=1, inplace=True)
        except (ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "core", "_clean_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Data type error during CORE paper cleaning: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Continue with uncleaned papers rather than failing completely
        except KeyError as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "core", "_clean_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Missing required column during CORE paper cleaning: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "core", "_clean_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Unexpected error during CORE paper cleaning: {type(ex).__name__}: {str(ex)}"
            )
            error_info = get_standard_error_info("unexpected_error")
            ErrorHandler.handle_error(ex, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        
        return papers
    
    def _get_abstracts(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Get abstracts for papers."""
        pass
