import pandas as pd
import json
from .base_client import DatabaseClient
from .apis.generic import Generic
from os.path import exists
import logging
import time
from tqdm import tqdm
from util.error_standards import (
    ErrorHandler, create_error_context, ErrorSeverity, ErrorCategory,
    get_standard_error_info
)
from util.logging_standards import LogCategory


class SpringerClient(DatabaseClient):
    """Springer client implementation using the DatabaseClient base class."""
    
    def __init__(self):
        super().__init__(
            database_name='springer',
            max_papers=25,
            waiting_time=2,
            max_retries=3,
            client_fields={'title': 'title'},
            quota=500
        )
        
        # Load API access from config
        if exists('./config.json'):
            with open("./config.json", "r") as file:
                config = json.load(file)
            if 'api_access_springer' in config:
                self.api_access = config['api_access_springer']
            else:
                self.api_access = ''
        else:
            self.api_access = ''
        
        # Define API URL after API access is loaded
        self.api_url = 'http://api.springernature.com/metadata/json?q=type:Journal<dates>'
        self.start = 0
        self.client = Generic()

    def _has_api_access(self) -> bool:
        """Check if Springer API access is available."""
        return self.api_access != ''

    def _plan_requests(self, query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date) -> pd.DataFrame:
        """Plan the API requests for Springer."""
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
        request = self._create_request(parameters, dates, start_date, end_date, False)
        raw_papers = self._retry_request(self.client.request, request, 'get', {}, {})
        expected_papers = self._get_expected_papers(raw_papers)
        
        self.logger.info(LogCategory.DATABASE, "springer", "_plan_requests", f"Expected papers from springer: {expected_papers}...")
        
        # Calculate number of requests needed
        times = int(expected_papers / self.max_papers) - 1
        mod = int(expected_papers) % self.max_papers
        if mod > 0:
            times = times + 1
            
        # Check quota constraints
        if times >= self.quota:
            self.logger.info(LogCategory.DATABASE, "springer", "_plan_requests", f"The number of expected papers requires {times + 1} requests which exceeds the {self.database_name} quota of {self.quota} requests per day.")
            if len(syntactic_filters) > 0:
                self.logger.info(LogCategory.DATABASE, "springer", "_plan_requests", "Trying to reduce the number of requests using syntactic filters.")
                request = self._create_request(parameters, dates, start_date, end_date, True)
                raw_papers = self._retry_request(self.client.request, request, 'get', {}, {})
                expected_papers = self._get_expected_papers(raw_papers)
                self.logger.info(LogCategory.DATABASE, "springer", "_plan_requests", f"Expected papers from {self.database_name} using syntactic filters: {expected_papers}...")
                times = int(expected_papers / self.max_papers) - 1
                mod = int(expected_papers) % self.max_papers
                if mod > 0:
                    times = times + 1
                if times >= self.quota:
                    self.logger.info(LogCategory.DATABASE, "springer", "_plan_requests", f"The number of expected papers requires {times + 1} requests which exceeds the {self.database_name} quota of {self.quota} requests per day.")
                    self.logger.info(LogCategory.DATABASE, "springer", "_plan_requests", "Skipping to next repository. Try to redefine your search queries and syntactic filters. Using dates to limit your search can help in case you are not.")
                    return pd.DataFrame()
            else:
                self.logger.info(LogCategory.DATABASE, "springer", "_plan_requests", "Skipping to next repository. Please use syntactic filters to avoid this problem. Using dates to limit your search can help in case you are not.")
                return pd.DataFrame()
        
        # Execute requests
        papers = self._execute_requests(query, parameters, times, dates, start_date, end_date, False)
        return papers

    def _execute_requests(self, query, parameters, times, dates, start_date, end_date, syntactic_filter):
        """Execute the planned requests to retrieve papers."""
        papers = pd.DataFrame()
        
        for t in tqdm(range(0, times + 1)):
            time.sleep(self.waiting_time)
            self.start = t * self.max_papers
            
            request = self._create_request(parameters, dates, start_date, end_date, syntactic_filter)
            raw_papers = self._retry_request(self.client.request, request, 'get', {}, {})
            
            if raw_papers is None:
                continue
                
            papers_request = self._process_raw_papers(query, raw_papers)
            
            if len(papers) == 0:
                papers = papers_request
            else:
                papers = pd.concat([papers, papers_request])
        
        return papers

    def _create_request(self, parameters, dates, start_date, end_date, syntactic_filter):
        """Create the API request URL for Springer."""
        req = self.api_url
        if dates is True:
            req = req.replace('<dates>', '%20onlinedatefrom:' + str(start_date) +'%20onlinedateto:' + str(end_date) + '%20')
        else:
            req = req.replace('<dates>', '')
            
        if not syntactic_filter:
            req = req + self.client.default_query(parameters)
            req = req.replace('%28', '(').replace('%29', ')').replace('+', '%20')
            req = req.replace('title:', '')
        else:
            query = parameters['query']
            syntactic_filters = parameters['syntactic_filters']
            for word in syntactic_filters:
                query = query.replace('<AND>last', '<AND> ')
                query = query + "'" + word + "' <AND>last"
            query = query.replace(' <AND>last', '')
            parameters['query'] = query
            req = req + self.client.default_query(parameters)
            req = req.replace('%28', '(').replace('%29', ')').replace('+', '%20')
            req = req.replace('title:', '')
            
        req = req + '&s='+str(self.start)+'&p='+str(self.max_papers)+'&api_key=' + self.api_access
        return req

    def _get_expected_papers(self, raw_papers):
        """Get the expected number of papers from the API response."""
        total = 0
        if raw_papers.status_code == 200:
            try:
                json_results = json.loads(raw_papers.text)
                total = int(json_results['result'][0]['total'])
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "springer", "_get_expected_papers", 
                    ErrorSeverity.WARNING, 
                    ErrorCategory.DATA,
                    f"Data parsing error in Springer response: {type(e).__name__}: {str(e)}"
                )
                error_info = get_standard_error_info("data_validation_failed")
                ErrorHandler.handle_error(e, context, error_info, self.logger)
            except (ValueError, TypeError) as e:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "springer", "_get_expected_papers", 
                    ErrorSeverity.WARNING, 
                    ErrorCategory.DATA,
                    f"Data type error in Springer response: {type(e).__name__}: {str(e)}"
                )
                error_info = get_standard_error_info("data_validation_failed")
                ErrorHandler.handle_error(e, context, error_info, self.logger)
            except Exception as ex:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "springer", "_get_expected_papers", 
                    ErrorSeverity.ERROR, 
                    ErrorCategory.DATA,
                    f"Unexpected error parsing Springer response: {type(ex).__name__}: {str(ex)}"
                )
                error_info = get_standard_error_info("unexpected_error")
                ErrorHandler.handle_error(ex, context, error_info, self.logger)
        else:
            self._log_api_error(raw_papers, raw_papers.request.url if raw_papers.request else "")
        return total

    def _process_raw_papers(self, query, raw_papers):
        """Process the raw API response into a DataFrame."""
        query_name = list(query.keys())[0]
        query_value = query[query_name]
        papers_request = pd.DataFrame()
        
        if raw_papers.status_code == 200:
            try:
                json_results = json.loads(raw_papers.text)
                papers_request = pd.json_normalize(json_results['records'])
                papers_request.loc[:, 'database'] = self.database_name
                papers_request.loc[:, 'query_name'] = query_name
                papers_request.loc[:, 'query_value'] = query_value.replace('<AND>', 'AND').replace('<OR>', 'OR')
            except (json.JSONDecodeError, KeyError) as e:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "springer", "_process_raw_papers", 
                    ErrorSeverity.WARNING, 
                    ErrorCategory.DATA,
                    f"Data parsing error in Springer response: {type(e).__name__}: {str(e)}"
                )
                error_info = get_standard_error_info("data_validation_failed")
                ErrorHandler.handle_error(e, context, error_info, self.logger)
            except Exception as ex:
                # User-friendly message explaining what's happening
                context = create_error_context(
                    "springer", "_process_raw_papers", 
                    ErrorSeverity.ERROR, 
                    ErrorCategory.DATA,
                    f"Unexpected error parsing Springer response: {type(ex).__name__}: {str(ex)}"
                )
                error_info = get_standard_error_info("unexpected_error")
                ErrorHandler.handle_error(ex, context, error_info, self.logger)
        else:
            self._log_api_error(raw_papers, raw_papers.request.url if raw_papers.request else "")
        
        return papers_request

    def _filter_papers(self, papers: pd.DataFrame, dates, start_date, end_date) -> pd.DataFrame:
        """Filter papers based on criteria."""
        self.logger.info(LogCategory.DATA, "springer", "_filter_papers", "Filtering papers...")
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
            
            # Filter by language
            if 'language' in papers:
                papers = papers[papers['language'].str.contains('en')]
                
        except (ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "springer", "_filter_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Data type error during Springer paper filtering: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Continue with unfiltered papers rather than failing completely
        except KeyError as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "springer", "_filter_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Missing required column during Springer paper filtering: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "springer", "_filter_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Unexpected error during Springer paper filtering: {type(ex).__name__}: {str(ex)}"
            )
            error_info = get_standard_error_info("unexpected_error")
            ErrorHandler.handle_error(ex, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        
        return papers

    def _clean_papers(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize paper data."""
        self.logger.info(LogCategory.DATA, "springer", "_clean_papers", "Cleaning papers...")
        try:
            # Extract URLs
            urls = []
            if 'url' in papers:
                for paper in papers['url']:
                    url = paper[0]['value']
                    urls.append(url)
            
            # Remove unnecessary columns
            papers = papers.drop(columns=['url', 'creators', 'bookEditors', 'openaccess', 'printIsbn', 'electronicIsbn',
                                          'isbn', 'genre', 'copyright', 'conferenceInfo', 'issn', 'eIssn', 'volume',
                                          'publicationType', 'number', 'issueType', 'topicalCollection', 'startingPage',
                                          'endingPage', 'language', 'journalId', 'printDate', 'response', 'onlineDate',
                                          'coverDate', 'keyword'],
                                 errors='ignore')
            
            # Add cleaned URLs
            if len(urls) > 0:
                papers.loc[:, 'url'] = urls
            else:
                papers['url'] = ''
            
            # Clean empty values
            papers.replace('', float("NaN"), inplace=True)
            papers.dropna(how='all', axis=1, inplace=True)
            
        except (ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "springer", "_clean_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Data type error during Springer paper cleaning: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Continue with uncleaned papers rather than failing completely
        except KeyError as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "springer", "_clean_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"Missing required column during Springer paper cleaning: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        except (IndexError, AttributeError) as e:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "springer", "_clean_papers", 
                ErrorSeverity.WARNING, 
                ErrorCategory.DATA,
                f"URL extraction error during Springer paper cleaning: {type(e).__name__}: {str(e)}"
            )
            error_info = get_standard_error_info("data_validation_failed")
            ErrorHandler.handle_error(e, context, error_info, self.logger)
            # Continue with empty URLs rather than failing completely
        except Exception as ex:
            # User-friendly message explaining what's happening
            context = create_error_context(
                "springer", "_clean_papers", 
                ErrorSeverity.ERROR, 
                ErrorCategory.DATA,
                f"Unexpected error during Springer paper cleaning: {type(ex).__name__}: {str(ex)}"
            )
            error_info = get_standard_error_info("unexpected_error")
            ErrorHandler.handle_error(ex, context, error_info, self.logger)
            # Return papers as-is to prevent complete failure
        
        return papers
    
    def _get_abstracts(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Get abstracts for papers."""
        pass