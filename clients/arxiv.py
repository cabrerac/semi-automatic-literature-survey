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


class ArxivClient(DatabaseClient):
    """
    Refactored arXiv client using the Template Method pattern.
    """
    
    def __init__(self):
        super().__init__(
            database_name='arxiv',
            max_papers=5000,
            waiting_time=2,
            max_retries=3,
            client_fields={'title': 'ti', 'abstract': 'abs'}
        )
        self.api_url = 'http://export.arxiv.org/api/query?search_query='
        self.client = Generic()
        
    def _has_api_access(self) -> bool:
        """ArXiv is open access, so no API key is needed."""
        return True
    
    def _plan_requests(self, query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date) -> pd.DataFrame:
        """Plan the API requests for arXiv."""
        # Extract query value from the query dictionary (same as original arxiv.py)
        query_name = list(query.keys())[0]
        query_value = query[query_name]
        
        # Build query parameters
        c_fields = []
        for field in fields:
            if field in self.client_fields:
                c_fields.append(self.client_fields[field])
        
        parameters = {
            'query': query_value,  # Use query_value string, not the entire query dict
            'syntactic_filters': syntactic_filters,
            'synonyms': synonyms,
            'fields': c_fields,
            'types': types
        }
        
        # Create initial request to get total count
        request = self._create_request(parameters)
        raw_papers = self._retry_request(self.client.request, request, 'get', {}, {})
        expected_papers = self._get_expected_papers(raw_papers)
        
        self.logger.info(f"Expected papers from arxiv: {expected_papers}...")
        
        # Calculate number of requests needed
        times = int(expected_papers / self.max_papers) - 1
        mod = int(expected_papers) % self.max_papers
        if mod > 0:
            times = times + 1
            
        # Execute requests
        papers = self._execute_requests(query, parameters, times, expected_papers, mod)
        return papers
    
    def _execute_requests(self, query, parameters, times, expected_papers, mod):
        """Execute the planned requests to retrieve papers."""
        papers = pd.DataFrame()
        
        for t in tqdm(range(0, times + 1)):
            time.sleep(self.waiting_time)
            start = t * self.max_papers
            
            request = self._create_request(parameters, start)
            raw_papers = self._retry_request(self.client.request, request, 'get', {}, {})
            
            if raw_papers is None:
                continue
                
            papers_request = self._process_raw_papers(query, raw_papers)
            
            # Sometimes arXiv API doesn't respond with all papers, so retry
            expected_per_request = expected_papers
            if expected_papers > self.max_papers:
                expected_per_request = self.max_papers
            if t == times and mod > 0:
                expected_per_request = mod
                
            while len(papers_request) < expected_per_request:
                time.sleep(self.waiting_time)
                raw_papers = self._retry_request(self.client.request, request, 'get', {}, {})
                papers_request = self._process_raw_papers(query, raw_papers)
            
            if len(papers) == 0:
                papers = papers_request
            else:
                papers = pd.concat([papers, papers_request])
        
        return papers
    
    def _create_request(self, parameters, start=0):
        """Create the API request URL for arXiv."""
        req = self.api_url
        req = req + self.client.default_query(parameters)
        req = req + '&start=' + str(start)
        req = req + '&max_results=' + str(self.max_papers)
        req = req + '&sortBy=submittedDate&sortOrder=descending'
        return req
    
    def _get_expected_papers(self, raw_papers):
        """Get the expected number of papers from the API response."""
        total = 0
        if raw_papers.status_code == 200:
            try:
                total_text = raw_papers.text.split('opensearch:totalResults')[1]
                total = int(total_text.split('>')[1].replace('</', ''))
            except (IndexError, ValueError) as e:
                context = create_error_context(
                    module="arxiv",
                    function="_get_expected_papers",
                    operation="api_response_parsing",
                    severity=ErrorSeverity.WARNING,
                    category=ErrorCategory.API
                )
                
                error_info = get_standard_error_info("data_validation_failed")
                error_handler = ErrorHandler(self.logger)
                error_msg = error_handler.handle_error(
                    error=e,
                    context=context,
                    error_type="APIResponseParsingError",
                    error_description=f"Error parsing the API response: {type(e).__name__}: {str(e)}",
                    recovery_suggestion=error_info["recovery"],
                    next_steps=error_info["next_steps"]
                )
            except Exception as ex:
                context = create_error_context(
                    module="arxiv",
                    function="_get_expected_papers",
                    operation="api_response_parsing",
                    severity=ErrorSeverity.WARNING,
                    category=ErrorCategory.API
                )
                
                error_info = get_standard_error_info("data_validation_failed")
                error_handler = ErrorHandler(self.logger)
                error_msg = error_handler.handle_error(
                    error=ex,
                    context=context,
                    error_type="APIResponseParsingError",
                    error_description=f"Unexpected error parsing the API response: {type(ex).__name__}: {str(ex)}",
                    recovery_suggestion=error_info["recovery"],
                    next_steps=error_info["next_steps"]
                )
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
                papers_request = pd.read_xml(raw_papers.text, xpath='//feed:entry',
                                           namespaces={"feed": "http://www.w3.org/2005/Atom"})
                papers_request.loc[:, 'database'] = self.database_name
                papers_request.loc[:, 'query_name'] = query_name
                papers_request.loc[:, 'query_value'] = query_value.replace('<AND>', 'AND').replace('<OR>', 'OR')
            except (ValueError, TypeError) as e:
                context = create_error_context(
                    module="arxiv",
                    function="_process_raw_papers",
                    operation="api_response_parsing",
                    severity=ErrorSeverity.WARNING,
                    category=ErrorCategory.API
                )
                
                error_info = get_standard_error_info("data_validation_failed")
                error_handler = ErrorHandler(self.logger)
                error_msg = error_handler.handle_error(
                    error=e,
                    context=context,
                    error_type="APIResponseParsingError",
                    error_description=f"Error parsing the API response: {type(e).__name__}: {str(e)}",
                    recovery_suggestion=error_info["recovery"],
                    next_steps=error_info["next_steps"]
                )
            except Exception as ex:
                context = create_error_context(
                    module="arxiv",
                    function="_process_raw_papers",
                    operation="api_response_parsing",
                    severity=ErrorSeverity.WARNING,
                    category=ErrorCategory.API
                )
                
                error_info = get_standard_error_info("data_validation_failed")
                error_handler = ErrorHandler(self.logger)
                error_msg = error_handler.handle_error(
                    error=ex,
                    context=context,
                    error_type="APIResponseParsingError",
                    error_description=f"Unexpected error parsing the API response: {type(ex).__name__}: {str(ex)}",
                    recovery_suggestion=error_info["recovery"],
                    next_steps=error_info["next_steps"]
                )
        else:
            self._log_api_error(raw_papers, raw_papers.request.url if raw_papers.request else "")
        
        return papers_request
    
    def _filter_papers(self, papers: pd.DataFrame, dates, start_date, end_date) -> pd.DataFrame:
        """Filter papers based on criteria."""
        self.logger.info("Filtering papers...")
        try:
            # Filter by title
            papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
            papers.dropna(subset=['title'], inplace=True)
            papers.loc[:, 'title'] = papers['title'].str.lower()
            papers = papers.drop_duplicates('title')
            
            # Filter by abstract
            papers.loc[:, 'summary'] = papers['summary'].replace('', float("NaN"))
            papers.dropna(subset=['summary'], inplace=True)
            
            # Filter by dates if specified
            if dates is True:
                papers['published'] = pd.to_datetime(papers['published']).dt.date
                papers = papers[(papers['published'] >= start_date) & (papers['published'] <= end_date)]
                
        except (ValueError, TypeError) as e:
            context = create_error_context(
                module="arxiv",
                function="_filter_papers",
                operation="paper_filtering",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(self.logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="PaperFilteringError",
                error_description=f"Error filtering papers: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            # Continue with unfiltered papers rather than failing completely
        except KeyError as e:
            context = create_error_context(
                module="arxiv",
                function="_filter_papers",
                operation="paper_filtering",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(self.logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="PaperFilteringError",
                error_description=f"Missing required column during paper filtering: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            context = create_error_context(
                module="arxiv",
                function="_filter_papers",
                operation="paper_filtering",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(self.logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="PaperFilteringError",
                error_description=f"Unexpected error during paper filtering: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            # Return papers as-is to prevent complete failure
        
        return papers
    
    def _clean_papers(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize paper data."""
        self.logger.info("Cleaning papers...")
        try:
            # Remove unnecessary columns
            papers = papers.drop(columns=[
                'author', 'comment', 'link', 'primary_category', 'category', 
                'doi', 'journal_ref'
            ], errors='ignore')
            
            # Clean empty values
            papers.replace('', float("NaN"), inplace=True)
            papers.dropna(how='all', axis=1, inplace=True)
            
        except (ValueError, TypeError) as e:
            context = create_error_context(
                module="arxiv",
                function="_clean_papers",
                operation="paper_cleaning",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(self.logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="PaperCleaningError",
                error_description=f"Error cleaning papers: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            # Continue with uncleaned papers rather than failing completely
        except KeyError as e:
            context = create_error_context(
                module="arxiv",
                function="_clean_papers",
                operation="paper_cleaning",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(self.logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="PaperCleaningError",
                error_description=f"Missing required column during paper cleaning: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            context = create_error_context(
                module="arxiv",
                function="_clean_papers",
                operation="paper_cleaning",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.DATA
            )
            
            error_info = get_standard_error_info("data_validation_failed")
            error_handler = ErrorHandler(self.logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="PaperCleaningError",
                error_description=f"Unexpected error during paper cleaning: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            # Return papers as-is to prevent complete failure
        
        return papers

    def _get_abstracts(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Get abstracts for papers."""
        pass