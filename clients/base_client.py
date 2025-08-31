from abc import ABC, abstractmethod
import pandas as pd
import logging
import time
from tqdm import tqdm
from os.path import exists
from util import util


class DatabaseClient(ABC):
    """
    Abstract base class for database clients using the Template Method pattern.
    
    This class defines the workflow for retrieving papers from any database:
    1. Check if file exists
    2. Plan requests
    3. Execute requests
    4. Filter papers
    5. Clean papers
    6. Save results
    """
    
    def __init__(self, database_name: str, max_papers: int = 1000, waiting_time: int = 2, max_retries: int = 3, 
                 client_fields: dict = None, offset_limit: int = None, quota: int = None):
        self.database_name = database_name
        self.max_papers = max_papers
        self.waiting_time = waiting_time
        self.max_retries = max_retries
        self.client_fields = client_fields or {}
        self.offset_limit = offset_limit
        self.quota = quota
        self.logger = logging.getLogger('logger')
        self.file_handler = ''
        
    def get_papers(self, query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date, folder_name, search_date):
        """
        Template method that defines the paper retrieval workflow.
        """
        # Set up file handler for logging
        if len(self.logger.handlers) > 1:
            self.file_handler = self.logger.handlers[1].baseFilename
            
        query_name = list(query.keys())[0]
        query_value = query[query_name]
        
        # Generate file name for this query and database
        file_name = self._generate_file_name(folder_name, search_date, query_name)
        
        # Check if file already exists
        if exists(file_name):
            self.logger.info("File already exists.")
            return
            
        # Check if API access is available
        if not self._has_api_access():
            self.logger.info("API key access not provided. Skipping this client...")
            return
            
        # Execute the paper retrieval workflow
        try:
            # Step 1: Plan requests
            self.logger.info("Retrieving papers. It might take a while...")
            papers = self._plan_requests(query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date)
            
            if len(papers) > 0:
                # Step 2: Filter papers
                papers = self._filter_papers(papers, dates, start_date, end_date)
                
            if len(papers) > 0:
                # Step 3: Clean papers
                papers = self._clean_papers(papers)

            if self.database_name == 'scopus':
                # If the database is Scopus, get abstracts
                papers = self._get_abstracts(papers)
                
            if len(papers) > 0:
                # Step 4: Save papers
                util.save(file_name, papers, 'utf-8', 'a')
                
            self.logger.info(f"Retrieved papers after filters and cleaning: {len(papers)}")
            return file_name
            
        except (ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            self.logger.info("Error in paper retrieval workflow. Please see the log file for details: " + self.file_handler)
            # Detailed logging for debugging
            self.logger.debug(f"Data type error in paper retrieval workflow: {type(e).__name__}: {str(e)}")
        except Exception as ex:
            # User-friendly message explaining what's happening
            self.logger.info("Unexpected error in paper retrieval workflow. Please see the log file for details: " + self.file_handler)
            # Detailed logging for debugging
            self.logger.error(f"Unexpected error in paper retrieval workflow: {type(ex).__name__}: {str(ex)}")
    
    def _generate_file_name(self, folder_name, search_date, query_name):
        """Generate the file name for saving papers."""
        return f'./papers/{folder_name}/{str(search_date).replace("-", "_")}/raw_papers/{query_name.lower().replace(" ", "_")}_{self.database_name}.csv'
    
    @abstractmethod
    def _has_api_access(self) -> bool:
        """Check if API access is available for this database."""
        pass
    
    @abstractmethod
    def _plan_requests(self, query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date) -> pd.DataFrame:
        """Plan the API requests based on the query and parameters."""
        pass
    
    @abstractmethod
    def _filter_papers(self, papers: pd.DataFrame, dates, start_date, end_date) -> pd.DataFrame:
        """Filter papers based on criteria like dates, duplicates, etc."""
        pass
    
    @abstractmethod
    def _clean_papers(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize paper data."""
        pass
    
    @abstractmethod
    def _get_abstracts(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Get abstracts for papers."""
        pass

    def _retry_request(self, request_func, *args, **kwargs):
        """Common retry mechanism for API requests."""
        retry = 0
        while retry < self.max_retries:
            try:
                result = request_func(*args, **kwargs)
                if self._is_successful_response(result):
                    return result
            except (ValueError, TypeError) as e:
                self.logger.debug(f"Request failed due to data type error (attempt {retry + 1}): {type(e).__name__}: {str(e)}")
            except Exception as ex:
                self.logger.debug(f"Request failed due to unexpected error (attempt {retry + 1}): {type(ex).__name__}: {str(ex)}")
            
            retry += 1
            if retry < self.max_retries:
                delay = util.exponential_backoff(retry, self.waiting_time, 64)
                time.sleep(delay)
        if result is not None and result.status_code == 404:
            return result
        if result is not None and result.status_code == 429:
            result = self._retry_request(request_func, *args, **kwargs)
            return result
        if result is None:
            result = {
                "status": "error",
                "status_code": 999,
                "message": "There was an error processing your request. Please try again later or contact support if the issue persists.",
                "attempts": retry,
                "max_retries": self.max_retries,
                "database": self.database_name
            }
            return result
        return result
    
    def _is_successful_response(self, response) -> bool:
        """Check if the API response is successful."""
        if hasattr(response, 'status_code'):
            return response.status_code == 200
        return True  # Default to True for responses without status codes
    
    def _log_api_error(self, response, request_info=""):
        """Log API errors consistently across all clients."""
        self.logger.info(f"Error requesting the API. Skipping to next request. Please see the log file for details: {self.file_handler}")
        if hasattr(response, 'text'):
            self.logger.debug(f"API response: {response.text}")
        if hasattr(response, 'request') and response.request is not None:
            self.logger.debug(f"Request: {request_info}")
