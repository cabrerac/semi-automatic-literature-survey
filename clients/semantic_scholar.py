import time
import pandas as pd
import json
from .apis.generic import Generic
from .base_client import DatabaseClient
from os.path import exists
from util import util
from tqdm import tqdm
import logging
import datetime


class SemanticScholarClient(DatabaseClient):
    """Semantic Scholar client implementation using the DatabaseClient base class."""
    
    def __init__(self):
        super().__init__(
            database_name='semantic_scholar',
            max_papers=100,
            waiting_time=3,
            max_retries=3,
            client_fields={'title': 'title', 'abstract': 'keyword'},
            offset_limit=1000
        )
        
        # Load API access from config
        if exists('./config.json'):
            with open("./config.json", "r") as file:
                config = json.load(file)
            if 'api_access_semantic_scholar' in config:
                self.api_access = config['api_access_semantic_scholar']
            else:
                self.api_access = ''
        else:
            self.api_access = ''
        
        # Define API URLs after API access is loaded
        self.api_url = 'https://api.semanticscholar.org/graph/v1/paper/search/?query=<query>&offset=<offset>&limit=<max_papers>&' \
                      'fields=title,abstract,url,year,venue,externalIds&publicationTypes=JournalArticle,BookSection,Study' \
                      '&sort=citationCount:desc'
        self.citations_url = 'https://api.semanticscholar.org/graph/v1/paper/{paper_id}/citations?fields=title,abstract,url,year,' \
                            'venue&offset=<offset>&limit=<max_papers>'
        self.start = 0
        self.client = Generic()
            
    
    def _has_api_access(self) -> bool:
        """Check if API access is available for Semantic Scholar."""
        return True
    
    def _plan_requests(self, query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date) -> pd.DataFrame:
        """Plan the API requests for Semantic Scholar."""
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
        
        papers = pd.DataFrame()
        requests = self._create_request(parameters, dates, start_date, end_date)
        planned_requests = []
        self.logger.info("Planning queries...")
        for request in tqdm(requests):
            req = self.api_url.replace('<query>', request['query']).replace('<offset>', str(self.start)).replace('<max_papers>', str(self.max_papers))
            headers = {}
            if len(self.api_access) > 0:
                headers = {'x-api-key': self.api_access}
            raw_papers = self._retry_request(self.client.request, req, 'get', {}, headers)
            papers_request, next_paper, total = self._process_raw_papers(query, raw_papers, False)
            if total > 0:
                if total < self.offset_limit:
                    planned_requests.append(request['query'])
                else:
                    que = ''
                    syntactic_filters = parameters['syntactic_filters']
                    if len(syntactic_filters) > 0:
                        for word in syntactic_filters:
                            que = que.replace('<AND>last', '<AND> ')
                            que = que + "'" + word + "' <AND>last"
                        que = que.replace(' <AND>last', '')
                    else:
                        que = parameters['query']
                    parameters_syn = parameters
                    parameters_syn['query'] = que
                    start_d = datetime.datetime(request['initial_year'], 1, 1)
                    end_d = datetime.datetime(request['end_year'], 1, 1)
                    requests_syn = self._create_request(parameters_syn, dates, start_d, end_d)
                    for request_syn in requests_syn:
                        req = self.api_url.replace('<query>', request_syn['query']).replace('<offset>', str(self.start)).replace(
                            '<max_papers>', str(self.max_papers))
                        headers = {}
                        if len(self.api_access) > 0:
                            headers = {'x-api-key': self.api_access}
                        raw_papers = self._retry_request(self.client.request, req, 'get', {}, headers)
                        papers_request, next_paper, total = self._process_raw_papers(query, raw_papers, False)
                        if total > 0:
                            if total < self.offset_limit:
                                planned_requests.append(request_syn['query'])
                            else:
                                planned_requests.append(request['query'])
        papers = self._request_papers(query, planned_requests)
        return papers
    
    def _create_request(self, parameters, dates, start_date, end_date):
        """Create request parameters for Semantic Scholar."""
        queries = []
        queries_temp = self.client.ieeexplore_query(parameters)
        for query_temp in queries_temp:
            query_temp = query_temp.replace('(', '').replace('OR', '+').replace('AND', '+').replace('"', '').replace(')', '') \
                .replace(' ', '+')
            query = query_temp + '<dates>'
            initial_year = start_date.year
            final_year = end_date.year
            while initial_year <= final_year:
                query = query.replace('<dates>', '&year=' + str(initial_year) + '-' + str(initial_year+1))
                queries.append({'query': query, 'initial_year': initial_year, 'end_year': initial_year+1})
                query = query_temp + '<dates>'
                initial_year = initial_year + 1
        return queries
    
    def _request_papers(self, query, requests):
        """Request papers from Semantic Scholar API."""
        papers = pd.DataFrame()
        self.logger.info("There will be " + str(len(requests)) + " different queries to the " + self.database_name + " API...")
        current_request = 0
        for request in tqdm(requests):
            current_request = current_request + 1
            req = self.api_url.replace('<query>', request).replace('<offset>', str(self.start)).replace('<max_papers>', str(self.max_papers))
            headers = {}
            if len(self.api_access) > 0:
                headers = {'x-api-key': self.api_access}
            raw_papers = self._retry_request(self.client.request, req, 'get', {}, headers)
            papers_request, next_paper, total = self._process_raw_papers(query, raw_papers, True)
            if len(papers) == 0:
                papers = papers_request
            else:
                papers = pd.concat([papers, papers_request])
            if total > self.offset_limit:
                self.logger.info("Query " + str(current_request) + "...")
                self.logger.info("The query returns more papers than the " + self.database_name + " limit...")
                self.logger.info("The query returns " + str(total) + " papers, please consider using a more specific query or syntactic filters to reduce the number of papers...")
                self.logger.info("Retrieving the first " + str(self.offset_limit) + " more cited papers instead...")
                total = self.offset_limit
            while next_paper != -1 and next_paper < self.offset_limit:
                time.sleep(self.waiting_time)
                req = self.api_url.replace('<query>', request).replace('<offset>', str(next_paper))
                req = req.replace('<max_papers>', str(self.max_papers))
                raw_papers = self._retry_request(self.client.request, req, 'get', {}, headers)
                papers_request, next_paper, total = self._process_raw_papers(query, raw_papers, True)
                if len(papers) == 0:
                    papers = papers_request
                else:
                    papers = pd.concat([papers, papers_request])
        return papers
    
    def _process_raw_papers(self, query, raw_papers, print_error):
        """Process raw papers from Semantic Scholar API response."""
        query_name = list(query.keys())[0]
        query_value = query[query_name]
        papers_request = pd.DataFrame()
        next_paper = -1
        total = -1
        if raw_papers.status_code == 200:
            try:
                raw_json = json.loads(raw_papers.text)
                if 'next' in raw_json:
                    next_paper = raw_json['next']
                if 'total' in raw_json:
                        total = raw_json['total']
                if total > 0:
                    papers_request = pd.json_normalize(raw_json['data'])
                    papers_request.loc[:, 'database'] = self.database_name
                    papers_request.loc[:, 'query_name'] = query_name
                    papers_request.loc[:, 'query_value'] = query_value.replace('<AND>', 'AND').replace('<OR>', 'OR')
                    if 'abstract' not in papers_request:
                        papers_request = pd.DataFrame()
                else:
                    papers_request = pd.DataFrame()
            except (json.JSONDecodeError, KeyError) as e:
                if print_error:
                    # User-friendly message explaining what's happening
                    self.logger.info("Error parsing the API response. Skipping to next request. Please see the log file for details: " + self.file_handler)
                    # Detailed logging for debugging
                    self.logger.debug(f"Data parsing error in Semantic Scholar response: {type(e).__name__}: {str(e)}")
            except Exception as ex:
                if print_error:
                    # User-friendly message explaining what's happening
                    self.logger.info("Unexpected error parsing the API response. Skipping to next request. Please see the log file for details: " + self.file_handler)
                    # Detailed logging for debugging
                    self.logger.error(f"Unexpected error parsing Semantic Scholar response: {type(ex).__name__}: {str(ex)}")
        else:
            if print_error:
                self.logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                            + self.file_handler)
                self.logger.debug("API response: " + raw_papers.text)
                self.logger.debug("Request: " + raw_papers.request.url)
        return papers_request, next_paper, total
    
    def _filter_papers(self, papers, dates, start_date, end_date):
        """Filter papers based on criteria."""
        self.logger.info("Filtering papers...")
        try:
            papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
            papers = papers.dropna(subset=['title'])
            papers.loc[:, 'title'] = papers['title'].str.lower()
            papers = papers.drop_duplicates('title')
            papers.loc[:, 'abstract'] = papers['abstract'].replace('', float("NaN"))
            papers = papers.dropna(subset=['abstract'])
            if dates:
                papers = papers[(papers['publication_date'] >= start_date.year) & (papers['publication_date'] <= end_date.year)]
        except (ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            self.logger.info("Error filtering papers. Please see the log file for details: " + self.file_handler)
            # Detailed logging for debugging
            self.logger.debug(f"Data type error during Semantic Scholar paper filtering: {type(e).__name__}: {str(e)}")
            # Continue with unfiltered papers rather than failing completely
        except KeyError as e:
            # User-friendly message explaining what's happening
            self.logger.info("Error filtering papers. Please see the log file for details: " + self.file_handler)
            # Detailed logging for debugging
            self.logger.debug(f"Missing required column during Semantic Scholar paper filtering: {type(e).__name__}: {str(e)}")
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            # User-friendly message explaining what's happening
            self.logger.info("Unexpected error filtering papers. Please see the log file for details: " + self.file_handler)
            # Detailed logging for debugging
            self.logger.error(f"Unexpected error during Semantic Scholar paper filtering: {type(ex).__name__}: {str(ex)}")
            # Return papers as-is to prevent complete failure
        return papers
    
    def _clean_papers(self, papers):
        """Clean papers data."""
        self.logger.info("Cleaning papers...")
        try:
            papers = papers.drop(columns=['externalIds.MAG', 'externalIds.DBLP', 'externalIds.PubMedCentral',
                                          'externalIds.PubMed', 'externalIds.ArXiv', 'externalIds.CorpusId',
                                          'externalIds.ACL'], errors='ignore')
            papers.replace('', float("NaN"), inplace=True)
            papers.dropna(how='all', axis=1, inplace=True)
        except (ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            self.logger.info("Error cleaning papers. Please see the log file for details: " + self.file_handler)
            # Detailed logging for debugging
            self.logger.debug(f"Data type error during Semantic Scholar paper cleaning: {type(e).__name__}: {str(e)}")
            # Continue with uncleaned papers rather than failing completely
        except KeyError as e:
            # User-friendly message explaining what's happening
            self.logger.info("Error cleaning papers. Please see the log file for details: " + self.file_handler)
            # Detailed logging for debugging
            self.logger.debug(f"Missing required column during Semantic Scholar paper cleaning: {type(e).__name__}: {str(e)}")
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            # User-friendly message explaining what's happening
            self.logger.info("Unexpected error cleaning papers. Please see the log file for details: " + self.file_handler)
            # Detailed logging for debugging
            self.logger.error(f"Unexpected error during Semantic Scholar paper cleaning: {type(ex).__name__}: {str(ex)}")
            # Return papers as-is to prevent complete failure
        return papers
    
    def get_citations(self, folder_name, search_date, step, dates, start_date, end_date):
        """Get citations for papers."""
        self.logger.info("Retrieving citation papers. It might take a while...")
        papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + '_manually_filtered_by_full_text_papers.csv'
        papers = pd.read_csv(papers_file)
        citations = pd.DataFrame()
        pbar = tqdm(total=len(papers))
        for index, row in papers.iterrows():
            paper_id = 'DOI:' + row['doi']
            if 'http' in paper_id or paper_id == '':
                paper_id = 'URL:' + row['doi']
            if paper_id != '':
                papers_request = self._request_citations(paper_id)
                if len(citations) == 0:
                    citations = papers_request
                else:
                    citations = pd.concat([citations, papers_request])
            pbar.update(1)
        pbar.close()
        if len(citations) > 0:
            citations = self._filter_papers(citations, dates, start_date, end_date)
        if len(citations) > 0:
            citations = self._clean_papers(citations)
        if len(citations) > 0:
            citations.loc[:, 'type'] = 'preprocessed'
            citations.loc[:, 'status'] = 'unknown'
            citations.loc[:, 'id'] = list(range(1, len(citations) + 1))
        self.logger.info("Retrieved papers after filters and cleaning: " + str(len(citations)))
        return citations
    
    def _request_citations(self, paper_id):
        """Request citations for a specific paper."""
        papers = pd.DataFrame()
        next_paper = 0
        while next_paper != -1 and next_paper < self.offset_limit:
            time.sleep(self.waiting_time)
            request = self.citations_url.replace('{paper_id}', str(paper_id))
            request = request.replace('<offset>', str(next_paper)).replace('<max_papers>', str(self.max_papers))
            headers = {}
            if len(self.api_access) > 0:
                headers = {'x-api-key': self.api_access}
            raw_citations = self._retry_request(self.client.request, request, 'get', {}, headers)
            papers_request, next_paper = self._process_raw_citations(raw_citations)
            if len(papers) == 0:
                papers = papers_request
            else:
                papers = pd.concat([papers, papers_request])
        return papers
    
    def _process_raw_citations(self, raw_citations):
        """Process raw citations from API response."""
        next_paper = -1
        papers = pd.DataFrame()
        if raw_citations.status_code == 200:
            try:
                raw_json = json.loads(raw_citations.text)
                if 'next' in raw_json:
                    next_paper = raw_json['next']
                papers = pd.json_normalize(raw_json['data'])
                if len(papers) > 0:
                    papers = papers.rename(columns={"citingPaper.paperId": "doi", "citingPaper.url": "url",
                                                    "citingPaper.title": "title", "citingPaper.abstract": "abstract",
                                                    "citingPaper.venue": "publisher",
                                                    "citingPaper.year": "publication_date"})
                    papers.loc[:, 'database'] = self.database_name
                    papers.loc[:, 'query_name'] = 'citation'
                    papers.loc[:, 'query_value'] = 'citation'
            except (json.JSONDecodeError, KeyError) as e:
                # User-friendly message explaining what's happening
                self.logger.info("Error parsing the API response. Skipping to next request. Please see the log file for details: " + self.file_handler)
                # Detailed logging for debugging
                self.logger.debug(f"Data parsing error in Semantic Scholar citations response: {type(e).__name__}: {str(e)}")
            except Exception as ex:
                # User-friendly message explaining what's happening
                self.logger.info("Unexpected error parsing the API response. Skipping to next request. Please see the log file for details: " + self.file_handler)
                # Detailed logging for debugging
                self.logger.error(f"Unexpected error parsing Semantic Scholar citations response: {type(ex).__name__}: {str(ex)}")
        else:
            self.logger.info("Error requesting the API for citations. Skipping to next request. Please see the log file for "
                        "details: " + self.file_handler)
            self.logger.debug("API response: " + raw_citations.text)
            self.logger.debug("Request: " + raw_citations.request.url)
        return papers, next_paper
    
    def _get_abstracts(self, papers: pd.DataFrame) -> pd.DataFrame:
        """Get abstracts for papers."""
        pass