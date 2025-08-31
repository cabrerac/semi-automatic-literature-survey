import time
import pandas as pd
import json
from os.path import exists
from util import util
from .apis.generic import Generic
from bs4 import BeautifulSoup
from .base_client import DatabaseClient
import logging
import random
from tqdm import tqdm
import os

class ElsevierClient(DatabaseClient):
    """Elsevier/Scopus client implementation using the DatabaseClient base class."""
    
    def __init__(self):
        super().__init__(
            database_name='scopus',
            max_papers=25,
            waiting_time=1,
            max_retries=3,
            client_fields={'scopus': {'title': 'TITLE-ABS-KEY'}},
            quota=2000
        )
        
        # Load API access from config
        if exists('./config.json'):
            with open("./config.json", "r") as file:
                config = json.load(file)
            if 'api_access_elsevier' in config:
                self.api_access = config['api_access_elsevier']
            else:
                self.api_access = ''
        else:
            self.api_access = ''
        
        # Define client-specific fields and API URL after API access is loaded
        self.client_fields = {'scopus': {'title': 'TITLE-ABS-KEY'}}
        self.api_url = 'https://api.elsevier.com/content/<type>/'
        self.start = 0
        self.client = Generic()

    def _has_api_access(self) -> bool:
        """Check if Elsevier API access is available."""
        return self.api_access != ''

    def _plan_requests(self, query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date) -> pd.DataFrame:
        """Plan the API requests for Elsevier/Scopus."""
        # Extract query value from the query dictionary
        query_name = list(query.keys())[0]
        query_value = query[query_name]
        
        # Build query parameters
        c_fields = []
        for field in fields:
            if field in self.client_fields['scopus']:
                c_fields.append(self.client_fields['scopus'][field])
        
        parameters = {
            'query': query_value,
            'syntactic_filters': syntactic_filters,
            'synonyms': synonyms,
            'fields': c_fields,
            'types': types
        }
        
        # Create initial requests to get total count
        end_date = end_date.replace(year=end_date.year + 1)
        self.logger.info("Planning requests...")
        papers = pd.DataFrame()
        start_year = end_date.year - 1
        end_year = end_date.year
        expected_papers = 0
        list_years = []
        total_requests = 0
        
        while start_year >= start_date.year:
            request = self._create_request(query, parameters, True, start_year, end_year)
            headers = {'X-ELS-APIKey': self.api_access}
            raw_papers = self._retry_request(self.client.request, request, 'get', {}, headers)
            total_requests = total_requests + 1
            expected_papers_request = self._get_expected_papers(raw_papers)
            if expected_papers_request > 0:
                list_years.append({'start_year': start_year, 'end_year': end_year,
                                   'expected_papers': expected_papers_request})
            end_year = end_year - 1
            start_year = start_year - 1
            expected_papers = expected_papers + expected_papers_request
        
        times = int(expected_papers / self.max_papers) - 1
        mod = int(expected_papers) % self.max_papers
        if mod > 0:
            times = times + 1
            
        # Check quota constraints
        if times < self.quota:
            papers = self._execute_requests(query, parameters, list_years)
        else:
            self.logger.info(f"The number of expected papers requires {times + total_requests} requests which exceeds the {self.database_name} quota of {self.quota} requests per day.")
            if len(parameters['syntactic_filters']) > 0:
                self.logger.info("Trying to reduce the number of requests using syntactic filters.")
                que = ''
                syntactic_filters = parameters['syntactic_filters']
                for word in syntactic_filters:
                    que = que.replace('<AND>last', '<AND> ')
                    que = que + "'" + word + "' <AND>last"
                que = que.replace(' <AND>last', '')
                parameters['query'] = que
                start_year = end_date.year - 1
                end_year = end_date.year
                expected_papers = 0
                list_years = []
                total_requests = 0
                while start_year >= start_date.year:
                    request = self._create_request(query, parameters, True, start_year, end_year)
                    headers = {'X-ELS-APIKey': self.api_access}
                    raw_papers = self._retry_request(self.client.request, request, 'get', {}, headers)
                    total_requests = total_requests + 1
                    expected_papers_request = self._get_expected_papers(raw_papers)
                    if expected_papers_request > 0:
                        list_years.append({'start_year': start_year, 'end_year': end_year,
                                           'expected_papers': expected_papers_request})
                    end_year = end_year - 1
                    start_year = start_year - 1
                    expected_papers = expected_papers + expected_papers_request
                self.logger.info(f"Expected papers from {self.database_name} using syntactic filters: {expected_papers}...")
                times = int(expected_papers / self.max_papers) - 1
                mod = int(expected_papers) % self.max_papers
                if mod > 0:
                    times = times + 1
                if times < self.quota:
                    papers = self._execute_requests(query, parameters, list_years)
                else:
                    self.logger.info(f"The number of expected papers requires {times + total_requests} requests which exceeds the {self.database_name} quota of {self.quota} requests per day.")
                    self.logger.info("Skipping to next repository. Try to redefine your search queries and syntactic filters. Using dates to limit your search can help in case you are not.")
            else:
                self.logger.info("Skipping to next repository. Please use syntactic filters to avoid this problem. Using dates to limit your search can help in case you are not.")
        
        return papers

    def _execute_requests(self, query, parameters, list_years):
        """Execute the planned requests to retrieve papers."""
        current_request = 0
        papers = pd.DataFrame()
        self.logger.info(f"There will be {len(list_years)} different queries to the {self.database_name} API...")
        
        for years in tqdm(list_years):
            current_request = current_request + 1
            expected_papers = years['expected_papers']
            start_year = years['start_year']
            end_year = years['end_year']
            times = int(expected_papers / self.max_papers) - 1
            mod = int(expected_papers) % self.max_papers
            if mod > 0:
                times = times + 1
                
            for t in range(0, times + 1):
                time.sleep(self.waiting_time)
                self.start = t * self.max_papers
                request = self._create_request(query, parameters, True, start_year, end_year)
                headers = {'X-ELS-APIKey': self.api_access}
                raw_papers = self._retry_request(self.client.request, request, 'get', {}, headers)
                
                if raw_papers is None:
                    continue
                    
                papers_request = self._process_raw_papers(query, raw_papers)
                
                if len(papers) == 0:
                    papers = papers_request
                else:
                    papers = pd.concat([papers, papers_request])
        
        return papers

    def _create_request(self, query, parameters, dates, start_date, end_date):
        """Create the API request URL for Elsevier/Scopus."""
        request = self.api_url.replace('<type>', 'search')
        request = request + 'scopus?' + 'start=' + str(self.start) + '&count=' + str(self.max_papers)
        query_str = self.client.default_query(parameters)
        query_str = 'LANGUAGE(english) AND (DOCTYPE(ar) OR DOCTYPE(ch) OR DOCTYPE(cp)) AND ' + query_str
        request = request + '&query=' + query_str
        request = request.replace('%28', '(').replace('%29', ')')
        request = request.replace(':%22', '(').replace('%22+', ') ').replace('+', ' ')
        request = request.replace('%22', ')')
        if dates:
            request = request + '&date=' + str(start_date) + '-' + str(end_date)
        return request

    def _get_expected_papers(self, raw_papers):
        """Get the expected number of papers from the API response."""
        total = 0
        if raw_papers.status_code == 200:
            try:
                json_results = json.loads(raw_papers.text)
                total = int(json_results['search-results']['opensearch:totalResults'])
            except Exception as ex:
                self.logger.info("Error parsing the API response. Skipping to next request. Please see the log file for details: " + self.file_handler)
                self.logger.debug(f"Exception: {type(ex)} - {str(ex)}")
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
                raw_papers_data = pd.json_normalize(json_results['search-results']['entry'])
                # DOI
                papers_request['id'] = raw_papers_data.get('prism:doi', '')
                # EID / SCOPUS_ID
                if 'eid' in raw_papers_data:
                    papers_request['scopus_id'] = raw_papers_data['eid']
                elif 'dc:identifier' in raw_papers_data:
                    # Sometimes provided as 'SCOPUS_ID:XXXXXXXX'
                    def _extract_scopus_id(val):
                        """Extract Scopus ID from identifier string."""
                        try:
                            if isinstance(val, str) and 'SCOPUS_ID:' in val:
                                return val.split('SCOPUS_ID:')[-1]
                            return ''
                        except (AttributeError, IndexError) as e:
                            # Log specific error for debugging
                            self.logger.debug(f"Error extracting Scopus ID from '{val}': {type(e).__name__}: {str(e)}")
                            return ''
                        except Exception as e:
                            # Log unexpected errors
                            self.logger.warning(f"Unexpected error extracting Scopus ID from '{val}': {type(e).__name__}: {str(e)}")
                            return ''
                    papers_request['scopus_id'] = raw_papers_data['dc:identifier'].apply(_extract_scopus_id)
                else:
                    papers_request['scopus_id'] = ''
                if 'pii' in raw_papers_data:
                    papers_request['pii'] = raw_papers_data['pii']
                else:
                    papers_request['pii'] = ''
                papers_request['url'] = raw_papers_data['link']
                papers_request['type'] = raw_papers_data['subtypeDescription']
                papers_request['publication'] = raw_papers_data['dc:identifier']
                papers_request['publisher'] = self.database_name
                papers_request['publication_date'] = raw_papers_data['prism:coverDate']
                papers_request['database'] = self.database_name
                papers_request['query_name'] = query_name
                papers_request['query_value'] = query_value.replace('<AND>', 'AND').replace('<OR>', 'OR')
                papers_request['title'] = raw_papers_data['dc:title']
            except Exception as ex:
                self.logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                                "details: " + self.file_handler)
                self.logger.debug(f"Exception: {type(ex)} - {str(ex)}")
        else:
            self._log_api_error(raw_papers, raw_papers.request.url if raw_papers.request else "")
        
        return papers_request

    def _filter_papers(self, papers, dates, start_date, end_date):
        """Filter papers based on criteria."""
        self.logger.info("Filtering papers...")
        try:
            papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
            papers = papers.dropna(subset=['title'])
            papers.loc[:, 'title'] = papers['title'].str.lower()
            papers = papers.drop_duplicates('title')
            if dates is True:
                papers.loc[:, 'publication_date'] = pd.to_datetime(papers['publication_date']).dt.date
                papers = papers[(papers['publication_date'] >= start_date) & (papers['publication_date'] <= end_date)]
        except (ValueError, TypeError) as e:
            # Handle data type conversion errors (e.g., non-string titles, invalid dates)
            self.logger.warning(f"Data type error during paper filtering: {type(e).__name__}: {str(e)}")
            # Continue with unfiltered papers rather than failing completely
        except KeyError as e:
            # Handle missing column errors
            self.logger.error(f"Missing required column during paper filtering: {type(e).__name__}: {str(e)}")
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            # Handle unexpected errors
            self.logger.error(f"Unexpected error during paper filtering: {type(ex).__name__}: {str(ex)}")
            # Return papers as-is to prevent complete failure
        return papers

    def _clean_papers(self, papers):
        """Clean and standardize paper data."""
        self.logger.info("Cleaning papers...")
        try:
            papers.replace('', float("NaN"), inplace=True)
            papers.dropna(how='all', axis=1, inplace=True)
        except (ValueError, TypeError) as e:
            # Handle data type conversion errors
            self.logger.warning(f"Data type error during paper cleaning: {type(e).__name__}: {str(e)}")
            # Continue with uncleaned papers rather than failing completely
        except KeyError as e:
            # Handle missing column errors
            self.logger.error(f"Missing required column during paper cleaning: {type(e).__name__}: {str(e)}")
            # Return papers as-is to prevent complete failure
        except Exception as ex:
            # Handle unexpected errors
            self.logger.error(f"Unexpected error during paper cleaning: {type(ex).__name__}: {str(ex)}")
            # Return papers as-is to prevent complete failure
        return papers

    def _get_abstracts(self, papers):
        """Retrieve abstracts for papers from Scopus."""
        self.logger.info("Retrieving abstracts for " + str(len(papers)) + " papers from Scopus. It might take a while...")
        links = []
        abstracts = []
        pbar = tqdm(total=len(papers))
        for index, paper in papers.iterrows():
            try:
                urls = paper['url']
                link = ''
                for record in urls:
                    if record['@ref'] == self.database_name:
                        link = record['@href']
                        break
                links.append(link)
                abstract = self._get_abstract(paper)
                abstracts.append(abstract)
            except (KeyError, AttributeError) as e:
                # Handle missing field or attribute errors
                self.logger.warning(f"Missing field error getting abstract: {type(e).__name__}: {str(e)}")
                abstracts.append('')
            except (ValueError, TypeError) as e:
                # Handle data type conversion errors
                self.logger.warning(f"Data type error getting abstract: {type(e).__name__}: {str(e)}")
                abstracts.append('')
            except Exception as ex:
                # Handle unexpected errors
                self.logger.error(f"Unexpected error getting abstract: {type(ex).__name__}: {str(ex)}")
                abstracts.append('')
            pbar.update(1)
        pbar.close()
        papers['url'] = links
        papers['abstract'] = abstracts
        return papers

    def _get_abstract(self, paper):
        """Get abstract for a specific paper."""
        abstract = ''
        doi = ''
        scopus_id = ''
        pii = ''
        try:
            if 'id' in paper and str(paper['id']) != 'nan':
                doi = str(paper['id'])
            if 'scopus_id' in paper and str(paper['scopus_id']) != 'nan':
                scopus_id = str(paper['scopus_id'])
            if 'pii' in paper and str(paper['pii']) != 'nan':
                pii = str(paper['pii'])
        except (KeyError, AttributeError) as e:
            # Handle missing field or attribute errors
            self.logger.debug(f"Missing field during abstract retrieval: {type(e).__name__}: {str(e)}")
        except (ValueError, TypeError) as e:
            # Handle data type conversion errors
            self.logger.debug(f"Data type error during abstract retrieval: {type(e).__name__}: {str(e)}")
        except Exception as e:
            # Handle unexpected errors
            self.logger.warning(f"Unexpected error during abstract retrieval: {type(e).__name__}: {str(e)}")

        # 1) Semantic Scholar by DOI
        abstract = self._get_abstract_via_semantic_scholar(doi)
        if abstract:
            return abstract
        
        # 2) Elsevier Abstract Retrieval API by DOI or SCOPUS_ID
        abstract = self._get_abstract_via_elsevier(doi=doi, scopus_id=scopus_id, pii=pii)
        if abstract:
            return abstract

        # 3) Final fallback: attempt legacy HTML parsing (may fail due to dynamic rendering)
        if abstract == '':
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
                }
                urls = paper['url']
                scopus_url = ''
                for url in urls:
                    if url['@ref'] == self.database_name:
                        scopus_url = url['@href']
                        break
                if len(scopus_url) > 0:
                    result = self.client.request(scopus_url, 'get', {}, headers)
                    abstract = self._parse_abstract(result, 'html')
            except (KeyError, AttributeError) as e:
                # Handle missing field or attribute errors
                self.logger.debug(f"Missing field during HTML parsing: {type(e).__name__}: {str(e)}")
                abstract = ''
            except (ValueError, TypeError) as e:
                # Handle data type conversion errors
                self.logger.debug(f"Data type error during HTML parsing: {type(e).__name__}: {str(e)}")
                abstract = ''
            except Exception as e:
                # Handle unexpected errors
                self.logger.warning(f"Unexpected error during HTML parsing: {type(e).__name__}: {str(e)}")
                abstract = ''

        time.sleep(self.waiting_time + random.random())
        return abstract

    def _get_abstract_via_elsevier(self, doi: str, scopus_id: str, pii: str) -> str:
        """Try Elsevier's APIs to retrieve an abstract using DOI, SCOPUS_ID or PII."""
        if not self._has_api_access():
            return ''

        headers = {
            'X-ELS-APIKey': self.api_access,
            'Accept': 'application/json'
        }

        # Order: DOI → SCOPUS_ID → PII
        endpoints = []
        if doi:
            endpoints.append(self.api_url.replace('<type>', 'abstract') + f'doi/{doi}?view=FULL')
        if scopus_id:
            # Scopus abstracts retrieval expects scopus_id path
            endpoints.append(self.api_url.replace('<type>', 'abstract') + f'scopus_id/{scopus_id}?view=FULL')
        if pii:
            # Article endpoint by PII sometimes contains coredata description
            endpoints.append(self.api_url.replace('<type>', 'article') + f'pii/{pii}')

        for url in endpoints:
            try:
                resp = self._retry_request(self.client.request, url, 'get', {}, headers)
                if resp is None or resp.status_code != 200:
                    continue
                abstract = self._parse_abstract(resp, 'json')
                if abstract:
                    return abstract
            except (KeyError, AttributeError) as e:
                # Handle missing field or attribute errors
                self.logger.debug(f"Missing field during Elsevier API call: {type(e).__name__}: {str(e)}")
                continue
            except (ValueError, TypeError) as e:
                # Handle data type conversion errors
                self.logger.debug(f"Data type error during Elsevier API call: {type(e).__name__}: {str(e)}")
                continue
            except Exception as e:
                # Handle unexpected errors
                self.logger.warning(f"Unexpected error during Elsevier API call: {type(e).__name__}: {str(e)}")
                continue
        return ''

    def _get_abstract_via_semantic_scholar(self, doi: str) -> str:
        """Fallback to Semantic Scholar Graph API by DOI."""
        if not doi:
            return ''
        try:
            url = f'https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=abstract'
            resp = self._retry_request(self.client.request, url, 'get', {}, {})
            if resp is not None and resp.status_code == 200:
                data = json.loads(resp.text)
                if 'abstract' in data and isinstance(data['abstract'], str):
                    return data['abstract'] or ''
        except (KeyError, AttributeError) as e:
            # Handle missing field or attribute errors
            self.logger.debug(f"Missing field during Semantic Scholar API call: {type(e).__name__}: {str(e)}")
        except (ValueError, TypeError) as e:
            # Handle data type conversion errors
            self.logger.debug(f"Data type error during Semantic Scholar API call: {type(e).__name__}: {str(e)}")
        except Exception as e:
            # Handle unexpected errors
            self.logger.warning(f"Unexpected error during Semantic Scholar API call: {type(e).__name__}: {str(e)}")
        return ''

    def _parse_abstract(self, result, option):
        """Parse abstract from API response."""
        abstract = ''
        if result.status_code == 200 and option == 'json':
            try:
                json_result = json.loads(result.text)
                # Elsevier Article API
                if 'full-text-retrieval-response' in json_result:
                    core = json_result['full-text-retrieval-response'].get('coredata', {})
                    abstract = core.get('dc:description', '') or abstract
                    if abstract:
                        return abstract
                # Elsevier Abstract Retrieval API
                if 'abstracts-retrieval-response' in json_result:
                    coredata = json_result['abstracts-retrieval-response'].get('coredata', {})
                    abstract = coredata.get('dc:description', '') or abstract
                    if abstract:
                        return abstract
                    # Some records keep abstract text deeper in bibrecord
                    item = json_result['abstracts-retrieval-response'].get('item', {})
                    bib = item.get('bibrecord', {}).get('head', {}).get('abstracts', {})
                    # bib may be a dict with 'abstract' which can be list/dict
                    abstr = bib.get('abstract') if isinstance(bib, dict) else None
                    if isinstance(abstr, dict):
                        abstract = abstr.get('abstractText', '') or ''
                    elif isinstance(abstr, list) and len(abstr) > 0:
                        # pick first
                        part = abstr[0]
                        if isinstance(part, dict):
                            abstract = part.get('abstractText', '') or ''
                # Generic top-level fallbacks
                if not abstract and isinstance(json_result, dict):
                    abstract = json_result.get('abstract', '') or ''
            except (KeyError, AttributeError) as e:
                # Handle missing field or attribute errors
                self.logger.debug(f"Missing field during JSON parsing: {type(e).__name__}: {str(e)}")
                abstract = ''
            except (ValueError, TypeError) as e:
                # Handle data type conversion errors
                self.logger.debug(f"Data type error during JSON parsing: {type(e).__name__}: {str(e)}")
                abstract = ''
            except Exception as e:
                # Handle unexpected errors
                self.logger.warning(f"Unexpected error during JSON parsing: {type(e).__name__}: {str(e)}")
                abstract = ''
        
        if result.status_code == 200 and option == 'html':
            try:
                soup = BeautifulSoup(result.text, 'html.parser')
                abstract_section = soup.find('section', {'id': 'abstractSection', 'class': 'row'})
                if abstract_section:
                    abstract = abstract_section.get_text()
            except (KeyError, AttributeError) as e:
                # Handle missing field or attribute errors
                self.logger.debug(f"Missing field during HTML parsing: {type(e).__name__}: {str(e)}")
                abstract = ''
            except (ValueError, TypeError) as e:
                # Handle data type conversion errors
                self.logger.debug(f"Data type error during HTML parsing: {type(e).__name__}: {str(e)}")
                abstract = ''
            except Exception as e:
                # Handle unexpected errors
                self.logger.warning(f"Unexpected error during HTML parsing: {type(e).__name__}: {str(e)}")
                abstract = ''
        
        return abstract
