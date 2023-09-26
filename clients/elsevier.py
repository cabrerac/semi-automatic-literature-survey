import time
import pandas as pd
import json
from os.path import exists
from analysis import util
from .apis.generic import Generic
from bs4 import BeautifulSoup
import logging


database = 'scopus'
client_fields = {'scopus': {'title': 'TITLE-ABS-KEY'}}
f = 'utf-8'
client = Generic()
api_url = 'https://api.elsevier.com/content/<type>/'
api_access = ''
if exists('./config.json'):
    with open("./config.json", "r") as file:
        config = json.load(file)
    api_access = config['api_access_elsevier']

start = 0
max_papers = 25
limit = 5000
waiting_time = 1
max_retries = 3
file_handler = ''
logger = logging.getLogger('logger')


def get_papers(query, synonyms, fields, types, dates, start_date, end_date, folder_name, search_date):
    global logger
    logger = logging.getLogger('logger')
    global  file_handler
    file_handler = logger.handlers[1].baseFilename
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + query_name.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists(file_name):
        c_fields = []
        for field in fields:
            if field in client_fields[database]:
                c_fields.append(client_fields[database][field])
        parameters = {'query': query_value, 'synonyms': synonyms, 'fields': c_fields, 'types': types}
        papers = request_papers(query, parameters, dates, start_date, end_date)
        if len(papers) > 0:
            papers = filter_papers(papers, dates, start_date, end_date)
        if len(papers) > 0:
            papers = clean_papers(papers)
        if len(papers) > 0:
            papers = get_abstracts(papers)
        if len(papers) > 0:
            util.save(file_name, papers, f, 'a')
        logger.info("Retrieved papers after filters and cleaning: " + str(len(papers)))
    else:
        logger.info("File already exists.")


def request_papers(query, parameters, dates, start_date, end_date):
    dates = True
    logger.info("Retrieving papers. It might take a while...")
    papers = pd.DataFrame()
    request = create_request(parameters, dates, start_date.year, end_date.year)
    headers = {'X-ELS-APIKey': api_access}
    raw_papers = client.request(request, 'get', {}, headers)
    expected_papers = get_expected_papers(raw_papers)
    list_years = []
    if expected_papers > limit:
        start_year = end_date.year - 1
        end_year = end_date.year
        total_papers = 0
        while total_papers < expected_papers and start_year >= start_date.year:
            request = create_request(parameters, True, start_year, end_year)
            headers = {'X-ELS-APIKey': api_access}
            raw_papers = client.request(request, 'get', {}, headers)
            expected_papers_request = get_expected_papers(raw_papers)
            if expected_papers_request > 0:
                list_years.append({'start_year': start_year, 'end_year': end_year,
                                   'expected_papers': expected_papers_request})
            end_year = start_year - 1
            start_year = end_year - 1
            total_papers = total_papers + expected_papers_request
    else:
        list_years.append({'start_year': start_date.year, 'end_year': end_date.year,
                           'expected_papers': expected_papers})

    for years in list_years:
        expected_papers = years['expected_papers']
        start_year = years['start_year']
        end_year = years['end_year']
        times = int(expected_papers / max_papers) - 1
        mod = int(expected_papers) % max_papers
        if mod > 0:
            times = times + 1
        for t in range(0, times + 1):
            time.sleep(waiting_time)
            global start
            start = t * max_papers
            request = create_request(parameters, dates, start_year, end_year)
            headers = {'X-ELS-APIKey': api_access}
            raw_papers = client.request(request, 'get', {}, headers)
            # if there is an exception from the API, retry request
            retry = 0
            while raw_papers.status_code != 200 and retry < max_retries:
                time.sleep(waiting_time)
                retry = retry + 1
                headers = {'X-ELS-APIKey': api_access}
                raw_papers = client.request(request, 'get', {}, headers)
            papers_request = process_raw_papers(query, raw_papers)
            if len(papers) == 0:
                papers = papers_request
            else:
                papers = papers.append(papers_request)
    return papers


def create_request(parameters, dates, start_date, end_date):
    request = api_url.replace('<type>', 'search')
    request = request + 'scopus?' + 'start=' + str(start) + '&count=' + str(max_papers)
    query = client.default_query(parameters)
    query = 'LANGUAGE(english) AND (DOCTYPE(ar) OR DOCTYPE(ch) OR DOCTYPE(cp)) AND ' + query
    request = request + '&query=' + query
    request = request.replace('%28', '(').replace('%29', ')')
    request = request.replace(':%22', '(').replace('%22+', ') ').replace('+', ' ')
    request = request.replace('%22', ')')
    if dates:
        request = request + '&date=' + str(start_date) + '-' + str(end_date)
    return request


def get_expected_papers(raw_papers):
    total = 0
    if raw_papers.status_code == 200:
        try:
            json_results = json.loads(raw_papers.text)
            total = int(json_results['search-results']['opensearch:totalResults'])
        except Exception as ex:
            logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                        "details: " + file_handler)
            logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    else:
        logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("API response: " + str(raw_papers.text))
        logger.debug("Request: " + raw_papers.request.url)
    return total


def process_raw_papers(query, raw_papers):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    papers_request = pd.DataFrame()
    if raw_papers.status_code == 200:
        try:
            json_results = json.loads(raw_papers.text)
            raw_papers = pd.json_normalize(json_results['search-results']['entry'])
            papers_request['id'] = raw_papers['prism:doi']
            if 'pii' in raw_papers:
                papers_request['pii'] = raw_papers['pii']
            else:
                papers_request['pii'] = ''
            papers_request['url'] = raw_papers['link']
            papers_request['type'] = raw_papers['subtypeDescription']
            papers_request['publication'] = raw_papers['dc:identifier']
            papers_request['publisher'] = database
            papers_request['publication_date'] = raw_papers['prism:coverDate']
            papers_request['database'] = database
            papers_request['query_name'] = query_name
            papers_request['query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
            papers_request['title'] = raw_papers['dc:title']
        except Exception as ex:
            logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                        "details: " + file_handler)
            logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    else:
        logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("API response: " + raw_papers.text)
        logger.debug("Request: " + raw_papers.request.url)
    return papers_request


def get_abstracts(papers):
    logger.info("Retrieving abstracts from Scopus. It might take a while...")
    try:
        links = []
        abstracts = []
        for index, paper in papers.iterrows():
            urls = paper['url']
            link = ''
            for record in urls:
                if record['@ref'] == database:
                    link = record['@href']
                    break
            links.append(link)
            abstract = get_abstract(paper)
            abstracts.append(abstract)
        papers['url'] = links
        papers['abstract'] = abstracts
    except Exception as ex:
        logger.info("Error getting abstract. Skipping to next request. Please see the log file for "
                    "details: " + file_handler)
        logger.debug("Error getting abstract: " + str(type(ex)) + ' - ' + str(ex))
    return papers


def get_abstract(paper):
    abstract = ''
    if 'pii' in paper:
        pii = str(paper['pii'])
        if pii != 'nan':
            req = api_url.replace('<type>', 'article') + 'pii/' + pii + '?apiKey=' + api_access
            result = client.request(req, 'get', {}, {})
            abstract = parse_abstract(result, 'json')
    if abstract == '':
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                     'Chrome/58.0.3029.110 Safari/537.36'}
            urls = paper['url']
            scopus_url = ''
            for url in urls:
                if url['@ref'] == database:
                    scopus_url = url['@href']
                    break
            if len(scopus_url) > 0:
                result = client.request(scopus_url, 'get', {}, headers)
                abstract = parse_abstract(result, 'html')
        except Exception as ex:
            abstract = ''
    time.sleep(waiting_time)
    return abstract


def parse_abstract(result, option):
    abstract = ''
    if result.status_code == 200 and option == 'json':
        try:
            json_result = json.loads(result.text)
            if 'full-text-retrieval-response' in json_result:
                if 'coredata' in json_result['full-text-retrieval-response']:
                    if 'dc:description' in json_result['full-text-retrieval-response']['coredata']:
                        abstract = json_result['full-text-retrieval-response']['coredata']['dc:description']
        except:
            abstract = ''
    if result.status_code == 200 and option == 'html':
        try:
            soup = BeautifulSoup(result.text, 'html.parser')
            abstract_section = soup.find('section', {'id': 'abstractSection', 'class': 'row'})
            if abstract_section:
                # Print the abstract section's content
                abstract = abstract_section.get_text()
        except:
            abstract = ''
    return abstract


def filter_papers(papers, dates, start_date, end_date):
    logger.info("Filtering papers...")
    try:
        papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
        papers = papers.dropna(subset=['title'])
        papers.loc[:, 'title'] = papers['title'].str.lower()
        papers = papers.drop_duplicates('title')
        if dates is True:
            papers.loc[:, 'publication_date'] = pd.to_datetime(papers['publication_date']).dt.date
            papers = papers[(papers['publication_date'] >= start_date) & (papers['publication_date'] <= end_date)]
    except Exception as ex:
        logger.info("Error filtering papers. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers


def clean_papers(papers):
    logger.info("Cleaning papers...")
    try:
        papers.replace('', float("NaN"), inplace=True)
        papers.dropna(how='all', axis=1, inplace=True)
    except Exception as ex:
        logger.info("Error cleaning papers. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers
