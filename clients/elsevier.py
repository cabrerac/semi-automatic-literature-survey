import time
from elsapy.elsclient import ElsClient
import config as config
import pandas as pd
import json
from elsapy.elssearch import ElsSearch
from os.path import exists
from analysis import util
from .apis.generic import Generic
from bs4 import BeautifulSoup
import logging


database = 'scopus'
client_fields = {'scopus': {'title': 'TITLE-ABS-KEY'}}
f = 'utf-8'
clientG = Generic()
api_url = 'https://api.elsevier.com/content/<type>/'
api_access = config.api_access_elsevier
client = ElsClient(api_access)
waiting_time = 2
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
        papers = request_papers(parameters)
        papers = process_raw_papers(query, papers)
        if len(papers) > 0:
            papers = filter_papers(papers, dates, start_date, end_date)
        if len(papers) > 0:
            papers = clean_papers(papers)
        if len(papers) > 0:
            util.save(file_name, papers, f, 'a')
        logger.info("Retrieved papers after filters and cleaning: " + str(len(papers)))
    else:
        logger.info("File already exists.")


def request_papers(parameters):
    logger.info("Retrieving papers. It might take a while...")
    papers = pd.DataFrame()
    request = create_request(parameters)
    try:
        doc_srch = ElsSearch(request, database)
        doc_srch.execute(client, get_all=True)
        total = len(doc_srch.results)
        if total > 0:
            papers = doc_srch.results_df
    except Exception as ex:
        logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Error requesting the API: " + str(type(ex)) + ' - ' + str(ex))
        logger.debug("Request: " + request)
        papers = pd.DataFrame()
    return papers


def create_request(parameters):
    request = clientG.default_query(parameters)
    request = request.replace('%28', '(').replace('%29', ')')
    request = request.replace(':%22', '(').replace('%22+', ') ').replace('+', ' ')
    request = request.replace('%22', ')')
    return request


def process_raw_papers(query, raw_papers):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    papers_request = pd.DataFrame()
    try:
        papers_request = pd.DataFrame(columns=['id', 'type', 'publication', 'publisher', 'publication_date', 'database',
                                               'title', 'url', 'abstract', 'query_name', 'query_value'])
        papers_request['id'] = raw_papers['prism:doi']
        papers_request['type'] = raw_papers['prism:aggregationType']
        papers_request['publication'] = raw_papers['prism:publicationName']
        papers_request['publisher'] = database
        papers_request['publication_date'] = raw_papers['prism:coverDate']
        papers_request['database'] = database
        papers_request['query_name'] = query_name
        papers_request['query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
        papers_request['title'] = raw_papers['dc:title']
        links = []
        abstracts = []
        for index, paper in raw_papers.iterrows():
            link = paper['link']
            if 'scopus' in link:
                link = link['scopus']
            elif 'self' in link:
                link = link['self']
            links.append(link)
            abstract = get_abstract(paper)
            abstracts.append(abstract)
        papers_request['url'] = links
        papers_request['abstract'] = abstracts
    except Exception as ex:
        logger.info("Error processing raw papers. Skipping to next request. Please see the log file for "
                    "details: " + file_handler)
        logger.debug("Error processing raw papers: " + str(type(ex)) + ' - ' + str(ex))
    return papers_request


def get_abstract(paper):
    abstract = ''
    if 'pii' in paper:
        pii = str(paper['pii'])
        if pii != 'nan':
            req = api_url.replace('<type>', 'article') + 'pii/' + pii + '?apiKey=' + api_access
            result = clientG.request(req, 'get', {}, '')
            abstract = parse_abstract(result, 'json')
    if abstract == '':
        try:
            scopus_url = paper.link['scopus']
            result = clientG.request(scopus_url, 'get', {}, '')
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
        papers['title'].replace('', float("NaN"), inplace=True)
        papers.dropna(subset=['title'], inplace=True)
        papers['title'] = papers['title'].str.lower()
        papers = papers.drop_duplicates('title')
        papers['abstract'].replace('', float("NaN"), inplace=True)
        papers.dropna(subset=['abstract'], inplace=True)
        if dates is True:
            papers['publication_date'] = pd.to_datetime(papers['publication_date']).dt.date
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
