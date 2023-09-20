from elsapy.elsclient import ElsClient
import config as config
import numpy as np
import pandas as pd
from elsapy.elssearch import ElsSearch
import json
from os.path import exists
from analysis import util
from .apis.generic import Generic
import logging

database = 'scopus'
client_fields = {'scopus': {'title': 'TITLE-ABS-KEY'}}
f = 'utf-8'
clientG = Generic()
api_url = 'https://api.elsevier.com/content/<type>/'
api_access = config.api_access_elsevier
client = ElsClient(api_access)
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
                + query_name.lower().replace(' ', '_') + '_sciencedirect.csv'
    if not exists(file_name):
        c_fields = []
        for field in fields:
            if field in client_fields[database]:
                c_fields.append(client_fields[database][field])
        parameters = {'query': query_value, 'synonyms': synonyms, 'fields': c_fields, 'types': types}
        papers = request_papers(parameters)
        papers = process_raw_papers(query, papers)
        papers = filter_papers(papers, dates, start_date, end_date)
        papers = clean_papers(papers)
        if len(papers) > 0:
            util.save(file_name, papers, f)
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
    except:
        logger.info("Error when requesting the API. Skipping to next request. Please see the log file for details: " + file_handler)
        logger.debug("Error when requesting the API: ")
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
        pii = str(paper['pii'])
        doi = str(paper['prism:doi'])
        abstract = get_abstract(pii, doi)
        abstracts.append(abstract)
    papers_request['url'] = links
    papers_request['abstract'] = abstracts
    return papers_request


def get_abstract(pii, doi):
    abstract = ''
    if pii != 'nan':
        req = api_url.replace('<type>', 'article') + 'pii/' + pii + '?apiKey=' + config['apikey']
        result = clientG.request(req, 'get', {})
        if result != {}:
            abstract = parse_abstract(result)
    if abstract == '' and doi != 'nan':
        req = api_url.replace('<type>', 'article') + 'doi/' + doi + '?apiKey=' + config['apikey']
        result = clientG.request(req, 'get', {})
        if result != {}:
            abstract = parse_abstract(result)
    return abstract


def parse_abstract(xml):
    abstract = ''
    try:
        df = pd.read_xml(xml, namespaces={"dc": "http://purl.org/dc/elements/1.1/",
                                          "ce": "http://www.elsevier.com/xml/ani/common"})
        if 'description' in df:
            abstract = df['description'][0]
        if abstract == '':
            if '<ce:para>' in xml:
                abstract = xml.split('</ce:para>')[0].split('<ce:para>')[1]
        if abstract == '':
            if '<dc:description>' in xml:
                abstract.split('</dc:description>')[0].split('<dc:description>')[1]
    except:
        abstract = ''
    return abstract


def filter_papers(papers, dates, start_date, end_date):
    if len(papers) > 0:
        if dates is True:
            papers['publication_date'] = pd.to_datetime(papers['publication_date']).dt.date
            papers = papers[(papers['publication_date'] >= start_date) & (papers['publication_date'] <= end_date)]
        papers = papers.drop_duplicates(subset=['id'])
    return papers


def clean_papers(papers):
    if len(papers) > 0:
        papers['abstract'].replace('', np.nan, inplace=True)
        papers.dropna(subset=['abstract'], inplace=True)
        papers.replace('', float("NaN"), inplace=True)
        papers.dropna(how='all', axis=1, inplace=True)
    return papers
