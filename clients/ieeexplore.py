import time
from .apis.xploreapi import XPLORE
from .apis.generic import Generic
import json
import pandas as pd
from os.path import exists
from analysis import util
import logging

api_access = ''
if exists('./config.json'):
    with open("./config.json", "r") as file:
        config = json.load(file)
    api_access = config['api_access_ieee']
start = 0
max_papers = 200
client_fields = {'title': 'article_title', 'abstract': 'abstract'}
client_types = {'conferences': 'Conferences', 'early access': 'Early Access', 'journals': 'Journals', 'standards': 'Standards'}
database = 'ieeexplore'
f = 'utf-8'
client = Generic()
waiting_time = 5
max_retries = 3
file_handler = ''
logger = logging.getLogger('logger')


def get_papers(query, synonyms, fields, types, folder_name, search_date):
    global logger
    logger = logging.getLogger('logger')
    global file_handler
    file_handler = logger.handlers[1].baseFilename
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + query_name.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists(file_name):
        c_fields = []
        for field in fields:
            if field in client_fields:
                c_fields.append(client_fields[field])
        c_types = []
        for t in types:
            if t in client_types:
                c_types.append(client_types[t])
        parameters = {'query': query_value, 'synonyms': synonyms, 'fields': c_fields, 'types': c_types}
        papers = request_papers(query, parameters)
        if len(papers) > 0:
            papers = filter_papers(papers)
        if len(papers) > 0:
            papers = clean_papers(papers)
        if len(papers) > 0:
            util.save(file_name, papers, f, 'a')
        logger.info("Retrieved papers after filters and cleaning: " + str(len(papers)))
    else:
        logger.info("File already exists.")


def request_papers(query, parameters):
    logger.info("Retrieving papers. It might take a while...")
    papers = pd.DataFrame()
    reqs = create_request(parameters)
    fields = parameters['fields']
    types = parameters['types']
    current_request = 0
    for req in reqs:
        for field in fields:
            for p_type in types:
                current_request = current_request + 1
                global start
                raw_papers = request(req, field, p_type, start)
                expected_papers = get_expected_papers(raw_papers, request)
                times = int(expected_papers / max_papers) - 1
                mod = int(expected_papers) % max_papers
                if mod > 0:
                    times = times + 1
                for t in range(0, times + 1):
                    time.sleep(waiting_time)
                    start = max_papers * t
                    raw_papers = request(req, field, p_type, start)
                    # if there is an exception from the API, retry request
                    retry = 0
                    while raw_papers.status_code != 200 and retry < max_retries:
                        time.sleep(waiting_time)
                        retry = retry + 1
                        raw_papers = request(req, field, p_type, start)
                    papers_request = process_raw_papers(query, raw_papers)
                    if len(papers) == 0:
                        papers = papers_request
                    else:
                        papers = papers.append(papers_request)
    return papers


def create_request(parameters):
    reqs = client.ieeexplore_query(parameters)
    return reqs


def get_expected_papers(raw_papers, req):
    total = 0
    try:
        if raw_papers.status_code == 200:
            try:
                raw_json = json.loads(raw_papers.text)
                if 'articles' in raw_json:
                    total = raw_json['total_records']
            except Exception as ex:
                logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                            "details: " + file_handler)
                logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
        else:
            logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                        + file_handler)
            logger.debug("API response: " + str(raw_papers.text))
            logger.debug("Request: " + raw_papers.request.url)
    except Exception as ex:
        logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return total


def request(query, field, p_type, start_record):
    client_ieee = XPLORE(api_access)
    client_ieee.searchField(field, query)
    client_ieee.resultsFilter("content_type", p_type)
    client_ieee.startingResult(start_record)
    client_ieee.maximumResults(max_papers)
    raw_papers = client_ieee.callAPI()
    return raw_papers


def process_raw_papers(query, raw_papers):
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
                papers_request.loc[:, 'database'] = database
                papers_request.loc[:, 'query_name'] = query_name
                papers_request.loc[:, 'query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
            except Exception as ex:
                logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                            "details: " + file_handler)
                logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
        else:
            logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                        + file_handler)
            logger.debug("API response: " + raw_papers.text)
            logger.debug("Request: " + raw_papers.request.url)
    except Exception as ex:
        logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers_request


def filter_papers(papers):
    logger.info("Filtering papers...")
    try:
        papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
        papers = papers.dropna(subset=['title'])
        papers.loc[:, 'title'] = papers['title'].str.lower()
        papers = papers.drop_duplicates('title')
        papers.loc[:, 'abstract'] = papers['abstract'].replace('', float("NaN"))
        papers = papers.dropna(subset=['abstract'])
        papers = papers.drop_duplicates(subset=['doi'])
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
