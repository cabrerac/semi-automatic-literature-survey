import time
import pandas as pd
import json
from .apis.generic import Generic
from os.path import exists
from util import util
from tqdm import tqdm
import logging


api_url = 'https://api.core.ac.uk/v3/search/works'
api_access = ''
if exists('./config.json'):
    with open("./config.json", "r") as file:
        config = json.load(file)
    if 'api_access_core' in config:
        api_access = config['api_access_core']
start = 0
max_papers = 1000
quota = 1000
client_fields = {'title': 'title', 'abstract': 'abstract'}
database = 'core'
f = 'utf-8'
client = Generic()
waiting_time = 2
max_retries = 3
file_handler = ''
logger = logging.getLogger('logger')


def get_papers(query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date, folder_name, search_date):
    global logger
    logger = logging.getLogger('logger')
    global file_handler
    file_handler = logger.handlers[1].baseFilename
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + query_name.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists(file_name):
        if api_access != '':
            c_fields = []
            for field in fields:
                if field in client_fields:
                    c_fields.append(client_fields[field])
            parameters = {'query': query_value, 'syntactic_filters': syntactic_filters, 'synonyms': synonyms, 'fields': c_fields, 'types': types}
            papers = plan_requests(query, parameters, dates, start_date, end_date)
            if len(papers) > 0:
                papers = filter_papers(papers)
            if len(papers) > 0:
                papers = clean_papers(papers)
            if len(papers) > 0:
                util.save(file_name, papers, f, 'a')
            logger.info("Retrieved papers after filters and cleaning: " + str(len(papers)))
        else:
            logger.info("API key access not provided. Skipping this client...")
    else:
        logger.info("File already exists.")


def plan_requests(query, parameters, dates, start_date, end_date):
    logger.info("Retrieving papers. It might take a while...")
    papers = pd.DataFrame()
    request = create_request(parameters, dates, start_date, end_date)
    headers = {'Authorization': 'Bearer ' + api_access}
    raw_papers = client.request(api_url, 'post', request, headers)
    expected_papers = get_expected_papers(raw_papers)
    logger.info("Expected papers from core: " + str(expected_papers) + "...")
    times = int(expected_papers / max_papers) - 1
    mod = int(expected_papers) % max_papers
    if mod > 0:
        times = times + 1
    if times < quota:
        papers = request_papers(query, parameters, dates, start_date, end_date)
    else:
        logger.info("The number of expected papers requires " + str(times) + " requests which exceeds the " + database + " quota of " + str(quota) + " requests per day.")
        if len(parameters['syntactic_filters']) > 0:
            logger.info("Trying to reduce the number of requests using syntactic filters.")
            que = ''
            syntactic_filters = parameters['syntactic_filters']
            for word in syntactic_filters:
                que = que.replace('<AND>last', '<AND> ')
                que = que + "'" + word + "' <AND>last"
            que = que.replace(' <AND>last', '')
            parameters['query'] = que
            request = create_request(parameters, dates, start_date, end_date)
            headers = {'Authorization': 'Bearer ' + api_access}
            raw_papers = client.request(api_url, 'post', request, headers)
            expected_papers = get_expected_papers(raw_papers)
            logger.info("Expected papers from core: " + str(expected_papers) + "...")
            times = int(expected_papers / max_papers) - 1
            mod = int(expected_papers) % max_papers
            if mod > 0:
                times = times + 1
            if times < quota:
                papers = request_papers(query, parameters, dates, start_date, end_date)
            else:
                logger.info("The number of expected papers requires " + str(times) + " requests which exceeds the " + database + " quota of " + str(quota) + " requests per day.")
                logger.info("Skipping to next repository. Try to redefine your search queries and syntactic filters. Using dates to limit your search can help in case you are not.")
        else:
            logger.info("Skipping to next repository. Please use syntactic filters to avoid this problem. Using dates to limit your search can help in case you are not.")
    return papers


def request_papers(query, parameters, dates, start_date, end_date):
    papers = pd.DataFrame()
    request = create_request(parameters, dates, start_date, end_date)
    headers = {'Authorization': 'Bearer ' + api_access}
    raw_papers = client.request(api_url, 'post', request, headers)
    expected_papers = get_expected_papers(raw_papers)
    past_papers = -1
    pbar = tqdm(total=expected_papers)
    while len(papers) < expected_papers:
        if past_papers == len(papers):
            break
        time.sleep(waiting_time)
        global start
        start = len(papers)
        request = create_request(parameters, dates, start_date, end_date)
        headers = {'Authorization': 'Bearer ' + api_access}
        raw_papers = client.request(api_url, 'post', request, headers)
        # if there is an exception from the API, retry request
        retry = 0
        while raw_papers.status_code != 200 and retry < max_retries:
            delay = util.exponential_backoff(retry, waiting_time, 64)
            time.sleep(delay)
            retry = retry + 1
            headers = {'Authorization': 'Bearer ' + api_access}
            raw_papers = client.request(api_url, 'post', request, headers)
        papers_request = process_raw_papers(query, raw_papers)
        past_papers = len(papers)
        if len(papers) == 0:
            papers = papers_request
        else:
            papers = pd.concat([papers, papers_request])
        pbar.update(len(papers_request))
    pbar.close()
    return papers


def create_request(parameters, dates, start_date, end_date):
    req = {}
    start_year = start_date.year
    end_year = end_date.year
    query = client.core_query(parameters)
    if dates:
        query = '(yearPublished>=' + str(start_year) + ' AND yearPublished<=' + str(end_year) + ') AND ' + query
    req['q'] = query
    req['limit'] = max_papers
    req['offset'] = start
    return req


def get_expected_papers(raw_papers):
    total = 0
    try:
        if raw_papers.status_code == 200:
            raw_json = json.loads(raw_papers.content)
            total = raw_json['totalHits']
        else:
            logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                        + file_handler)
            logger.debug("API response: " + str(raw_papers.text))
            logger.debug("Request: " + raw_papers.request.body)
    except Exception as ex:
        logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                    "details: " + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return total


def process_raw_papers(query, raw_papers):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    papers_request = pd.DataFrame()
    try:
        if raw_papers.status_code == 200:
            raw_json = json.loads(raw_papers.content)
            papers_request = pd.json_normalize(raw_json['results'])
            papers_request.loc[:, 'database'] = database
            papers_request.loc[:, 'query_name'] = query_name
            papers_request.loc[:, 'query_value'] = query_value.replace('<AND>', 'AND').replace('<OR>', 'OR')
            if 'downloadUrl' not in papers_request:
                papers_request[:, 'downloadUrl'] = ''
        else:
            logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                        + file_handler)
            logger.debug("API response: " + raw_papers.text)
            logger.debug("Request: " + raw_papers.request.body)
    except Exception as ex:
        logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                    "details: " + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers_request


def filter_papers(papers):
    logger.info("Filtering papers...")
    try:
        papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
        papers = papers.dropna(subset=['title'])
        papers.loc[:, 'title'] = papers['title'].str.lower()
        papers = papers.drop_duplicates('title')
        papers = papers.dropna(subset=['abstract'])
    except Exception as ex:
        logger.info("Error filtering papers. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers


def clean_papers(papers):
    logger.info("Cleaning papers...")
    try:
        papers = papers.drop(columns=['acceptedDate', 'createdDate', 'arxivId', 'authors', 'citationCount',
                                      'contributors', 'outputs', 'createDate', 'dataProviders', 'depositedDate',
                                      'documentType', 'identifiers', 'fieldOfStudy', 'fullText', 'identifiers',
                                      'relations', 'magId', 'oaiIds', 'pubmedId', 'links', 'references',
                                      'sourceFulltextUrls', 'updatedDate', 'yearPublished', 'language.code',
                                      'language.id', 'language.name'], errors='ignore')
        papers.replace('', float("NaN"), inplace=True)
        papers.dropna(how='all', axis=1, inplace=True)
    except Exception as ex:
        logger.info("Error cleaning papers. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers
