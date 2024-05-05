import time
import pandas as pd
from .apis.generic import Generic
from os.path import exists
from util import util
from tqdm import tqdm
import logging

api_url = 'http://export.arxiv.org/api/query?search_query='
start = 0
max_papers = 5000
client_fields = {'title': 'ti', 'abstract': 'abs'}
database = 'arxiv'
f = 'utf-8'
client = Generic()
waiting_time = 2
max_retries = 3
file_handler = ''
logger = logging.getLogger()


def get_papers(query, syntactic_filters, synonyms, fields, types, dates, start_date, end_date, folder_name, search_date):
    global logger
    logger = logging.getLogger('logger')
    print(logger.handlers)
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
        parameters = {'query': query_value, 'syntactic_filters': syntactic_filters, 'synonyms': synonyms, 'fields': c_fields, 'types': types}
        papers = plan_requests(query, parameters)
        if len(papers) > 0:
            papers = filter_papers(papers, dates, start_date, end_date)
        if len(papers) > 0:
            papers = clean_papers(papers)
        if len(papers) > 0:
            util.save(file_name, papers, f, 'a')
        logger.info("Retrieved papers after filters and cleaning: " + str(len(papers)))
    else:
        logger.info("File already exists.")


def plan_requests(query, parameters):
    logger.info("Retrieving papers. It might take a while...")
    request = create_request(parameters)
    raw_papers = client.request(request, 'get', {}, {})
    expected_papers = get_expected_papers(raw_papers)
    logger.info("Expected papers from arxiv: " + str(expected_papers) + "...")
    times = int(expected_papers / max_papers) - 1
    mod = int(expected_papers) % max_papers
    if mod > 0:
        times = times + 1
    papers = request_papers(query, parameters, times, expected_papers, mod)
    return papers


def request_papers(query, parameters, times, expected_papers, mod):
    papers = pd.DataFrame()
    for t in tqdm(range(0, times + 1)):
        time.sleep(waiting_time)
        global start
        start = t * max_papers
        request = create_request(parameters)
        raw_papers = client.request(request, 'get', {}, {})
        # if there is an exception from the API, retry request
        retry = 0
        while raw_papers.status_code != 200 and retry < max_retries:
            delay = util.exponential_backoff(retry, waiting_time, 64)
            time.sleep(delay)
            retry = retry + 1
            raw_papers = client.request(request, 'get', {}, {})
        papers_request = process_raw_papers(query, raw_papers)
        # sometimes the arxiv API does not respond with all the papers, so we request again
        expected_per_request = expected_papers
        if expected_papers > max_papers:
            expected_per_request = max_papers
        if t == times and mod > 0:
            expected_per_request = mod
        while len(papers_request) < expected_per_request:
            time.sleep(waiting_time)
            raw_papers = client.request(request, 'get', {}, {})
            papers_request = process_raw_papers(query, raw_papers)
        if len(papers) == 0:
            papers = papers_request
        else:
            papers = pd.concat([papers, papers_request])
    return papers


def create_request(parameters):
    req = api_url
    req = req + client.default_query(parameters)
    req = req + '&start=' + str(start)
    req = req + '&max_results='+str(max_papers)
    req = req + '&sortBy=submittedDate&sortOrder=descending'
    return req


def get_expected_papers(raw_papers):
    total = 0
    if raw_papers.status_code == 200:
        try:
            total_text = raw_papers.text.split('opensearch:totalResults')[1]
            total = int(total_text.split('>')[1].replace('</', ''))
        except Exception as ex:
            logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                        "details: " + file_handler)
            logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    else:
        logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        if raw_papers.request is not None:
            logger.debug("API response: " + str(raw_papers.text))
            logger.debug("Request: " + raw_papers.request.url)
        else:
            logger.debug("API response: " + str(raw_papers.content))
    return total


def process_raw_papers(query, raw_papers):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    papers_request = pd.DataFrame()
    if raw_papers.status_code == 200:
        try:
            papers_request = pd.read_xml(raw_papers.text, xpath='//feed:entry',
                                         namespaces={"feed": "http://www.w3.org/2005/Atom"})
            papers_request.loc[:, 'database'] = database
            papers_request.loc[:, 'query_name'] = query_name
            papers_request.loc[:, 'query_value'] = query_value.replace('<AND>', 'AND').replace('<OR>', 'OR')
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


def filter_papers(papers, dates, start_date, end_date):
    logger.info("Filtering papers...")
    try:
        papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
        papers.dropna(subset=['title'], inplace=True)
        papers.loc[:, 'title'] = papers['title'].str.lower()
        papers = papers.drop_duplicates('title')
        papers.loc[:, 'summary'] = papers['summary'].replace('', float("NaN"))
        papers.dropna(subset=['summary'], inplace=True)
        if dates is True:
            papers['published'] = pd.to_datetime(papers['published']).dt.date
            papers = papers[(papers['published'] >= start_date) & (papers['published'] <= end_date)]
    except Exception as ex:
        logger.info("Error filtering papers. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers


def clean_papers(papers):
    logger.info("Cleaning papers...")
    try:
        papers = papers.drop(columns=['author', 'comment', 'link', 'primary_category', 'category', 'doi',
                                      'journal_ref'], errors='ignore')
        papers.replace('', float("NaN"), inplace=True)
        papers.dropna(how='all', axis=1, inplace=True)
    except Exception as ex:
        logger.info("Error cleaning papers. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers
