import time
import config as config
import pandas as pd
import json
from .apis.generic import Generic
from os.path import exists
from analysis import util
import logging


api_url = 'http://api.springernature.com/metadata/json?q=language:en<dates>'
api_access = config.api_access_springer
start = 0
max_papers = 50
client_fields = {'title': 'title'}
database = 'springer'
f = 'utf-8'
client = Generic()
waiting_time = 2
max_retries = 3
file_handler = ''
logger = logging.getLogger('logger')


def get_papers(query, fields, types, dates, start_date, end_date, folder_name, search_date):
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
        parameters = {'query': query_value, 'synonyms': {}, 'fields': c_fields, 'types': types}
        papers = request_papers(query, parameters, dates, start_date, end_date)
        if len(papers) > 0:
            papers = filter_papers(papers)
        if len(papers) > 0:
            papers = clean_papers(papers)
        if len(papers) > 0:
            util.save(file_name, papers, f, 'a')
        logger.info("Retrieved papers after filters and cleaning: " + str(len(papers)))
    else:
        logger.info("File already exists.")


def request_papers(query, parameters, dates, start_date, end_date):
    logger.info("Retrieving papers. It might take a while...")
    papers = pd.DataFrame()
    request = create_request(parameters, dates, start_date, end_date)
    raw_papers = client.request(request, 'get', {}, '')
    expected_papers = get_expected_papers(raw_papers)
    times = int(expected_papers / max_papers) - 1
    mod = int(expected_papers) % max_papers
    if mod > 0:
        times = times + 1
    for t in range(0, times + 1):
        time.sleep(waiting_time)
        global start
        start = t * max_papers
        request = create_request(parameters, dates, start_date, end_date)
        raw_papers = client.request(request, 'get', {}, '')
        # if there is an exception from the API, retry request
        retry = 0
        while raw_papers.status_code != 200 and retry < max_retries:
            time.sleep(waiting_time)
            retry = retry + 1
            raw_papers = client.request(request, 'get', {}, '')
        papers_request = process_raw_papers(query, raw_papers)
        if len(papers) == 0:
            papers = papers_request
        else:
            papers = papers.append(papers_request)
    return papers


def create_request(parameters, dates, start_date, end_date):
    req = api_url
    if dates is True:
        req = req.replace('<dates>', '%20onlinedatefrom:' + str(start_date) +'%20onlinedateto:' + str(end_date) + '%20')
    else:
        req = req.replace('<dates>', '')
    req = req + client.default_query(parameters)
    req = req + '&s='+str(start)+'&p='+str(max_papers)+'&api_key=' + api_access
    req = req.replace('%28', '(').replace('%29', ')').replace('+', '%20')
    req = req.replace('title:', '')
    return req


def get_expected_papers(raw_papers):
    total = 0
    if raw_papers.status_code == 200:
        try:
            json_results = json.loads(raw_papers.text)
            total = int(json_results['result'][0]['total'])
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
            papers_request = pd.json_normalize(json_results['records'])
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
    return papers_request


def filter_papers(papers):
    logger.info("Filtering papers...")
    try:
        papers['title'].replace('', float("NaN"), inplace=True)
        papers.dropna(subset=['title'], inplace=True)
        papers['title'] = papers['title'].str.lower()
        papers = papers.drop_duplicates('title')
        papers['abstract'].replace('', float("NaN"), inplace=True)
        papers.dropna(subset=['abstract'], inplace=True)
        papers = papers.drop_duplicates(subset=['doi'])
        if 'language' in papers:
            papers = papers[papers['language'].str.contains('en')]
    except Exception as ex:
        logger.info("Error filtering papers. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers


def clean_papers(papers):
    logger.info("Cleaning papers...")
    try:
        urls = []
        if 'url' in papers:
            for paper in papers['url']:
                url = paper[0]['value']
                urls.append(url)
        papers = papers.drop(columns=['url', 'creators', 'bookEditors', 'openaccess', 'printIsbn', 'electronicIsbn',
                                      'isbn', 'genre', 'copyright', 'conferenceInfo', 'issn', 'eIssn', 'volume',
                                      'publicationType', 'number', 'issueType', 'topicalCollection', 'startingPage',
                                      'endingPage', 'language', 'journalId', 'printDate', 'response', 'onlineDate',
                                      'coverDate', 'keyword'],
                             errors='ignore')
        if len(urls) > 0:
            papers.loc[:, 'url'] = urls
        else:
            papers['url'] = ''
        papers.replace('', float("NaN"), inplace=True)
        papers.dropna(how='all', axis=1, inplace=True)
    except Exception as ex:
        logger.info("Error cleaning papers. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers
