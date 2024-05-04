import time
import pandas as pd
import json
from .apis.generic import Generic
from os.path import exists
from util import util
from tqdm import tqdm
import logging
import datetime

client = Generic()
database = 'semantic_scholar'
api_url = 'https://api.semanticscholar.org/graph/v1/paper/search/?query=<query>&offset=<offset>&limit=<max_papers>&' \
          'fields=title,abstract,url,year,venue,externalIds&publicationTypes=JournalArticle,BookSection,Study' \
          '&sort=citationCount:desc'
citations_url = 'https://api.semanticscholar.org/graph/v1/paper/{paper_id}/citations?fields=title,abstract,url,year,' \
                'venue&offset=<offset>&limit=<max_papers>'
api_access = ''
if exists('./config.json'):
    with open("./config.json", "r") as file:
        config = json.load(file)
    api_access = config['api_access_semantic_scholar']
max_papers = 100
start = 0
client_fields = {'title': 'title', 'abstract': 'keyword'}
database = 'semantic_scholar'
fr = 'utf-8'
client = Generic()
waiting_time = 3
max_retries = 3
offset_limit = 1000
file_handler = ''
logger = logging.getLogger('logger')


def get_papers(query, syntactic_filters, types, dates, start_date, end_date, folder_name, search_date):
    global logger
    logger = logging.getLogger('logger')
    global file_handler
    file_handler = logger.handlers[1].baseFilename
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + query_name.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists(file_name):
        parameters = {'query': query_value, 'syntactic_filters': syntactic_filters, 'synonyms': {}, 'types': types}
        papers = plan_requests(query, parameters, dates, start_date, end_date)
        if len(papers) > 0:
            papers = filter_papers(papers, dates, start_date, end_date)
        if len(papers) > 0:
            papers = clean_papers(papers)
        if len(papers) > 0:
            util.save(file_name, papers, fr, 'a')
        logger.info("Retrieved papers after filters and cleaning: " + str(len(papers)))
    else:
        logger.info("File already exists.")


def plan_requests(query, parameters, dates, start_date, end_date):
    logger.info("Retrieving papers. It might take a while...")
    papers = pd.DataFrame()
    requests = create_request(parameters, dates, start_date, end_date)
    planned_requests = []
    logger.info("Planning queries...")
    for request in tqdm(requests):
        req = api_url.replace('<query>', request['query']).replace('<offset>', str(start)).replace('<max_papers>', str(max_papers))
        headers = {}
        if len(api_access) > 0:
            headers = {'x-api-key': api_access}
        raw_papers = client.request(req, 'get', {}, headers=headers)
        # if there is an exception from the API, retry request
        retry = 0
        while raw_papers.status_code != 200 and retry < max_retries:
            delay = util.exponential_backoff(retry, waiting_time, 64)
            time.sleep(delay)
            retry = retry + 1
            raw_papers = client.request(req, 'get', {}, headers=headers)
        papers_request, next_paper, total = process_raw_papers(query, raw_papers, False)
        if total > 0:
            if total < offset_limit:
                planned_requests.append(request['query'])
            else:
                que = ''
                syntactic_filters = parameters['syntactic_filters']
                for word in syntactic_filters:
                    que = que.replace('<AND>last', '<AND> ')
                    que = que + "'" + word + "' <AND>last"
                que = que.replace(' <AND>last', '')
                parameters_syn = parameters
                parameters_syn['query'] = que
                start_d = datetime.datetime(request['initial_year'], 1, 1)
                end_d = datetime.datetime(request['end_year'], 1, 1)
                requests_syn = create_request(parameters_syn, dates, start_d, end_d)
                for request_syn in requests_syn:
                    req = api_url.replace('<query>', request_syn['query']).replace('<offset>', str(start)).replace(
                        '<max_papers>', str(max_papers))
                    headers = {}
                    if len(api_access) > 0:
                        headers = {'x-api-key': api_access}
                    raw_papers = client.request(req, 'get', {}, headers=headers)
                    # if there is an exception from the API, retry request
                    retry = 0
                    while raw_papers.status_code != 200 and retry < max_retries:
                        delay = util.exponential_backoff(retry, waiting_time, 64)
                        time.sleep(delay)
                        retry = retry + 1
                        raw_papers = client.request(req, 'get', {}, headers=headers)
                    papers_request, next_paper, total = process_raw_papers(query, raw_papers, False)
                    if total > 0:
                        if total < offset_limit:
                            planned_requests.append(request_syn['query'])
                        else:
                            planned_requests.append(request['query'])
    papers = request_papers(query, planned_requests)
    return papers


def request_papers(query, requests):
    papers = pd.DataFrame()
    logger.info("There will be " + str(len(requests)) + " different queries to the " + database + " API...")
    current_request = 0
    for request in requests:
        current_request = current_request + 1
        logger.info("Query " + str(current_request) + "...")
        req = api_url.replace('<query>', request).replace('<offset>', str(start)).replace('<max_papers>', str(max_papers))
        headers = {}
        if len(api_access) > 0:
            headers = {'x-api-key': api_access}
        raw_papers = client.request(req, 'get', {}, headers=headers)
        # if there is an exception from the API, retry request
        retry = 0
        while raw_papers.status_code != 200 and retry < max_retries:
            delay = util.exponential_backoff(retry, waiting_time, 64)
            time.sleep(delay)
            retry = retry + 1
            raw_papers = client.request(req, 'get', {}, headers=headers)
        papers_request, next_paper, total = process_raw_papers(query, raw_papers, True)
        if len(papers) == 0:
            papers = papers_request
        else:
            papers = pd.concat([papers, papers_request])
        if total > offset_limit:
            logger.info("The query returns more papers than the " + database + " limit...")
            logger.info("Retrieving the first " + str(offset_limit) + " more cited papers instead...")
            total = offset_limit
        pbar = tqdm(total=total)
        pbar.update(len(papers_request))
        while next_paper != -1 and next_paper < offset_limit:
            time.sleep(waiting_time)
            req = api_url.replace('<query>', request).replace('<offset>', str(next_paper))
            req = req.replace('<max_papers>', str(max_papers))
            raw_papers = client.request(req, 'get', {}, {})
            retry = 0
            while raw_papers.status_code != 200 and retry < max_retries:
                delay = util.exponential_backoff(retry, waiting_time, 64)
                time.sleep(delay)
                retry = retry + 1
                raw_papers = client.request(req, 'get', {}, headers=headers)
            papers_request, next_paper, total = process_raw_papers(query, raw_papers, True)
            if len(papers) == 0:
                papers = papers_request
            else:
                papers = pd.concat([papers, papers_request])
            pbar.update(len(papers_request))
        pbar.close()
    return papers


def create_request(parameters, dates, start_date, end_date):
    queries = []
    queries_temp = client.ieeexplore_query(parameters)
    for query_temp in queries_temp:
        query_temp = query_temp.replace('(', '').replace('OR', '+').replace('AND', '+').replace('"', '').replace(')', '') \
            .replace(' ', '+')
        query = query_temp + '<dates>'
        initial_year = start_date.year
        final_year = end_date.year
        while initial_year < final_year:
            query = query.replace('<dates>', '&year=' + str(initial_year) + '-' + str(initial_year+1))
            queries.append({'query': query, 'initial_year': initial_year, 'end_year': initial_year+1})
            query = query_temp + '<dates>'
            initial_year = initial_year + 1
    return queries


def process_raw_papers(query, raw_papers, print_error):
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
                papers_request.loc[:, 'database'] = database
                papers_request.loc[:, 'query_name'] = query_name
                papers_request.loc[:, 'query_value'] = query_value.replace('<AND>', 'AND').replace('<OR>', 'OR')
                if 'abstract' not in papers_request:
                    papers_request = pd.DataFrame()
            else:
                papers_request = pd.DataFrame()
        except Exception as ex:
            if print_error:
                logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                            "details: " + file_handler)
                logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    else:
        if print_error:
            logger.info("Error requesting the API. Skipping to next request. Please see the log file for details: "
                        + file_handler)
            logger.debug("API response: " + raw_papers.text)
            logger.debug("Request: " + raw_papers.request.url)
    return papers_request, next_paper, total


def filter_papers(papers, dates, start_date, end_date):
    logger.info("Filtering papers...")
    try:
        papers.loc[:, 'title'] = papers['title'].replace('', float("NaN"))
        papers = papers.dropna(subset=['title'])
        papers.loc[:, 'title'] = papers['title'].str.lower()
        papers = papers.drop_duplicates('title')
        papers.loc[:, 'abstract'] = papers['abstract'].replace('', float("NaN"))
        papers = papers.dropna(subset=['abstract'])
        if dates:
            papers = papers[(papers['year'] >= start_date.year) & (papers['year'] <= end_date.year)]
    except Exception as ex:
        logger.info("Error filtering papers. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers


def clean_papers(papers):
    logger.info("Cleaning papers...")
    try:
        papers = papers.drop(columns=['externalIds.MAG', 'externalIds.DBLP', 'externalIds.PubMedCentral',
                                      'externalIds.PubMed', 'externalIds.ArXiv', 'externalIds.CorpusId',
                                      'externalIds.ACL'], errors='ignore')
        papers.replace('', float("NaN"), inplace=True)
        papers.dropna(how='all', axis=1, inplace=True)
    except Exception as ex:
        logger.info("Error cleaning papers. Skipping to next request. Please see the log file for details: "
                    + file_handler)
        logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    return papers


def get_citations(folder_name, search_date, step, dates, start_date, end_date):
    logger.info("Retrieving citation papers. It might take a while...")
    papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + '_manually_filtered_by_full_text_papers.csv'
    papers = pd.read_csv(papers_file)
    citations = pd.DataFrame()
    pbar = tqdm(total=len(papers))
    for index, row in papers.iterrows():
        paper_id = 'DOI:' + row['doi']
        if 'http' in paper_id or paper_id == '':
            paper_id = 'URL:' + row['doi']
        if paper_id != '':
            papers_request = request_citations(paper_id)
            if len(citations) == 0:
                citations = papers_request
            else:
                citations = pd.concat([citations, papers_request])
        pbar.update(1)
    pbar.close()
    if len(citations) > 0:
        citations = filter_papers(citations, dates, start_date, end_date)
    if len(citations) > 0:
        citations = clean_papers(citations)
    if len(citations) > 0:
        citations.loc[:, 'type'] = 'preprocessed'
        citations.loc[:, 'status'] = 'unknown'
        citations.loc[:, 'id'] = list(range(1, len(citations) + 1))
    logger.info("Retrieved papers after filters and cleaning: " + str(len(citations)))
    return citations


def request_citations(paper_id):
    papers = pd.DataFrame()
    next_paper = 0
    while next_paper != -1 and next_paper < offset_limit:
        time.sleep(waiting_time)
        request = citations_url.replace('{paper_id}', str(paper_id))
        request = request.replace('<offset>', str(next_paper)).replace('<max_papers>', str(max_papers))
        headers = {}
        if len(api_access) > 0:
            headers = {'x-api-key': api_access}
        raw_citations = client.request(request, 'get', {}, headers=headers)
        papers_request, next_paper = process_raw_citations(raw_citations)
        if len(papers) == 0:
            papers = papers_request
        else:
            papers = pd.concat([papers, papers_request])
    return papers


def process_raw_citations(raw_citations):
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
                papers.loc[:, 'database'] = database
                papers.loc[:, 'query_name'] = 'citation'
                papers.loc[:, 'query_value'] = 'citation'
        except Exception as ex:
            logger.info("Error parsing the API response. Skipping to next request. Please see the log file for "
                        "details: " + file_handler)
            logger.debug("Exception: " + str(type(ex)) + ' - ' + str(ex))
    else:
        logger.info("Error requesting the API for citations. Skipping to next request. Please see the log file for "
                    "details: " + file_handler)
        logger.debug("API response: " + raw_citations.text)
        logger.debug("Request: " + raw_citations.request.url)
    return papers, next_paper
