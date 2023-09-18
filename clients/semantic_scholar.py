from .apis.generic import Generic
from os.path import exists
import pandas as pd
import json
from analysis import util
import time

client = Generic()
database = 'semantic_scholar'
api_url = 'http://api.semanticscholar.org/graph/v1/paper/search?query=<query>&offset=<offset>&limit=<max_papers>&' \
          'fields=title,abstract,url,year,venue,externalIds'
citations_url = 'https://api.semanticscholar.org/graph/v1/paper/{paper_id}/citations?fields=title,abstract,url,year,' \
                'venue&offset=<offset>&limit=<max_papers>'
max_papers = 100
start = 0
client_fields = {'title': 'title', 'abstract': 'keyword'}
database = 'semantic_scholar'
fr = 'utf-8'
client = Generic()
waiting_time = 5
max_retries = 3


def get_papers(query, types, dates, start_date, end_date, folder_name, search_date):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + query_name.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists(file_name):
        parameters = {'query': query_value, 'synonyms': {}, 'types': types}
        papers = request_papers(query, parameters)
        papers = filter_papers(papers, dates, start_date, end_date)
        papers = clean_papers(papers)
        if len(papers) > 0:
            util.save(file_name, papers, f)
        print("Retrieved papers after filters and cleaning: " + str(len(papers)))
    else:
        print("File already exists.")


def request_papers(query, parameters):
    papers = pd.DataFrame()
    requests = create_request(parameters)
    for request in requests:
        req = api_url.replace('<query>', request).replace('<offset>', str(start)).replace('<max_papers>', str(max_papers))
        raw_papers = client.request(req, 'retrieve', {})
        # if there is an exception from the API, retry request
        retry = 0
        while isinstance(raw_papers, dict) and retry < max_retries:
            time.sleep(waiting_time)
            retry = retry + 1
            raw_papers = client.request(request, 'get', {})
        if not isinstance(raw_papers, dict):
            papers_request, next_papers = process_raw_papers(query, raw_papers)
            if len(papers) == 0:
                papers = papers_request
            else:
                papers = papers.append(papers_request)
        while next_papers != -1:
            time.sleep(waiting_time)
            req = api_url.replace('<query>', request).replace('<offset>', str(next_papers))
            req = req.replace('<max_papers>', str(max_papers))
            raw_papers = client.request(req, 'retrieve', {})
            retry = 0
            while isinstance(raw_papers, dict) and retry < max_retries:
                time.sleep(waiting_time)
                retry = retry + 1
                raw_papers = client.request(req, 'retrieve', {})
            if not isinstance(raw_papers, dict):
                papers_request, next_papers = process_raw_papers(query, raw_papers)
                if len(papers) == 0:
                    papers = papers_request
                else:
                    papers = papers.append(papers_request)
    return papers


def create_request(parameters):
    queries = []
    queries_temp = client.ieeexplore_query(parameters)
    for query in queries_temp:
        query = query.replace('(', '').replace('OR', '+').replace('AND', '+').replace('"', '').replace(')', '') \
            .replace(' ', '+')
        queries.append(query)
    return queries


def process_raw_papers(query, raw_papers):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    raw_json = json.loads(raw_papers)
    next_papers = -1
    if 'next' in raw_json:
        next_papers = raw_json['next']
    papers_request = pd.json_normalize(raw_json['data'])
    papers_request.loc[:, 'database'] = database
    papers_request.loc[:, 'query_name'] = query_name
    papers_request.loc[:, 'query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
    if 'abstract' not in papers_request:
        papers_request = pd.DataFrame()
    return papers_request, next_papers


def filter_papers(papers, dates, start_date, end_date):
    if dates is True:
        print('Applying dates filters...', end="\r")
        papers = papers[(papers['year'] >= start_date.year & papers['year'] >= end_date.year)]
    return papers


def clean_papers(papers):
    papers = papers.drop(columns=['externalIds.MAG', 'externalIds.DBLP', 'externalIds.PubMedCentral',
                                  'externalIds.PubMed', 'externalIds.ArXiv', 'externalIds.CorpusId',
                                  'externalIds.ACL'], errors='ignore')
    nan_value = float("NaN")
    papers.replace('', nan_value, inplace=True)
    papers.dropna(how='all', axis=1, inplace=True)
    return papers


def get_citations(folder_name, search_date, step):
    preprocessed_file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) + \
                             '_preprocessed_papers.csv'
    if not exists(preprocessed_file_name):
        papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + \
                      '_manually_filtered_by_full_text_papers.csv'
        papers = pd.read_csv(papers_file)
        for index, row in papers.iterrows():
            paper_id = row['doi']
            if paper_id == '':
                paper_id = row['url']
            if paper_id != '':
                req = citations_url.replace('{paper_id}', str(paper_id))
                req = req.replace('<offset>', '0').replace('<max_papers>', str(max_papers))
                raw_citations = client.request(req, 'retrieve', {})
                papers, nxt = process_raw_citations(raw_citations)
                if len(papers) != 0:
                    papers = papers.rename(columns={"citingPaper.paperId" : "doi", "citingPaper.url" : "url",
                                           "citingPaper.title" : "title", "citingPaper.abstract" : "abstract",
                                           "citingPaper.venue" : "publisher", "citingPaper.year" : "publication_date"})
                    papers['database'] = database
                    papers['query_name'] = 'citation'
                    papers['query_value'] = 'citation'
                    with open('./papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) +
                              '_preprocessed_papers.csv', 'a+', newline='', encoding=fr) as f:
                        papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                while nxt != -1:
                    time.sleep(5)
                    req = citations_url.replace('{paper_id}', str(paper_id)).replace('<offset>', str(nxt))
                    req = req.replace('<max_papers>', str(max_papers))
                    raw_citations = client.request(req, 'retrieve', {})
                    papers, nxt = process_raw_citations(raw_citations)
                    if len(papers) != 0:
                        papers = papers.rename(columns={"citingPaper.paperId": "doi", "citingPaper.url": "url",
                                               "citingPaper.title": "title", "citingPaper.abstract": "abstract",
                                               "citingPaper.venue": "publisher", "citingPaper.year": "publication_date"})
                        papers['database'] = database
                        papers['query_name'] = 'citation'
                        papers['query_value'] = 'citation'
                        with open('./papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) +
                                  '_preprocessed_papers.csv', 'a+', newline='', encoding=fr) as f:
                            papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                time.sleep(5)
    return preprocessed_file_name


def process_raw_citations(raw_citations):
    nxt = -1
    papers = {}
    if type(raw_citations) is not dict:
        raw_json = json.loads(raw_citations)
        if 'next' in raw_json:
            nxt = raw_json['next']
        papers = pd.json_normalize(raw_json['data'])
    return papers, nxt
