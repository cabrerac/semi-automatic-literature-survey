import time
import urllib.request
import pandas as pd
import json
from .apis.generic import Generic

api_url = 'http://api.springernature.com/meta/v2/json?q='
api_access = 'd5a276d7379bb80d31ec868f7e34e386'
start = 1
max_papers = 100
client_fields = {'title': 'title', 'abstract': 'keyword'}
database = 'springer'
client = Generic()


def get_papers(domains, interests, keywords, synonyms, fields, types, retrieve):
    c_fields = []
    papers = []
    for field in fields:
        if field in client_fields:
            c_fields.append(client_fields[field])
    parameters = {'domains': domains, 'synonyms': synonyms,
                  'fields': c_fields, 'types': types}
    req = create_request(parameters)
    raw_papers = client.request(req, 'get', {})
    stats, total, papers = process_raw_papers(papers, raw_papers, retrieve)
    if total > 100 and retrieve:
        times = int(total/100) - 1
        mod = int(total) % 100
        if mod > 0:
            times = times + 1
        for t in range(1, times + 1):
            print(str(t))
            time.sleep(5)
            global start
            start = (max_papers * t) + 1
            req = create_request(parameters)
            raw_papers = client.request(req, 'get', {})
            if raw_papers != {}:
                stats, total, papers = process_raw_papers(papers, raw_papers, retrieve)
    if retrieve:
        filters_interest = []
        for interest in interests:
            filters_interest.append(interest)
            synonyms_interest = synonyms[interest]
            for synonym in synonyms_interest:
                filters_interest.append(synonym)
        papers = client.filterByField(papers, 'abstract', filters_interest)
        papers = client.filterByField(papers, 'abstract', keywords)
        stats[2] = len(papers.index)
        papers = papers.drop(columns=['creators', 'bookEditors', 'openaccess', 'printIsbn', 'electronicIsbn',
                                      'isbn', 'genre', 'copyright', 'conferenceInfo', 'issn', 'eIssn', 'volume',
                                      'number', 'issueType', 'topicalCollection', 'startingPage', 'endingPage',
                                      'journalId', 'printDate', 'coverDate', 'keyword'])
    stats.append(domains[0])
    return stats, papers


def create_request(parameters):
    req = api_url
    req = req + client.default_query(parameters)
    req = req + '&s='+str(start)+'&p='+str(max_papers)+'&api_key=' + api_access
    req = req.replace('%28', '(').replace('%29', ')').replace('+', '%20')
    return req


def process_raw_papers(papers, raw_papers, retrieve):
    json_results = json.loads(raw_papers)
    total = int(json_results['result'][0]['total'])
    if 'records' in json_results:
        df = pd.json_normalize(json_results['records'])
        if len(papers) == 0:
            papers = df
        else:
            papers = papers.append(df)
    papers['database'] = database
    stats = [database, retrieve, int(total)]
    return stats, total, papers
