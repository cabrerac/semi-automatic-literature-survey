import time
from .apis.xploreapi import XPLORE
from .apis.generic import Generic
import json
import pandas as pd

api_access = 'xmknyxp8j436aun5c5tj7g75'
max_papers = 200
client_fields = {'title': 'article_title', 'abstract': 'abstract'}
client_types = {'books': 'Books', 'courses':'Courses', 'conferences': 'Conferences',
              'early access': 'Early Access', 'journals': 'Journals', 'standards': 'Standards'}
database = 'ieeexplore'
client = Generic()


def get_papers(domains, interests, keywords, synonyms, fields, types, retrieve):
    stats = []
    papers = []
    c_fields = []
    for field in fields:
        if field in client_fields:
            c_fields.append(client_fields[field])
    c_types = []
    for t in types:
        if t in client_types:
            c_types.append(client_types[t])
    parameters = {'domains': domains, 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                  'fields': c_fields, 'types': c_types}
    if len(keywords) > 0:
        queries = client.ieeexplore_query(parameters, 'keywords')
    else:
        queries = client.ieeexplore_query(parameters, 'domains')
    total_requests = len(queries) * len(c_fields) * len(c_types)
    print(str(total_requests))
    current_request = 1
    for query in queries:
        for field in c_fields:
            for t in c_types:
                print(str(current_request))
                raw_papers = request(query, field, t)
                if raw_papers != {}:
                    stats, papers = process_raw_data(papers, raw_papers, retrieve)
                time.sleep(10)
                current_request = current_request + 1
    papers = papers.drop_duplicates(subset=['doi'])
    if retrieve:
        papers = client.filterByField(papers, 'abstract', keywords)
    stats[2] = len(papers.index)
    stats.append(domains[0])
    #papers = client.filterByField(papers, 'abstract', keywords)
    return stats, papers


def request(query, field, t):
    client_ieee = XPLORE(api_access)
    client_ieee.searchField(field, query)
    client_ieee.resultsFilter("content_type", t)
    client_ieee.maximumResults(max_papers)
    raw_papers = client_ieee.callAPI()
    return raw_papers


def process_raw_data(papers, raw_papers, retrieve):
    raw_json = json.loads(raw_papers)
    total = raw_json['total_records']
    if 'articles' in raw_json:
        df = pd.json_normalize(raw_json['articles'])
        if len(papers) == 0:
            papers = df
        else:
            papers = papers.append(df)
        papers['database'] = database
    stats = [database, retrieve, total]
    return stats, papers
