import time
from .xplore.xploreapi import XPLORE
import json
import pandas as pd

api_access = 'xmknyxp8j436aun5c5tj7g75'
ieee_fields = {'title': 'article_title', 'abstract': 'abstract'}
ieee_types = {'books': 'Books', 'courses':'Courses', 'conferences': 'Conferences',
              'early access': 'Early Access', 'journals': 'Journals', 'standards': 'Standards'}


def get_papers(domains, interests, keywords, synonyms, fields, types):
    i_fields = []
    for field in fields:
        i_fields.append(ieee_fields[field])
    i_types = []
    for type in types:
        i_types.append(ieee_types[type])
    queries = create_query(domains, interests, keywords, synonyms, i_fields)
    all_papers = []
    for query in queries:
        for field in i_fields:
            for type in i_types:
                client = XPLORE(api_access)
                client.searchField(field, query)
                client.resultsFilter("content_type", type)
                client.maximumResults(200)
                print(query)
                print(field)
                print(type)
                raw_papers = client.callAPI()
                all_papers = process_raw_data(all_papers, raw_papers)
                time.sleep(10)
    print(all_papers)
    papers = all_papers.drop_duplicates(subset=['doi'])
    print(papers)
    return papers


def create_query(domains, interests, keywords, synonyms, a_fields):
    queries = []
    for keyword in keywords:
        query = '"' + keyword + '"'

        query_domains = ''
        for domain in domains:
            query_domains = query_domains + '"' + domain + '"' + 'OR'
            domain_synonyms = synonyms[domain]
            for synonym in domain_synonyms:
                query_domains = query_domains + '"' + synonym + '"' + 'OR'
        if len(query_domains) > 0:
            query = query + 'AND(' + query_domains + ')'
        query = query.replace('OR)', ')')

        query_interests = ''
        for interest in interests:
            query_interests = query_interests + '"' + interest + '"' + 'OR'
            interest_synonyms = synonyms[interest]
            for synonym in interest_synonyms:
                query_interests = query_interests + '"' + synonym + '"' + 'OR'
        if len(query_interests) > 0:
            query = query + 'AND(' + query_interests + ')'
        query = query.replace('OR)', ')')
        queries.append(query)
    return queries


def process_raw_data(papers, raw_papers):
    raw_json = json.loads(raw_papers)
    print("Results: " + str(raw_json['total_records']))
    if 'articles' in raw_json:
        df = pd.json_normalize(raw_json['articles'])
        if len(papers)==0:
            papers = df
        else:
            papers = papers.append(df)
    return papers



