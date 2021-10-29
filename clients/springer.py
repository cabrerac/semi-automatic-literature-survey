import time
import urllib.request
import pandas as pd
import json

api_url = 'http://api.springernature.com/meta/v2/json'
springer_fields = {'title': 'title', 'abstract': 'keyword'}
api_access = 'd5a276d7379bb80d31ec868f7e34e386'


def get_papers(domains, interests, keywords, synonyms, fields, types):
    s_fields = []
    for field in fields:
        if field in springer_fields:
            s_fields.append(springer_fields[field])
    all_papers = []
    query = create_query(domains, interests, keywords, synonyms, s_fields)
    start = 1
    query_url = api_url + query + '&s='+str(start)+'&p=100&api_key=' + api_access
    raw_result = urllib.request.urlopen(query_url).read().decode('utf-8')
    json_result = json.loads(raw_result)
    total = json_result['result'][0]['total']
    all_papers = process_raw_data(all_papers, json_result)
    print(total)
    if int(total) > 100:
        times = int(int(total)/100) - 1
        print(times)
        mod = int(total) % 100
        if mod > 0:
            times = times + 1
        print(times)
        for t in range(1, times+1):
            time.sleep(5)
            start = (100*t) + 1
            print(str(start))
            query_url = api_url + query + '&s=' + str(start) + '&p=100&api_key=' + api_access
            raw_result = urllib.request.urlopen(query_url).read().decode('utf-8')
            json_result = json.loads(raw_result)
            all_papers = process_raw_data(all_papers, json_result)

    print(all_papers)
    return all_papers


def create_query(domains, interests, keywords, synonyms, fields):
    query = ''
    query_domains = ''
    for domain in domains:
        query_domains = query_domains + '<field>:%22' + domain + '%22'
        if domain in synonyms:
            domain_synonyms = synonyms[domain]
            for synonym in domain_synonyms:
                query_domains = query_domains + ' OR <field>:%22' + synonym + '%22'
        query_domains = query_domains + ' OR '
    query_domains = '(' + query_domains + ')'
    query_domains = query_domains.replace(' OR )', ')')
    query = query + query_domains

    query_interests = ''
    for interest in interests:
        query_interests = query_interests + '<field>:%22' + interest + '%22'
        if interest in synonyms:
            interest_synonyms = synonyms[interest]
            for synonym in interest_synonyms:
                query_interests = query_interests + ' OR <field>:%22' + synonym + '%22'
        query_interests = query_interests + ' OR '
    query_interests = '(' + query_interests + ')'
    query_interests = query_interests.replace(' OR )', ')')
    if len(query) > 0 and len(query_interests) > 0:
        query = query + ' AND ' + query_interests
    if len(query) == 0 and len(query_interests) > 0:
        query = query + query_interests

    """query_keywords = ''
    for keyword in keywords:
        query_keywords = query_keywords + '<field>:%22' + keyword + '%22 OR '
    query_keywords = '(' + query_keywords + ')'
    query_keywords = query_keywords.replace(' OR )', ')')
    if len(query) > 0 and len(query_keywords) > 0:
        query = query + ' AND ' + query_keywords
    if len(query) == 0 and len(query_keywords) > 0:
        query = query + query_keywords"""

    query = '(' + query + ')'
    qf = ''
    for field in fields:
        qf = qf + query.replace('<field>', field) + ' OR '
    query = qf[:-4]
    query = query.replace(' ', '%20')
    query = '?q=' + query
    print(query)
    return query


def process_raw_data(papers, json_results):
    if 'records' in json_results:
        df = pd.json_normalize(json_results['records'])
        if len(papers) == 0:
            papers = df
        else:
            papers = papers.append(df)
    return papers