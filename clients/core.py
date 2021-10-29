from urllib import request, parse
import pandas as pd
import json

api_url = 'https://core.ac.uk/api-v2articles/search?metadata=true&fulltext=false&citations=false&similar=false&duplicate=false&urls=false&faithfulMetadata=false'
core_fields = {'title': 'title', 'abstract': 'description'}
api_access = 'pScrltW8j9MansPfbmA63OVZNCFeXo2T'


def get_papers(domains, interests, keywords, synonyms, fields, types):
    c_fields = []
    for field in fields:
        c_fields.append(core_fields[field])
    query = create_query(domains, interests, keywords, synonyms, c_fields)
    data = parse.urlencode(query).encode()
    req = request.Request(api_url + '&apiKey=' + api_access, data=data)
    resp = request.urlopen(req)
    print(resp)


def create_query(domains, interests, keywords, synonyms, c_fields):
    search_query = ''
    query_domains = ''
    for domain in domains:
        query_domains = query_domains + '<field>:%22' + domain + '%22'
        if domain in synonyms:
            domain_synonyms = synonyms[domain]
            for synonym in domain_synonyms:
                query_domains = query_domains + '+OR+<field>:%22' + synonym + '%22'
        query_domains = query_domains + '+OR+'
    query_domains = '%28' + query_domains + '%29'
    query_domains = query_domains.replace('+OR+%29', '%29')
    #query = query + query_domains

    query_interests = ''
    for interest in interests:
        query_interests = query_interests + '<field>:%22' + interest + '%22'
        if interest in synonyms:
            interest_synonyms = synonyms[interest]
            for synonym in interest_synonyms:
                query_interests = query_interests + '+OR+<field>:%22' + synonym + '%22'
        query_interests = query_interests + '+OR+'
    query_interests = '%28' + query_interests + '%29'
    query_interests = query_interests.replace('+OR+%29', '%29')
    #if len(query) > 0 and len(query_interests) > 0:
        #query = query + '+AND+' + query_interests
    #if len(query) == 0 and len(query_interests) > 0:
        #query = query + query_interests

    query_keywords = ''
    for keyword in keywords:
        query_keywords = query_keywords + '<field>:%22' + keyword + '%22+OR+'
    query_keywords = '%28' + query_keywords + '%29'
    query_keywords = query_keywords.replace('+OR+%29', '%29')
    #if len(query) > 0 and len(query_keywords) > 0:
        #query = query + '+AND+' + query_keywords
    #if len(query) == 0 and len(query_keywords) > 0:
        #query = query + query_keywords

    query = {}
    query['query'] = search_query
    query['pageSize'] = 100
    return query


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