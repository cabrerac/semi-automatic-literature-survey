import urllib.request
import pandas as pd

api_url = 'http://export.arxiv.org/api/'
arxiv_fields = {'title': 'ti', 'abstract': 'abs'}


def get_papers(domains, interests, keywords, synonyms, fields, types):
    a_fields = []
    for field in fields:
        a_field = arxiv_fields[field]
        a_fields.append(a_field)
    query = create_query(domains, interests, keywords, synonyms, a_fields)
    query_url = api_url + query
    raw_xml = urllib.request.urlopen(query_url).read().decode('utf-8')
    papers = process_raw_data(raw_xml)
    return papers


def create_query(domains, interests, keywords, synonyms, fields):
    query = ''
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
    query = query + query_domains

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
    if len(query) > 0 and len(query_interests) > 0:
        query = query + '+AND+' + query_interests
    if len(query) == 0 and len(query_interests) > 0:
        query = query + query_interests

    query_keywords = ''
    for keyword in keywords:
        query_keywords = query_keywords + '<field>:%22' + keyword + '%22+OR+'
    query_keywords = '%28' + query_keywords + '%29'
    query_keywords = query_keywords.replace('+OR+%29', '%29')
    if len(query) > 0 and len(query_keywords) > 0:
        query = query + '+AND+' + query_keywords
    if len(query) == 0 and len(query_keywords) > 0:
        query = query + query_keywords

    query = '%28' + query + '%29'
    query = query.replace(' ', '+')
    qf = ''
    for field in fields:
        qf = qf + query.replace('<field>',field) + '+OR+'
    query = qf[:-4]
    query = 'query?search_query=' + query + '&sortBy=lastUpdatedDate&sortOrder=ascending&max_results=5000'
    print(query)
    return query


def process_raw_data(raw_xml):
    papers = pd.read_xml(raw_xml, xpath='//feed:entry', namespaces={"feed": "http://www.w3.org/2005/Atom"})
    return papers
