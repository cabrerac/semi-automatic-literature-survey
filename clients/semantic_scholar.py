from .apis.generic import Generic
from os.path import exists
import pandas as pd
import json
from analysis import util
from analysis import retrieve
import time

client = Generic()
database = 'semantic_scholar'
api_url = 'http://api.semanticscholar.org/graph/v1/paper/search?query=<query>&offset=<offset>&limit=<max_papers>&' \
          'fields=title,abstract,url,year,venue,externalIds'
citations_url = 'https://api.semanticscholar.org/graph/v1/paper/{paper_id}/citations?fields=title,abstract,url,year,' \
                'venue&offset=<offset>&limit=<max_papers>'
max_papers = 100
format = 'utf-8'


def get_papers(domain, interests, keywords, synonyms, fields, types):
    file_name = 'domains/' + domain.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists('./papers/' + file_name):
        parameters = {'domains': [domain], 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                      'types': types}
        queries = create_request(parameters)
        print('queries:' + str(len(queries)))
        for query in queries:
            req = api_url.replace('<query>', query).replace('<offset>', str(0)).replace('<max_papers>', str(max_papers))
            raw_papers = client.request(req, 'retrieve', {})
            total, papers, next = process_raw_papers(raw_papers)
            print(str(total))
            if len(papers) != 0:
                util.save(file_name, papers, format)
            while next != -1:
                time.sleep(5)
                print(str(next))
                req = api_url.replace('<query>', query).replace('<offset>', str(next))
                req = req.replace('<max_papers>', str(max_papers))
                raw_papers = client.request(req, 'retrieve', {})
                total, papers, next = process_raw_papers(raw_papers)
                if len(papers) != 0:
                    util.save(file_name, papers, format)


def get_citations():
    not_found = []
    file_name = 'citations_papers.csv'
    papers = pd.read_csv('./papers/final_papers.csv')
    for index, row in papers.iterrows():
        paper_id = row['doi']
        if paper_id == '':
            paper_id = row['url']
        if paper_id != '':
            req = citations_url.replace('{paper_id}', str(paper_id))
            req = req.replace('<offset>', '0').replace('<max_papers>', str(max_papers))
            raw_citations = client.request(req, 'retrieve', {})
            if raw_citations == {}:
                not_found.append(row['title'])
                print(row['title'])
            papers, next = process_raw_citations(raw_citations)
            if len(papers) != 0:
                util.save(file_name, papers, format)
            else:
                not_found.append(row['title'])
                print(row['title'])
            while next != -1:
                time.sleep(5)
                print(str(next))
                req = citations_url.replace('{paper_id}', str(paper_id)).replace('<offset>', str(next))
                req = req.replace('<max_papers>', str(max_papers))
                raw_citations = client.request(req, 'retrieve', {})
                if raw_citations == {}:
                    not_found.append(row['title'])
                    print(row['title'])
                papers, next = process_raw_citations(raw_citations)
                if len(papers) != 0:
                    util.save(file_name, papers, format)
            time.sleep(5)
        else:
            not_found.append(row['title'])
            print(row['title'])
    util.save('not_found.csv', not_found, format)

def create_request(parameters):
    queries = []
    synonyms = parameters['synonyms'][parameters['domains'][0]]
    parameters['synonyms'] = []
    query = client.default_query(parameters)
    query = query.replace('%28', '').replace('+OR+', '+').replace('+AND+', '+').replace('%22', '').replace('%29', '')
    query = query.replace('<field>:', '')
    queries.append(query)
    for synonym in synonyms:
        parameters['domains'][0] = synonym
        query = client.default_query(parameters)
        query = query.replace('%28', '').replace('+OR+', '+').replace('+AND+', '+').replace('%22', '').replace('%29', '')
        query = query.replace('<field>:', '')
        queries.append(query)
    return queries


def process_raw_papers(raw_papers):
    raw_json = json.loads(raw_papers)
    total = raw_json['total']
    next = -1
    if 'next' in raw_json:
        next = raw_json['next']
    papers = pd.json_normalize(raw_json['data'])
    papers = papers.drop(columns=['externalIds.MAG', 'externalIds.DBLP', 'externalIds.PubMedCentral',
                                  'externalIds.PubMed', 'externalIds.ArXiv'], errors='ignore')
    papers['database'] = database
    return total, papers, next


def process_raw_citations(raw_citations):
    next = -1
    papers = {}
    if raw_citations != {}:
        raw_json = json.loads(raw_citations)
        if 'next' in raw_json:
            next = raw_json['next']
        papers = pd.json_normalize(raw_json['data'])
    return papers, next
