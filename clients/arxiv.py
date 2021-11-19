import pandas as pd
from .apis.generic import Generic
from analysis import util


api_url = 'http://export.arxiv.org/api/query?search_query='
max = 5000
client_fields = {'title': 'ti', 'abstract': 'abs'}
database = 'arxiv'
format = 'utf-8'
client = Generic()


def get_papers(domain, interests, keywords, synonyms, fields, types):
    c_fields = []
    for field in fields:
        if field in client_fields:
            c_fields.append(client_fields[field])
    parameters = {'domains': [domain], 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                  'fields': c_fields, 'types': types}
    req = create_request(parameters)
    raw_papers = client.request(req, 'get', {})
    papers = process_raw_papers(raw_papers)
    papers = papers.drop(columns=['author', 'comment', 'link', 'primary_category', 'category', 'doi', 'journal_ref'])
    file_name = domain.lower().replace(' ', '_') + '_' + database + '.csv'
    util.save(file_name, papers, format)


def create_request(parameters):
    req = api_url
    req = req + client.default_query(parameters)
    req = req + '&max_results='+str(max)
    return req


def process_raw_papers(raw_papers):
    papers = pd.read_xml(raw_papers, xpath='//feed:entry', namespaces={"feed": "http://www.w3.org/2005/Atom"})
    papers['database'] = database
    return papers
