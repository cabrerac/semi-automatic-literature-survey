import time

import pandas as pd
from .apis.generic import Generic
from os.path import exists
from analysis import util
from scholarly import scholarly
from scholarly import ProxyGenerator

client_fields = {'title': 'title'}
database = 'google-scholar'
format = 'utf-8'
client = Generic()

#pg = ProxyGenerator()
#success = pg.SingleProxy(https=client.get_proxy('proxies.txt'), http=client.get_proxy('proxies.txt'))
#scholarly.use_proxy(pg)


def get_papers(domain, interests, keywords, synonyms, fields, types):
    file_name = 'domains/' + domain.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists('./papers/' + file_name):
        c_fields = []
        for field in fields:
            if field in client_fields:
                c_fields.append(client_fields[field])
        parameters = {'domains': [domain], 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                      'fields': c_fields, 'types': types}
        req = create_request(parameters)
        search_query = scholarly.search_pubs(req)
        print(str(search_query.total_results))
        papers = process_raw_papers(search_query, file_name.replace('.csv', '_temp.csv'))
        util.save(file_name, papers, format)


def create_request(parameters):
    req = client.default_query(parameters)
    req = req.replace('%28', '(').replace('%29', ')').replace('%22', '"').replace('title:', '').replace('+', ' ')
    return req


def process_raw_papers(search_query, file_name):
    processed = 0
    papers_list = []
    for pub in search_query:
        if 'bib' in pub:
            bib = pub['bib']
            if 'abstract' in bib:
                paper = {}
                if 'bib_id' in bib:
                    paper['id'] = bib['bib_id']
                else:
                    paper['id'] = 'google-scholar' + str(processed)
                if 'pub_year' in bib:
                    paper['published'] = bib['pub_year']
                else:
                    paper['published'] = ''
                if 'venue' in bib:
                    paper['publisher'] = bib['venue']
                else:
                    paper['publisher'] = ''
                if 'journal' in bib:
                    paper['publication'] = bib['journal']
                else:
                    paper['publication'] = ''
                if 'pub_type' in bib:
                    paper['type'] = bib['pub_type']
                else:
                    paper['type'] = ''
                if 'title' in bib:
                    paper['title'] = bib['title']
                else:
                    paper['title'] = ''
                paper['abstract'] = bib['abstract']
                paper['database'] = database
                papers_list.append(paper)
        processed = processed + 1
        papers = pd.DataFrame(papers_list, columns=['id', 'published', 'publisher', 'publication', 'type', 'title',
                                                    'abstract', 'database'])
        print('Processed :: ' + str(processed), end="\r")
    return papers
