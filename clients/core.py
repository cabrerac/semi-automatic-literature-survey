from .apis.generic import Generic
from os.path import exists
import pandas as pd
import json
from analysis import util
from analysis import retrieve
import time

database = 'core'
api_access = 'pScrltW8j9MansPfbmA63OVZNCFeXo2T'
api_url = 'https://core.ac.uk:443/api-v2/articles/search?apiKey='+api_access
client_fields = {'title': 'title', 'abstract': 'description'}
max_papers = 100
page = 1
format = 'utf-8'

client = Generic()


def get_papers(domain, interests, keywords, synonyms, fields, types):
    file_name = 'domains/' + domain.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists('./papers/' + file_name):
        c_fields = []
        for field in fields:
            if field in client_fields:
                c_fields.append(client_fields[field])
        parameters = {'domains': [domain], 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                      'fields': c_fields, 'types': types}
        data = create_request(parameters)
        raw_papers = client.request(api_url, 'post', data)
        total, papers = process_raw_papers(raw_papers)
        if len(papers) != 0:
            util.save(file_name, papers, format)
        print(str(total))
        if total > max_papers:
            times = int(total / max_papers) - 1
            mod = int(total) % max_papers
            if mod > 0:
                times = times + 1
            for t in range(1, times + 1):
                print(str(t))
                time.sleep(5)
                global page
                page = page + 1
                data = create_request(parameters)
                raw_papers = client.request(api_url, 'post', data)
                if raw_papers != {}:
                    total, papers = process_raw_papers(raw_papers)
                    if len(papers) != 0:
                        util.save(file_name, papers, format)
        time.sleep(5)

def create_request(parameters):
    reqs = []
    req = {}
    query = client.core_query(parameters)
    req['query'] = query
    req['page'] = page
    req['pageSize'] = max_papers
    reqs.append(req)
    return reqs


def process_raw_papers(raw_papers):
    raw_json = json.loads(raw_papers.content)
    total = raw_json[0]['totalHits']
    papers = pd.json_normalize(raw_json[0]['data'])
    papers = papers.drop(columns=['authors', 'contributors', 'identifiers', 'relations', 'repositories', 'subjects',
                                  'topics', 'types', 'year', 'oai', 'repositoryDocument.pdfStatus',
                                  'repositoryDocument.metadataAdded', 'repositoryDocument.metadataUpdated',
                                  'repositoryDocument.depositedDate', 'fulltextIdentifier', 'language.code',
                                  'language.id', 'language.name'], errors='ignore')
    papers['database'] = database
    return total, papers
