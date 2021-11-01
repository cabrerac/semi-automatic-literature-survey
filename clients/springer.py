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


def get_papers(domain, interests, keywords, synonyms, fields, types):
    c_fields = []
    papers = []
    for field in fields:
        if field in client_fields:
            c_fields.append(client_fields[field])
    parameters = {'domains': [domain], 'interests': interests, 'synonyms': synonyms,
                  'fields': c_fields, 'types': types}
    req = create_request(parameters)
    raw_papers = client.request(req, 'get', {})
    total, papers = process_raw_papers(raw_papers)
    file_name = domain.lower().replace(' ', '_') + '_' + database + '.csv'
    client.save(file_name, papers)
    print(str(total))
    if total > max_papers:
        times = int(total/max_papers) - 1
        mod = int(total) % max_papers
        if mod > 0:
            times = times + 1
        for t in range(1, times + 1):
            print(str(t))
            time.sleep(5)
            global start
            start = (max_papers * t)
            req = create_request(parameters)
            raw_papers = client.request(req, 'get', {})
            if raw_papers != {}:
                total, papers = process_raw_papers(raw_papers)
                file_name = domain.lower().replace(' ', '_') + '_' + database + '.csv'
                client.save(file_name, papers)


def create_request(parameters):
    req = api_url
    req = req + client.default_query(parameters)
    req = req + '&s='+str(start)+'&p='+str(max_papers)+'&api_key=' + api_access
    req = req.replace('%28', '(').replace('%29', ')').replace('+', '%20')
    return req


def process_raw_papers(raw_papers):
    papers = []
    json_results = json.loads(raw_papers)
    total = int(json_results['result'][0]['total'])
    if 'records' in json_results:
        df = pd.json_normalize(json_results['records'])
        if len(papers) == 0:
            papers = df
        else:
            papers = papers.append(df)
    papers = papers[papers['language'].str.contains('en')]
    urls = []
    for record in papers['url']:
        url = record[0]['value']
        urls.append(url)

    papers = papers.drop(columns=['url', 'creators', 'bookEditors', 'openaccess', 'printIsbn', 'electronicIsbn',
                        'isbn', 'genre', 'copyright', 'conferenceInfo', 'issn', 'eIssn', 'volume', 'publicationType',
                        'number', 'issueType', 'topicalCollection', 'startingPage', 'endingPage', 'language',
                        'journalId', 'printDate', 'coverDate', 'keyword'])
    papers['url'] = urls
    papers['database'] = database
    return total, papers
