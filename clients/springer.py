import time
import pandas as pd
import json
from .apis.generic import Generic
from os.path import exists
from analysis import retrieve
from analysis import util


api_url = 'http://api.springernature.com/meta/v2/json?q=<dates>'
api_access = 'd5a276d7379bb80d31ec868f7e34e386'
start = 0
max_papers = 100
client_fields = {'title': 'title', 'abstract': 'keyword'}
database = 'springer'
f = 'utf-8'
client = Generic()
waiting_time = 10
max_retries = 3


def get_papers(query, optionals, synonyms, fields, types, dates, since, to, folder_name, search_date):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    for optional in optionals:
        query_value = query_value.replace(optional, '')
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + query_name.lower().replace(' ', '_') + '_' + database + '.csv'
    global start
    start = 0
    if not exists(file_name):
        c_fields = []
        for field in fields:
            if field in client_fields:
                c_fields.append(client_fields[field])
        parameters = {'query': query_value, 'synonyms': synonyms, 'fields': c_fields, 'types': types}
        req = create_request(parameters, dates, since, to)
        raw_papers = client.request(req, 'get', {})
        retry = 0
        total = 0
        retrieved = 0
        while isinstance(raw_papers, dict) and retry < max_retries:
            time.sleep(waiting_time)
            retry = retry + 1
            raw_papers = client.request(req, 'get', {})
        if not isinstance(raw_papers, dict):
            total, papers = process_raw_papers(query, raw_papers)
            retrieved = len(papers)
        print('Total papers found: ' + str(total))
        if retrieved > 0:
            util.save(file_name, papers, f)
        if total > 0:
            print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' + str(int((retrieved / total) * 100))
                  + '% ...', end="\r")
        if total > max_papers:
            times = int(total/max_papers) - 1
            mod = int(total) % max_papers
            if mod > 0:
                times = times + 1
            for t in range(1, times + 1):
                time.sleep(waiting_time)
                start = (max_papers * t)
                req = create_request(parameters, dates, since, to)
                raw_papers = client.request(req, 'get', {})
                retry = 0
                while isinstance(raw_papers, dict) and retry < max_retries:
                    time.sleep(waiting_time)
                    retry = retry + 1
                    print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                          str(int((retrieved / total) * 100)) + '% ::: Exception from API: ' +
                          raw_papers['exception'] + " ::: Retry " + str(retry) + "/" + str(max_retries) + "...",
                          end="\r")
                    raw_papers = client.request(req, 'get', {})
                if not isinstance(raw_papers, dict):
                    total, papers = process_raw_papers(query, raw_papers)
                    retrieved = retrieved + len(papers)
                    if retrieved > 0:
                        util.save(file_name, papers, f)
                        print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                              str(int((retrieved / total) * 100)) + '% ...', end="\r")
                else:
                    print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' + str(
                        int((retrieved / total) * 100)) + '% ::: Exception from API: ' + raw_papers['exception'] +
                          " ::: Skipping to next batch...", end="\r")
        if total > 0:
            print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' + str(int((retrieved / total) * 100))
                  + '%')
            print("Final numbers can vary as papers without abstract are removed...")
        else:
            print("Retrieved papers: " + str(retrieved))


def create_request(parameters, dates, since, to):
    req = api_url
    if dates is True:
        req = req.replace('<dates>', '(onlinedatefrom:' + str(since) +'%20onlinedateto:' + str(to) + ')')
    else:
        req = req.replace('<dates>', '')
    req = req + client.default_query(parameters)
    req = req + '&s='+str(start)+'&p='+str(max_papers)+'&api_key=' + api_access
    req = req.replace('%28', '(').replace('%29', ')').replace('+', '%20')
    return req


def process_raw_papers(query, raw_papers):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    if not isinstance(raw_papers, dict):
        papers = []
        json_results = json.loads(raw_papers)
        total = int(json_results['result'][0]['total'])
        if total > 0:
            if 'records' in json_results:
                df = pd.json_normalize(json_results['records'])
                if len(papers) == 0:
                    papers = df
                else:
                    papers = papers.append(df)
            if 'language' in papers:
                papers = papers[papers['language'].str.contains('en')]
            urls = []
            if 'url' in papers:
                for record in papers['url']:
                    url = record[0]['value']
                    if "[{'snippet-format':" in url:
                        print('here')
                    urls.append(url)
            papers = papers.drop(columns=['url', 'creators', 'bookEditors', 'openaccess', 'printIsbn', 'electronicIsbn',
                            'isbn', 'genre', 'copyright', 'conferenceInfo', 'issn', 'eIssn', 'volume', 'publicationType',
                            'number', 'issueType', 'topicalCollection', 'startingPage', 'endingPage', 'language',
                            'journalId', 'printDate', 'response', 'onlineDate', 'coverDate', 'keyword'], errors='ignore')
            if len(urls) > 0:
                papers['url'] = urls
            else:
                papers['url'] = ''
            papers['database'] = database
            papers['query_name'] = query_name
            papers['query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
            nan_value = float("NaN")
            papers.replace("", nan_value, inplace=True)
            papers.dropna(how='all', axis=1, inplace=True)
            if len(papers) > 0:
                return total, papers
            else:
                return 0, {}
    return 0, {}
