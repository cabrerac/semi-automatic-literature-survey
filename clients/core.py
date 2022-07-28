from .apis.generic import Generic
from os.path import exists
import pandas as pd
import json
from analysis import util
from analysis import retrieve
import time

api_access = 'pScrltW8j9MansPfbmA63OVZNCFeXo2T'
api_url = 'https://core.ac.uk:443/api-v2/articles/search?apiKey='+api_access
start = 1
max_papers = 100
client_fields = {'title': 'title', 'abstract': 'description'}
database = 'core'
f = 'utf-8'
client = Generic()
waiting_time = 5
max_retries = 3


def get_papers(query, synonyms, fields, types, dates, since, to, folder_name, search_date):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + query_name.lower().replace(' ', '_') + '_' + database + '.csv'
    global start
    start = 1
    if not exists(file_name):
        c_fields = []
        for field in fields:
            if field in client_fields:
                c_fields.append(client_fields[field])
        parameters = {'query': query_value, 'synonyms': synonyms, 'fields': c_fields, 'types': types}
        data = create_request(parameters)
        raw_papers = client.request(api_url, 'post', data)
        retry = 0
        total_first = 0
        retrieved = 0
        while isinstance(raw_papers, dict) and retry < max_retries:
            time.sleep(waiting_time)
            retry = retry + 1
            raw_papers = client.request(api_url, 'get', data)
        if not isinstance(raw_papers, dict):
            total_first, papers = process_raw_papers(query, raw_papers, dates, since, to)
            retrieved = len(papers)
        total = total_first
        print('Total papers found: ' + str(total))
        if retrieved > 0:
            util.save(file_name, papers, f)
        if total_first > 0:
            print("Retrieved papers: " + str(retrieved) + "/" + str(total_first) + ' ::: ' + str(int((retrieved / total) * 100))
                  + '% ...', end="\r")
        else:
            print("Papers not found!")
        if total > max_papers:
            times = int(total / max_papers) - 1
            mod = int(total) % max_papers
            if mod > 0:
                times = times + 1
                for t in range(1, times + 1):
                    time.sleep(waiting_time)
                    start = start + 1
                    data = create_request(parameters)
                    raw_papers = client.request(api_url, 'post', data)
                    retry = 0
                    while isinstance(raw_papers, dict) and retry < max_retries:
                        time.sleep(waiting_time)
                        retry = retry + 1
                        print("Retrieved papers: " + str(retrieved) + "/" + str(total_first) + ' ::: ' +
                              str(int((retrieved / total_first) * 100)) + '% ::: Exception from API: ' +
                              raw_papers['exception'] + " ::: Retry " + str(retry) + "/" + str(max_retries) + "...",
                              end="\r")
                        raw_papers = client.request(api_url, 'post', data)
                    if not isinstance(raw_papers, dict):
                        total, papers = process_raw_papers(query, raw_papers, dates, since, to)
                        retrieved = retrieved + len(papers)
                        if retrieved > total_first:
                            retrieved = total_first
                        if retrieved > 0:
                            util.save(file_name, papers, f)
                            print("Retrieved papers: " + str(retrieved) + "/" + str(total_first) + ' ::: ' +
                                  str(int((retrieved / total_first) * 100)) + '% ...', end="\r")
                    else:
                        print("Retrieved papers: " + str(retrieved) + "/" + str(total_first) + ' ::: ' + str(
                            int((retrieved / total_first) * 100)) + '% ::: Exception from API: ' + raw_papers['exception'] +
                              " ::: Skipping to next batch...", end="\r")
        if total_first > 0:
            print("Retrieved papers: " + str(retrieved) + "/" + str(total_first) + ' ::: '
                + str(int((retrieved / total_first) * 100)) + '%')
            print("Final numbers can vary as non-english papers are removed...")


def create_request(parameters):
    reqs = []
    req = {}
    query = client.core_query(parameters)
    req['query'] = query
    req['page'] = start
    req['pageSize'] = max_papers
    reqs.append(req)
    return reqs


def process_raw_papers(query, raw_papers, dates, since, to):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    raw_json = json.loads(raw_papers.content)
    total = raw_json[0]['totalHits']
    try:
        papers = pd.json_normalize(raw_json[0]['data'])
        papers = papers[(papers['language.code'] == 'en') | (papers['language.code'].isna())]
        papers = papers.drop(columns=['authors', 'contributors', 'identifiers', 'relations', 'repositories', 'subjects',
                                  'topics', 'types', 'year', 'oai', 'repositoryDocument.pdfStatus',
                                  'repositoryDocument.metadataAdded', 'repositoryDocument.metadataUpdated',
                                  'repositoryDocument.depositedDate', 'fulltextIdentifier', 'language.code',
                                  'language.id', 'language.name'], errors='ignore')
        papers['database'] = database
        papers['query_name'] = query_name
        papers['query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
        if 'datePublished' in papers.columns:
            if dates is True:
                papers = papers[(papers['datePublished'] >= str(since)) & (papers['datePublished'] <= str(to))]
            nan_value = float("NaN")
            papers.replace('', nan_value, inplace=True)
            papers.dropna(how='all', axis=1, inplace=True)
        else:
            papers = pd.DataFrame()
    except:
        papers = pd.DataFrame()
    return total, papers
