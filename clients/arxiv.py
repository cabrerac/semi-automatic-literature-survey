import time
import pandas as pd
from .apis.generic import Generic
from os.path import exists
from analysis import util


api_url = 'http://export.arxiv.org/api/query?search_query='
start = 0
max_papers = 5000
client_fields = {'title': 'ti', 'abstract': 'abs'}
database = 'arxiv'
f = 'utf-8'
client = Generic()
waiting_time = 10
max_retries = 3


def get_papers(query, synonyms, fields, types, dates, since, to, folder_name, search_date):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + query_name.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists(file_name):
        c_fields = []
        for field in fields:
            if field in client_fields:
                c_fields.append(client_fields[field])
        parameters = {'query': query_value, 'synonyms': synonyms, 'fields': c_fields, 'types': types}
        req = create_request(parameters)
        total = 0
        retrieved = 0
        raw_papers = client.request(req, 'get', {})
        if not isinstance(raw_papers, dict):
            total, papers = process_raw_papers(query, raw_papers, dates, since, to)
            retrieved = len(papers)
        print('Total papers found: ' + str(total))
        if retrieved > 0:
            papers = papers.drop(columns=['author', 'comment', 'link', 'primary_category', 'category', 'doi',
                                          'journal_ref'], errors='ignore')
            util.save(file_name, papers, f)
        if total > 0:
            print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' + str(int((retrieved / total) * 100))
                  + '% ...', end="\r")
        else:
            print("Papers not found!")
        global start
        start = 0
        if total > max_papers:
            times = int(total / max_papers) - 1
            mod = int(total) % max_papers
            if mod > 0:
                times = times + 1
            for t in range(1, times + 1):
                time.sleep(waiting_time)
                start = (max_papers * t)
                req = create_request(parameters)
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
                    total, papers = process_raw_papers(query, raw_papers, dates, since, to)
                    retrieved = retrieved + len(papers)
                    if retrieved > 0:
                        papers = papers.drop(
                            columns=['author', 'comment', 'link', 'primary_category', 'category', 'doi',
                                     'journal_ref'], errors='ignore')
                        util.save(file_name, papers, f)
                        print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                              str(int((retrieved / total) * 100)) + '% ...', end="\r")
                else:
                    print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' + str(
                        int((retrieved / total) * 100)) + '% ::: Exception from API: ' + raw_papers['exception'] +
                          " ::: Skipping to next batch...", end="\r")

        print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' + str(int((retrieved / total) * 100))
              + '%')


def create_request(parameters):
    req = api_url
    req = req + client.default_query(parameters)
    req = req + '&start=' + str(start)
    req = req + '&max_results='+str(max_papers)
    req = req + '&sortBy=submittedDate&sortOrder=descending'
    return req


def process_raw_papers(query, raw_papers, dates, since, to):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    total_text = raw_papers.split('opensearch:totalResults')[1]
    total = int(total_text.split('>')[1].replace('</', ''))
    papers = pd.read_xml(raw_papers, xpath='//feed:entry', namespaces={"feed": "http://www.w3.org/2005/Atom"})
    papers['database'] = database
    papers['query_name'] = query_name
    papers['query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
    if dates is True:
        papers = papers[(papers['published'] >= str(since)) & (papers['published'] <= str(to))]
    nan_value = float("NaN")
    papers.replace('', nan_value, inplace=True)
    papers.dropna(how='all', axis=1, inplace=True)
    return total, papers
