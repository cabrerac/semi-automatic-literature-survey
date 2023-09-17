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
        papers = request_papers(query, parameters)
        papers = filter_papers(papers, dates, since, to)
        papers = clean_papers(papers)
        if len(papers) > 0:
            util.save(file_name, papers, f)
            print("Retrieved papers after filters: " + str(len(papers)))
    else:
        print("File already exists.")


def create_request(parameters):
    req = api_url
    req = req + client.default_query(parameters)
    req = req + '&start=' + str(start)
    req = req + '&max_results='+str(max_papers)
    req = req + '&sortBy=submittedDate&sortOrder=descending'
    return req


def request_papers(query, parameters):
    retrieved = 0
    papers = pd.DataFrame()
    request = create_request(parameters)
    raw_papers = client.request(request, 'get', {})
    expected_papers = get_expected_papers(raw_papers)
    times = int(expected_papers / max_papers) - 1
    mod = int(expected_papers) % max_papers
    if mod > 0:
        times = times + 1
    for t in range(0, times + 1):
        time.sleep(waiting_time)
        global start
        start = t * max_papers
        request = create_request(parameters)
        raw_papers = client.request(request, 'get', {})
        # if there is an exception from the API, retry request
        retry = 0
        while isinstance(raw_papers, dict) and retry < max_retries:
            time.sleep(waiting_time)
            retry = retry + 1
            print("Retrieved papers: " + str(retrieved) + "/" + str(expected_papers) + ' ::: ' +
                  str(int((retrieved / expected_papers) * 100)) + '% ::: Exception from API: ' +
                  raw_papers['exception'] + " ::: Retry " + str(retry) + "/" + str(max_retries) + "...",
                  end="\r")
            raw_papers = client.request(request, 'get', {})
        if not isinstance(raw_papers, dict):
            papers_request = process_raw_papers(query, raw_papers)
            # sometimes the arxiv API does not respond with all the papers, so we request again
            expected_per_request = expected_papers
            if expected_papers > max_papers:
                expected_per_request = max_papers
            if t == times and mod > 0:
                expected_per_request = mod
            while len(papers_request) < expected_per_request:
                time.sleep(waiting_time)
                raw_papers = client.request(request, 'get', {})
                if not isinstance(raw_papers, dict):
                    papers_request = process_raw_papers(query, raw_papers)
            retrieved = retrieved + len(papers_request)
            if len(papers) == 0:
                papers = papers_request
            else:
                papers = papers.append(papers_request)
                papers = util.remove_repeated_df(papers)
            if retrieved > expected_papers:
                retrieved = expected_papers
            print("Retrieved papers: " + str(retrieved) + "/" + str(expected_papers) + ' ::: ' + str(
                int((retrieved / expected_papers) * 100))
                  + '% ...', end="\r")
        else:
            print("Retrieved papers: " + str(retrieved) + "/" + str(expected_papers) + ' ::: ' + str(
                int((retrieved / expected_papers) * 100)) + '% ::: Exception from API: ' + raw_papers['exception'] +
                  " ::: Skipping to next batch...", end="\r")
    return papers


def get_expected_papers(raw_papers):
    total_text = raw_papers.split('opensearch:totalResults')[1]
    total = int(total_text.split('>')[1].replace('</', ''))
    print('Total papers found in arxiv: ' + str(total))
    return total


def process_raw_papers(query, raw_papers):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    total_text = raw_papers.split('opensearch:totalResults')[1]
    total = int(total_text.split('>')[1].replace('</', ''))
    if total > 0:
        try:
            papers_request = pd.read_xml(raw_papers, xpath='//feed:entry', namespaces={"feed": "http://www.w3.org/2005/Atom"})
            papers_request['database'] = database
            papers_request['query_name'] = query_name
            papers_request['query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
        except:
            papers_request = pd.DataFrame()
    return papers_request


def filter_papers(papers, dates, since, to):
    if dates is True:
        print('Applying dates filters...')
        papers = papers[(papers['published'] >= str(since)) & (papers['published'] <= str(to))]
    nan_value = float("NaN")
    papers.replace('', nan_value, inplace=True)
    papers.dropna(how='all', axis=1, inplace=True)
    return papers


def clean_papers(papers):
    papers = papers.drop(columns=['author', 'comment', 'link', 'primary_category', 'category', 'doi',
                                  'journal_ref'], errors='ignore')
    return papers
