import time
from .apis.xploreapi import XPLORE
from .apis.generic import Generic
import json
import pandas as pd
from os.path import exists
from analysis import util

api_access = 'xmknyxp8j436aun5c5tj7g75'
start = 0
max_papers = 200
client_fields = {'title': 'article_title', 'abstract': 'abstract'}
client_types = {'conferences': 'Conferences',
              'early access': 'Early Access', 'journals': 'Journals', 'standards': 'Standards'}
database = 'ieeexplore'
f = 'utf-8'
client = Generic()
waiting_time = 10
max_retries = 3


def get_papers(query, synonyms, fields, types, dates, since, to, folder_name, search_date):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + query_name.lower().replace(' ', '_') + '_' + database + '.csv'
    global start
    start = 0
    if not exists(file_name):
        c_fields = []
        for field in fields:
            if field in client_fields:
                c_fields.append(client_fields[field])
        c_types = []
        for t in types:
            if t in client_types:
                c_types.append(client_types[t])
        parameters = {'query': query_value, 'synonyms': synonyms, 'fields': c_fields, 'types': types}
        reqs = create_request(parameters)
        print("Total number of IEEE Xplore requests = " + str(len(reqs)) + "...")
        current_request = 0
        for req in reqs:
            current_request = current_request + 1
            for c_field in c_fields:
                for c_type in c_types:
                    print("Requesting IEEE Xplore request " + str(current_request) + "/" + str(len(reqs)) +
                          " for field = " + c_field + " and type " + c_type + "...")
                    raw_papers = request(req, c_field, c_type, start, dates, str(since.year))
                    retry = 0
                    total = 0
                    retrieved = 0
                    while isinstance(raw_papers, dict) and retry < max_retries:
                        time.sleep(waiting_time)
                        retry = retry + 1
                        print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                              str(int((retrieved / total) * 100)) + '% ::: Exception from API: ' +
                              raw_papers['exception'] + " ::: Retry " + str(retry) + "/" + str(max_retries) + "...",
                              end="\r")
                        raw_papers = request(req, c_field, c_type, start, dates, str(since.year))
                    if not isinstance(raw_papers, dict):
                        total, papers = process_raw_papers(query, raw_papers)
                        retrieved = len(papers)
                    print('Total papers found: ' + str(total))
                    if retrieved > 0:
                        util.save(file_name, papers, f)
                    if total > 0:
                        print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                              str(int((retrieved / total) * 100)) + '% ...', end="\r")
                    else:
                        print("Papers not found!")
                    if total > max_papers:
                        times = int(total / max_papers) - 1
                        mod = int(total) % max_papers
                        if mod > 0:
                            times = times + 1
                        for t in range(1, times + 1):
                            time.sleep(waiting_time)
                            start = (max_papers * t)
                            raw_papers = request(req, c_field, c_type, start, dates, str(since.year))
                            retry = 0
                            while isinstance(raw_papers, dict) and retry < max_retries:
                                time.sleep(waiting_time)
                                retry = retry + 1
                                print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                                      str(int((retrieved / total) * 100)) + '% ::: Exception from API: ' +
                                      raw_papers['exception'] + " ::: Retry " + str(retry) + "/" + str(
                                    max_retries) + "...",
                                      end="\r")
                                raw_papers = request(req, c_field, c_type, start, dates, str(since.year))
                            if not isinstance(raw_papers, dict):
                                total, papers = process_raw_papers(query, raw_papers)
                                retrieved = retrieved + len(papers)
                                if retrieved > 0:
                                    util.save(file_name, papers, f)
                                    print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                                          str(int((retrieved / total) * 100)) + '% ...', end="\r")
                            else:
                                print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' + str(
                                    int((retrieved / total) * 100)) + '% ::: Exception from API: ' + raw_papers[
                                          'exception'] +
                                    " ::: Skipping to next batch...", end="\r")

                    if total > 0:
                        print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' + str(
                            int((retrieved / total) * 100))
                            + '%')
        if exists(file_name):
            total = util.remove_repeated_ieee(file_name)
            print("Total retrieved from IEEE: " + str(total))


def create_request(parameters):
    reqs = client.ieeexplore_query(parameters)
    return reqs


def request(query, field, t, start_record, dates, year):
    client_ieee = XPLORE(api_access)
    if dates is True:
        client_ieee.publicationYear(year)
    client_ieee.searchField(field, query)
    client_ieee.resultsFilter("content_type", t)
    client_ieee.startingResult(start_record)
    client_ieee.maximumResults(max_papers)
    raw_papers = client_ieee.callAPI()
    return raw_papers


def process_raw_papers(query, raw_papers):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    raw_json = json.loads(raw_papers)
    total = raw_json['total_records']
    if 'articles' not in raw_json:
        return 0, []
    temp_papers = pd.json_normalize(raw_json['articles'])
    papers = temp_papers[['doi', 'title', 'publisher', 'content_type', 'abstract', 'html_url', 'publication_title',
                              'publication_date']]
    papers = papers.drop_duplicates(subset=['doi'])
    papers['database'] = database
    papers['query_name'] = query_name
    papers['query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
    nan_value = float("NaN")
    papers.replace('', nan_value, inplace=True)
    papers.dropna(how='all', axis=1, inplace=True)
    return total, papers
