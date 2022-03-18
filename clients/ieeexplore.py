import time
from .apis.xploreapi import XPLORE
from .apis.generic import Generic
import json
import pandas as pd
from os.path import exists
from analysis import util

api_access = 'xmknyxp8j436aun5c5tj7g75'
max_papers = 200
client_fields = {'title': 'article_title', 'abstract': 'abstract'}
client_types = {'books': 'Books', 'courses':'Courses', 'conferences': 'Conferences',
              'early access': 'Early Access', 'journals': 'Journals', 'standards': 'Standards'}
database = 'ieeexplore'
format = 'utf-8'
client = Generic()


def get_papers(domain, interests, keywords, synonyms, fields, types, dates, since, to, file_name, search_date):
    file_name = './papers/' + file_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                + domain.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists(file_name):
        c_fields = []
        for field in fields:
            if field in client_fields:
                c_fields.append(client_fields[field])
        c_types = []
        for t in types:
            if t in client_types:
                c_types.append(client_types[t])
        parameters = {'domains': [domain], 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                      'fields': c_fields, 'types': c_types}
        queries = create_request(parameters, 'domains')
        total_requests = len(queries) * len(c_fields) * len(c_types)
        print(str(total_requests))
        current_request = 1
        for query in queries:
            for field in c_fields:
                for t in c_types:
                    print(str(current_request))
                    papers = []
                    start_record = 1
                    raw_papers = request(query, field, t, start_record, dates, str(since.year))
                    if raw_papers != {}:
                        total, papers = process_raw_data(raw_papers, papers)
                        if total > max_papers:
                            while len(papers) < total:
                                start_record = start_record + max_papers
                                raw_papers = request(query, field, t, start_record, dates, str(since.year))
                                if raw_papers != {}:
                                    total, papers = process_raw_data(raw_papers, papers)
                        if len(papers) > 0:
                            util.save(file_name, papers, format)
                    time.sleep(10)
                    current_request = current_request + 1


def create_request(parameters, first_parameter):
    queries = client.ieeexplore_query(parameters, first_parameter)
    return queries


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


def process_raw_data(raw_papers, papers):
    raw_json = json.loads(raw_papers)
    total = raw_json['total_records']
    if 'articles' not in raw_json:
        return len(papers), papers
    temp_papers = pd.json_normalize(raw_json['articles'])
    if len(papers) == 0:
        papers = temp_papers[['doi', 'title', 'publisher', 'content_type', 'abstract', 'html_url', 'publication_title',
                              'publication_date']]
    else:
        papers = papers.append(temp_papers[['doi', 'title', 'publisher', 'content_type', 'abstract', 'html_url',
                                            'publication_title', 'publication_date']])
    papers = papers.drop_duplicates(subset=['doi'])
    papers['database'] = database
    return total, papers
