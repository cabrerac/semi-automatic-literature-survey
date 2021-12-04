from .apis.generic import Generic
from os.path import exists
import pandas as pd
import json
from analysis import util
from analysis import retrieve
import time

client = Generic()
database = 'project-academic'
api_access = '2c1fb09ccc2e4df99551514b4380a876'
api_url = 'https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate?expr=<query>&count=<max_papers>&' \
          'attributes=Ti,IA,D,BT,PB,DOI&subscription-key=' + api_access
max_papers = 10000
format = 'utf-8'


def get_papers(domain, interests, keywords, synonyms, fields, types):
    file_name = 'domains/' + domain.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists('./papers/' + file_name):
        parameters = {'domains': [domain], 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                      'types': types}
        query = create_request(parameters)
        req = api_url.replace('<query>', query).replace('<max_papers>', str(max_papers))
        raw_papers = client.request(req, 'retrieve', {})
        papers = process_raw_papers(raw_papers)
        if len(papers) != 0:
            papers = retrieve.filter_by_keywords(papers, keywords)
            util.save(file_name, papers, format)


def create_request(parameters):
    query = client.project_academic_query(parameters)
    return query


def process_raw_papers(raw_papers):
    raw_json = json.loads(raw_papers)
    p = pd.json_normalize(raw_json['entities'])
    entities = raw_json['entities']
    abstracts = []
    for entity in entities:
        abstracts.append(entity['IA'])
    papers = pd.DataFrame()
    papers['doi'] = p['DOI']
    papers['publication_date'] = p['D']
    papers['publication'] = p['BT']
    papers['publisher'] = p['PB']
    papers['title'] = p['Ti']
    papers['abstract'] = get_abstracts(abstracts)
    papers['database'] = database
    return papers


def get_abstracts(abstracts):
    abs = []
    for abstract in abstracts:
        length = abstract['IndexLength']
        abstract_dict = {}
        for index in range(0, length):
            abstract_dict[index] = get_word(abstract['InvertedIndex'], index)
        abstract_string = get_abstract_string(abstract_dict)
        abs.append(abstract_string)
    return abs


def get_word(inverted_index, index):
    for key, items in inverted_index.items():
        if index in items:
            return key


def get_abstract_string(abstract_dict):
    abstract_string = ''
    for key, item in abstract_dict.items():
        abstract_string = abstract_string + ' ' + str(item)
    return abstract_string
