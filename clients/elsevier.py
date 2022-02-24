from elsapy.elsclient import ElsClient
import numpy as np
import pandas as pd
from elsapy.elssearch import ElsSearch
import json
from os.path import exists
from analysis import util
from .apis.generic import Generic


databases = ['scopus']
client_fields = {}
client_fields['scopus'] = {'title': 'TITLE-ABS-KEY'}
format = 'utf-8'

con_file = open('config-elsevier.json')
config = json.load(con_file)
con_file.close()
client = ElsClient(config['apikey'])
clientG = Generic()
api_url = 'https://api.elsevier.com/content/<type>/'


def get_papers(domain, interests, keywords, synonyms, fields, types, since, to, file_name):
    reqs = []
    for database in databases:
        c_fields = []
        for field in fields:
            if field in client_fields[database]:
                c_fields.append(client_fields[database][field])
        parameters = {'domains': [domain], 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                      'fields': c_fields, 'types': types}
        reqs.append(create_request(database, parameters))
    for req in reqs:
        file_name = 'domains/' + file_name + '_' + domain.replace(' ', '_') + '_' + req[1] + '_' + str(to).replace('-', '') + '_raw.csv'
        if not exists('./papers/' + file_name):
            print('Getting papers from: ' + req[1])
            print('Request length: ' + str(len(req[0])))
            doc_srch = ElsSearch(req[0], req[1])
            doc_srch.execute(client, get_all=True)
            print("doc_srch has", len(doc_srch.results), "results.")
            results = doc_srch.results_df
            results = results[(results['prism:coverDate'] >= str(since)) & (results['prism:coverDate'] <= str(to))]
            util.save(file_name, results, format)


def create_request(database, parameters):
    req = []
    query = clientG.default_query(parameters)
    query = query.replace('%28', '(').replace('%29', ')')
    query = query.replace(':%22', '(').replace('%22+', ') ').replace('+', ' ')
    query = query.replace('%22', ')')
    req.append(query)
    req.append(database)
    return req


def process_raw_papers(domain, file_name, to):
    for database in databases:
        file_name = 'domains/' + file_name + '_' + domain.replace(' ', '_') + '_' + database + '_' + str(to).replace('-', '') + '_raw.csv'
        if exists('./papers/' + file_name):
            raw_papers = pd.read_csv('./papers/' + file_name)
            file_name_sciencedirect = file_name.replace('_raw.csv', '.csv').replace('scopus', 'sciencedirect')
            if not exists('./papers/'+file_name_sciencedirect):
                papers = pd.DataFrame(columns=['id', 'type', 'publication', 'publisher', 'publication_date', 'database', 'title',
                                        'url', 'abstract', 'domain'])
                papers['id'] = raw_papers['prism:doi']
                if database == 'scopus':
                    papers['type'] = raw_papers['prism:aggregationType']
                else:
                    papers['type'] = 'journal'
                papers['publication'] = raw_papers['prism:publicationName']
                papers['publisher'] = database
                papers['publication_date'] = raw_papers['prism:coverDate']
                papers['database'] = 'sciencedirect'
                papers['title'] = raw_papers['dc:title']
                links = []
                abstracts = []
                for index, paper in raw_papers.iterrows():
                    link = paper['link']
                    link = link.replace('\'', '"')
                    link = json.loads(link)
                    link = link['self']
                    links.append(link)
                    pii = str(paper['pii'])
                    doi = str(paper['prism:doi'])
                    abstract = get_abstract(pii, doi)
                    abstracts.append(abstract)
                papers['url'] = links
                papers['abstract'] = abstracts
                papers['domain'] = domain
                papers['abstract'].replace('', np.nan, inplace=True)
                papers.dropna(subset=['abstract'], inplace=True)
                file_name = file_name.replace('_raw.csv', '.csv').replace('scopus', 'sciencedirect')
                util.save(file_name, papers, format)


def get_abstract(pii, doi):
    abstract = ''
    if pii != 'nan':
        req = api_url.replace('<type>', 'article') + 'pii/' + pii + '?apiKey=' + config['apikey']
        result = clientG.request(req, 'get', {})
        if result != {}:
            abstract = parse_abstract(result)
    if abstract != '' and doi != 'nan':
        req = api_url.replace('<type>', 'article') + 'doi/' + doi + '?apiKey=' + config['apikey']
        result = clientG.request(req, 'get', {})
        if result != {}:
            abstract = parse_abstract(result)
    return abstract


def parse_abstract(xml):
    abstract = ''
    df = pd.read_xml(xml, namespaces={"dc": "http://purl.org/dc/elements/1.1/", "ce": "http://www.elsevier.com/xml/ani/common"})
    if 'description' in df:
        abstract = df['description'][0]
    if abstract == '':
        if '<ce:para>' in xml:
            abstract = xml.split('</ce:para>')[0].split('<ce:para>')[1]
    if abstract == '':
        if '<dc:description>' in xml:
            abstract.split('</dc:description>')[0].split('<dc:description>')[1]
    if abstract == '':
        print('here')
    return abstract
