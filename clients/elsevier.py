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


def get_papers(query, synonyms, fields, types, dates, since, to, folder_name, search_date):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    reqs = []
    for database in databases:
        c_fields = []
        for field in fields:
            if field in client_fields[database]:
                c_fields.append(client_fields[database][field])
        parameters = {'query': query_value, 'synonyms': synonyms, 'fields': c_fields, 'types': types}
        reqs.append(create_request(database, parameters))
    for req in reqs:
        file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                    + query_name.lower().replace(' ', '_') + '_' + database + '_metadata.csv'
        if not exists(file_name):
            doc_srch = ElsSearch(req[0], req[1])
            doc_srch.execute(client, get_all=True)
            total = retrieved = len(doc_srch.results)
            if total > 0:
                print("Retrieved papers: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                      str(int((retrieved / total) * 100)) + '%')
            else:
                print("Papers not found!")
            results = doc_srch.results_df
            if dates is True:
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


def process_raw_papers(query, folder_name, to, search_date):
    query_name = list(query.keys())[0]
    query_value = query[query_name]
    for database in databases:
        file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' \
                    + query_name.lower().replace(' ', '_') + '_' + database + '_metadata.csv'
        if exists(file_name):
            raw_papers = pd.read_csv(file_name)
            file_name_sciencedirect = file_name.replace('_metadata.csv', '.csv').replace('scopus', 'sciencedirect')
            if not exists(file_name_sciencedirect):
                papers = pd.DataFrame(
                    columns=['id', 'type', 'publication', 'publisher', 'publication_date', 'database', 'title',
                             'url', 'abstract', 'query_name', 'query_value'])
                papers['id'] = raw_papers['prism:doi']
                if database == 'scopus':
                    papers['type'] = raw_papers['prism:aggregationType']
                else:
                    papers['type'] = 'journal'
                papers['publication'] = raw_papers['prism:publicationName']
                papers['publisher'] = database
                papers['publication_date'] = raw_papers['prism:coverDate']
                papers['database'] = 'sciencedirect'
                papers['query_name'] = query_name
                papers['query_value'] = query_value.replace('&', 'AND').replace('Â¦', 'OR')
                papers['title'] = raw_papers['dc:title']
                links = []
                abstracts = []
                total = len(raw_papers.index)
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
                    retrieved = len(abstracts)
                    print("Request for abstract: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                          str(int((retrieved / total) * 100)) + '% ...', end="\r")
                print("Requested abstracts: " + str(retrieved) + "/" + str(total) + ' ::: ' +
                      str(int((retrieved / total) * 100)) + '%')
                papers['url'] = links
                papers['abstract'] = abstracts
                papers['abstract'].replace('', np.nan, inplace=True)
                papers.dropna(subset=['abstract'], inplace=True)
                nan_value = float("NaN")
                papers.replace('', nan_value, inplace=True)
                papers.dropna(how='all', axis=1, inplace=True)
                file_name = file_name.replace('_metadata.csv', '.csv').replace('scopus', 'sciencedirect')
                print("Total papers retrieved from science direct: " + str(len(papers.index)) + ".")
                util.save(file_name, papers, format)
                print("Final numbers can vary as papers without abstract are removed...")


def get_abstract(pii, doi):
    abstract = ''
    if pii != 'nan':
        req = api_url.replace('<type>', 'article') + 'pii/' + pii + '?apiKey=' + config['apikey']
        result = clientG.request(req, 'get', {})
        if result != {}:
            abstract = parse_abstract(result)
    if abstract == '' and doi != 'nan':
        req = api_url.replace('<type>', 'article') + 'doi/' + doi + '?apiKey=' + config['apikey']
        result = clientG.request(req, 'get', {})
        if result != {}:
            abstract = parse_abstract(result)
    return abstract


def parse_abstract(xml):
    abstract = ''
    try:
        df = pd.read_xml(xml, namespaces={"dc": "http://purl.org/dc/elements/1.1/",
                                          "ce": "http://www.elsevier.com/xml/ani/common"})
        if 'description' in df:
            abstract = df['description'][0]
        if abstract == '':
            if '<ce:para>' in xml:
                abstract = xml.split('</ce:para>')[0].split('<ce:para>')[1]
        if abstract == '':
            if '<dc:description>' in xml:
                abstract.split('</dc:description>')[0].split('<dc:description>')[1]
    except:
        abstract = ''
    return abstract
