from elsapy.elsclient import ElsClient
from elsapy.elsdoc import FullDoc, AbsDoc
import pandas as pd
from elsapy.elssearch import ElsSearch
import json
from os.path import exists
from analysis import util
from .apis.generic import Generic


databases = ['scopus']
format = 'utf-8'

con_file = open('config-elsevier.json')
config = json.load(con_file)
con_file.close()
client = ElsClient(config['apikey'])
clientG = Generic()
api_url = 'https://api.elsevier.com/content/<type>/'

n = 0
y = 0


def get_papers(domain, interests, keywords, synonyms, fields, types):
    reqs = []
    parameters = {'domains': [domain], 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                  'fields': fields, 'types': types}
    for database in databases:
        reqs.append(create_request(database, parameters))
    for req in reqs:
        file_name = domain.replace(' ', '_') + '_' + req[1] + '_raw.csv'
        if not exists('./papers/' + file_name):
            doc_srch = ElsSearch(req[0], req[1])
            doc_srch.execute(client, get_all=True)
            print("doc_srch has", len(doc_srch.results), "results.")
            results = doc_srch.results_df
            util.save(file_name, results, format)
        process_raw_papers(file_name, domain, req[1])


def create_request(database, parameters):
    req = []
    query = ''
    domains = parameters['domains']
    interests = parameters['interests']
    if database == 'sciencedirect':
        for domain in domains:
            query = '<field>(' + domain + ')'
        interests_query = ''
        for interest in interests:
            interests_query = interests_query + '<field>(' + interest + ') OR '
            synonyms = parameters['synonyms'][interest]
            for synonym in synonyms:
                interests_query = interests_query + '<field>(' + synonym + ') OR '
        query = query + ' AND (' + interests_query + ')'
        query = query.replace(' OR )', ')')
        query = query.replace('<field>', 'ALL')
    if database == 'scopus':
        for domain in domains:
            query = 'TITLE-ABS-KEY(' + domain + ')'
        interests_query = ''
        for interest in interests:
            interests_query = interests_query + 'TITLE-ABS-KEY(' + interest + ') OR '
            synonyms = parameters['synonyms'][interest]
            for synonym in synonyms:
                interests_query = interests_query + 'TITLE-ABS-KEY(' + synonym + ') OR '
        query = query + ' AND (' + interests_query + ')'
        query = query.replace(' OR )', ')')
    req.append(query)
    req.append(database)
    return req


def process_raw_papers(file_name, domain, database):
    raw_papers = pd.read_csv('./papers/' + file_name)
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
    papers['database'] = database
    papers['title'] = raw_papers['dc:title']
    links = []
    abstracts = []
    for index, paper in raw_papers.iterrows():
        link = paper['link']
        link = link.replace('\'', '"')
        link = json.loads(link)
        link = link['self']
        links.append(link)
        id = paper['dc:identifier']
        id = id.split(':')[1]
        pii = str(paper['pii'])
        doi = str(paper['prism:doi'])
        eid = str(paper['eid'])
        abstract = get_abstract(id, eid, doi, pii, database)
        if abstract == '':
            global n
            n = n + 1
            print(eid + ':: NO :: ' + str(n))
        if abstract != '':
            global y
            y = y + 1
            print(eid + ':: YES :: ' + str(y))
        abstracts.append(abstract)
    papers['url'] = links
    papers['abstract'] = abstracts
    papers['domain'] = domain
    file_name = file_name.replace('_raw.csv', '.csv')
    util.save(file_name, papers, format)


def get_abstract(id, eid, doi, pii, database):
    abstract = ''
    if eid != '':
        result = clientG.request(api_url.replace('<type>', 'article') + 'eid/' + eid + '?apiKey=' + config['apikey'],'get',{})
        abstract = parse_abstract(result, 0)
        if abstract == '':
            result = clientG.request(api_url.replace('<type>', 'abstract') + 'eid/' + eid + '?apiKey=' + config['apikey'], 'get', {})
            abstract = parse_abstract(result, 1)
    if doi != '' and abstract == '':
        result = clientG.request(api_url + 'doi/' + doi + '?apiKey=' + config['apikey'], 'get', {})
        abstract = parse_abstract(result, 0)
        if abstract == '':
            result = clientG.request(api_url.replace('<type>', 'abstract') + 'doi/' + doi + '?apiKey=' + config['apikey'], 'get', {})
            abstract = parse_abstract(result, 1)
    if pii != '' and abstract == '':
        result = clientG.request(api_url + 'pii/' + pii + '?apiKey=' + config['apikey'], 'get', {})
        abstract = parse_abstract(result, 0)
        if abstract == '':
            result = clientG.request(api_url.replace('<type>', 'abstract') + 'pii/' + pii + '?apiKey=' + config['apikey'], 'get', {})
            abstract = parse_abstract(result, 1)
    if id != '' and abstract == '':
        result = clientG.request(api_url.replace('<type>', 'abstract') + 'scopus_id/' + id + '?apiKey=' + config['apikey'], 'get', {})
        abstract = parse_abstract(result, 1)
    """if database == 'scopus':
        scp_doc = AbsDoc(scp_id=id)
        if scp_doc.read(client):
            if 'dc:description' in scp_doc.data['coredata']:
                abstract = scp_doc.data['coredata']['dc:description']
            if abstract == '':
                if 'pii' in scp_doc.data['coredata']:
                    pii = scp_doc.data['coredata']['pii']
                    pii_doc = FullDoc(sd_pii=pii)
                    if pii_doc.read(client):
                        if 'dc:description' in pii_doc.data['coredata']:
                            abstract = pii_doc.data['coredata']['dc:description']
            if abstract == '':
                if 'prism:doi' in scp_doc.data['coredata']:
                    doi = scp_doc.data['coredata']['prism:doi']
                    pii_doc = FullDoc(doi=doi)
                    if pii_doc.read(client):
                        if 'dc:description' in pii_doc.data['coredata']:
                            abstract = pii_doc.data['coredata']['dc:description']
            if abstract == '':
                if 'uri' in scp_doc:
                    uri = scp_doc.uri
                    pii_doc = FullDoc(uri=uri)
                    if pii_doc.read(client):
                        if 'dc:description' in pii_doc.data['coredata']:
                            abstract = pii_doc.data['coredata']['dc:description']
        else:
            if eid!= '':
                scp_doc = AbsDoc(eid=eid)
                if scp_doc.read(client):
                    if 'dc:description' in scp_doc.data['coredata']:
                        abstract = scp_doc.data['coredata']['dc:description']
            if pii != '':
                pii_doc = FullDoc(sd_pii=pii)
                if pii_doc.read(client):
                    if 'dc:description' in pii_doc.data['coredata']:
                        abstract = pii_doc.data['coredata']['dc:description']
            if abstract == '':
                if doi != '':
                    pii_doc = FullDoc(doi=doi)
                    if pii_doc.read(client):
                        if 'dc:description' in pii_doc.data['coredata']:
                            abstract = pii_doc.data['coredata']['dc:description']
        if abstract == '':
            global n
            n = n + 1
            print(str(n))
    else:
        pii_doc = FullDoc(sd_pii=id)
        if pii_doc.read(client):
            abstract = pii_doc.data['coredata']['dc:description']
            if abstract == '' or abstract == 'unknown':
                print('here')
        else:
            print('here')"""
    return abstract


def parse_abstract(xml, type):
    abstract = ''
    if xml != {} and type == 0:
        df = pd.read_xml(xml, namespaces={"dc": "http://purl.org/dc/elements/1.1/", "ce": "http://www.elsevier.com/xml/ani/common"})
        if 'description' in df:
            description = df['description']
            abstract = df['description'][0]
    if xml != {} and type == 1:
        if '<ce:para>' in xml:
            abstract = xml.split('</ce:para>')[0].split('<ce:para>')[1]
    return abstract
