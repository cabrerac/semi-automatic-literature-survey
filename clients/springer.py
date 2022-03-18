import time
import pandas as pd
import json
from .apis.generic import Generic
from os.path import exists
from analysis import retrieve
from analysis import util


api_url = 'http://api.springernature.com/meta/v2/json?q=<dates>'
api_access = 'd5a276d7379bb80d31ec868f7e34e386'
start = 1
max_papers = 100
client_fields = {'title': 'title', 'abstract': 'keyword'}
database = 'springer'
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
        parameters = {'domains': [domain], 'interests': interests, 'synonyms': synonyms,
                    'fields': c_fields, 'types': types}
        req = create_request(parameters, dates, since, to)
        raw_papers = client.request(req, 'get', {})
        total, papers = process_raw_papers(raw_papers)
        if len(papers) != 0:
            #if len(keywords) > 0:
                #papers = retrieve.filter_by_keywords(papers, keywords)
            util.save(file_name, papers, format)
        print(str(total))
        if total > max_papers:
            times = int(total/max_papers) - 1
            mod = int(total) % max_papers
            if mod > 0:
                times = times + 1
            for t in range(1, times + 1):
                print(str(t))
                time.sleep(5)
                global start
                start = (max_papers * t)
                req = create_request(parameters, dates, since, to)
                raw_papers = client.request(req, 'get', {})
                if raw_papers != {}:
                    total, papers = process_raw_papers(raw_papers)
                    if len(papers) != 0:
                        #papers = retrieve.filter_by_keywords(papers, keywords)
                        util.save(file_name, papers, format)


def create_request(parameters, dates, since, to):
    req = api_url
    if dates is True:
        req = req.replace('<dates>', '(onlinedatefrom:'+ str(since) +'%20onlinedateto:' + str(to) + ')')
    else:
        req = req.replace('<dates>', '')
    req = req + client.default_query(parameters)
    req = req + '&s='+str(start)+'&p='+str(max_papers)+'&api_key=' + api_access
    req = req.replace('%28', '(').replace('%29', ')').replace('+', '%20')
    return req


def process_raw_papers(raw_papers):
    if raw_papers != {}:
        papers = []
        json_results = json.loads(raw_papers)
        total = int(json_results['result'][0]['total'])
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
                        'journalId', 'printDate', 'response', 'coverDate', 'keyword'], errors='ignore')
        if len(urls) > 0:
            papers['url'] = urls
        else:
            papers['url'] = ''
        papers['database'] = database
        nan_value = float("NaN")
        papers.replace("", nan_value, inplace=True)
        papers.dropna(how='all', axis=1, inplace=True)
        if len(papers) > 0:
            if 'abstract' in papers:
                papers.drop(papers.index[papers['abstract'] == ''], inplace=True)
            if 'onlineDate' not in papers and 'publicationDate' in papers:
                papers['onlineDate'] = papers['publicationDate']
            return total, papers
        else:
            return 0, {}
    return 0, {}
