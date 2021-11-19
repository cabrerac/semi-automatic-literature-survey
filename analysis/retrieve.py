import pandas as pd
import re
import numpy as np
from clients import arxiv
from clients import ieeexplore
from clients import springer
from clients import elsevier
from analysis import util

f = 'utf-8'


def get_papers(domain, interests, keywords, synonyms, fields, types):

    #print("Requesting ArXiv for " + domain + " related papers...")
    #arxiv.get_papers(domain, interests, keywords, synonyms, fields, types)

    #print("Requesting Springer for " + domain + " related papers...")
    #springer.get_papers(domain, interests, keywords, synonyms, fields, types)

    #print("Requesting IEEE Xplore for " + domain + " related papers...")
    #ieeexplore.get_papers(domain, interests, keywords, synonyms, fields, types)

    print("Requesting Elsevier for " + domain + " related papers...")
    elsevier.get_papers(domain, interests, keywords, synonyms, fields, types)


def preprocess(domains, databases):
    papers = pd.DataFrame()
    for domain in domains:
        for database in databases:
            df = pd.read_csv('./papers/'+domain.lower().replace(' ', '_') + '_' + database + '.csv')
            if database == 'ieeexplore':
                df = df.drop_duplicates(subset=['doi'])
                dates = df['publication_date']
                df['publication_date'] = parse_dates(dates)
                papers_ieee = pd.DataFrame({'doi': df['doi'], 'type': df['content_type'],
                                            'publication': df['publication_title'], 'publisher': df['publisher'],
                                            'publication_date': df['publication_date'], 'database': df['database'],
                                            'title': df['title'], 'url': df['html_url'], 'abstract': df['abstract']})
                papers_ieee['domain'] = domain
                papers = papers.append(papers_ieee)
            if database == 'springer':
                df = df.drop_duplicates(subset=['doi'])
                dates = df['publicationDate']
                df['publication_date'] = parse_dates(dates)
                papers_springer = pd.DataFrame({'doi': df['doi'], 'type': df['contentType'],
                                                'publication': df['publicationName'], 'publisher': df['publisher'],
                                                'publication_date': df['publication_date'], 'database': df['database'],
                                                'title': df['title'], 'url': df['url'], 'abstract': df['abstract']})
                papers_springer['domain'] = domain
                papers = papers.append(papers_springer)
            if database == 'arxiv':
                df = df.drop_duplicates(subset=['id'])
                dates = df['published']
                df['publication_date'] = parse_dates(dates)
                papers_arxiv = pd.DataFrame({'doi': df['id'], 'type': df['database'], 'publication': df['database'],
                                            'publisher': df['database'], 'publication_date': df['publication_date'],
                                            'database': df['database'], 'title': df['title'], 'url': df['id'],
                                            'abstract': df['summary']})
                papers_arxiv['domain'] = domain
                papers = papers.append(papers_arxiv)
            if database == 'sciencedirect':
                df = df.drop_duplicates(subset=['id'])
                papers_sciencedirect = pd.DataFrame({'doi': df['id'], 'type': df['type'], 'publication': df['publication'],
                                            'publisher': df['publisher'], 'publication_date': df['publication_date'],
                                            'database': df['database'], 'title': df['title'], 'url': df['url'],
                                            'abstract': df['abstract'], 'domain': df['domain']})
                papers = papers.append(papers_sciencedirect)
            if database == 'scopus':
                df = df.drop_duplicates(subset=['id'])
                papers_scopus = pd.DataFrame(
                    {'doi': df['id'], 'type': df['type'], 'publication': df['publication'],
                     'publisher': df['publisher'], 'publication_date': df['publication_date'],
                     'database': df['database'], 'title': df['title'], 'url': df['url'],
                     'abstract': df['abstract'], 'domain': df['domain']})
                papers = papers.append(papers_scopus)
    papers = papers.drop_duplicates(subset=['doi', 'title'])
    with open('./papers/preprocessed_papers.csv', 'a', newline='', encoding=format) as f:
        papers.to_csv(f, encoding=format, index=False, header=f.tell() == 0)


def parse_dates(dates):
    new_dates = []
    for date in dates:
        if date == '1 Aug1, 2021':
            print('')
        if len(date) == 4:
            date = '01/Jan/' + date
        if re.match('[A-z]+. [0-9]+', date):
            if '.' in date:
                date = '01/' + date.split('.')[0] + '/' + date.split('.')[1].replace(' ', '')
            else:
                date = '01/' + date.split(' ')[0] + '/' + date.split(' ')[1].replace(' ', '')
        if re.match('[A-z]+-[0-9]+', date):
            date = '01/' + date.split('-')[0] + '/' + date.split('-')[1]
        if re.match('[0-9]+-[0-9]+ [A-z]+. [0-9]+', date):
            date = date.split('-')[1]
            date = date.split(' ')[0] + '/' + date.split(' ')[1].split('.')[0] + '/' + date.split(' ')[2]
        if re.match('[0-9]+-[0-9]+ [A-z]+ [0-9]+', date):
            date = date.split('-')[1]
            date = date.split(' ')[0] + '/' + date.split(' ')[1] + '/' + date.split(' ')[2]
        if re.match('[0-9]+ [A-z]+-[0-9]+ [A-z]+. [0-9]+', date):
            date = date.split('-')[1].split(' ')[0] + '/' + date.split('-')[1].split(' ')[1] + '/' + \
                   date.split('-')[1].split(' ')[2]
        if re.match('[0-9]+ [A-z]+.-[0-9]+ [A-z]+. [0-9]+', date):
            date = date.split('-')[1].split(' ')[0] + '/' + date.split('-')[1].split(' ')[1] + '/' + \
                   date.split('-')[1].split(' ')[2]
        if re.match('[0-9]+ [A-z]+-[A-z]+. [0-9]+', date):
            date = '01/' + date.split('-')[1].split(' ')[0] + '/' + date.split('-')[1].split(' ')[1]
        if re.match('[0-9]+ [A-z]+.-[A-z]+. [0-9]+', date):
            date = '01/' + date.split('-')[1].split(' ')[0] + '/' + date.split('-')[1].split(' ')[1]
        if re.match('[0-9]+ [A-z]+[0-9]+, [0-9]+', date):
            sub = date.split(' ')[1]
            sub = sub.replace(',', '')
            r = re.sub('[0-9]+', '', sub)
            date = date.split(' ')[0] + '/' + r + '/' + date.split(' ')[2]
        if re.match('[0-9] [A-z]+[0-9], [0-9]+', date):
            sub = date.split(' ')[1]
            sub = sub.replace(',', '')
            r = re.sub('[0-9]+', '', sub)
            date = date.split(' ')[0] + '/' + r + '/' + date.split(' ')[2]
        if re.match('[0-9]+ [A-z]+[0-9]+, [0-9]+', date):
            sub = date.split(' ')[1]
            sub = sub.replace(',', '')
            r = re.sub('[0-9]+', '', sub)
            date = date.split(' ')[0] + '/' + r + '/' + date.split(' ')[2]
        if 'Firstquarter' in date:
            date = '01/Mar/' + date.split(' ')[1]
        if 'Secondquarter' in date:
            if '/Secondquarter/' in date:
                date = date.replace('Secondquarter', 'Jun')
            else:
                date = '01/Jun/' + date.split(' ')[1]
        if 'Thirdquarter' in date:
            date = '01/Sep/' + date.split(' ')[1]
        if 'thirdquarter' in date:
            date = date.replace('thirdquarter', 'Sep')
        if 'Fourthquarter' in date:
            if '/Fourthquarter/' in date:
                date = date.replace('Fourthquarter', 'Dec')
            else:
                date = '01/Dec/' + date.split(' ')[1]
        date = date.replace('.', '')
        date = pd.to_datetime(date)
        new_dates.append(date)
    return new_dates


def filter_papers(abstract, keywords):
    preprocessed_papers = pd.read_csv('./papers/preprocessed_papers.csv')
    preprocessed_papers['type'] = 'preprocessed'
    filtered_papers = filter_by_field(preprocessed_papers, 'abstract', keywords)
    filtered_papers['type'] = 'filtered'
    util.save('filtered_papers.csv', filtered_papers, f)


def filter_by_field(papers, field, keywords):
    papers[field].replace('', np.nan, inplace=True)
    papers.dropna(subset=[field], inplace=True)
    filtered_papers = []
    for keyword in keywords:
        if len(filtered_papers) == 0:
            filtered_papers = papers[papers[field].str.contains(keyword)]
        else:
            filtered_papers = filtered_papers.append(papers[papers[field].str.contains(keyword)])
    if len(filtered_papers) > 0:
        filtered_papers = filtered_papers.drop_duplicates(subset=['doi'])
    return filtered_papers
