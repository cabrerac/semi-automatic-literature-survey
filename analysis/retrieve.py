import pandas as pd
import re
import numpy as np
from datetime import date
from clients import arxiv
from clients import ieeexplore
from clients import springer
from clients import elsevier
from clients import core
from clients import semantic_scholar
from clients import project_academic
from analysis import util
from os.path import exists

fr = 'utf-8'


def get_papers(domains, interests, keywords, synonyms, fields, types, file_name, dates, since, to, search_date):
    for domain in domains:
        print("Requesting ArXiv for " + domain + " related papers...")
        arxiv.get_papers(domain, interests, keywords, synonyms, fields, types, dates, since, to, file_name, search_date)

        print("Requesting Springer for " + domain + " related papers...")
        springer.get_papers(domain, interests, keywords, synonyms, fields, types, dates, since, to, file_name, search_date)

        print("Requesting IEEE Xplore for " + domain + " related papers...")
        ieeexplore.get_papers(domain, interests, keywords, synonyms, fields, types, dates, since, to, file_name, search_date)

        print("Requesting Elsevier for " + domain + " related papers...")
        elsevier.get_papers(domain, interests, keywords, synonyms, fields, types, dates, since, to, file_name, search_date)
        # 2.1 Getting abstracts from elsevier
        print('2.1 Getting abstracts from Sciencedirect...')
        get_abstracts_elsevier(domain, file_name, to, search_date)

        print("Requesting CORE for " + domain + " related papers...")
        core.get_papers(domain, interests, keywords, synonyms, fields, types, dates, since, to, file_name, search_date)

        print("Requesting Semantic Scholar for " + domain + " related papers...")
        semantic_scholar.get_papers(domain, interests, keywords, synonyms, fields, types, dates, since, to, file_name, search_date)

        print("Requesting Microsoft Research for " + domain + " related papers...")
        project_academic.get_papers(domain, interests, keywords, synonyms, fields, types, dates,  since, to, file_name, search_date)


def get_abstracts_elsevier(domain, file_name, to, search_date):
    print('Domain: ' + domain)
    elsevier.process_raw_papers(domain, file_name, to, search_date)


def get_citations(folder_name, search_date, step):
    print("Requesting Semantic Scholar for papers citations...")
    semantic_scholar.get_citations(folder_name, search_date, step)


def preprocess(domains, databases, folder_name, search_date, since, to, step):
    papers = pd.DataFrame()
    for domain in domains:
        for database in databases:
            file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' + \
                        domain.lower().replace(' ', '_') + '_' + database.lower().replace('-', '_') + '.csv'
            if exists(file_name):
                print(file_name)
                df = pd.read_csv(file_name)
                if database == 'ieeexplore':
                    df = df.drop_duplicates('doi')
                    dates = df['publication_date']
                    df['publication_date'] = parse_dates(dates)
                    papers_ieee = pd.DataFrame(
                        {'doi': df['doi'], 'type': df['content_type'],
                        'publication': df['publication_title'], 'publisher': df['publisher'],
                        'publication_date': df['publication_date'], 'database': df['database'],
                        'title': df['title'], 'url': df['html_url'], 'abstract': df['abstract']}
                    )
                    papers_ieee['domain'] = domain
                    papers = papers.append(papers_ieee)
                if database == 'springer':
                    df = df.drop_duplicates('doi')
                    dates = df['publicationDate']
                    df['publication_date'] = parse_dates(dates)
                    papers_springer = pd.DataFrame(
                        {'doi': df['doi'], 'type': df['contentType'],
                        'publication': df['publicationName'], 'publisher': df['publisher'],
                        'publication_date': df['publication_date'], 'database': df['database'],
                        'title': df['title'], 'url': df['url'], 'abstract': df['abstract']}
                    )
                    papers_springer['domain'] = domain
                    papers = papers.append(papers_springer)
                if database == 'arxiv':
                    df = df.drop_duplicates('id')
                    dates = df['published']
                    df['publication_date'] = parse_dates(dates)
                    papers_arxiv = pd.DataFrame(
                        {'doi': df['id'], 'type': df['database'], 'publication': df['database'],
                        'publisher': df['database'], 'publication_date': df['publication_date'],
                        'database': df['database'], 'title': df['title'], 'url': df['id'],
                        'abstract': df['summary']}
                    )
                    papers_arxiv['domain'] = domain
                    papers = papers.append(papers_arxiv)
                if database == 'sciencedirect':
                    df = df.drop_duplicates('id')
                    papers_sciencedirect = pd.DataFrame(
                        {'doi': df['id'], 'type': df['type'], 'publication': df['publication'],
                        'publisher': df['publisher'], 'publication_date': df['publication_date'],
                        'database': df['database'], 'title': df['title'], 'url': df['url'],
                        'abstract': df['abstract']}
                    )
                    papers_sciencedirect['domain'] = domain
                    papers = papers.append(papers_sciencedirect)
                if database == 'scopus':
                    df = df.drop_duplicates('id')
                    papers_scopus = pd.DataFrame(
                        {'doi': df['id'], 'type': df['type'], 'publication': df['publication'],
                        'publisher': df['publisher'], 'publication_date': df['publication_date'],
                        'database': df['database'], 'title': df['title'], 'url': df['url'],
                        'abstract': df['abstract']}
                    )
                    papers_scopus['domain'] = domain
                    papers = papers.append(papers_scopus)
                if database == 'core':
                    df = df.drop_duplicates('id')
                    dates = df['datePublished']
                    df['publication_date'] = parse_dates(dates)
                    df['id'] = getIds(df, database)
                    papers_core = pd.DataFrame(
                        {'doi': df['id'], 'type': df['database'], 'publication': df['journals'],
                         'publisher': df['publisher'], 'publication_date': df['publication_date'],
                         'database': df['database'], 'title': df['title'], 'url': df['downloadUrl'],
                         'abstract': df['description']}
                    )
                    papers_core['domain'] = domain
                    papers_core['database'] = database
                    papers_core['publication'] = database
                    papers = papers.append(papers_core)
                if database == 'semantic-scholar':
                    df = df.drop_duplicates('paperId')
                    df_dates = df['year']
                    dates = []
                    for df_date in df_dates:
                        df_date = str(df_date).split('.')[0]
                        dates.append(df_date)
                    df['publication_date'] = parse_dates(dates)
                    df['id'] = getIds(df, database)
                    papers_semantic = pd.DataFrame(
                        {'doi': df['id'], 'type': df['database'], 'publication': df['database'],
                         'publisher': df['venue'], 'publication_date': df['publication_date'],
                         'database': df['database'], 'title': df['title'], 'url': df['url'],
                         'abstract': df['abstract']}
                    )
                    papers_semantic['domain'] = domain
                    papers = papers.append(papers_semantic)
                if database == 'project-academic':
                    dates = df['publication_date']
                    df['publication_date'] = parse_dates(dates)
                    df['id'] = getIds(df, database)
                    papers_academic = pd.DataFrame(
                        {'doi': df['id'], 'type': df['database'], 'publication': df['database'],
                         'publisher': df['publisher'], 'publication_date': df['publication_date'],
                         'database': df['database'], 'title': df['title'], 'url': df['database'],
                         'abstract': df['abstract']}
                    )
                    papers_academic['domain'] = domain
                    papers = papers.append(papers_academic)
    papers['title'] = papers['title'].str.lower()
    papers = papers.drop_duplicates('title')
    papers['doi'] = papers['doi'].str.lower()
    papers['doi'].replace(r'\s+', 'nan', regex=True)
    nan_doi = papers.loc[papers['doi'] == 'nan']
    papers = papers.drop_duplicates('doi')
    papers = papers.append(nan_doi)
    papers['type'] = 'preprocessed'
    papers['abstract'].replace('', np.nan, inplace=True)
    papers.dropna(subset=['abstract'], inplace=True)
    papers['title'].replace('', np.nan, inplace=True)
    papers.dropna(subset=['title'], inplace=True)
    with open('./papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) +
              '_preprocessed_papers.csv', 'a+', newline='', encoding=fr) as f:
        papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def getIds(df, database):
    ids = []
    for index, row in df.iterrows():
        if 'doi' in row:
            if len(str(row['doi']).strip()) > 0:
                ids.append(str(row['doi']))
            else:
                if database == 'core':
                    ids.append(str(row['id']))
                if database == 'semantic-scholar':
                    ids.append(str(row['paperId']))
                if database == 'project-academic':
                    ids.append(database + '-' + str(index))
        else:
            if database == 'core':
                ids.append(str(row['id']))
            if database == 'semantic-scholar':
                ids.append(str(row['paperId']))
            if database == 'project-academic':
                ids.append(database + '-' + str(index))
    return ids


def parse_dates(dates):
    new_dates = []
    for date in dates:
        date = str(date)
        #print(date)
        date = date.replace('[', '').replace(']', '').replace('Issued on: ', '').replace('[[issued]]', '').replace('issued', '')
        date = date.replace('First Quarter ', '')
        date = date.split('T')[0]
        if date == '10000-01-01' or date == '0':
            date = '2000'
        if date == '2021.0':
            date = '2021'
        if len(date) == 4:
            if int(date) < 1900 or int(date) > 2022:
                date = '2000'
            date = '01/Jan/' + date
        if re.match('[A-z]+. [0-9]+, [0-9]+', date):
            date = '01/Jan/' + date.split(',')[1].replace(' ', '')
        if re.match('[A-z]+-[A-z]+ [0-9]+', date):
            date = '01/' + date.split('-')[0] + '/' + date.split(' ')[1]
        if re.match('[A-z]+.-[A-z]+. [0-9]+', date):
            date = date.replace('.', '')
            date = '01/' + date.split('-')[0] + '/' + date.split(' ')[1]
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


def filter_papers(keywords, folder_name, search_date, step):
    to_filter = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + \
                '_preprocessed_papers.csv';
    preprocessed_papers = pd.read_csv(to_filter)
    preprocessed_papers.dropna(subset=["abstract"], inplace=True)
    filtered_papers = filter_by_keywords(preprocessed_papers, keywords)
    filtered_papers['type'] = 'filtered'
    with open('./papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) +
              '_syntactic_filtered_papers.csv', 'a+', newline='', encoding=fr) as f:
        filtered_papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


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


def filter_by_keywords(papers, keywords):
    papers = papers.dropna(subset=['abstract'])
    filtered_papers = []
    if len(keywords) == 0:
        papers = papers.drop_duplicates('title')
        papers['id'] = list(range(1, len(papers) + 1))
        return papers
    for keyword in keywords:
        if not isinstance(keyword, str):
            keys = keyword.keys()
            for key in keys:
                terms = keyword[key]
                s = ''
                for term in terms:
                    s = s + r'(?:\s|^)' + term.replace('-', '').lower() + '(?:\s|$)|'
                s = s[:-1]
                """papers['abstract_lower'] = papers['abstract'].str.lower()
                papers['abstract_lower'] = papers['abstract_lower'].str.replace('-', ' ')"""
                terms_papers = papers.loc[papers['abstract'].str.contains(s)]
                if len(filtered_papers) == 0:
                    filtered_papers = terms_papers.loc[terms_papers['abstract'].str.contains(r'(?:\s|^)' + key.replace('-', '').lower() + '(?:\s|$)')]
                    #filtered_papers = terms_papers.loc[terms_papers['abstract_lower'].str.contains(key.replace('-', ' ').lower())]
                else:
                    filtered_papers = filtered_papers.append(terms_papers.loc[terms_papers['abstract'].str.contains(r'(?:\s|^)' + key.replace('-', '').lower() + '(?:\s|$)')])
                    #filtered_papers = filtered_papers.append(terms_papers.loc[terms_papers['abstract_lower'].str.contains(key.replace('-', ' ').lower())])
        else:
            #papers['abstract_lower'] = papers['abstract'].str.lower()
            if len(filtered_papers) == 0:
                #filtered_papers = papers.loc[papers['abstract_lower'].str.contains(keyword.lower())]
                filtered_papers = papers.loc[papers['abstract'].str.contains(keyword.lower())]
            else:
                #filtered_papers = filtered_papers.append(papers.loc[papers['abstract_lower'].str.contains(keyword.lower())])
                filtered_papers = filtered_papers.append(papers.loc[papers['abstract'].str.contains(keyword.lower())])
    #filtered_papers = filtered_papers.drop(['abstract_lower'], axis=1)
    filtered_papers = filtered_papers.drop_duplicates('title')
    filtered_papers['id'] = list(range(1, len(filtered_papers) + 1))
    return filtered_papers
