import pandas as pd
import re
import numpy as np
from . import util
from clients import arxiv
from clients import ieeexplore
from clients import springer
from clients import elsevier
from clients import core
from clients import semantic_scholar
from os.path import exists
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import strip_tags
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import WhitespaceTokenizer

fr = 'utf-8'
lemma = WordNetLemmatizer()
w_tokenizer = WhitespaceTokenizer()


def get_papers(queries, synonyms, databases, fields, types, folder_name, dates, since, to, search_date):
    for query in queries:
        key = list(query.keys())[0]

        if 'arxiv' in databases:
            print("- Requesting ArXiv for query: " + list(query.keys())[0] + "...")
            arxiv.get_papers(query, synonyms, fields, types, dates, since, to, folder_name, search_date)

        if 'springer' in databases:
            # Springer searches over all the paper metadata. Synonyms are not needed in this case.
            print("- Requesting Springer for query: " + list(query.keys())[0] + "...")
            springer.get_papers(query, fields, types, dates, since, to, folder_name, search_date)

        if 'ieeexplore' in databases:
            print("- Requesting IEEE Xplore for query: " + list(query.keys())[0] + "...")
            ieeexplore.get_papers(query, synonyms, fields, types, dates, since, to, folder_name, search_date)

        # Scopus provides papers metadata then abstracts must be retrieved from the science direct database.
        # Scopus indexes different databases which are queried separately (e.g., ieeeXplore).
        # So the number of returned papers from scopus is always greater than the number of final abstracts retrieved
        # from science direct.
        if 'sciencedirect' in databases:
            print("- Requesting Scopus for query: " + list(query.keys())[0] + "...")
            elsevier.get_papers(query, synonyms, fields, types, dates, since, to, folder_name, search_date)
            # Getting abstracts from science direct
            print('-- Getting abstracts from science direct...')
            get_abstracts_elsevier(query, folder_name, to, search_date)

        if 'core' in databases:
            print("- Requesting CORE for query: " + list(query.keys())[0] + "...")
            core.get_papers(query, synonyms, fields, types, dates, since, to, folder_name, search_date)

        if 'semantic_scholar' in databases:
            # Semantic Scholar searches over its knowledge graph. Synonyms are not needed in this case.
            print("- Requesting Semantic Scholar query: " + list(query.keys())[0] + "...")
            semantic_scholar.get_papers(query, types, dates, since, to, folder_name, search_date)


def get_abstracts_elsevier(query, file_name, to, search_date):
    query_name = list(query.keys())[0]
    elsevier.process_raw_papers(query, file_name, to, search_date)


def get_citations(folder_name, search_date, step):
    print("Requesting Semantic Scholar for papers citations...")
    file_name = semantic_scholar.get_citations(folder_name, search_date, step)
    return file_name


def preprocess(queries, databases, folder_name, search_date, since, to, step):
    preprocessed_file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) + \
                             '_preprocessed_papers.csv'
    if not exists(preprocessed_file_name):
        papers = pd.DataFrame()
        for query in queries:
            for database in databases:
                query_name = list(query.keys())[0]
                file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/raw_papers/' + \
                            query_name.lower().replace(' ', '_') + '_' + database + '.csv'
                if exists(file_name):
                    print('Processing file: ' + file_name)
                    df = pd.read_csv(file_name)
                    if database == 'ieeexplore':
                        df = df.drop_duplicates('doi')
                        dates = df['publication_date']
                        df['publication_date'] = parse_dates(dates)
                        papers_ieee = pd.DataFrame(
                            {
                                'doi': df['doi'], 'type': df['content_type'], 'query_name': df['query_name'],
                                'query_value': df['query_value'], 'publication': df['publication_title'],
                                'publisher': df['publisher'], 'publication_date': df['publication_date'],
                                'database': df['database'], 'title': df['title'], 'url': df['html_url'],
                                'abstract': df['abstract']
                            }
                        )
                        papers = papers.append(papers_ieee)
                    if database == 'springer':
                        df = df.drop_duplicates('doi')
                        dates = df['publicationDate']
                        df['publication_date'] = parse_dates(dates)
                        papers_springer = pd.DataFrame(
                            {
                                'doi': df['doi'], 'type': df['contentType'], 'query_name': df['query_name'],
                                'query_value': df['query_value'], 'publication': df['publicationName'],
                                'publisher': df['publisher'], 'publication_date': df['publication_date'],
                                'database': df['database'], 'title': df['title'], 'url': df['url'],
                                'abstract': df['abstract']
                            }
                        )
                        papers = papers.append(papers_springer)
                    if database == 'arxiv':
                        df = df.drop_duplicates('id')
                        dates = df['published']
                        df['publication_date'] = parse_dates(dates)
                        papers_arxiv = pd.DataFrame(
                            {
                                'doi': df['id'], 'type': df['database'], 'query_name': df['query_name'],
                                'query_value': df['query_value'], 'publication': df['database'],
                                'publisher': df['database'], 'publication_date': df['publication_date'],
                                'database': df['database'], 'title': df['title'], 'url': df['id'],
                                'abstract': df['summary']
                            }
                        )
                        papers = papers.append(papers_arxiv)
                    if database == 'sciencedirect':
                        df = df.drop_duplicates('id')
                        papers_sciencedirect = pd.DataFrame(
                            {
                                'doi': df['id'], 'type': df['type'], 'query_name': df['query_name'],
                                'query_value': df['query_value'], 'publication': df['publication'],
                                'publisher': df['publisher'], 'publication_date': df['publication_date'],
                                'database': df['database'], 'title': df['title'], 'url': df['url'],
                                'abstract': df['abstract']
                            }
                        )
                        papers = papers.append(papers_sciencedirect)
                    if database == 'core':
                        df = df.drop_duplicates('id')
                        dates = df['datePublished']
                        df['publication_date'] = parse_dates(dates)
                        df['id'] = get_ids(df, database)
                        papers_core = pd.DataFrame(
                            {
                                'doi': df['id'], 'type': df['database'], 'query_name': df['query_name'],
                                'query_value': df['query_value'], 'publication': df['journals'],
                                'publisher': df['publisher'], 'publication_date': df['publication_date'],
                                'database': df['database'], 'title': df['title'], 'url': df['downloadUrl'],
                                'abstract': df['description']
                            }
                        )
                        papers_core['database'] = database
                        papers_core['publication'] = database
                        papers = papers.append(papers_core)
                    if database == 'semantic_scholar':
                        df = df.drop_duplicates('paperId')
                        df_dates = df['year']
                        dates = []
                        for df_date in df_dates:
                            df_date = str(df_date).split('.')[0]
                            dates.append(df_date)
                        df['publication_date'] = parse_dates(dates)
                        df['id'] = get_ids(df, database)
                        papers_semantic = pd.DataFrame(
                            {
                                'doi': df['id'], 'type': df['database'], 'query_name': df['query_name'],
                                'query_value': df['query_value'], 'publication': df['database'],
                                'publisher': df['venue'], 'publication_date': df['publication_date'],
                                'database': df['database'], 'title': df['title'], 'url': df['url'],
                                'abstract': df['abstract']
                            }
                        )
                        papers = papers.append(papers_semantic)
        papers['title'] = papers['title'].str.lower()
        papers = papers.drop_duplicates('title')
        papers['doi'] = papers['doi'].str.lower()
        papers['doi'].replace(r'\s+', np.nan, regex=True)
        nan_doi = papers.loc[papers['doi'] == np.nan]
        papers = papers.drop_duplicates('doi')
        papers = papers.append(nan_doi)
        papers['type'] = 'preprocessed'
        papers['abstract'].replace('', np.nan, inplace=True)
        papers.dropna(subset=['abstract'], inplace=True)
        papers['title'].replace('', np.nan, inplace=True)
        papers.dropna(subset=['title'], inplace=True)
        papers.dropna(subset=['doi'], inplace=True)
        with open(preprocessed_file_name, 'a+', newline='', encoding=fr) as f:
            papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
        util.remove_repeated(preprocessed_file_name)
        util.clean_papers(preprocessed_file_name)
    return preprocessed_file_name


def get_ids(df, database):
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
        else:
            if database == 'core':
                ids.append(str(row['id']))
            if database == 'semantic_scholar':
                ids.append(str(row['externalIds.DOI']))
    return ids


def parse_dates(dates):
    new_dates = []
    for date in dates:
        date = str(date)
        # print(date)
        date = date.replace('[', '').replace(']', '').replace('Issued on: ', '').replace('[[issued]]', '').replace(
            'issued', '')
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


def filter_papers(keywords, synonyms, folder_name, search_date, step):
    syntactic_filtered_file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' \
                                   + str(step) + '_syntactic_filtered_papers.csv'
    if not exists(syntactic_filtered_file_name):
        to_filter = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step - 1) + \
                    '_preprocessed_papers.csv';
        preprocessed_papers = pd.read_csv(to_filter)
        preprocessed_papers.dropna(subset=["abstract"], inplace=True)
        filtered_papers = filter_by_keywords(preprocessed_papers, keywords, synonyms)
        if len(filtered_papers) > 0:
            filtered_papers['type'] = 'filtered'
            filtered_papers['status'] = 'unknown'
            with open('./papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) +
                      '_syntactic_filtered_papers.csv', 'a+', newline='', encoding=fr) as f:
                filtered_papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
    return syntactic_filtered_file_name


def filter_by_keywords(papers, keywords, synonyms):
    papers = papers.dropna(subset=['abstract'])
    filtered_papers = papers
    filtered_papers['abstract_lower'] = filtered_papers['abstract'].str.replace('-', ' ')
    filtered_papers['abstract_lower'] = filtered_papers['abstract_lower'].str.lower()
    filtered_papers['abstract_lower'] = filtered_papers['abstract_lower'].str.replace('\n', ' ')
    filtered_papers['abstract_lower'] = filtered_papers['abstract_lower'].apply(lemmatize_text)

    for keyword in keywords:
        terms = [r'\b' + keyword.lower() + r'\b']
        if keyword in synonyms:
            synonym_list = synonyms[keyword]
            for synonym in synonym_list:
                terms.append(r'\b' + synonym.lower() + r'\b')
        filtered_papers = filtered_papers[filtered_papers['abstract_lower'].str.contains('|'.join(terms), na=False)]
    filtered_papers = filtered_papers.drop(['abstract_lower'], axis=1)
    filtered_papers = filtered_papers.drop_duplicates('title')
    filtered_papers['id'] = list(range(1, len(filtered_papers) + 1))
    return filtered_papers


def tokenize(doc):
    return simple_preprocess(strip_tags(doc), deacc=True, min_len=2, max_len=15)


def lemmatize_text(text):
    return ' '.join([lemma.lemmatize(word) for word in w_tokenizer.tokenize(text)])
