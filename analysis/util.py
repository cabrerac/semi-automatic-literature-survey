import yaml
import pandas as pd
import numpy as np
import os
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from datetime import datetime
import spacy
from os.path import exists
import logging
import shutil

logger = logging.getLogger('logger')


@Language.factory('language_detector')
def language_detector(nlp, name):
    return LanguageDetector()


nlp = spacy.load('en_core_web_sm')

nlp.add_pipe('language_detector', last=True)

fr = 'utf-8'


def read_parameters(parameters_file_name):
    with open(parameters_file_name) as file:
        parameters = yaml.load(file, Loader=yaml.FullLoader)

    if 'queries' in parameters:
        queries = parameters['queries']
    else:
        queries = []

    if 'syntactic_filters' in parameters:
        syntactic_filters = parameters['syntactic_filters']
    else:
        syntactic_filters = []

    if 'semantic_filters' in parameters:
        semantic_filters = parameters['semantic_filters']
    else:
        semantic_filters = []

    fields = ['title', 'abstract']
    types = ['conferences', 'journals']

    synonyms = {}
    for query in queries:
        query_name = list(query.keys())[0]
        words = query[query_name].replace("'", '*').split('*')
        for word in words:
            if word in parameters and word not in synonyms.keys():
                synonyms[word] = parameters[word]
    for syntactic_filter in syntactic_filters:
        if syntactic_filter in parameters and syntactic_filter not in synonyms.keys():
            synonyms[syntactic_filter] = parameters[syntactic_filter]

    if 'databases' in parameters:
        databases = parameters['databases']
    else:
        logger.debug('Databases missing in parameters file. Using default values: arxiv, springer, ieeexplore, '
                    'scopus, core, semantic_scholar')
        databases = ['arxiv', 'springer', 'ieeexplore', 'scopus', 'core', 'semantic_scholar']

    dates = False
    if 'start_date' in parameters:
        start_date = parameters['start_date']
        dates = True
    else:
        start_date = datetime.strptime('1950-01-01', '%Y-%m-%d')
    if 'end_date' in parameters:
        end_date = parameters['end_date']
        dates = True
    else:
        end_date = datetime.today()

    if not dates:
        logger.debug('Search dates missing in parameters file. Searching without considering dates...')

    if 'search_date' in parameters:
        search_date = parameters['search_date']
    else:
        logger.debug('Search date missing in parameters file. Using current date: '
                    + datetime.today().strftime('%Y-%m-%d'))
        search_date = datetime.today().strftime('%Y-%m-%d')

    if 'folder_name' in parameters:
        folder_name = parameters['folder_name']
    else:
        folder_name = parameters_file_name.replace('.yaml', '')

    return queries, syntactic_filters, semantic_filters, fields, types, synonyms, databases, dates, start_date, \
        end_date, search_date, folder_name


def save(file_name, papers, fmt, option):
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    with open(file_name, option, newline='', encoding=fmt) as f:
        papers.to_csv(f, encoding=fmt, index=False, header=f.tell() == 0)


def merge_papers(merge_step_1, merge_step_2, folder_name, search_date):
    file1 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(merge_step_1) + \
            '_manually_filtered_by_full_text_papers.csv'
    file2 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(merge_step_2) + \
            '_manually_filtered_by_full_text_papers.csv'
    result = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/11_final_list_papers.csv'
    if not exists(result):
        if exists(file1) and exists(file2):
            df1 = pd.read_csv(file1)
            df2 = pd.read_csv(file2)
            df_result = pd.concat([df1, df2])
            df_result['title'] = df_result['title'].str.lower()
            df_result = df_result.drop_duplicates('title')
            df_result['doi'] = df_result['doi'].str.lower()
            df_result['doi'].replace(r'\s+', 'nan', regex=True)
            nan_doi = df_result.loc[df_result['doi'] == 'nan']
            df_result = df_result.drop_duplicates('doi')
            df_result = df_result.append(nan_doi)
            df_result['id'] = list(range(1, len(df_result) + 1))
            with open(result, 'a+', newline='', encoding=fr) as f:
                df_result.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
            remove_repeated(result)
        elif exists(file1):
            df_result = pd.read_csv(file1)
            with open(result, 'a+', newline='', encoding=fr) as f:
                df_result.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
            remove_repeated(result)
    return result


def remove_repeated(file):
    df = pd.read_csv(file)
    df['title_lower'] = df['title'].str.lower()
    df['title_lower'] = df['title_lower'].str.replace('-', ' ')
    df['title_lower'] = df['title_lower'].str.replace('\n', '')
    df['title_lower'] = df['title_lower'].str.replace(' ', '')
    df = df.drop_duplicates('title_lower')
    df['abstract'].replace('', np.nan, inplace=True)
    df.dropna(subset=['abstract'], inplace=True)
    df['title'].replace('', np.nan, inplace=True)
    df.dropna(subset=['title'], inplace=True)
    df['abstract_lower'] = df['abstract'].str.lower()
    df['abstract_lower'] = df['abstract_lower'].str.replace('-', ' ')
    df['abstract_lower'] = df['abstract_lower'].str.replace('\n', '')
    df['abstract_lower'] = df['abstract_lower'].str.replace(' ', '')
    df = df.drop_duplicates('abstract_lower')
    df = df.drop(['abstract_lower', 'title_lower'], axis=1)
    with open(file, 'w', newline='', encoding=fr) as f:
        df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def remove_repeated_df(df):
    df['title_lower'] = df['title'].str.lower()
    df['title_lower'] = df['title_lower'].str.replace('-', ' ')
    df['title_lower'] = df['title_lower'].str.replace('\n', '')
    df['title_lower'] = df['title_lower'].str.replace(' ', '')
    df = df.drop_duplicates('title_lower')
    df = df.drop(columns=['title_lower'], errors='ignore')
    return df


def remove_repeated_ieee(file):
    df = pd.read_csv(file)
    df = df.drop_duplicates('doi')
    df.dropna(subset=['abstract'], inplace=True)
    df['title_lower'] = df['title'].str.lower()
    df['title_lower'] = df['title_lower'].str.replace('-', ' ')
    df['title_lower'] = df['title_lower'].str.replace('\n', '')
    df['title_lower'] = df['title_lower'].str.replace(' ', '')
    df = df.drop_duplicates('title_lower')
    df['abstract'].replace('', np.nan, inplace=True)
    df.dropna(subset=['abstract'], inplace=True)
    df['title'].replace('', np.nan, inplace=True)
    df.dropna(subset=['title'], inplace=True)
    df['abstract_lower'] = df['abstract'].str.lower()
    df['abstract_lower'] = df['abstract_lower'].str.replace('-', ' ')
    df['abstract_lower'] = df['abstract_lower'].str.replace('\n', '')
    df['abstract_lower'] = df['abstract_lower'].str.replace(' ', '')
    df = df.drop_duplicates('abstract_lower')
    df = df.drop(['abstract_lower', 'title_lower'], axis=1)
    with open(file, 'w', newline='', encoding=fr) as f:
        df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
    return len(df.index)


def clean_papers(file):
    remove_repeated(file)
    df = pd.read_csv(file)
    values_to_remove = ['survey', 'review', 'progress']
    pattern = '|'.join(values_to_remove)
    df = df.loc[~df['title'].str.contains(pattern, case=False)]
    pattern = '(?<!\w)thesis(?!\w)'
    df = df.loc[~df['abstract'].str.contains(pattern, case=False)]
    not_included = 0
    df['language'] = 'english'
    total_papers = len(df.index)
    current_paper = 0
    for index, row in df.iterrows():
        current_paper = current_paper + 1
        print('Paper ' + str(current_paper) + '/' + str(total_papers) + ' ::: ' +
              str(int((current_paper / total_papers) * 100)) + '% ...', end="\r")
        doc = nlp(row['abstract'])
        detect_language = doc._.language
        if detect_language['language'] != 'en':
            row['language'] = 'not english'
            not_included = not_included + 1
        else:
            if detect_language['score'] < 0.99:
                row['language'] = 'not english'
                not_included = not_included + 1
        df.loc[index] = row
    df = df[df['language'] != 'not english']
    df = df.drop(columns=['language'])
    with open(file, 'w', newline='', encoding=fr) as f:
        df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


# Util functions to play with csv files
def pass_papers(file1, file2, file3, result):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    included_papers = 0
    for index1, row1 in df1.iterrows():
        found1 = False
        for index2, row2 in df2.iterrows():
            if row1['title'] == row2['title']:
                found1 = True
                row2['status'] = row1['status']
                df2.loc[index2] = row2
                with open(file2, 'w', newline='', encoding=fr) as f:
                    df2.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                if row1['status'] == 'included':
                    found2 = False
                    df3 = pd.read_csv(file3)
                    for index3, row3 in df3.iterrows():
                        if row3['title'] == row2['title']:
                            found2 = True
                            included_papers = included_papers + 1
                            paper_dict = [{'id': str(included_papers), 'doi': row3['doi'],
                                           'publisher': row3['publisher'], 'database': row3['database'],
                                           'query_name': row2['query_name'], 'query_value': row2['query_value'],
                                           'url': row3['url'], 'publication_date': row3['publication_date'],
                                           'title': row3['title'], 'abstract': row3['abstract'],
                                           'status': row3['status']}]
                            paper_df = pd.DataFrame(paper_dict)
                            with open(result, 'a+', newline='', encoding=fr) as f:
                                paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                    if not found2:
                        print(row1['title'])
        if not found1 and row1['status'] == 'included':
            df3 = pd.read_csv(file3)
            for index3, row3 in df3.iterrows():
                if row3['title'] == row1['title'] and row3['status'] == 'architecture':
                    print(row1['title'])


def pass_papers_semantic(file1, file2, file3):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    included_papers = 0
    for index1, row1 in df1.iterrows():
        if row1['status'] != 'unknown':
            found = False
            for index2, row2 in df2.iterrows():
                if row1['title'] == row2['title']:
                    row2['status'] = row1['status']
                    df2.loc[index2] = row2
                    with open(file2, 'w', newline='', encoding=fr) as f:
                        df2.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                    found = True
            if not found and row1['status'] == 'included':
                included_papers = included_papers + 1
                paper_dict = [
                    {
                        'id': included_papers, 'status': 'missing', 'doi': row1['doi'], 'publisher': row1['publisher'],
                        'database': row1['database'], 'query_name': row1['query_name'],
                        'query_value': row1['query_value'],
                        'url': row1['url'], 'publication_date': row1['publication_date'], 'title': row1['title'],
                        'abstract': row1['abstract']
                    }
                ]
                paper_df = pd.DataFrame.from_dict(paper_dict)
                with open(file3, 'a+', newline='', encoding=fr) as f:
                    paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def pass_papers_semantic_manual(file1, file2, file3, file4):
    df1 = pd.read_csv(file1) # semantic
    included_papers = 0
    for index1, row1 in df1.iterrows():
        if row1['status'] == 'included':
            included_papers = included_papers + 1
            paper_dict = [
                {
                    'id': included_papers, 'status': 'unknown', 'doi': row1['doi'], 'publisher': row1['publisher'],
                    'database': row1['database'], 'query_name': row1['query_name'], 'query_value': row1['query_value'],
                    'url': row1['url'], 'publication_date': row1['publication_date'], 'title': row1['title'],
                    'abstract': row1['abstract']
                }
            ]
            paper_df = pd.DataFrame.from_dict(paper_dict)
            with open(file2, 'a+', newline='', encoding=fr) as f:
                paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
    df2 = pd.read_csv(file2)  # manually by abstract
    df3 = pd.read_csv(file3)  # passed manually by full text
    included_papers = 0
    for index2, row2 in df2.iterrows():
        for index3, row3 in df3.iterrows():
            if row2['title'] == row3['title']:
                row2['status'] = 'included'
                df2.loc[index2] = row2
                with open(file2, 'w', newline='', encoding=fr) as f:
                    df2.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                included_papers = included_papers + 1
                paper_dict = [
                    {
                        'id': included_papers, 'status': 'unknown', 'doi': row2['doi'],
                        'publisher': row2['publisher'], 'database': row2['database'],
                        'query_name': row2['query_name'], 'query_value': row2['query_value'],
                        'url': row2['url'], 'publication_date': row2['publication_date'],
                        'title': row2['title'], 'abstract': row2['abstract']
                    }
                ]
                paper_df = pd.DataFrame.from_dict(paper_dict)
                with open(file4, 'a+', newline='', encoding=fr) as f:
                    paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def compare_papers(file1, file2, file3, file4):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    included_papers = 0
    for index1, row1 in df1.iterrows():
        found = False
        for index2, row2 in df2.iterrows():
            if row1['title'] == row2['title']:
                found = True
        included_papers = included_papers + 1
        paper_dict = [
            {
                'id': included_papers, 'status': 'unknown', 'doi': row1['doi'],
                'publisher': row1['publisher'], 'database': row1['database'],
                'query_name': row1['query_name'], 'query_value': row1['query_value'],
                'url': row1['url'], 'publication_date': row1['publication_date'],
                'title': row1['title'], 'abstract': row1['abstract']
            }
        ]
        paper_df = pd.DataFrame.from_dict(paper_dict)
        if not found:
            with open(file3, 'a+', newline='', encoding=fr) as f:
                paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
        else:
            with open(file4, 'a+', newline='', encoding=fr) as f:
                paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def check(file1, file2, file3, file4, file5, file6):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    df3 = pd.read_csv(file3)
    df4 = pd.read_csv(file4)
    df5 = pd.read_csv(file5)
    df6 = pd.read_csv(file6)

    for index1, row1 in df1.iterrows():
        if row1['missing'] == 'unknown':
            for index2, row2 in df2.iterrows():
                if row2['title'] == row1['title']:
                    row1['missing'] = 'filtered_by_full_text'
                    df1.loc[index1] = row1
                    with open(file1, 'w', newline='', encoding=fr) as f:
                        df1.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
        if row1['missing'] == 'unknown':
            for index3, row3 in df3.iterrows():
                if row3['title'] == row1['title']:
                    row1['missing'] = 'filtered_by_abstract'
                    df1.loc[index1] = row1
                    with open(file1, 'w', newline='', encoding=fr) as f:
                        df1.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
        if row1['missing'] == 'unknown':
            for index4, row4 in df4.iterrows():
                if row4['title'] == row1['title']:
                    row1['missing'] = 'semantic_filtered'
                    df1.loc[index1] = row1
                    with open(file1, 'w', newline='', encoding=fr) as f:
                        df1.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
        if row1['missing'] == 'unknown':
            for index5, row5 in df5.iterrows():
                if row5['title'] == row1['title']:
                    row1['missing'] = 'syntactic_filtered'
                    df1.loc[index1] = row1
                    with open(file1, 'w', newline='', encoding=fr) as f:
                        df1.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
        if row1['missing'] == 'unknown':
            for index6, row6 in df6.iterrows():
                if row6['title'] == row1['title']:
                    row1['missing'] = 'preprocessed'
                    df1.loc[index1] = row1
                    with open(file1, 'w', newline='', encoding=fr) as f:
                        df1.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
        if row1['missing'] == 'unknown':
            row1['missing'] = 'not_retrieved'
            df1.loc[index1] = row1
            with open(file1, 'w', newline='', encoding=fr) as f:
                df1.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def pass_papers_previous_included(file1, file2, file3, file4, file5):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    df3 = pd.read_csv(file3)
    df4 = pd.read_csv(file4)
    df5 = pd.read_csv(file5)
    for index1, row1 in df1.iterrows():
        for index3, row3 in df3.iterrows():
            if row1['title'].lower() == row3['title'].lower() and row3['status'] == 'included':
                row3['status'] = 'included'
                df3.loc[index3] = row3
                with open(file3, 'w', newline='', encoding=fr) as f:
                    df3.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                paper_dict = [
                    {
                        'id': len(df4.index) + 1, 'status': 'included', 'doi': row3['doi'],
                        'publisher': row3['publisher'], 'database': row3['database'],
                        'query_name': row3['query_name'], 'query_value': row3['query_value'],
                        'url': row3['url'], 'publication_date': row3['publication_date'],
                        'title': row3['title'], 'abstract': row3['abstract']
                    }
                ]
                paper_df = pd.DataFrame.from_dict(paper_dict)
                with open(file4, 'a+', newline='', encoding=fr) as f:
                    paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                paper_dict = [
                    {
                        'id': len(df5.index) + 1, 'status': 'included', 'doi': row3['doi'],
                        'publisher': row3['publisher'], 'database': row3['database'],
                        'query_name': row3['query_name'], 'query_value': row3['query_value'],
                        'url': row3['url'], 'publication_date': row3['publication_date'],
                        'title': row3['title'], 'abstract': row3['abstract']
                    }
                ]
                paper_df = pd.DataFrame.from_dict(paper_dict)
                with open(file5, 'a+', newline='', encoding=fr) as f:
                    paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
    for index2, row2 in df2.iterrows():
        for index3, row3 in df3.iterrows():
            if row2['title'].lower() == row3['title'].lower() and row3['status'] == 'included':
                row3['status'] = 'included'
                df3.loc[index3] = row3
                with open(file3, 'w', newline='', encoding=fr) as f:
                    df3.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                paper_dict = [
                    {
                        'id': len(df4.index) + 1, 'status': 'included', 'doi': row3['doi'],
                        'publisher': row3['publisher'], 'database': row3['database'],
                        'query_name': row3['query_name'], 'query_value': row3['query_value'],
                        'url': row3['url'], 'publication_date': row3['publication_date'],
                        'title': row3['title'], 'abstract': row3['abstract']
                    }
                ]
                paper_df = pd.DataFrame.from_dict(paper_dict)
                with open(file4, 'a+', newline='', encoding=fr) as f:
                    paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
                paper_dict = [
                    {
                        'id': len(df5.index) + 1, 'status': 'included', 'doi': row3['doi'],
                        'publisher': row3['publisher'], 'database': row3['database'],
                        'query_name': row3['query_name'], 'query_value': row3['query_value'],
                        'url': row3['url'], 'publication_date': row3['publication_date'],
                        'title': row3['title'], 'abstract': row3['abstract']
                    }
                ]
                paper_df = pd.DataFrame.from_dict(paper_dict)
                with open(file5, 'a+', newline='', encoding=fr) as f:
                    paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def check_manually_filtered_by_abstract(file1, file2):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    for index2, row2 in df2.iterrows():
        for index1, row1 in df1.iterrows():
            if row1['title'].lower() == row2['title'].lower():
                row1['status'] = 'included'
                df1.loc[index1] = row1
                with open(file1, 'w', newline='', encoding=fr) as f:
                    df1.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def remove_elsevier_log():
    try:
        files_to_remove = [f for f in os.listdir('./logs/') if f.startswith('elsapy-')]
        for file_name in files_to_remove:
            file_path = os.path.join('./logs/', file_name)
            os.remove(file_path)
        shutil.rmtree('./data/')
    except Exception as ex:
        logger.debug("Exception removing elsevier log files: " + str(ex))
