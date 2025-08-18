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
import random
from . import parser as par
from tqdm import tqdm


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

    for query in queries:
        keys = query.keys()
        for key in keys:
            query[key] = query[key].replace('&', '<AND>').replace('Â', '').replace('¦', '<OR>')

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
        logger.debug('Including dates can reduce the searching time...')

    if 'search_date' in parameters:
        search_date = str(parameters['search_date'])
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


def merge_papers(step, merge_step_1, merge_step_2, folder_name, search_date):
    file1 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(merge_step_1) + \
            '_manually_filtered_by_full_text_papers.csv'
    file2 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(merge_step_2) + \
            '_manually_filtered_by_full_text_papers.csv'
    result = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) + \
             '_final_list_papers.csv'
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
            df_result = pd.concat([df_result, nan_doi])
            df_result['id'] = list(range(1, len(df_result) + 1))
            save(result, df_result, fr, 'a+')
            remove_repeated(result)
        elif exists(file1):
            df_result = pd.read_csv(file1)
            save(result, df_result, fr, 'a+')
            remove_repeated(result)
    return result


def remove_repeated(file):
    df = pd.read_csv(file)
    df['doi'] = df['doi'].str.lower()
    df['doi'].replace(r'\s+', np.nan, regex=True)
    nan_doi = df.loc[df['doi'] == np.nan]
    df = df.drop_duplicates('doi')
    df = pd.concat([df, nan_doi])
    df['title_lower'] = df['title'].str.lower()
    df['title_lower'] = df['title_lower'].str.replace('-', ' ')
    df['title_lower'] = df['title_lower'].str.replace('\n', '')
    df['title_lower'] = df['title_lower'].str.replace(' ', '')
    df = df.drop_duplicates('title_lower')
    df.loc[:, 'abstract'] = df['abstract'].replace('', float("NaN"))
    df.dropna(subset=['abstract'], inplace=True)
    df.loc[:, 'title'] = df['title'].replace('', float("NaN"))
    df.dropna(subset=['title'], inplace=True)
    df['abstract_lower'] = df['abstract'].str.lower()
    df['abstract_lower'] = df['abstract_lower'].str.replace('-', ' ')
    df['abstract_lower'] = df['abstract_lower'].str.replace('\n', '')
    df['abstract_lower'] = df['abstract_lower'].str.replace(' ', '')
    df = df.drop_duplicates('abstract_lower')
    df = df.drop(['abstract_lower', 'title_lower'], axis=1)
    logger.info('Number of papers: ' + str(len(df)))
    save(file, df, fr, 'w')


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
    logger.info('Number of papers: ' + str(len(df)))
    save(file, df, fr, 'w')
    return len(df.index)


def clean_papers(file):
    df = pd.read_csv(file)
    df['abstract'].replace('', np.nan, inplace=True)
    df.dropna(subset=['abstract'], inplace=True)
    df['title'].replace('', np.nan, inplace=True)
    df.dropna(subset=['title'], inplace=True)
    values_to_remove = ['survey', 'review', 'progress']
    pattern = '|'.join(values_to_remove)
    df = df.loc[~df['title'].str.contains(pattern, case=False)]
    pattern = '(?<!\w)thesis(?!\w)'
    df = df.loc[~df['abstract'].str.contains(pattern, case=False)]
    not_included = 0
    df.loc[:, 'language'] = 'english'
    total_papers = len(df.index)
    current_paper = 0
    pbar = tqdm(total=len(df.index))
    for index, row in df.iterrows():
        current_paper = current_paper + 1
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
        pbar.update(1)
    pbar.close()
    print('', end="\r")
    df = df[df['language'] != 'not english']
    df = df.drop(columns=['language'])
    logger.info('Number of papers: ' + str(len(df)))
    save(file, df, fr, 'w')


def exponential_backoff(attempt, base_delay=1, max_delay=64):
    delay = min(base_delay * (2 ** attempt), max_delay)
    delay_with_jitter = delay * (random.random() + 0.5)
    return delay_with_jitter


def parse_queries(queries):
    parsed_queries = []
    valid = True
    if len(queries) > 0:
        for query in queries:
            key = list(query.keys())[0]
            value = query[key]
            parsed_query, valid = par.parse_boolean_expression(value)
            if not valid:
                break
            parsed_queries.append({key: parsed_query})
    else:
        valid = False
    return parsed_queries, valid
