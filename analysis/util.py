import yaml
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import os
from spacy_langdetect import LanguageDetector
from spacy.language import Language
import spacy


@Language.factory('language_detector')
def language_detector(nlp, name):
    return LanguageDetector()


nlp = spacy.load('en_core_web_sm')
nlp.add_pipe('language_detector', last=True)

fr = 'utf-8'


def read_parameters(parameters_file_name):
    with open(parameters_file_name) as file:
        parameters = yaml.load(file, Loader=yaml.FullLoader)
    queries = parameters['queries']
    optionals = parameters['optionals']
    if 'syntactic_filters' in parameters:
        syntactic_filters = parameters['syntactic_filters']
    else:
        syntactic_filters = []
    if 'semantic_filters' in parameters:
        semantic_filters = parameters['semantic_filters']
    else:
        semantic_filters = []
    fields = parameters['fields']
    types = parameters['types']
    synonyms = {}
    for query in queries:
        query_name = list(query.keys())[0]
        words = query[query_name].replace("'", '*').split('*')
        for word in words:
            if word in parameters and word not in synonyms:
                synonyms[word] = parameters[word]
    databases = parameters['databases']
    dates = parameters['dates']
    since = parameters['since']
    to = parameters['to']
    search_date = parameters['search_date']
    folder_name = parameters['folder_name']

    return queries, optionals, syntactic_filters, semantic_filters, fields, types, synonyms, databases, dates, since, \
        to, search_date, folder_name


def save(file_name, papers, fmt):
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    with open(file_name, 'a', newline='', encoding=fmt) as f:
        papers.to_csv(f, encoding=fmt, index=False, header=f.tell() == 0)


def plot():
    preprocessed_papers = pd.read_csv('./papers/preprocessed_papers.csv')
    series = preprocessed_papers.groupby(by=['domain']).count()['id']
    df = series.to_frame()
    df = df.rename(columns={'id': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    g = sns.barplot(x='domain', y='papers', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    g.set_xticklabels(['vehicles', 'health', 'industry', 'media', 'robotics', 'science', 'smart cities'])
    plt.title('1. Papers from databases')
    plt.xticks(rotation=0)
    plt.ylim((0, 4200))
    plt.savefig('preprocessed.png', bbox_inches="tight")
    print("1. Preprocessed done!")

    filtered_papers = pd.read_csv('./papers/filtered_papers.csv')
    series = filtered_papers.groupby(by=['domain']).count()['id']
    df = series.to_frame()
    df = df.rename(columns={'id': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    g = sns.barplot(x='domain', y='papers', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    g.set_xticklabels(['vehicles', 'health', 'industry', 'media', 'robotics', 'science', 'smart cities'])
    plt.title('2. Syntactic filter')
    plt.xticks(rotation=0)
    plt.ylim((0, 1400))
    plt.savefig('filtered.png', bbox_inches="tight")
    print("2. Filtered done!")

    to_check_papers = pd.read_csv('./papers/to_check_papers.csv')
    series = to_check_papers.groupby(by=['domain']).count()['id']
    df = series.to_frame()
    df = df.rename(columns={'id': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    g = sns.barplot(x='domain', y='papers', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    g.set_xticklabels(['vehicles', 'health', 'industry', 'media', 'robotics', 'science', 'smart cities'])
    plt.title('3. Semantic filter')
    plt.xticks(rotation=0)
    plt.ylim((0, 300))
    plt.savefig('to_check.png', bbox_inches="tight")
    print("3. To check done!")

    filtered_by_abstract = pd.read_csv('./papers/filtered_by_abstract.csv')
    series = filtered_by_abstract.groupby(by=['domain']).count()['id']
    df = series.to_frame()
    df = df.rename(columns={'id': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    g = sns.barplot(x='domain', y='papers', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    g.set_xticklabels(['vehicles', 'health', 'industry', 'media', 'robotics', 'science', 'smart cities'])
    plt.title('4. Manual filter - Abstract')
    plt.xticks(rotation=0)
    plt.ylim((0, 60))
    plt.savefig('filtered_by_abstract.png', bbox_inches="tight")
    print("4. Filtered by abstract done!")

    final_papers = pd.read_csv('./papers/final_papers.csv')
    series = final_papers.groupby(by=['domain', 'type']).count()['id']
    df = series.to_frame()
    df = df.rename(columns={'id': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    g = sns.barplot(x='domain', y='papers', hue='type', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    g.set_xticklabels(['vehicles', 'health', 'industry', 'media', 'robotics', 'science', 'smart cities'])
    plt.title('5. Manual filter - Full text')
    plt.ylim((0, 35))
    plt.savefig('final_papers.png', bbox_inches="tight")
    print("5. Final papers done!")

    final_papers = pd.read_csv('./papers/final_papers.csv')
    final_papers['publication_date'] = pd.to_datetime(final_papers['publication_date'])
    series = final_papers.groupby(by=[final_papers['publication_date'].dt.year, 'type']).count()['id']
    df = series.to_frame()
    df = df.rename(columns={'id': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    g = sns.barplot(x='publication_date', y='papers', hue='type', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    plt.title('Papers per year')
    plt.xticks(rotation=90)
    plt.legend(loc='upper left')
    plt.ylim((0, 20))
    plt.savefig('papers_year.png', bbox_inches="tight")
    print("6. Papers year done!")

    final_papers = pd.read_csv('./papers/final_papers_merged.csv')
    series = final_papers.groupby(by=['domain', 'type']).count()['id']
    df = series.to_frame()
    df = df.rename(columns={'id': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    g = sns.barplot(x='domain', y='papers', hue='type', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    g.set_xticklabels(['vehicles', 'health', 'industry', 'media', 'robotics', 'science', 'smart cities'])
    plt.title('6. Selected papers')
    plt.ylim((0, 35))
    plt.savefig('final_papers_merged.png', bbox_inches="tight")
    print("7. Final papers merged done!")

    final_papers = pd.read_csv('./papers/final_papers_merged.csv')
    final_papers['publication_date'] = pd.to_datetime(final_papers['publication_date'])
    series = final_papers.groupby(by=[final_papers['publication_date'].dt.year, 'type']).count()['id']
    df = series.to_frame()
    df = df.rename(columns={'id': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    g = sns.barplot(x='publication_date', y='papers', hue='type', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    plt.title('Papers per year')
    plt.xticks(rotation=90)
    plt.legend(loc='upper left')
    plt.ylim((0, 25))
    plt.savefig('papers_year.png', bbox_inches="tight")
    print("8. Final papers year done!")


def merge_papers(merge_step_1, merge_step_2, folder_name, search_date):
    file1 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(merge_step_1) + '_manually_filtered_by_full_text_papers.csv'
    file2 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(merge_step_2) + '_manually_filtered_by_full_text_papers.csv'
    result = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/11_final_list_papers.csv'
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


def remove_repeated_ieee(file):
    df = pd.read_csv(file)
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
    for index, row in df.iterrows():
        doc = nlp(row['abstract'])
        detect_language = doc._.language
        if detect_language['language'] != 'en':
            row['status'] = 'not included'
            print("Abstract: " + row['abstract'])
            not_included = not_included + 1
        else:
            if detect_language['score'] < 0.99:
                row['status'] = 'not included'
                print("Abstract Score: " + row['abstract'])
                not_included = not_included + 1
        df.loc[index] = row
    print(str(not_included))
    with open(file, 'w', newline='', encoding=fr) as f:
        df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
