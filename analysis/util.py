import yaml
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import os

fr = 'utf-8'


def read_parameters(parameters_file_name):
    with open(parameters_file_name) as file:
        parameters = yaml.load(file, Loader=yaml.FullLoader)
    domains = parameters['domains']
    if domains is None:
        domains = []
    interests = parameters['interests']
    if interests is None:
        interests = []
    keywords = parameters['keywords']
    if keywords is None:
        keywords = []
    fields = parameters['fields']
    types = parameters['types']
    dates = parameters['dates']
    since = parameters['since']
    to = parameters['to']
    search_date = parameters['search_date']
    folder_name = parameters['folder_name']
    syntactic_filters = parameters['syntactic_filters']
    semantic_filters = parameters['semantic_filters']
    synonyms = {}
    for domain in domains:
        if domain in parameters:
            synonyms[domain] = parameters[domain]
    for interest in interests:
        if interest in parameters:
            synonyms[interest] = parameters[interest]
    databases = parameters['databases']
    return domains, interests, keywords, synonyms, fields, types, databases, dates, since, to, search_date, \
           folder_name, syntactic_filters, semantic_filters


def save(file_name, papers, format):
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    with open(file_name, 'a', newline='', encoding=format) as f:
        papers.to_csv(f, encoding=format, index=False, header=f.tell() == 0)


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


def merge_papers(file1, file2, result):
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


def pass_papers(file1, file2, result):
    """df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    included_papers = 0
    for index1, row1 in df1.iterrows():
        if row1['status'] == 'included':
            for index2, row2 in df2.iterrows():
                if row1['title'] == row2['title']:
                    included_papers = included_papers + 1
                    paper_dict = [{'id': str(included_papers), 'doi': row2['doi'],
                                  'publisher': row2['publisher'], 'database': row2['database'],
                                  'url': row2['url'], 'domain': row2['domain'],
                                  'publication_date': row2['publication_date'],
                                  'algorithm_type': '', 'training_schema': '',
                                  'algorithm_goal': '', 'architecture': '',
                                  'title': row2['title'], 'abstract': row2['abstract'],
                                  'status': row2['status']}]
                    paper_df = pd.DataFrame(paper_dict)
                    with open(result, 'a+', newline='', encoding=fr) as f:
                        paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)"""
    df1 = pd.read_csv(file1)
    included_papers = 0
    for index1, row1 in df1.iterrows():
        if row1['status'] == 'experiments' or row1['status'] == 'architecture':
            included_papers = included_papers + 1
            paper_dict = [{'id': str(included_papers), 'type': row1['status'], 'doi': row1['doi'],
                           'publisher': row1['publisher'], 'database': row1['database'],
                           'url': row1['url'], 'domain': row1['domain'],
                           'publication_date': row1['publication_date'],
                           'title': row1['title'], 'abstract': row1['abstract']}]
            paper_df = pd.DataFrame(paper_dict)
            with open(result, 'a+', newline='', encoding=fr) as f:
                paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def remove_repeated(file):
    df = pd.read_csv(file)
    df['title'] = df['title'].str.lower()
    df = df.drop_duplicates('title')
    df['abstract'].replace('', np.nan, inplace=True)
    df.dropna(subset=['abstract'], inplace=True)
    df['title'].replace('', np.nan, inplace=True)
    df.dropna(subset=['title'], inplace=True)
    with open(file, 'w', newline='', encoding=fr) as f:
        df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def update_to_check_papers_by_title(to_check_papers, title, included, result):
    for index, row in to_check_papers.iterrows():
        if row['title'] == title:
            row['status'] = included
            to_check_papers.loc[index] = row
    with open(result, 'w', newline='', encoding=fr) as f:
        to_check_papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)