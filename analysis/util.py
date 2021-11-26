import yaml
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def read_parameters(file_name):
    with open(file_name) as file:
        parameters = yaml.load(file, Loader=yaml.FullLoader)
    domains = parameters['domains']
    interests = parameters['interests']
    keywords = parameters['keywords']
    fields = parameters['fields']
    types = parameters['types']
    synonyms = {}
    for domain in domains:
        synonyms[domain] = parameters[domain]
    for interest in interests:
        synonyms[interest] = parameters[interest]
    databases = parameters['databases']
    return domains, interests, keywords, synonyms, fields, types, databases


def save(file_name, papers, format):
    with open('./papers/' + file_name, 'a', newline='', encoding=format) as f:
        papers.to_csv(f, encoding=format, index=False, header=f.tell() == 0)


def plot():
    preprocessed_papers = pd.read_csv('./papers/preprocessed_papers.csv')
    series = preprocessed_papers.groupby(by=['domain']).count()['doi']
    df = series.to_frame()
    df = df.rename(columns={'doi': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    sns.barplot(x='domain', y='papers', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    plt.title('1. Papers from databases')
    plt.xticks(rotation=30)
    plt.ylim((0, 4000))
    plt.savefig('preprocessed.png', bbox_inches="tight")

    filtered_papers = pd.read_csv('./papers/filtered_papers.csv')
    series = filtered_papers.groupby(by=['domain']).count()['doi']
    df = series.to_frame()
    df = df.rename(columns={'doi': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    sns.barplot(x='domain', y='papers', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    plt.title('2. Syntactic filter')
    plt.xticks(rotation=30)
    plt.ylim((0, 3000))
    plt.savefig('filtered.png', bbox_inches="tight")

    to_check_papers = pd.read_csv('./papers/to_check_papers.csv')
    series = to_check_papers.groupby(by=['domain']).count()['doi']
    df = series.to_frame()
    df = df.rename(columns={'doi': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    sns.barplot(x='domain', y='papers', data=df, ci=0, ax=ax)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    plt.title('3. Semantic filter')
    plt.xticks(rotation=30)
    plt.ylim((0, 500))
    plt.savefig('to_check.png', bbox_inches="tight")



