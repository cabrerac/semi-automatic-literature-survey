import yaml
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def read_parameters(parameters_file_name):
    with open(parameters_file_name) as file:
        parameters = yaml.load(file, Loader=yaml.FullLoader)
    domains = parameters['domains']
    interests = parameters['interests']
    keywords = parameters['keywords']
    fields = parameters['fields']
    types = parameters['types']
    since = parameters['since']
    to = parameters['to']
    file_name = parameters['file_name']
    synonyms = {}
    for domain in domains:
        synonyms[domain] = parameters[domain]
    for interest in interests:
        synonyms[interest] = parameters[interest]
    databases = parameters['databases']
    return domains, interests, keywords, synonyms, fields, types, databases, since, to, file_name


def save(file_name, papers, format):
    with open('./papers/' + file_name, 'a', newline='', encoding=format) as f:
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




