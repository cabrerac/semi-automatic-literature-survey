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
    filtered_papers = pd.read_csv('./papers/filtered_papers.csv')
    to_check_papers = pd.read_csv('./papers/to_check_papers.csv')
    df = preprocessed_papers
    df = df.append(filtered_papers)
    df = df.append(to_check_papers)
    df = pd.DataFrame(columns=['database', 'domain', 'papers'])
    series = preprocessed_papers.groupby(by=['domain', 'database', 'type']).count()['doi']
    series = series.append(filtered_papers.groupby(by=['domain', 'database', 'type']).count()['doi'])
    df = series.to_frame()
    df = df.rename(columns={'doi': 'papers'})
    df = df.reset_index()
    fig, ax = plt.subplots()
    ax = sns.barplot(x='database', y='papers', hue='type', data=df, ci=0)
    for p in ax.patches:
        ax.annotate(format(p.get_height(), '.0f'), (p.get_x() + p.get_width() / 2., p.get_height()), ha='center',
                    va='center', size=10, xytext=(0, 9), textcoords='offset points')
    plt.ylim((0, 4000))
    plt.savefig('all_stats.png')

