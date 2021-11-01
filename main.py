import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import yaml
from clients import arxiv
from clients import ieeexplore
from clients import springer


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
    return domains, interests, keywords, synonyms, fields, types


def get_papers(domains, interests, keywords, synonyms, fields, types, retrieve):
    stats = []
    papers = []
    for domain in domains:
        print("Requesting IEEE Xplore for " + domain+ " related papers...")
        stats_ieee, ieee_explore_papers = ieeexplore.get_papers([domain], interests, keywords, synonyms, fields, types, retrieve)
        print(stats_ieee)

        #print("Requesting Springer for " + domain + " related papers...")
        #stats_springer, springer_papers = springer.get_papers([domain], interests, keywords, synonyms, fields, types, retrieve)
        #springer_papers.abstract.str.encode('utf-8')
        #print(stats_springer)

        #print("Requesting ArXiv for " + domain + " related papers...")
        #stats_arxiv, arxiv_papers = arxiv.get_papers([domain], interests, keywords, synonyms, fields, types, retrieve)
        #print(stats_arxiv)

        # Collecting stats in dataframe
        stats.append(stats_ieee)
        #stats.append(stats_springer)
        #stats.append(stats_arxiv)
        stats = pd.DataFrame(stats, columns=['database', 'filtered', 'papers', 'domain'])
        if retrieve:
            with open('ieee.csv', 'a', newline='', encoding='utf-8') as f:
                ieee_explore_papers.to_csv(f, encoding='utf-8', index=False, header=f.tell() == 0)
            #with open('springer.csv', 'a', newline='', encoding='utf-8') as f:
                #springer_papers.to_csv(f, encoding='utf-8', index=False, header=f.tell() == 0)
            #with open('arxiv.csv', 'a', newline='', encoding='utf-8') as f:
                #arxiv_papers.to_csv(f, encoding='utf-8', index=False, header=f.tell() == 0)
            with open('stats_filtered.csv', 'a', newline='', encoding='utf-8') as f:
                stats.to_csv(f, encoding='utf-8', index=False, header=f.tell() == 0)
        else:
            with open('stats_all.csv', 'a', newline='', encoding='utf-8') as f:
                stats.to_csv(f, encoding='utf-8', index=False, header=f.tell() == 0)


#print('Reading parameters file...')
#domains, interests, keywords, synonyms, fields, types = read_parameters('parameters.yaml')

#print('Getting all stats...')
#get_papers(domains, [], [], synonyms, fields, types, False)

#print('Getting filtered stats and papers...')
#get_papers(domains, interests, keywords, synonyms, fields, types, True)

stats_all = pd.read_csv('stats_all.csv')
stats_filtered = pd.read_csv('stats_filtered.csv')
stats_all = stats_all.append(stats_filtered)
fig, ax = plt.subplots()
ax = sns.barplot(x="database", y="papers", hue="filtered", data=stats_all)
plt.savefig('all_stats.png')
