import pandas as pd
import numpy as np
import textacy
import networkx as nx
import matplotlib.pyplot as plt
from itertools import count


from analysis import util

f = 'utf-8'


def get_sentences(keywords):
    sentences_abstract = {}
    filtered_papers = pd.read_csv('./papers/filtered_papers.csv')
    for index, paper in filtered_papers.iterrows():
        doi = paper['doi']
        abstract = paper['abstract']
        doc = textacy.make_spacy_doc(abstract, lang='en_core_web_sm')
        sentences = []
        for word in keywords:
            keyword_sentences = [s for s in doc.sents if word in s.lemma_]
            for s in keyword_sentences:
                d = {'keyword': word, 'sentence': s}
                sentences.append(d)
        sentences_abstract[doi] = sentences
    return sentences_abstract


def get_to_check_papers(sentences_abstract):
    to_check = []
    for doi, sentences in sentences_abstract.items():
        dependencies = []
        for element in sentences:
            keywords = element['keyword'].lower().split(' ')
            sentence = element['sentence']

            # build list of nodes with number and attributes
            nodes = []
            for token in sentence:
                nodes.append(
                    (token.i, {'text': token.text,
                               'idx': token.i,
                               'pos': token.pos_,
                               'tag': token.tag_,
                               'dep': token.dep_}
                     )
                )

            # construct edges from tokens to children
            edges = []
            for token in sentence:
                for child in token.children:
                    edges.append((token.i, child.i))

            # add nodes and edges to graph
            G = nx.Graph()
            G.add_nodes_from(nodes)
            G.add_edges_from(edges)

            # get nodes which are nominal subjects and nodes which are the keywords
            subjects = [(x, y) for x, y in G.nodes(data=True) if y['dep'] == 'nsubj']
            noun_nodes = []
            for x, y in G.nodes(data=True):
                print(y['text'])
                if y['text'].lower() in keywords:
                    noun_nodes.append((x, y))

            # calculate shortest distance between nominal subjects and keywords
            for (subj, node1) in subjects:
                for (noun, node2) in noun_nodes:
                    d = nx.shortest_path(G, source=subj, target=noun)
                    dictionary = {'subject': None, 'noun': None, 'distance': None, 'path': None}
                    dictionary['subject'] = node1['text']
                    dictionary['noun'] = node2['text']
                    dictionary['distance'] = len(d)
                    dictionary['path'] = d
                    dictionary['nodes'] = len(nodes)
                    dependencies.append(dictionary)
        for dependency in dependencies:
            if dependency['distance'] < (dependency['nodes']/10):
                if doi not in to_check:
                    to_check.append(doi)
    filtered_papers = pd.read_csv('./papers/filtered_papers.csv')
    boolean_series = filtered_papers.doi.isin(to_check)
    to_check_papers = filtered_papers[boolean_series]
    to_check_papers['type'] = 'to_check'
    util.save('to_check_papers.csv', to_check_papers, f)