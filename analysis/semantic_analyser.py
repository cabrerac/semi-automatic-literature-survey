import pandas as pd
import textacy
import networkx as nx


from analysis import util

fr = 'utf-8'


def get_to_check_papers(keywords, folder_name, search_date, step):
    papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + \
                  '_syntactic_filtered_papers.csv'
    filtered_papers = pd.read_csv(papers_file)
    sentences_abstract = get_sentences(keywords, filtered_papers)
    to_check = []
    for id, sentences in sentences_abstract.items():
        dependencies = get_dependencies(sentences)
        for dependency in dependencies:
            if dependency['distance'] < (dependency['nodes']/10):
                if id not in to_check:
                    to_check.append(id)
    boolean_series = filtered_papers.id.isin(to_check)
    to_check_papers = filtered_papers[boolean_series]
    to_check_papers['type'] = 'to_check'
    to_check_papers['status'] = 'unknown'
    to_check_papers['id'] = list(range(1, len(to_check_papers) + 1))
    with open('./papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) +
              '_semantic_filtered_papers.csv', 'a+', newline='', encoding=fr) as f:
        to_check_papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def get_sentences(keywords, papers):
    sentences_abstract = {}
    words = []
    for keyword in keywords:
        if not isinstance(keyword, str):
            for key, terms in keyword.items():
                for term in terms:
                    if term not in words:
                        words.append(term)
        else:
            words.append(keyword)
    for index, paper in papers.iterrows():
        id = paper['id']
        abstract = paper['abstract']
        try:
            doc = textacy.make_spacy_doc(abstract, lang='en_core_web_sm')
            sentences = []
            for word in words:
                keyword_sentences = [s for s in doc.sents if word in s.lemma_]
                for s in keyword_sentences:
                    d = {'keyword': word, 'sentence': s}
                    sentences.append(d)
            sentences_abstract[id] = sentences
        except Exception as ex:
            print(ex)
    return sentences_abstract


def get_dependencies(sentences):
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
    return dependencies
