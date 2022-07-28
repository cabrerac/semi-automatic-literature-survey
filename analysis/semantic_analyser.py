import pandas as pd
import textacy
from textacy.extract.kwic import keyword_in_context
from textacy.extract import keyterms as kt
import networkx as nx
from . import topic_modeling as tp
import pandas as pd
from matplotlib import pyplot as plt
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import strip_tags
from gensim.models.doc2vec import TaggedDocument
from lbl2vec import Lbl2Vec
from nltk.stem.wordnet import WordNetLemmatizer
from os.path import exists


from analysis import util

fr = 'utf-8'
lemma = WordNetLemmatizer()


def get_to_check_papers(keywords, folder_name, search_date, step):
    papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + \
                  '_syntactic_filtered_papers.csv'
    filtered_papers = pd.read_csv(papers_file)
    sentences_abstract = get_sentences(keywords, filtered_papers)
    to_check = []
    for index, sentences in sentences_abstract.items():
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


def semantic_analysis(keywords, synonyms, folder_name, search_date, step):
    papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + \
                  '_syntactic_filtered_papers.csv'
    filtered_papers = pd.read_csv(papers_file)
    keywords_contexts = get_keywords_contexts(keywords, filtered_papers)
    to_check = []
    for index, contexts in keywords_contexts.items():
        words = []
        for keyword in keywords:
            if not isinstance(keyword, str):
                for key, terms in keyword.items():
                    words.append(key)
                    if key in synonyms:
                        synonym = synonyms[key]
                        for s in synonym:
                            words.append(s)
            else:
                words.append(keyword)
                if keyword in synonyms:
                    synonym = synonyms[key]
                    for s in synonym:
                        words.append(s.lower())
        for context in contexts:
            for word in words:
                if word.lower() in context[0] or word.lower() in context[2]:
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


def get_keywords_contexts(keywords, papers):
    keywords_contexts = {}
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
        abstract = paper['abstract'].lower()
        try:
            doc = textacy.make_spacy_doc(abstract, lang='en_core_web_sm')
            paper_contexts = []
            for word in words:
                contexts = list(keyword_in_context(doc, word, window_width=250, pad_context=True))
                if len(contexts) > 0:
                    for context in contexts:
                        paper_contexts.append(context)
            if len(paper_contexts) > 0:
                keywords_contexts[id] = paper_contexts
        except Exception as ex:
            print(ex)
    return keywords_contexts


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
        abstract = paper['abstract'].lower()
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


def semantic_topic_modeling(training_file, papers_file):
    training_papers = pd.read_csv(training_file)
    training_abstracts = get_abstracts(training_papers)
    training_abstracts = [tp.clean(abstract).split() for abstract in training_abstracts]
    dictionary = tp.get_dictionary(training_abstracts)
    doc_term_matrix = tp.get_doc_term_matrix(dictionary, training_abstracts)
    lda_model = tp.run_lda(doc_term_matrix, 10, dictionary, 3)

    papers = pd.read_csv(papers_file)
    words = []
    for key, paper in papers.iterrows():
        if paper['title'].lower() not in training_papers['title']:
            abstract = [tp.clean(paper['abstract']).split()]
            unseen_doc = tp.get_bow_doc(abstract)
            topics = lda_model[unseen_doc]
            max_topic = [-1, -1]
            for topic in topics:
                if max_topic[0] == -1:
                    max_topic[0] = topic[0]
                    max_topic[1] = topic[1]
                else:
                    if topic[1] > max_topic[1]:
                        max_topic[0] = topic[0]
                        max_topic[1] = topic[1]
            t = lda_model.print_topic(max_topic[0], 1)
            word = t.split('*')[1]
            words.append(word)
    plt.rcParams["figure.figsize"] = [7.50, 3.50]
    plt.rcParams["figure.autolayout"] = True

    fig, ax = plt.subplots()

    df = pd.DataFrame({'words': words})
    df['words'].value_counts().plot(ax=ax, kind='bar', xlabel='topics', ylabel='frequency')

    plt.show()
    plt.savefig('topics.png', bbox_inches="tight")


def get_abstracts(papers):
    abstracts = []
    for id, paper in papers.iterrows():
        abstracts.append(paper['abstract'])
    return abstracts


def lbl2vec(keywords, folder_name, search_date, step):
    semantic_filtered_file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' \
                                   + str(step) + '_semantic_filtered_papers.csv'
    if not exists(semantic_filtered_file_name):
        for keyword in keywords:
            if 'classes' in keyword:
                classes = keyword['classes']
            if 'excluded_classes' in keyword:
                excluded_classes = keyword['excluded_classes']
        papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + \
                      '_syntactic_filtered_papers.csv'
        papers = pd.read_csv(papers_file)
        labels = []
        index = 0
        for cls in classes:
            for key, items in cls.items():
                label = {'class_index': index, 'class_name': key}
                words = [lemma.lemmatize(key)]
                for item in items:
                    words.append(lemma.lemmatize(item))
                label['keywords'] = words
                label['number_of_keywords'] = len(words)
                index = index + 1
                labels.append(label)
        labels = pd.DataFrame(labels)

        papers['clean_abstracts'] = [tp.clean(abstract) for abstract in papers['abstract']]
        papers['tagged_abstract'] = papers.apply(lambda row: TaggedDocument(tokenize(row['clean_abstracts']),
                                                                            [str(row.name)]), axis=1)
        papers['abstract_key'] = papers.index.astype(str)
        lbl2vec_model = Lbl2Vec(keywords_list=list(labels['keywords']), tagged_documents=papers['tagged_abstract'],
                                label_names=list(labels['class_name']), similarity_threshold=1.0,
                                min_num_docs=500, epochs=10)
        lbl2vec_model.fit()
        model_docs_lbl_similarities = lbl2vec_model.predict_model_docs()
        papers = papers.merge(model_docs_lbl_similarities, left_on='abstract_key', right_on='doc_key')
        included_classes = []
        for cls in classes:
            for key, items in cls.items():
                if key not in excluded_classes:
                    included_classes.append(key)
        papers = papers.loc[papers['most_similar_label'].isin(included_classes)]
        papers = papers.drop(['tagged_abstract', 'abstract_key', 'most_similar_label', 'highest_similarity_score',
                              'clean_abstracts', 'doc_key'], axis=1)
        columns_to_drop = included_classes
        for cls in excluded_classes:
            columns_to_drop.append(cls)
        papers = papers.drop(columns_to_drop, axis=1)
        papers['id'] = list(range(1, len(papers) + 1))
        papers['id'] = papers.index.astype(str)
        papers['type'] = 'to_check'
        papers['status'] = 'unknown'
        with open('./papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) +
                  '_semantic_filtered_papers.csv', 'a+', newline='', encoding=fr) as f:
            papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
    return semantic_filtered_file_name


def tokenize(doc):
    return simple_preprocess(strip_tags(doc), deacc=True, min_len=2, max_len=15)




