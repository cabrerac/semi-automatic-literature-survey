import pandas as pd
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import strip_tags
from gensim.models.doc2vec import TaggedDocument
from lbl2vec import Lbl2Vec
from nltk.stem.wordnet import WordNetLemmatizer
from sentence_transformers import SentenceTransformer
from sentence_transformers import util as sentence_util
from os.path import exists
from . import util

fr = 'utf-8'
lemma = WordNetLemmatizer()


def search(semantic_filters, folder_name, next_file, search_date, step):
    search_algorithm = ''
    file_name = ''
    for keyword in semantic_filters:
        if 'type' in keyword:
            search_algorithm = keyword['type']

    if search_algorithm == 'lbl2vec':
        file_name = lbl2vec(semantic_filters, folder_name, next_file, search_date, step)
    if search_algorithm == 'bert':
        file_name = semantic_search(semantic_filters, folder_name, next_file, search_date, step)
    return file_name


def lbl2vec(keywords, folder_name, next_file, search_date, step):
    semantic_filtered_file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' \
                                   + str(step) + '_semantic_filtered_papers.csv'
    if not exists(semantic_filtered_file_name):
        for keyword in keywords:
            if 'classes' in keyword:
                classes = keyword['classes']
            if 'excluded_classes' in keyword:
                excluded_classes = keyword['excluded_classes']
        papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + next_file
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
        if len(papers.index) <= 100:
            papers['id'] = list(range(1, len(papers) + 1))
            papers['id'] = papers.index.astype(str)
            papers['type'] = 'to_check'
            papers['status'] = 'unknown'
            util.save(semantic_filtered_file_name, papers, fr, 'a+')
        else:
            min_num_docs = 500
            min_count = 50
            papers['tagged_abstract'] = papers.apply(lambda row: TaggedDocument(tokenize(row['abstract']), [str(row.name)]), axis=1)
            papers['abstract_key'] = papers.index.astype(str)
            lbl2vec_model = Lbl2Vec(keywords_list=list(labels['keywords']), tagged_documents=papers['tagged_abstract'],
                                    label_names=list(labels['class_name']), similarity_threshold=1.0,
                                    min_num_docs=min_num_docs, epochs=10, min_count=min_count)
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
                                  'doc_key'], axis=1)
            columns_to_drop = included_classes
            for cls in excluded_classes:
                columns_to_drop.append(cls)
            papers = papers.drop(columns_to_drop, axis=1)
            papers['id'] = list(range(1, len(papers) + 1))
            papers['id'] = papers.index.astype(str)
            papers['type'] = 'to_check'
            papers['status'] = 'unknown'
            util.save(semantic_filtered_file_name, papers, fr, 'a+')
        util.clean_papers(semantic_filtered_file_name)
    return semantic_filtered_file_name


def tokenize(doc):
    return simple_preprocess(strip_tags(doc), deacc=True, min_len=2, max_len=15)


def semantic_search(semantic_filters, folder_name, next_file, search_date, step):
    semantic_filtered_file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' \
                                  + str(step) + '_semantic_filtered_papers.csv'
    if not exists(semantic_filtered_file_name):
        papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + next_file
        papers = pd.read_csv(papers_file)
        found_papers = pd.DataFrame()
        model = SentenceTransformer('nq-distilbert-base-v1')
        papers['concatenated'] = (papers['title'] + ' ' + papers['abstract'])
        papers_array = papers['concatenated'].values
        encoded_papers = model.encode(papers_array, batch_size=32, convert_to_tensor=True, show_progress_bar=True)
        encoded_papers.shape
        queries = []
        score = 0.0
        for keyword in semantic_filters:
            if 'queries' in keyword:
                queries = keyword['queries']
            if 'score' in keyword:
                score = keyword['score']
        for query in queries:
            query_embedding = model.encode(query, convert_to_tensor=True)
            hits = sentence_util.semantic_search(query_embedding, encoded_papers, top_k=len(papers_array))
            for hit in hits[0]:
                if hit['score'] > score:
                    paper_array = papers_array[hit['corpus_id']]
                    if len(found_papers) == 0:
                        found_papers = papers[papers['concatenated'] == paper_array]
                    else:
                        found_papers = found_papers.append(papers[papers['concatenated'] == paper_array])
        columns_to_drop = ['concatenated']
        found_papers = found_papers.drop(columns_to_drop, axis=1)
        found_papers['id'] = list(range(1, len(found_papers) + 1))
        found_papers['id'] = found_papers.index.astype(str)
        found_papers['type'] = 'to_check'
        found_papers['status'] = 'unknown'
        util.save(semantic_filtered_file_name, found_papers, fr, 'a+')
        util.clean_papers(semantic_filtered_file_name)
    return semantic_filtered_file_name
