import pandas as pd
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import strip_tags
from gensim.models.doc2vec import TaggedDocument
from lbl2vec import Lbl2Vec
from nltk.stem.wordnet import WordNetLemmatizer
from os.path import exists
from . import util


fr = 'utf-8'
lemma = WordNetLemmatizer()


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

        papers['tagged_abstract'] = papers.apply(lambda row: TaggedDocument(tokenize(row['abstract']), [str(row.name)]), axis=1)
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
                              'doc_key'], axis=1)
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
        util.clean_papers(semantic_filtered_file_name)
    return semantic_filtered_file_name


def tokenize(doc):
    return simple_preprocess(strip_tags(doc), deacc=True, min_len=2, max_len=15)
