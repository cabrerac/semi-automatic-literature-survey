import pandas as pd
from sentence_transformers import SentenceTransformer
from sentence_transformers import util as sentence_util
from os.path import exists
from . import util

fr = 'utf-8'


def search(semantic_filters, folder_name, next_file, search_date, step):
    search_algorithm = ''
    file_name = ''
    for keyword in semantic_filters:
        if 'type' in keyword:
            search_algorithm = keyword['type']

    if search_algorithm == 'bert':
        file_name = bert_search(semantic_filters, folder_name, next_file, search_date, step)
    else:
        file_name = next_file
    return file_name


def bert_search(semantic_filters, folder_name, next_file, search_date, step):
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
                        found_papers = pd.concat([found_papers, papers[papers['concatenated'] == paper_array]])
        columns_to_drop = ['concatenated']
        found_papers = found_papers.drop(columns_to_drop, axis=1)
        found_papers['id'] = list(range(1, len(found_papers) + 1))
        found_papers['id'] = found_papers.index.astype(str)
        found_papers['type'] = 'to_check'
        found_papers['status'] = 'unknown'
        util.save(semantic_filtered_file_name, found_papers, fr, 'a+')
        util.clean_papers(semantic_filtered_file_name)
    return semantic_filtered_file_name
