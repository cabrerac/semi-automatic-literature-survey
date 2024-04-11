import pandas as pd
from sentence_transformers import SentenceTransformer
from sentence_transformers import util as sentence_util
from os.path import exists
from . import util
from tqdm import tqdm
import logging

fr = 'utf-8'
logger = logging.getLogger('logger')


def search(semantic_filters, folder_name, next_file, search_date, step):
    search_algorithm = ''
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
        model = SentenceTransformer('allenai-specter')
        papers['concatenated'] = (papers['title'] + '[SEP]' + papers['abstract'])
        papers['semantic_score'] = 0.0
        papers_array = papers['concatenated'].values
        logger.info("# Creating the embeddings for the preprocessed papers...")
        encoded_papers = model.encode(papers_array, convert_to_tensor=True, show_progress_bar=True)
        queries = []
        score = 0.0
        for keyword in semantic_filters:
            if 'queries' in keyword:
                queries = keyword['queries']
            if 'score' in keyword:
                score = keyword['score']
        logger.info("# Queries semantic matching...")
        for query in queries:
            query_embedding = model.encode(query, convert_to_tensor=True)
            hits = sentence_util.semantic_search(query_embedding, encoded_papers, top_k=len(papers_array))
            for hit in hits[0]:
                if hit['score'] >= score:
                    paper_array = papers_array[hit['corpus_id']]
                    if len(found_papers) == 0:
                        papers.loc[papers['concatenated'] == paper_array, 'semantic_score'] = hit['score']
                        found_papers = papers[papers['concatenated'] == paper_array]
                    else:
                        papers.loc[papers['concatenated'] == paper_array, 'semantic_score'] = hit['score']
                        found_papers = pd.concat([found_papers, papers[papers['concatenated'] == paper_array]])
        if len(found_papers) > 0:
            columns_to_drop = ['concatenated']
            found_papers = found_papers.drop(columns_to_drop, axis=1)
            found_papers['id'] = list(range(1, len(found_papers) + 1))
            found_papers['id'] = found_papers.index.astype(str)
            found_papers['type'] = 'to_check'
            found_papers['status'] = 'unknown'
            util.save(semantic_filtered_file_name, found_papers, fr, 'a+')
            util.clean_papers(semantic_filtered_file_name)
        else:
            semantic_filtered_file_name = ''
    return semantic_filtered_file_name


def get_relevant_papers(folder_name, search_date, step, semantic_filters, citations_papers, removed_papers):
    relevant_papers = pd.DataFrame()
    original_papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + '1_preprocessed_papers.csv'
    selected_papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + '_manually_filtered_by_full_text_papers.csv'
    if exists(selected_papers_file):
        selected_papers = pd.read_csv(selected_papers_file)
        search_algorithm = ''
        for keyword in semantic_filters:
            if 'type' in keyword:
                search_algorithm = keyword['type']
        if exists(original_papers_file):
            original_papers = pd.read_csv(original_papers_file)
            original_papers = original_papers.drop(['id'], axis=1)
            citations_papers['publication'] = 'semantic_scholar'
            citations_papers = citations_papers.drop(['id'], axis=1)
            original_papers = pd.concat([original_papers, selected_papers, citations_papers, removed_papers]).drop_duplicates(keep=False, subset=['doi'])
            original_papers.loc[:, 'id'] = list(range(1, len(original_papers) + 1))
            if search_algorithm == 'bert':
                relevant_papers = bert_search_relevant_papers(semantic_filters, original_papers, selected_papers)
        else:
            if search_algorithm == 'bert':
                relevant_papers = bert_search_relevant_papers(semantic_filters, citations_papers, selected_papers)
    else:
        relevant_papers = citations_papers
    return relevant_papers


def bert_search_relevant_papers(semantic_filters, original_papers, selected_papers):
    model = SentenceTransformer('allenai-specter')
    selected_papers['concatenated'] = (selected_papers['title'] + '[SEP]' + selected_papers['abstract'])
    selected_papers_array = selected_papers['concatenated'].values
    logger.info("# Creating the embeddings for the selected papers...")
    encoded_selected_papers = model.encode(selected_papers_array, convert_to_tensor=True, show_progress_bar=True)
    score = 0.0
    for keyword in semantic_filters:
        if 'score' in keyword:
            score = keyword['score']
    original_papers['concatenated'] = (original_papers['title'] + '[SEP]' + original_papers['abstract'])
    original_papers['semantic_score'] = 0.0
    logger.info("# Semantic comparison of " + str(len(original_papers)) + " preprocessed papers...")
    pbar = tqdm(total=len(original_papers))
    for index, original_paper in original_papers.iterrows():
        concatenated = original_paper['concatenated']
        concatenated_embedding = model.encode(concatenated, convert_to_tensor=True)
        hits = sentence_util.semantic_search(encoded_selected_papers, concatenated_embedding)
        avg_score = 0.0
        for hit in hits:
            avg_score = avg_score + hit[0]['score']
        avg_score = avg_score/len(hits)
        original_papers.loc[index, 'semantic_score'] = avg_score
        pbar.update(1)
    pbar.close()
    found_papers = original_papers[original_papers['semantic_score'] >= score]
    found_papers = found_papers.drop(['id'], axis=1)
    found_papers = found_papers.drop(['concatenated'], axis=1)
    found_papers.loc[:, 'id'] = list(range(1, len(found_papers) + 1))
    return found_papers
