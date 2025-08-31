import pandas as pd
from sentence_transformers import SentenceTransformer
from sentence_transformers import util as sentence_util
from os.path import exists
from util import util
from tqdm import tqdm
import logging

fr = 'utf-8'
logger = logging.getLogger('logger')


def search(semantic_filters, folder_name, next_file, search_date, step):
    try:
        search_algorithm = ''
        for keyword in semantic_filters:
            if 'type' in keyword:
                search_algorithm = keyword['type']

        if search_algorithm == 'bert':
            file_name = bert_search(semantic_filters, folder_name, next_file, search_date, step)
        else:
            file_name = next_file
        return file_name
    except (KeyError, AttributeError, TypeError) as e:
        # User-friendly message explaining what's happening
        logger.info("Error in semantic search configuration. Using default file. Please see the log file for details.")
        # Detailed logging for debugging
        logger.debug(f"Semantic search configuration error: {type(e).__name__}: {str(e)}")
        return next_file
    except Exception as ex:
        # User-friendly message explaining what's happening
        logger.info("Unexpected error in semantic search. Using default file. Please see the log file for details.")
        # Detailed logging for debugging
        logger.error(f"Unexpected semantic search error: {type(ex).__name__}: {str(ex)}")
        return next_file


def bert_search(semantic_filters, folder_name, next_file, search_date, step):
    semantic_filtered_file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' \
                                  + str(step) + '_semantic_filtered_papers.csv'
    if not exists(semantic_filtered_file_name):
        try:
            papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + next_file
            papers = pd.read_csv(papers_file)
            found_papers = pd.DataFrame()
            
            # Initialize BERT model with error handling
            try:
                model = SentenceTransformer('allenai-specter')
            except Exception as model_ex:
                # User-friendly message explaining what's happening
                logger.info("Error loading BERT model. Skipping semantic filtering. Please see the log file for details.")
                # Detailed logging for debugging
                logger.error(f"BERT model loading error: {type(model_ex).__name__}: {str(model_ex)}")
                return next_file
            
            try:
                papers['concatenated'] = (papers['title'] + '[SEP]' + papers['abstract'])
                papers['semantic_score'] = 0.0
                papers_array = papers['concatenated'].values
                logger.info("# Creating the embeddings for the preprocessed papers...")
                encoded_papers = model.encode(papers_array, convert_to_tensor=True, show_progress_bar=True)
            except (KeyError, ValueError, TypeError) as e:
                # User-friendly message explaining what's happening
                logger.info("Error preparing papers for BERT processing. Skipping semantic filtering. Please see the log file for details.")
                # Detailed logging for debugging
                logger.debug(f"Paper preparation error: {type(e).__name__}: {str(e)}")
                return next_file
            except Exception as ex:
                # User-friendly message explaining what's happening
                logger.info("Unexpected error preparing papers for BERT processing. Skipping semantic filtering. Please see the log file for details.")
                # Detailed logging for debugging
                logger.error(f"Unexpected paper preparation error: {type(ex).__name__}: {str(ex)}")
                return next_file
            
            try:
                queries = []
                score = 0.0
                for keyword in semantic_filters:
                    if 'description' in keyword:
                        description = keyword['description']
                    if 'score' in keyword:
                        score = keyword['score']
                
                if 'description' not in locals() or not description:
                    # User-friendly message explaining what's happening
                    logger.info("No description found in semantic filters. Skipping semantic filtering. Please see the log file for details.")
                    # Detailed logging for debugging
                    logger.debug("Missing description in semantic filters")
                    return next_file
                
                logger.info("# Abstracts semantic matching...")
                query_embedding = model.encode(description, convert_to_tensor=True)
                hits = sentence_util.semantic_search(query_embedding, encoded_papers, top_k=len(papers_array))
            except (KeyError, ValueError, TypeError) as e:
                # User-friendly message explaining what's happening
                logger.info("Error in semantic search configuration. Skipping semantic filtering. Please see the log file for details.")
                # Detailed logging for debugging
                logger.debug(f"Semantic search configuration error: {type(e).__name__}: {str(e)}")
                return next_file
            except Exception as ex:
                # User-friendly message explaining what's happening
                logger.info("Unexpected error in semantic search. Skipping semantic filtering. Please see the log file for details.")
                # Detailed logging for debugging
                logger.error(f"Unexpected semantic search error: {type(ex).__name__}: {str(ex)}")
                return next_file
            
            try:
                for hit in hits[0]:
                    if hit['score'] >= score:
                        paper_array = papers_array[hit['corpus_id']]
                        if len(found_papers) == 0:
                            papers.loc[papers['concatenated'] == paper_array, 'semantic_score'] = hit['score']
                            found_papers = papers[papers['concatenated'] == paper_array]
                        else:
                            papers.loc[papers['concatenated'] == paper_array, 'semantic_score'] = hit['score']
                            found_papers = pd.concat([found_papers, papers[papers['concatenated'] == paper_array]])
            except (KeyError, ValueError, TypeError, IndexError) as e:
                # User-friendly message explaining what's happening
                logger.info("Error processing semantic search results. Skipping semantic filtering. Please see the log file for details.")
                # Detailed logging for debugging
                logger.debug(f"Result processing error: {type(e).__name__}: {str(e)}")
                return next_file
            except Exception as ex:
                # User-friendly message explaining what's happening
                logger.info("Unexpected error processing semantic search results. Skipping semantic filtering. Please see the log file for details.")
                # Detailed logging for debugging
                logger.error(f"Unexpected result processing error: {type(ex).__name__}: {str(ex)}")
                return next_file
            
            try:
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
                    semantic_filtered_file_name = next_file
            except (KeyError, ValueError, TypeError) as e:
                # User-friendly message explaining what's happening
                logger.info("Error saving semantic filtering results. Skipping semantic filtering. Please see the log file for details.")
                # Detailed logging for debugging
                logger.debug(f"Result saving error: {type(e).__name__}: {str(e)}")
                return next_file
            except Exception as ex:
                # User-friendly message explaining what's happening
                logger.info("Unexpected error saving semantic filtering results. Skipping semantic filtering. Please see the log file for details.")
                # Detailed logging for debugging
                logger.error(f"Unexpected result saving error: {type(ex).__name__}: {str(ex)}")
                return next_file
                
        except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
            # User-friendly message explaining what's happening
            logger.info(f"Error reading papers file. Skipping semantic filtering. Please see the log file for details.")
            # Detailed logging for debugging
            logger.debug(f"File reading error: {type(e).__name__}: {str(e)}")
            return next_file
        except Exception as ex:
            # User-friendly message explaining what's happening
            logger.info(f"Unexpected error in BERT search. Skipping semantic filtering. Please see the log file for details.")
            # Detailed logging for debugging
            logger.error(f"Unexpected BERT search error: {type(ex).__name__}: {str(ex)}")
            return next_file
    return semantic_filtered_file_name


def get_relevant_papers(folder_name, search_date, step, semantic_filters, citations_papers, removed_papers):
    try:
        relevant_papers = pd.DataFrame()
        original_papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + '1_preprocessed_papers.csv'
        selected_papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + '_manually_filtered_by_full_text_papers.csv'
        
        if exists(selected_papers_file):
            try:
                selected_papers = pd.read_csv(selected_papers_file)
                search_algorithm = ''
                for keyword in semantic_filters:
                    if 'type' in keyword:
                        search_algorithm = keyword['type']
                
                if exists(original_papers_file):
                    try:
                        original_papers = pd.read_csv(original_papers_file)
                        original_papers = original_papers.drop(['id'], axis=1)
                        citations_papers['publication'] = 'semantic_scholar'
                        citations_papers = citations_papers.drop(['id'], axis=1)
                        original_papers = pd.concat([original_papers, selected_papers, citations_papers, removed_papers]).drop_duplicates(keep=False, subset=['doi'])
                        original_papers.loc[:, 'id'] = list(range(1, len(original_papers) + 1))
                        if search_algorithm == 'bert':
                            relevant_papers = bert_search_relevant_papers(semantic_filters, original_papers, selected_papers)
                    except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
                        # User-friendly message explaining what's happening
                        logger.info("Error reading original papers file. Using citations papers only. Please see the log file for details.")
                        # Detailed logging for debugging
                        logger.debug(f"Original papers file reading error: {type(e).__name__}: {str(e)}")
                        if search_algorithm == 'bert':
                            relevant_papers = bert_search_relevant_papers(semantic_filters, citations_papers, selected_papers)
                    except (KeyError, ValueError, TypeError) as e:
                        # User-friendly message explaining what's happening
                        logger.info("Error processing original papers data. Using citations papers only. Please see the log file for details.")
                        # Detailed logging for debugging
                        logger.debug(f"Original papers processing error: {type(e).__name__}: {str(e)}")
                        if search_algorithm == 'bert':
                            relevant_papers = bert_search_relevant_papers(semantic_filters, citations_papers, selected_papers)
                    except Exception as ex:
                        # User-friendly message explaining what's happening
                        logger.info("Unexpected error processing original papers. Using citations papers only. Please see the log file for details.")
                        # Detailed logging for debugging
                        logger.error(f"Unexpected original papers error: {type(ex).__name__}: {str(ex)}")
                        if search_algorithm == 'bert':
                            relevant_papers = bert_search_relevant_papers(semantic_filters, citations_papers, selected_papers)
                else:
                    if search_algorithm == 'bert':
                        relevant_papers = bert_search_relevant_papers(semantic_filters, citations_papers, selected_papers)
            except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
                # User-friendly message explaining what's happening
                logger.info("Error reading selected papers file. Using citations papers only. Please see the log file for details.")
                # Detailed logging for debugging
                logger.debug(f"Selected papers file reading error: {type(e).__name__}: {str(e)}")
                relevant_papers = citations_papers
            except (KeyError, ValueError, TypeError) as e:
                # User-friendly message explaining what's happening
                logger.info("Error processing selected papers data. Using citations papers only. Please see the log file for details.")
                # Detailed logging for debugging
                logger.debug(f"Selected papers processing error: {type(e).__name__}: {str(e)}")
                relevant_papers = citations_papers
            except Exception as ex:
                # User-friendly message explaining what's happening
                logger.info("Unexpected error processing selected papers. Using citations papers only. Please see the log file for details.")
                # Detailed logging for debugging
                logger.error(f"Unexpected selected papers error: {type(ex).__name__}: {str(ex)}")
                relevant_papers = citations_papers
        else:
            relevant_papers = citations_papers
        return relevant_papers
    except Exception as ex:
        # User-friendly message explaining what's happening
        logger.info("Unexpected error in get_relevant_papers. Using citations papers only. Please see the log file for details.")
        # Detailed logging for debugging
        logger.error(f"Unexpected get_relevant_papers error: {type(ex).__name__}: {str(ex)}")
        return citations_papers


def bert_search_relevant_papers(semantic_filters, original_papers, selected_papers):
    try:
        # Initialize BERT model with error handling
        try:
            model = SentenceTransformer('allenai-specter')
        except Exception as model_ex:
            # User-friendly message explaining what's happening
            logger.info("Error loading BERT model for relevant papers search. Returning empty DataFrame. Please see the log file for details.")
            # Detailed logging for debugging
            logger.error(f"BERT model loading error in relevant papers search: {type(model_ex).__name__}: {str(model_ex)}")
            return pd.DataFrame()
        
        try:
            selected_papers['concatenated'] = (selected_papers['title'] + '[SEP]' + selected_papers['abstract'])
            selected_papers_array = selected_papers['concatenated'].values
            logger.info("# Creating the embeddings for the selected papers...")
            encoded_selected_papers = model.encode(selected_papers_array, convert_to_tensor=True, show_progress_bar=True)
        except (KeyError, ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            logger.info("Error preparing selected papers for BERT processing. Returning empty DataFrame. Please see the log file for details.")
            # Detailed logging for debugging
            logger.debug(f"Selected papers preparation error: {type(e).__name__}: {str(e)}")
            return pd.DataFrame()
        except Exception as ex:
            # User-friendly message explaining what's happening
            logger.info("Unexpected error preparing selected papers for BERT processing. Returning empty DataFrame. Please see the log file for details.")
            # Detailed logging for debugging
            logger.error(f"Unexpected selected papers preparation error: {type(ex).__name__}: {str(ex)}")
            return pd.DataFrame()
        
        try:
            score = 0.0
            for keyword in semantic_filters:
                if 'score' in keyword:
                    score = keyword['score']
            original_papers['concatenated'] = (original_papers['title'] + '[SEP]' + original_papers['abstract'])
            original_papers['semantic_score'] = 0.0
        except (KeyError, ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            logger.info("Error preparing original papers for BERT processing. Returning empty DataFrame. Please see the log file for details.")
            # Detailed logging for debugging
            logger.debug(f"Original papers preparation error: {type(e).__name__}: {str(e)}")
            return pd.DataFrame()
        except Exception as ex:
            # User-friendly message explaining what's happening
            logger.info("Unexpected error preparing original papers for BERT processing. Returning empty DataFrame. Please see the log file for details.")
            # Detailed logging for debugging
            logger.error(f"Unexpected original papers preparation error: {type(ex).__name__}: {str(ex)}")
            return pd.DataFrame()
        
        try:
            logger.info("# Semantic comparison of " + str(len(original_papers)) + " preprocessed papers...")
            pbar = tqdm(total=len(original_papers))
            for index, original_paper in original_papers.iterrows():
                try:
                    concatenated = original_paper['concatenated']
                    concatenated_embedding = model.encode(concatenated, convert_to_tensor=True)
                    hits = sentence_util.semantic_search(encoded_selected_papers, concatenated_embedding)
                    avg_score = 0.0
                    for hit in hits:
                        avg_score = avg_score + hit[0]['score']
                    avg_score = avg_score/len(hits)
                    original_papers.loc[index, 'semantic_score'] = avg_score
                except (KeyError, ValueError, TypeError, IndexError) as e:
                    # Skip individual papers that can't be processed
                    logger.debug(f"Error processing individual paper at index {index}: {type(e).__name__}: {str(e)}")
                    continue
                except Exception as ex:
                    # Skip individual papers with unexpected errors
                    logger.debug(f"Unexpected error processing individual paper at index {index}: {type(ex).__name__}: {str(ex)}")
                    continue
                pbar.update(1)
            pbar.close()
        except Exception as ex:
            # User-friendly message explaining what's happening
            logger.info("Error during semantic comparison. Returning empty DataFrame. Please see the log file for details.")
            # Detailed logging for debugging
            logger.error(f"Semantic comparison error: {type(ex).__name__}: {str(ex)}")
            return pd.DataFrame()
        
        try:
            found_papers = original_papers[original_papers['semantic_score'] >= score]
            found_papers = found_papers.drop(['id'], axis=1)
            found_papers = found_papers.drop(['concatenated'], axis=1)
            found_papers['status'] = 'unknown'
            found_papers.loc[:, 'id'] = list(range(1, len(found_papers) + 1))
            return found_papers
        except (KeyError, ValueError, TypeError) as e:
            # User-friendly message explaining what's happening
            logger.info("Error processing final results. Returning empty DataFrame. Please see the log file for details.")
            # Detailed logging for debugging
            logger.debug(f"Final result processing error: {type(e).__name__}: {str(e)}")
            return pd.DataFrame()
        except Exception as ex:
            # User-friendly message explaining what's happening
            logger.info("Unexpected error processing final results. Returning empty DataFrame. Please see the log file for details.")
            # Detailed logging for debugging
            logger.error(f"Unexpected final result processing error: {type(ex).__name__}: {str(ex)}")
            return pd.DataFrame()
            
    except Exception as ex:
        # User-friendly message explaining what's happening
        logger.info("Unexpected error in BERT relevant papers search. Returning empty DataFrame. Please see the log file for details.")
        # Detailed logging for debugging
        logger.error(f"Unexpected BERT relevant papers search error: {type(ex).__name__}: {str(ex)}")
        return pd.DataFrame()
