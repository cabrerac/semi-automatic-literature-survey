import pandas as pd
from os.path import exists
from util import util
from util.error_standards import (
    ErrorHandler, create_error_context, ErrorSeverity, ErrorCategory,
    get_standard_error_info
)
from util.logging_standards import LogCategory
import logging

fr = 'utf-8'
logger = logging.getLogger('logger')


# Manual filter by abstract
def manual_filter_by_abstract(folder_name, next_file, search_date, step):
    try:
        papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + next_file
        file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) + \
                    '_manually_filtered_by_abstract_papers.csv'
        next_file = str(step) + '_manually_filtered_by_abstract_papers.csv'
        
        if exists(papers_file):
            try:
                to_check_papers = pd.read_csv(papers_file)
                unknown_papers = len(to_check_papers.loc[to_check_papers['status'] == 'unknown'])
                
                while unknown_papers > 0:
                    try:
                        # Data preprocessing with error handling
                        to_check_papers['title'] = to_check_papers['title'].str.lower()
                        to_check_papers = to_check_papers.drop_duplicates('title')
                        to_check_papers['abstract_norm'] = to_check_papers['abstract'].str.lower()
                        to_check_papers['abstract_norm'] = to_check_papers['abstract_norm'].str.replace(' ', '')
                        to_check_papers = to_check_papers.drop_duplicates('abstract_norm')
                        to_check_papers = to_check_papers.drop(columns=['abstract_norm'])
                        
                        total_papers = len(to_check_papers)
                        unknown_papers = len(to_check_papers.loc[to_check_papers['status'] == 'unknown'])
                        included_papers = len(to_check_papers.loc[to_check_papers['status'] == 'included'])
                        excluded_papers = len(to_check_papers.loc[to_check_papers['status'] == 'not included'])
                        progress = round(((total_papers - unknown_papers) / total_papers) * 100, 2)
                        
                        print('::: Progress --> ' + str(progress) + '% :::')
                        print(' ::: Included (' + str(included_papers) + ') ::: Excluded(' + str(excluded_papers) + ') ::: Unknown('
                              + str(unknown_papers) + ') :::')
                        
                        if len(to_check_papers.loc[to_check_papers['status'] == 'unknown']) > 0:
                            try:
                                to_check_paper = to_check_papers.loc[to_check_papers['status'] == 'unknown'].sample()
                                print_paper_info(to_check_paper, file_name)
                                included, algorithm_type, training_schema, algorithm_goal, architecture = ask_manual_input()
                                paper_id = to_check_paper['id'].values[0]
                                
                                if included == 'included':
                                    try:
                                        paper_dict = {'id': (included_papers + 1), 'status': 'unknown', 'doi': to_check_paper['doi'],
                                                      'publisher': to_check_paper['publisher'], 'database': to_check_paper['database'],
                                                      'query_name': to_check_paper['query_name'], 'query_value': to_check_paper['query_value'],
                                                      'url': to_check_paper['url'], 'publication_date': to_check_paper['publication_date'],
                                                      'title': to_check_paper['title'], 'abstract': to_check_paper['abstract']
                                                      }
                                        if 'semantic_score' in to_check_paper:
                                            paper_dict['semantic_score'] = to_check_paper['semantic_score']
                                        paper_df = pd.DataFrame.from_dict(paper_dict)
                                        util.save(file_name, paper_df, fr, 'a+')
                                    except (KeyError, ValueError, TypeError) as e:
                                        print(f"Error creating paper dictionary: {type(e).__name__}: {str(e)}")
                                        continue
                                    except Exception as ex:
                                        print(f"Unexpected error creating paper dictionary: {type(ex).__name__}: {str(ex)}")
                                        continue
                                
                                try:
                                    update_semantic_filtered_papers(to_check_papers, papers_file, paper_id, included)
                                except Exception as update_ex:
                                    print(f"Error updating papers file: {type(update_ex).__name__}: {str(update_ex)}")
                                    continue
                                    
                            except (KeyError, ValueError, TypeError, IndexError) as e:
                                print(f"Error processing individual paper: {type(e).__name__}: {str(e)}")
                                continue
                            except Exception as ex:
                                print(f"Unexpected error processing individual paper: {type(ex).__name__}: {str(ex)}")
                                continue
                                
                    except (KeyError, ValueError, TypeError) as e:
                        print(f"Error during data preprocessing: {type(e).__name__}: {str(e)}")
                        break
                    except Exception as ex:
                        print(f"Unexpected error during data preprocessing: {type(ex).__name__}: {str(ex)}")
                        break
                        
            except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
                context = create_error_context(
                    module="manual",
                    function="manual_filter_by_abstract",
                    operation="file_reading",
                    severity=ErrorSeverity.WARNING,
                    category=ErrorCategory.FILE
                )
                
                error_info = get_standard_error_info("file_not_found")
                error_handler = ErrorHandler(logger)
                error_msg = error_handler.handle_error(
                    error=e,
                    context=context,
                    error_type="FileReadingError",
                    error_description=f"Error reading papers file: {type(e).__name__}: {str(e)}",
                    recovery_suggestion=error_info["recovery"],
                    next_steps=error_info["next_steps"]
                )
                return next_file, pd.DataFrame()
            except Exception as ex:
                context = create_error_context(
                    module="manual",
                    function="manual_filter_by_abstract",
                    operation="file_reading",
                    severity=ErrorSeverity.WARNING,
                    category=ErrorCategory.FILE
                )
                
                error_info = get_standard_error_info("file_not_found")
                error_handler = ErrorHandler(logger)
                error_msg = error_handler.handle_error(
                    error=ex,
                    context=context,
                    error_type="FileReadingError",
                    error_description=f"Unexpected error reading papers file: {type(ex).__name__}: {str(ex)}",
                    recovery_suggestion=error_info["recovery"],
                    next_steps=error_info["next_steps"]
                )
                return next_file, pd.DataFrame()
                
        try:
            to_check_papers = pd.read_csv(papers_file)
            removed_papers = to_check_papers.loc[to_check_papers['status'] == 'not included']
            return next_file, removed_papers
        except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
            context = create_error_context(
                module="manual",
                function="manual_filter_by_abstract",
                operation="final_file_reading",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=e,
                context=context,
                error_type="FileReadingError",
                error_description=f"Error reading final papers file: {type(e).__name__}: {str(e)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            return next_file, pd.DataFrame()
        except Exception as ex:
            context = create_error_context(
                module="manual",
                function="manual_filter_by_abstract",
                operation="final_file_reading",
                severity=ErrorSeverity.WARNING,
                category=ErrorCategory.FILE
            )
            
            error_info = get_standard_error_info("file_not_found")
            error_handler = ErrorHandler(logger)
            error_msg = error_handler.handle_error(
                error=ex,
                context=context,
                error_type="FileReadingError",
                error_description=f"Unexpected error reading final papers file: {type(ex).__name__}: {str(ex)}",
                recovery_suggestion=error_info["recovery"],
                next_steps=error_info["next_steps"]
            )
            return next_file, pd.DataFrame()
            
    except Exception as ex:
        print(f"Unexpected error in manual filter by abstract: {type(ex).__name__}: {str(ex)}")
        return next_file, pd.DataFrame()


def print_paper_info(to_check_paper, file_name):
    try:
        print(' :: Results can be found at: ' + file_name + ' ::')
        print('*** New paper ***')
        
        try:
            if 'domain' in to_check_paper:
                print(' :: Query Name :: ' + str(list(to_check_paper['query_name'])[0]) + ' ::')
                print(' :: Query Value :: ' + str(list(to_check_paper['query_value'])[0]) + ' ::')
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying query information: {type(e).__name__}: {str(e)}")
        
        try:
            print(' :: Publisher :: ' + str(list(to_check_paper['publisher'])[0]).title() + ' :: \n')
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying publisher information: {type(e).__name__}: {str(e)}")
        
        try:
            if 'semantic_score' in to_check_paper:
                print(' :: Semantic Score :: ' + str(list(to_check_paper['semantic_score'])[0]) + ' :: \n')
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying semantic score: {type(e).__name__}: {str(e)}")
        
        try:
            print(' :: Title :: ' + str(list(to_check_paper['title'])[0].replace('\n', '')).title() + ' :: \n')
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying title: {type(e).__name__}: {str(e)}")
        
        try:
            abstract = list(to_check_paper['abstract'])[0].replace('\n', ' ').replace('</p', '').split(' ')
            i = 0
            for word in abstract:
                print(word, end=' ')
                i = i + 1
                if i == 20:
                    print('', end='\n')
                    i = 0
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying abstract: {type(e).__name__}: {str(e)}")
            
    except Exception as ex:
        print(f"Unexpected error in print_paper_info: {type(ex).__name__}: {str(ex)}")


def ask_manual_input():
    try:
        print('\n\n*** Manual input ***')
        included = 'f'
        algorithm_type = ''
        training_schema = ''
        algorithm_goal = ''
        architecture = ''
        
        try:
            while included not in ['included', 'not included']:
                print('(0) not included')
                print('(1) included')
                choice = input("Select: ")
                if choice == '1':
                    included = 'included'
                elif choice == '0':
                    included = 'not included'
                elif choice.lower() in ['quit', 'exit', 'q']:
                    print("Exiting manual input...")
                    included = 'not included'  # Default to not included if user quits
                    break
        except (EOFError, KeyboardInterrupt) as e:
            print(f"\nInput interrupted: {type(e).__name__}")
            included = 'not included'  # Default to not included if input is interrupted
        except Exception as ex:
            print(f"Error in manual input: {type(ex).__name__}: {str(ex)}")
            included = 'not included'  # Default to not included on error
            
        return included, algorithm_type, training_schema, algorithm_goal, architecture
        
    except Exception as ex:
        print(f"Unexpected error in ask_manual_input: {type(ex).__name__}: {str(ex)}")
        return 'not included', '', '', '', ''  # Return safe defaults


def update_semantic_filtered_papers(to_check_papers, papers_file, paper_id, included):
    try:
        for index, row in to_check_papers.iterrows():
            try:
                if row['id'] == paper_id:
                    row['status'] = included
                    to_check_papers.loc[index] = row
            except (KeyError, ValueError, TypeError) as e:
                print(f"Error updating row at index {index}: {type(e).__name__}: {str(e)}")
                continue
            except Exception as ex:
                print(f"Unexpected error updating row at index {index}: {type(ex).__name__}: {str(ex)}")
                continue
                
        try:
            util.save(papers_file, to_check_papers, fr, 'w')
        except Exception as save_ex:
            print(f"Error saving updated papers file: {type(save_ex).__name__}: {str(save_ex)}")
            
    except Exception as ex:
        print(f"Unexpected error in update_semantic_filtered_papers: {type(ex).__name__}: {str(ex)}")


def manual_filter_by_full_text(folder_name, next_file, search_date, step):
    try:
        papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + next_file
        file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) + \
                    '_manually_filtered_by_full_text_papers.csv'
        next_file = str(step) + '_manually_filtered_by_full_text_papers.csv'
        
        if exists(papers_file):
            try:
                filtered_by_abstract = pd.read_csv(papers_file)
                not_classified = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'unknown'])
                
                while not_classified > 0:
                    try:
                        total_papers = len(filtered_by_abstract)
                        not_classified = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'unknown'])
                        included_papers = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'included'])
                        excluded = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'excluded'])
                        progress = round(((total_papers - not_classified) / total_papers) * 100, 2)
                        
                        print('::: Progress --> ' + str(progress) + '% :::')
                        print(' ::: Included Papers (' + str(included_papers) + ') ::: ')
                        print(' ::: Excluded (' + str(excluded) + ') ::: Not Classified(' + str(not_classified) + ') :::')
                        
                        if len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'unknown']) > 0:
                            try:
                                to_check_paper = filtered_by_abstract.loc[filtered_by_abstract['status'] == 'unknown'].sample()
                                print_paper_info_full_paper(to_check_paper, file_name)
                                t = ask_manual_input_full_paper()
                                title = to_check_paper['title']
                                paper_id = to_check_paper['id'].values[0]
                                
                                if t == 'included':
                                    try:
                                        paper_dict = {'id': (included_papers + 1), 'status': t, 'doi': to_check_paper['doi'],
                                                      'publisher': to_check_paper['publisher'], 'database': to_check_paper['database'],
                                                      'query_name': to_check_paper['query_name'], 'query_value': to_check_paper['query_value'],
                                                      'url': to_check_paper['url'], 'publication_date': to_check_paper['publication_date'],
                                                      'title': title, 'abstract': to_check_paper['abstract']
                                                      }
                                        if 'semantic_score' in to_check_paper:
                                            paper_dict['semantic_score'] = to_check_paper['semantic_score']
                                        paper_df = pd.DataFrame.from_dict(paper_dict)
                                        util.save(file_name, paper_df, fr, 'a+')
                                    except (KeyError, ValueError, TypeError) as e:
                                        print(f"Error creating paper dictionary: {type(e).__name__}: {str(e)}")
                                        continue
                                    except Exception as ex:
                                        print(f"Unexpected error creating paper dictionary: {type(ex).__name__}: {str(ex)}")
                                        continue
                                
                                try:
                                    update_filtered_papers_by_abstract(filtered_by_abstract, papers_file, paper_id, t)
                                except Exception as update_ex:
                                    print(f"Error updating papers file: {type(update_ex).__name__}: {str(update_ex)}")
                                    continue
                                    
                            except (KeyError, ValueError, TypeError, IndexError) as e:
                                print(f"Error processing individual paper: {type(e).__name__}: {str(e)}")
                                continue
                            except Exception as ex:
                                print(f"Unexpected error processing individual paper: {type(ex).__name__}: {str(ex)}")
                                continue
                                
                    except (KeyError, ValueError, TypeError) as e:
                        print(f"Error during progress calculation: {type(e).__name__}: {str(e)}")
                        break
                    except Exception as ex:
                        print(f"Unexpected error during progress calculation: {type(ex).__name__}: {str(ex)}")
                        break
                        
            except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
                print(f"Error reading papers file: {type(e).__name__}: {str(e)}")
                return next_file, pd.DataFrame()
            except Exception as ex:
                print(f"Unexpected error reading papers file: {type(ex).__name__}: {str(ex)}")
                return next_file, pd.DataFrame()
                
        try:
            filtered_by_abstract = pd.read_csv(papers_file)
            removed_papers = filtered_by_abstract.loc[filtered_by_abstract['status'] == 'excluded']
            return next_file, removed_papers
        except (pd.errors.EmptyDataError, pd.errors.ParserError, FileNotFoundError) as e:
            print(f"Error reading final papers file: {type(e).__name__}: {str(e)}")
            return next_file, pd.DataFrame()
        except Exception as ex:
            print(f"Unexpected error reading final papers file: {type(ex).__name__}: {str(ex)}")
            return next_file, pd.DataFrame()
            
    except Exception as ex:
        print(f"Unexpected error in manual filter by full text: {type(ex).__name__}: {str(ex)}")
        return next_file, pd.DataFrame()


def print_paper_info_full_paper(to_check_paper, file_name):
    try:
        print(' :: Results can be found at: ' + file_name + ' ::')
        print('*** New paper ***')
        
        try:
            print(' :: DOI :: ' + str(list(to_check_paper['doi'])[0]) + ' ::')
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying DOI: {type(e).__name__}: {str(e)}")
        
        try:
            print(' :: Publisher :: ' + str(list(to_check_paper['publisher'])[0]).title() + ' ::')
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying publisher: {type(e).__name__}: {str(e)}")
        
        try:
            print(' :: url :: [link=' + str(list(to_check_paper['url'])[0]) + ']'+str(list(to_check_paper['url'])[0])+'[/link] ::')
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying URL: {type(e).__name__}: {str(e)}")
        
        try:
            print(' :: Title :: ' + str(list(to_check_paper['title'])[0].replace('\n', '')).title() + ' :: \n')
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying title: {type(e).__name__}: {str(e)}")
        
        try:
            if 'semantic_score' in to_check_paper:
                print(' :: Semantic Score :: ' + str(list(to_check_paper['semantic_score'])[0]) + ' :: \n')
        except (KeyError, IndexError, TypeError) as e:
            print(f"Error displaying semantic score: {type(e).__name__}: {str(e)}")
            
    except Exception as ex:
        print(f"Unexpected error in print_paper_info_full_paper: {type(ex).__name__}: {str(ex)}")


def ask_manual_input_full_paper():
    try:
        print('*** Manual input ***')
        t = 'f'
        
        try:
            while t not in ['included', 'excluded']:
                print('(0) excluded')
                print('(1) included')
                choice = input("Select: ")
                if choice == '0':
                    t = 'excluded'
                elif choice == '1':
                    t = 'included'
                elif choice.lower() in ['quit', 'exit', 'q']:
                    print("Exiting manual input...")
                    t = 'excluded'  # Default to excluded if user quits
                    break
        except (EOFError, KeyboardInterrupt) as e:
            print(f"\nInput interrupted: {type(e).__name__}")
            t = 'excluded'  # Default to excluded if input is interrupted
        except Exception as ex:
            print(f"Error in manual input: {type(ex).__name__}: {str(ex)}")
            t = 'excluded'  # Default to excluded on error
            
        return t
        
    except Exception as ex:
        print(f"Unexpected error in ask_manual_input_full_paper: {type(ex).__name__}: {str(ex)}")
        return 'excluded'  # Return safe default


def update_filtered_papers_by_abstract(filtered_papers, papers_file, paper_id, included):
    try:
        for index, row in filtered_papers.iterrows():
            try:
                if row['id'] == paper_id:
                    row['status'] = included
                    filtered_papers.loc[index] = row
            except (KeyError, ValueError, TypeError) as e:
                print(f"Error updating row at index {index}: {type(e).__name__}: {str(e)}")
                continue
            except Exception as ex:
                print(f"Unexpected error updating row at index {index}: {type(ex).__name__}: {str(ex)}")
                continue
                
        try:
            util.save(papers_file, filtered_papers, fr, 'w')
        except Exception as save_ex:
            print(f"Error saving updated papers file: {type(save_ex).__name__}: {str(save_ex)}")
            
    except Exception as ex:
        print(f"Unexpected error in update_filtered_papers_by_abstract: {type(ex).__name__}: {str(ex)}")
