from util import util
from analysis import retrieve
from analysis import semantic_analyser
from analysis import manual
import sys
import logging
from datetime import datetime
import pandas as pd
import os


def main(parameters_file):
    try:
        # Create and configure logger with error handling
        try:
            logger = logging.getLogger("logger")
            logger.setLevel(logging.DEBUG)
            # Console log
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)  # Set the level for console output
            console_formatter = logging.Formatter('%(levelname)s: %(message)s')
            console_handler.setFormatter(console_formatter)
            # File log
            if not os.path.exists('./logs/'):
                os.makedirs('./logs/')
            log_file = './logs/' + parameters_file.replace('.yaml', '_' + datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p") + '.log')
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)  # Set the level for file output
            file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_formatter)

            logger.addHandler(console_handler)
            logger.addHandler(file_handler)
        except Exception as ex:
            print(f"Critical error setting up logging: {type(ex).__name__}: {str(ex)}")
            return

        # Reading search parameters with pipeline-level error handling
        try:
            queries, syntactic_filters, semantic_filters, fields, types, synonyms, databases, dates, start_date, end_date, \
                search_date, folder_name = util.read_parameters(parameters_file)
        except Exception as ex:
            logger.error(f"Critical error reading parameters file {parameters_file}: {type(ex).__name__}: {str(ex)}")
            logger.info("Pipeline cannot continue without valid parameters. Exiting.")
            return

        # Validate parsed queries with pipeline-level error handling
        try:
            parsed_queries, valid = util.parse_queries(queries)
            if not valid:
                logger.error("Invalid queries in parameters file. Pipeline cannot continue.")
                logger.info("Please check your query syntax and restart the pipeline.")
                return
        except Exception as ex:
            logger.error(f"Critical error parsing queries: {type(ex).__name__}: {str(ex)}")
            logger.info("Pipeline cannot continue with invalid queries. Exiting.")
            return

        # Pipeline execution with strategic error handling
        try:
            # Retrieving papers from the databases
            step = 0
            logger.info(str(step) + '. Retrieving papers from the databases...')
            try:
                retrieve.get_papers(queries, syntactic_filters, synonyms, databases, fields, types, folder_name, dates, start_date, end_date,
                                    search_date)
            except Exception as ex:
                logger.error(f"Critical error in paper retrieval step {step}: {type(ex).__name__}: {str(ex)}")
                logger.info("Pipeline cannot continue without retrieved papers. Exiting.")
                return

            # Preprocessing papers
            step = step + 1
            logger.info(str(step) + '. Preprocessing papers...')
            try:
                file_name = retrieve.preprocess(queries, databases, folder_name, search_date, dates, start_date, end_date, step)
                if not file_name or file_name == "":
                    logger.error(f"Critical error: Preprocessing step {step} returned no file name")
                    logger.info("Pipeline cannot continue without preprocessed papers. Exiting.")
                    return
                logger.info('# Preprocessing results can be found at: ' + file_name)
                next_file = str(step) + '_preprocessed_papers.csv'
            except Exception as ex:
                logger.error(f"Critical error in preprocessing step {step}: {type(ex).__name__}: {str(ex)}")
                logger.info("Pipeline cannot continue without preprocessed papers. Exiting.")
                return

            # Semantic filter by abstract
            if len(semantic_filters) > 0:
                step = step + 1
                logger.info(str(step) + '. Semantic filter by abstract...')
                try:
                    file_name = semantic_analyser.search(semantic_filters, folder_name, next_file, search_date, step)
                    logger.info('Semantic filtering results can be found at: ' + file_name)
                    if file_name == str(step-1) + '_preprocessed_papers.csv':
                        step = step - 1
                        next_file = str(step) + '_preprocessed_papers.csv'
                    else:
                        next_file = str(step) + '_semantic_filtered_papers.csv'
                except Exception as ex:
                    logger.error(f"Critical error in semantic filtering step {step}: {type(ex).__name__}: {str(ex)}")
                    logger.info("Continuing with preprocessed papers (semantic filtering failed)")
                    step = step - 1
                    next_file = str(step) + '_preprocessed_papers.csv'

            # Manual filtering by abstract
            step = step + 1
            logger.info(str(step) + '. Manual filtering by abstract...')
            try:
                next_file, removed_papers_abstract = manual.manual_filter_by_abstract(folder_name, next_file, search_date, step)
                if not next_file or next_file == "":
                    logger.error(f"Critical error: Manual abstract filtering step {step} returned no file name")
                    logger.info("Pipeline cannot continue without manually filtered papers. Exiting.")
                    return
            except Exception as ex:
                logger.error(f"Critical error in manual abstract filtering step {step}: {type(ex).__name__}: {str(ex)}")
                logger.info("Pipeline cannot continue without manual filtering. Exiting.")
                return

            # Manual filtering by full paper
            step = step + 1
            logger.info(str(step) + '. Manual filtering by full paper...')
            try:
                next_file, removed_papers_full = manual.manual_filter_by_full_text(folder_name, next_file, search_date, step)
                if not next_file or next_file == "":
                    logger.error(f"Critical error: Manual full-text filtering step {step} returned no file name")
                    logger.info("Pipeline cannot continue without manually filtered papers. Exiting.")
                    return
                merge_step_1 = step
            except Exception as ex:
                logger.error(f"Critical error in manual full-text filtering step {step}: {type(ex).__name__}: {str(ex)}")
                logger.info("Pipeline cannot continue without manual filtering. Exiting.")
                return

            # Snowballing process
            step = step + 1
            logger.info(str(step) + '. Snowballing...')
            try:
                # Validate removed papers before concatenation
                if removed_papers_abstract is None or removed_papers_full is None:
                    logger.warning("Some removed papers are None, using empty DataFrames for snowballing")
                    removed_papers_abstract = pd.DataFrame() if removed_papers_abstract is None else removed_papers_abstract
                    removed_papers_full = pd.DataFrame() if removed_papers_full is None else removed_papers_full
                
                removed_papers = pd.concat([removed_papers_abstract, removed_papers_full])
                file_name = retrieve.snowballing(folder_name, search_date, step, dates, start_date, end_date, semantic_filters, removed_papers)
                logger.info('Snowballing results can be found at: ' + file_name)
                next_file = str(step) + '_snowballing_papers.csv'
            except Exception as ex:
                logger.error(f"Critical error in snowballing step {step}: {type(ex).__name__}: {str(ex)}")
                logger.info("Continuing without snowballing papers")
                file_name = ""
                next_file = str(step-1) + '_manually_filtered_by_full_text_papers.csv'

            # Manual filtering by abstract for snowballing papers
            if file_name and len(file_name) > 0:
                step = step + 1
                logger.info(str(step) + '. Manual filtering by abstract snowballing papers...')
                try:
                    next_file, removed_papers_abstract_snowballing = manual.manual_filter_by_abstract(folder_name, next_file, search_date, step)
                    if not next_file or file_name == "":
                        logger.warning(f"Manual abstract filtering for snowballing step {step} returned no file name")
                        merge_step_2 = -1
                    else:
                        # Manual filtering by full paper for snowballing
                        step = step + 1
                        logger.info(str(step) + '. Manual filtering by full paper snowballing papers...')
                        try:
                            next_file, removed_papers_full_snowballing = manual.manual_filter_by_full_text(folder_name, next_file, search_date, step)
                            merge_step_2 = step
                        except Exception as ex:
                            logger.error(f"Error in manual full-text filtering for snowballing step {step}: {type(ex).__name__}: {str(ex)}")
                            merge_step_2 = -1
                except Exception as ex:
                    logger.error(f"Error in manual abstract filtering for snowballing step {step}: {type(ex).__name__}: {str(ex)}")
                    merge_step_2 = -1
            else:
                merge_step_2 = -1

            # Merge papers
            step = step + 1
            logger.info(str(step) + '. Merging papers...')
            try:
                file_name = util.merge_papers(step, merge_step_1, merge_step_2, folder_name, search_date)
                if not file_name or file_name == "":
                    logger.error(f"Critical error: Merging step {step} returned no file name")
                    logger.info("Pipeline completed but final merge failed. Check output files manually.")
                    return
                logger.info('Merged papers can be found at: ' + file_name)
                logger.info('Pipeline completed successfully!')
            except Exception as ex:
                logger.error(f"Critical error in merging step {step}: {type(ex).__name__}: {str(ex)}")
                logger.info("Pipeline completed but final merge failed. Check output files manually.")
                return
                
        except Exception as ex:
            logger.error(f"Critical error in pipeline execution: {type(ex).__name__}: {str(ex)}")
            logger.info("Pipeline execution failed. Check logs for details.")
            return
            
    except Exception as ex:
        print(f"Critical error in main pipeline: {type(ex).__name__}: {str(ex)}")
        print("Pipeline cannot start. Check your configuration and try again.")
        return


if __name__ == "__main__":
    try:
        if len(sys.argv) == 2:
            parameters_file = sys.argv[1]
            
            # Validate parameters file exists
            if not os.path.exists(parameters_file):
                print(f"Error: Parameters file '{parameters_file}' not found.")
                print("Please provide a valid parameters file path.")
                sys.exit(1)
                
            # Validate file extension
            if not parameters_file.endswith('.yaml') and not parameters_file.endswith('.yml'):
                print(f"Warning: Parameters file '{parameters_file}' doesn't have .yaml or .yml extension.")
                print("The file will be processed, but ensure it contains valid YAML content.")
            
            # Execute main pipeline
            main(parameters_file)
        else:
            print('Error: Incorrect number of arguments.')
            print('Usage: python main.py <parameters_file.yaml>')
            print('Example: python main.py parameters_ar.yaml')
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nPipeline execution interrupted by user.")
        sys.exit(1)
    except Exception as ex:
        print(f"Critical error in main entry point: {type(ex).__name__}: {str(ex)}")
        print("Pipeline cannot start. Check your configuration and try again.")
        sys.exit(1)
