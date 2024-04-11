from analysis import util
from analysis import retrieve
from analysis import semantic_analyser
from analysis import manual
import sys
import logging
from datetime import datetime
import pandas as pd
import os


def main(parameters_file):

    # Create and configure logger
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

    # Reading search parameters and getting papers from databases
    queries, syntactic_filters, semantic_filters, fields, types, synonyms, databases, dates, start_date, end_date, \
        search_date, folder_name = util.read_parameters(parameters_file)

    if len(queries) > 0:

        # Getting papers from databases
        step = 0
        logger.info(str(step) + '. Getting all papers...')
        retrieve.get_papers(queries, syntactic_filters, synonyms, databases, fields, types, folder_name, dates, start_date, end_date,
                            search_date)

        # Preprocessing papers
        step = step + 1
        logger.info(str(step) + '. Preprocessing papers...')
        file_name = retrieve.preprocess(queries, databases, folder_name, search_date, dates, start_date, end_date, step)
        logger.info('# Preprocessing results can be found at: ' + file_name)
        next_file = str(step) + '_preprocessed_papers.csv'

        # Semantic filter by abstract
        if len(semantic_filters) > 0:
            step = step + 1
            logger.info(str(step) + '. Semantic filter by abstract...')
            file_name = semantic_analyser.search(semantic_filters, folder_name, next_file, search_date, step)
            logger.info('Semantic filtering results can be found at: ' + file_name)
            if file_name == str(step-1) + '_preprocessed_papers.csv':
                step = step - 1
                next_file = str(step) + '_preprocessed_papers.csv'
            else:
                next_file = str(step) + '_semantic_filtered_papers.csv'

        # Manual filtering by abstract
        step = step + 1
        logger.info(str(step) + '. Manual filtering by abstract...')
        next_file, removed_papers_abstract = manual.manual_filter_by_abstract(folder_name, next_file, search_date, step)

        # Manual filtering by full paper
        step = step + 1
        logger.info(str(step) + '. Manual filtering by full paper...')
        next_file, removed_papers_full = manual.manual_filter_by_full_text(folder_name, next_file, search_date, step)
        merge_step_1 = step

        # Snowballing process
        step = step + 1
        logger.info(str(step) + '. Snowballing...')
        removed_papers = pd.concat([removed_papers_abstract, removed_papers_full])
        file_name = retrieve.snowballing(folder_name, search_date, step, dates, start_date, end_date, semantic_filters, removed_papers)
        logger.info('Snowballing results can be found at: ' + file_name)
        next_file = str(step) + '_snowballing_papers.csv'

        # Manual filtering by abstract
        if len(file_name) > 0:
            step = step + 1
            logger.info(str(step) + '. Manual filtering by abstract snowballing papers...')
            next_file, removed_papers_abstract_snowballing = manual.manual_filter_by_abstract(folder_name, next_file, search_date, step)

            # Manual filtering by full paper
            step = step + 1
            logger.info(str(step) + '. Manual filtering by full paper snowballing papers...')
            next_file, removed_papers_full_snowballing = manual.manual_filter_by_full_text(folder_name, next_file, search_date, step)
            merge_step_2 = step
        else:
            merge_step_2 = -1
        # Merge papers
        step = step + 1
        logger.info(str(step) + '. Merging papers...')
        file_name = util.merge_papers(step, merge_step_1, merge_step_2, folder_name, search_date)
        logger.info('Merged papers can be found at: ' + file_name)
    else:
        logger.info('Queries are missing in parameters file...')


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print('Please provide the search parameters file path in the correct format...')
