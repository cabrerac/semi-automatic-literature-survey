from analysis import util
from analysis import retrieve
from analysis import semantic_analyser
from analysis import manual
import sys


def main(parameters_file):

    # Reading search parameters and getting papers from databases
    queries, syntactic_filters, semantic_filters, fields, types, synonyms, databases, dates, since, to, \
        search_date, folder_name = util.read_parameters(parameters_file)

    if len(queries) > 0:

        # Getting papers from databases
        step = 0
        print(str(step) + '. Getting all papers...')
        retrieve.get_papers(queries, synonyms, databases, fields, types, folder_name, dates, since, to,
                            search_date)

        # Preprocessing papers
        step = step + 1
        print(str(step) + '. Preprocessing papers...')
        file_name = retrieve.preprocess(queries, databases, folder_name, search_date, since, to, step)
        print('Preprocessing results can be found at: ' + file_name)
        next_file = str(step) + '_preprocessed_papers.csv'

        # Syntactic filter by abstract
        if len(syntactic_filters) > 0:
            step = step + 1
            print(str(step) + '. Syntactic filter by abstract...')
            file_name = retrieve.filter_papers(syntactic_filters, synonyms, folder_name, search_date, step)
            print('Syntactic filtering results can be found at: ' + file_name)
            next_file = str(step) + '_syntactic_filtered_papers.csv'

        # Semantic filter by abstract
        if len(semantic_filters) > 0:
            step = step + 1
            print(str(step) + '. Semantic filter by abstract...')
            file_name = semantic_analyser.lbl2vec(semantic_filters, folder_name, search_date, step)
            print('Semantic filtering results can be found at: ' + file_name)
            next_file = str(step) + '_semantic_filtered_papers.csv'

        # Manual filtering by abstract
        step = step + 1
        print(str(step) + '. Manual filtering by abstract...')
        manual.manual_filter_by_abstract(folder_name, next_file, search_date, step)

        # Manual filtering by full paper
        step = step + 1
        print(str(step) + '. Manual filtering by full paper...')
        manual.manual_filter_by_full_text(folder_name, search_date, step)
        merge_step_1 = step

        # Snowballing process and apply filters on citing papers
        # Snowballing
        step = step + 1
        print(str(step) + '. Snowballing...')
        file_name = retrieve.get_citations(folder_name, search_date, step)
        print('Snowballing results can be found at: ' + file_name)
        next_file = str(step) + '_preprocessed_papers.csv'

        # Syntactic filter by abstract
        if len(syntactic_filters) > 0:
            step = step + 1
            print(str(step) + '. Syntactic filter by abstract snowballing papers...')
            file_name = retrieve.filter_papers(syntactic_filters, folder_name, search_date, step)
            print('Syntactic filtering results can be found at: ' + file_name)
            next_file = str(step) + '_syntactic_filtered_papers.csv'

        # Semantic filter by abstract
        if len(semantic_filters) > 0:
            step = step + 1
            print(str(step) + '. Semantic filter snowballing papers...')
            file_name = semantic_analyser.get_to_check_papers(semantic_filters, folder_name, search_date, step)
            print('Semantic filtering results can be found at: ' + file_name)
            next_file = str(step) + '_semantic_filtered_papers.csv'

        # Manual filtering by abstract
        step = step + 1
        print(str(step) + '. Manual filtering by abstract snowballing papers...')
        manual.manual_filter_by_abstract(folder_name, next_file, search_date, step)

        # Manual filtering by full paper
        step = step + 1
        print(str(step) + '. Manual filtering by full paper snowballing papers...')
        manual.manual_filter_by_full_text(folder_name, search_date, step)
        merge_step_2 = step

        # Merge papers
        print(str(step) + '. Merging papers...')
        file_name = util.merge_papers(merge_step_1, merge_step_2, folder_name, search_date)
        print('Merged papers can be found at: ' + file_name)
    else:
        print('Queries are missing in parameters file...')


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print('Please provide the search parameters file path in the correct format...')
