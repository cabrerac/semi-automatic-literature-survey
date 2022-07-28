import pandas as pd
from analysis import util
import os

fr = 'utf-8'

# Manual filter by abstract
def manual_filter_by_abstract(folder_name, search_date, step):
    papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + '_semantic_filtered_papers.csv'
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) + '_manually_filtered_by_abstract_papers.csv'
    unknown_papers = 1
    while unknown_papers > 0:
        to_check_papers = pd.read_csv(papers_file)
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
            to_check_paper = to_check_papers.loc[to_check_papers['status'] == 'unknown'].sample()
            print_paper_info(to_check_paper, file_name)
            included, algorithm_type, training_schema, algorithm_goal, architecture = ask_manual_input()
            paper_id = to_check_paper['id'].values[0]
            if included == 'included':
                paper_dict = {'id': (included_papers + 1), 'status': 'unknown', 'doi': to_check_paper['doi'],
                              'publisher': to_check_paper['publisher'], 'database': to_check_paper['database'],
                              'query_name': to_check_paper['query_name'], 'query_value': to_check_paper['query_value'],
                              'url': to_check_paper['url'], 'publication_date': to_check_paper['publication_date'],
                              'title': to_check_paper['title'], 'abstract': to_check_paper['abstract']
                              }
                paper_df = pd.DataFrame.from_dict(paper_dict)
                with open(file_name, 'a+', newline='', encoding=fr) as f:
                    paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
            update_semantic_filtered_papers(to_check_papers, papers_file, paper_id, included)


def print_paper_info(to_check_paper, file_name):
    print(' :: Results can be found at: ' + file_name + ' ::')
    print('*** New paper ***')
    if 'domain' in to_check_paper:
        print(' :: Query Name :: ' + str(list(to_check_paper['query_name'])[0]) + ' ::')
        print(' :: Query Value :: ' + str(list(to_check_paper['query_value'])[0]) + ' ::')
    print(' :: Title :: ' + str(list(to_check_paper['title'])[0].replace('\n', '')).title() + ' :: \n')
    abstract = list(to_check_paper['abstract'])[0].replace('\n', ' ').replace('</p', '').split(' ')
    i = 0
    for word in abstract:
        print(word, end=' ')
        i = i + 1
        if i == 20:
            print('', end='\n')
            i = 0


def ask_manual_input():
    print('\n\n*** Manual input ***')
    included = 'f'
    algorithm_type = ''
    training_schema = ''
    algorithm_goal = ''
    architecture = ''
    while included not in ['included', 'not included']:
        print('(0) not included')
        print('(1) included')
        choice = input("Select: ")
        if choice == '1':
            included = 'included'
        elif choice == '0':
            included = 'not included'
    return included, algorithm_type, training_schema, algorithm_goal, architecture


def update_semantic_filtered_papers(to_check_papers, papers_file, paper_id, included):
    for index, row in to_check_papers.iterrows():
        if row['id'] == paper_id:
            row['status'] = included
            to_check_papers.loc[index] = row
    with open(papers_file, 'w', newline='', encoding=fr) as f:
        to_check_papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def manual_filter_by_full_text(folder_name, search_date, step):
    papers_file = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step-1) + '_manually_filtered_by_abstract_papers.csv'
    file_name = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/' + str(step) + '_manually_filtered_by_full_text_papers.csv'
    not_classified = 1
    while not_classified > 0:
        filtered_by_abstract = pd.read_csv(papers_file)
        total_papers = len(filtered_by_abstract)
        not_classified = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'unknown'])
        included_papers = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'included'])
        excluded = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'excluded'])
        progress = round(((total_papers - not_classified) / total_papers) * 100, 2)
        print('::: Progress --> ' + str(progress) + '% :::')
        print(' ::: Included Papers (' + str(included_papers) + ') ::: ')
        print(' ::: Excluded (' + str(excluded) + ') ::: Not Classified(' + str(not_classified) + ') :::')
        if len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'unknown']) > 0:
            to_check_paper = filtered_by_abstract.loc[filtered_by_abstract['status'] == 'unknown'].sample()
            print_paper_info_full_paper(to_check_paper, file_name)
            t, title = ask_manual_input_full_paper()
            if len(title) == 0:
                title = to_check_paper['title']
            paper_id = to_check_paper['id'].values[0]
            if t == 'included':
                paper_dict = {'id': (included_papers + 1), 'status': t, 'doi': to_check_paper['doi'],
                              'publisher': to_check_paper['publisher'], 'database': to_check_paper['database'],
                              'query_name': to_check_paper['query_name'], 'query_value': to_check_paper['query_value'],
                              'url': to_check_paper['url'], 'publication_date': to_check_paper['publication_date'],
                              'title': title, 'abstract': to_check_paper['abstract']}
                paper_df = pd.DataFrame.from_dict(paper_dict)
                with open(file_name, 'a+', newline='', encoding=fr) as f:
                    paper_df.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
            update_filtered_papers_by_abstract(filtered_by_abstract, papers_file, paper_id, t)


def print_paper_info_full_paper(to_check_paper, file_name):
    print(' :: Results can be found at: ' + file_name + ' ::')
    print('*** New paper ***')
    print(' :: DOI :: ' + str(list(to_check_paper['doi'])[0]) + ' ::')
    print(' :: Publisher :: ' + str(list(to_check_paper['publisher'])[0]) + ' ::')
    print(' :: url :: ' + str(list(to_check_paper['url'])[0]) + ' ::')
    print(' :: Title :: ' + str(list(to_check_paper['title'])[0].replace('\n', '')).title() + ' :: \n')


def ask_manual_input_full_paper():
    print('*** Manual input ***')
    t = 'f'
    title = ''
    while t not in ['architecture', 'experiments', 'excluded']:
        print('(0) excluded')
        print('(1) included')
        choice = input("Select: ")
        if choice == '0':
            t = 'excluded'
        if choice == '1':
            t = 'included'
            print('Change title?')
            choice = input("y/n")
            if choice == "y":
                title = input("New title: ")
    return t, title


def update_filtered_papers_by_abstract(filtered_papers, papers_file, paper_id, included):
    for index, row in filtered_papers.iterrows():
        if row['id'] == paper_id:
            row['status'] = included
            filtered_papers.loc[index] = row
    with open(papers_file, 'w', newline='', encoding=fr) as f:
        filtered_papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)
