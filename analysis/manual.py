import pandas as pd
from analysis import util
import os

fr = 'utf-8'


def manual_filter_by_abstract(papers_file, output_file):
    #update_accepted()
    #add_publication_date()
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
            print_paper_info(to_check_paper)
            included, algorithm_type, training_schema, algorithm_goal, architecture = ask_manual_input()
            paper_id = to_check_paper['id'].values[0]
            if included == 'included':
                paper_dict = {'id': (included_papers + 1), 'doi': to_check_paper['doi'],
                              'publisher': to_check_paper['publisher'], 'database': to_check_paper['database'],
                              'url': to_check_paper['url'], 'domain': to_check_paper['domain'],
                              'publication_date': to_check_paper['publication_date'],
                            'algorithm_type': algorithm_type, 'training_schema': training_schema,
                            'algorithm_goal': algorithm_goal, 'architecture': architecture,
                            'title': to_check_paper['title'], 'abstract': to_check_paper['abstract']}
                paper_df = pd.DataFrame.from_dict(paper_dict)
                util.save(output_file, paper_df, fr)
            update_to_check_papers(to_check_papers, papers_file, paper_id, included)


def update_accepted():
    previous = pd.read_csv('./papers/to_check_papers_v1.csv')
    current = pd.read_csv('./papers/to_check_papers.csv')
    known = previous.loc[previous['status'] != 'unknown']
    for index, row in known.iterrows():
        update_to_check_papers_by_title(current, row['title'], row['status'])


def add_publication_date():
    to_check = pd.read_csv('./papers/to_check_papers.csv')
    manual = pd.read_csv('./papers/filtered_by_abstract.csv')
    papers = []
    for index, row in manual.iterrows():
        row['publication_date'] = get_update_date(to_check, row['title'])
        paper_dict = {'id': str(row['id']), 'doi': row['doi'],
                      'publisher': row['publisher'], 'database': row['database'],
                      'url': row['url'], 'domain': row['domain'],
                      'publication_date': row['publication_date'],
                      'algorithm_type': row['algorithm_type'], 'training_schema': row['training_schema'],
                      'algorithm_goal': row['algorithm_goal'], 'architecture': row['architecture'],
                      'title': row['title'], 'abstract': row['abstract']}
        papers.append(paper_dict)
    paper_df = pd.DataFrame.from_dict(papers)
    util.save('filtered_by_abstract_updated.csv', paper_df, fr)


def get_update_date(to_check_papers, title):
    for index, row in to_check_papers.iterrows():
        if row['title'] == title:
            publication_date = row['publication_date']
            return publication_date


def print_paper_info(to_check_paper):
    print('*** New paper ***')
    print(' :: Domain :: ' + str(list(to_check_paper['domain'])[0]).title() + ' ::')
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
            print('Algorithm type?')
            while algorithm_type not in ['supervised', 'unsupervised', 'semi-supervised', 'rl', 'not defined']:
                print('(1) supervised')
                print('(2) unsupervised')
                print('(3) semi-supervised')
                print('(4) rl')
                print('(5) not defined')
                choice = input("Select: ")
                if choice == '1':
                    algorithm_type = 'supervised'
                if choice == '2':
                    algorithm_type = 'unsupervised'
                if choice == '3':
                    algorithm_type = 'semi-supervised'
                if choice == '4':
                    algorithm_type = 'rl'
                if choice == '5':
                    algorithm_type = 'not defined'
            print('Training schema?')
            while training_schema not in ['batch', 'online', 'not defined']:
                print('(1) batch')
                print('(2) online')
                print('(3) not defined')
                choice = input("Select: ")
                if choice == '1':
                    training_schema = 'batch'
                if choice == '2':
                    training_schema = 'online'
                if choice == '3':
                    training_schema = 'not defined'
            print('Algorithm goal?')
            while algorithm_goal not in ['regression', 'classification', 'clustering', 'decision making',
                                         'association rule learning', 'blind source operation',
                                         'dimensionality reduction', 'not defined']:
                print('(1) regression')
                print('(2) classification')
                print('(3) clustering')
                print('(4) decision making')
                print('(5) association rule learning')
                print('(6) blind source operation')
                print('(7) dimensionality reduction')
                print('(8) not defined')
                choice = input("Select: ")
                if choice == '1':
                    algorithm_goal = 'regression'
                if choice == '2':
                    algorithm_goal = 'classification'
                if choice == '3':
                    algorithm_goal = 'clustering'
                if choice == '4':
                    algorithm_goal = 'decision making'
                if choice == '5':
                    algorithm_goal = 'association rule learning'
                if choice == '6':
                    algorithm_goal = 'blind source operation'
                if choice == '7':
                    algorithm_goal = 'dimensionality reduction'
                if choice == '8':
                    algorithm_goal = 'not defined'
            print('Architecture?')
            while architecture not in ['centralised', 'decentralised', 'hybrid', 'not defined']:
                print('(1) centralised')
                print('(2) decentralised')
                print('(3) hybrid')
                print('(4) not defined')
                choice = input("Select: ")
                if choice == '1':
                    architecture = 'centralised'
                if choice == '2':
                    architecture = 'decentralised'
                if choice == '3':
                    architecture = 'hybrid'
                if choice == '4':
                    architecture = 'not defined'
        elif choice == '0':
            included = 'not included'
    return included, algorithm_type, training_schema, algorithm_goal, architecture


def update_to_check_papers(to_check_papers, papers_file, paper_id, included):
    for index, row in to_check_papers.iterrows():
        if row['id'] == paper_id:
            row['status'] = included
            to_check_papers.loc[index] = row
    with open(papers_file, 'w', newline='', encoding=fr) as f:
        to_check_papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def update_to_check_papers_by_title(to_check_papers, title, included):
    for index, row in to_check_papers.iterrows():
        if row['title'] == title:
            row['status'] = included
            to_check_papers.loc[index] = row
    with open('./papers/to_check_papers.csv', 'w', newline='', encoding=fr) as f:
        to_check_papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)


def manual_filter_by_full_text(papers_file, output_file):
    not_classified = 1
    while not_classified > 0:
        filtered_by_abstract = pd.read_csv(papers_file)
        total_papers = len(filtered_by_abstract)
        not_classified = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'not classified'])
        architecture = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'architecture'])
        experiments = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'experiments'])
        excluded = len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'excluded'])
        included_papers = architecture + experiments
        progress = round(((total_papers - not_classified) / total_papers) * 100, 2)
        print('::: Progress --> ' + str(progress) + '% :::')
        print(' ::: Architecture (' + str(architecture) + ') ::: Experiments(' + str(experiments) + ') :::')
        print(' ::: Excluded (' + str(excluded) + ') ::: Not Classified(' + str(not_classified) + ') :::')
        if len(filtered_by_abstract.loc[filtered_by_abstract['status'] == 'not classified']) > 0:
            to_check_paper = filtered_by_abstract.loc[filtered_by_abstract['status'] == 'not classified'].sample()
            print_paper_info_full_paper(to_check_paper)
            t, title = ask_manual_input_full_paper()
            if len(title) == 0:
                title = to_check_paper['title']
            paper_id = to_check_paper['id'].values[0]
            if t != 'excluded':
                paper_dict = {'id': (included_papers + 1), 'type': t, 'doi': to_check_paper['doi'],
                              'publisher': to_check_paper['publisher'], 'database': to_check_paper['database'],
                              'url': to_check_paper['url'], 'domain': to_check_paper['domain'],
                              'publication_date': to_check_paper['publication_date'],
                              'title': title, 'abstract': to_check_paper['abstract']}
                paper_df = pd.DataFrame.from_dict(paper_dict)
                util.save(output_file, paper_df, fr)
            update_filtered_papers_by_abstract(filtered_by_abstract, papers_file, paper_id, t)


def print_paper_info_full_paper(to_check_paper):
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
        print('(1) architecture')
        print('(2) experiments')
        choice = input("Select: ")
        if choice == '0':
            t = 'excluded'
        if choice == '1':
            t = 'architecture'
        if choice == '2':
            t = 'experiments'
        if t != 'excluded':
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


def remove_repeated(papers_file):
    papers = pd.read_csv(papers_file)
    papers['title_norm'] = papers['title'].str.lower()
    papers['title_norm'] = papers['title_norm'].str.replace(' ', '')
    papers = papers.drop_duplicates('title_norm')
    papers = papers.drop(columns=['title_norm'])

    papers['abstract_norm'] = papers['abstract'].str.lower()
    papers['abstract_norm'] = papers['abstract_norm'].str.replace(' ', '')
    papers = papers.drop_duplicates('abstract_norm')
    papers = papers.drop(columns=['abstract_norm'])
    with open('./papers/final_papers.csv', 'w', newline='', encoding=fr) as f:
        papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)