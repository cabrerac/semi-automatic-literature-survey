import pandas as pd
from analysis import util
import os

fr = 'utf-8'


def manual_filter():
    unknown = 1
    while unknown > 0:
        to_check_papers = pd.read_csv('./papers/to_check_papers.csv')
        total_papers = len(to_check_papers)
        to_check_papers['id'] = list(range(1, total_papers + 1))
        unknown_papers = len(to_check_papers.loc[to_check_papers['status'] == 'unknown'])
        included_papers = len(to_check_papers.loc[to_check_papers['status'] == 'included'])
        excluded_papers = len(to_check_papers.loc[to_check_papers['status'] == 'not included'])
        to_check_paper = to_check_papers.loc[to_check_papers['status'] == 'unknown'].sample()
        progress = round(((total_papers - unknown_papers) / total_papers) * 100, 2)
        print('::: Progress --> ' + str(progress) + '% :::')
        print(' ::: Included (' + str(included_papers) + ') ::: Excluded(' + str(excluded_papers) + ') ::: Unknown('
              + str(unknown_papers) + ') :::')
        print_paper_info(to_check_paper)
        included, algorithm_type, training_schema, algorithm_goal, architecture = ask_manual_input()
        paper_id = to_check_paper['id'].values[0]
        if included == 'included':
            paper_dict = {'id': (included_papers + 1), 'doi': to_check_paper['doi'],
                          'publisher': to_check_paper['publisher'], 'database': to_check_paper['database'],
                          'url': to_check_paper['url'], 'domain': to_check_paper['domain'],
                          'algorithm_type': algorithm_type, 'training_schema': training_schema,
                          'algorithm_goal': algorithm_goal, 'architecture': architecture,
                          'title': to_check_paper['title'], 'abstract': to_check_paper['abstract']}
            paper_df = pd.DataFrame.from_dict(paper_dict)
            util.save('filtered_by_abstract.csv', paper_df, fr)
        update_to_check_papers(to_check_papers, paper_id, included)


def print_paper_info(to_check_paper):
    print('*** New paper ***')
    print(' :: Domain :: ' + str(list(to_check_paper['domain'])[0]).title())
    print(' :: Title :: ' + str(list(to_check_paper['title'])[0].replace('\n', '')).title() + ':: \n')
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


def update_to_check_papers(to_check_papers, paper_id, included):
    for index, row in to_check_papers.iterrows():
        if row['id'] == paper_id:
            row['status'] = included
            to_check_papers.loc[index] = row
    with open('./papers/to_check_papers.csv', 'w', newline='', encoding=fr) as f:
        to_check_papers.to_csv(f, encoding=fr, index=False, header=f.tell() == 0)