from analysis import util
from analysis import retrieve
from analysis import semantic_analyser
from analysis import manual

# Reading search parameters and getting papers from databases
print('Reading parameters file...')
domains, interests, keywords, synonyms, fields, types, databases, dates, since, to, search_date, folder_name, \
syntactic_filters, semantic_filters = util.read_parameters('parameters_doa.yaml')


# 0. Getting papers from databases
#print('2. Getting all papers...')
#retrieve.get_papers(domains, interests, keywords, synonyms, fields, types, folder_name, dates, since, to, search_date)


# 1. Preprocessing papers
#print('1. Preprocessing papers...')
#retrieve.preprocess(domains, databases, folder_name, search_date, since, to, 1)


# 2. Syntactic filter by abstract
#print('2. Syntactic filter by abstract...')
#retrieve.filter_papers(syntactic_filters, folder_name, search_date, 2)


# 3. Semantic filter by abstract
#print('3. Semantic filter by abstract')
#semantic_analyser.get_to_check_papers(semantic_filters, folder_name, search_date, 3)


# 4. Manual filtering by abstract
print('4. Manual filtering by abstract...')
manual.manual_filter_by_abstract(folder_name, search_date, 4)

# 5. Manual filtering by full paper
print('5. Manual filtering by full paper...')
manual.manual_filter_by_full_text(folder_name, search_date, 5)

#f1 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/4_manually_filtered_by_abstract_papers.csv'
#f2 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/passed/5_manually_filtered_by_full_text_papers.csv'
#result = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/5_manually_filtered_by_full_text_papers.csv'
#util.pass_papers(f1, result, result)
#util.remove_repeated(result)

# Snowballing process and repeat filters on citing papers

# 6. Snowballing
#print('6. Snowballing...')
#retrieve.get_citations(folder_name, search_date, 6)


# 7. Syntactic filter by abstract
#print('7. Syntactic filter by abstract...')
#retrieve.filter_papers(syntactic_filters, folder_name, search_date, 7)


# 8. Semantic filter by abstract
#print('8. Getting to check citations papers...')
#semantic_analyser.get_to_check_papers(semantic_filters, folder_name, search_date, 8)


# 9. Manual filtering by abstract
#print('9. Manual filtering by abstract...')
#manual.manual_filter_by_abstract(folder_name, search_date, 9)


# 10. Manual filtering by full paper
#print('10. Manual filtering by full paper...')
#manual.manual_filter_by_full_text(folder_name, search_date, 10)


# 11. Merge papers
#print('11. Merge papers...')
#f1 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/5_manually_filtered_by_full_text_papers.csv'
#f2 = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/10_manually_filtered_by_full_text_papers.csv'
#result = './papers/' + folder_name + '/' + str(search_date).replace('-', '_') + '/11_final_papers.csv'
#util.merge_papers(f1, f2, result)


# 12. Plot results
#print('12. Plotting results...')
#util.plot()
