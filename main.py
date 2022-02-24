from analysis import util
from analysis import retrieve
from analysis import semantic_analyser
from analysis import manual


# 1. Reading search parameters
print('1. Reading parameters file...')
domains, interests, keywords, synonyms, fields, types, databases, since, to, file_name = util.read_parameters(
    'parameters_doa.yaml')


# 2. Getting papers from databases
#print('2. Getting all papers...')
#retrieve.get_papers(domains, interests, keywords, synonyms, fields, types, file_name, since, to)


# 3. Preprocessing papers
#print('3. Preprocessing papers...')
#retrieve.preprocess(domains, databases, file_name, since, to)


# 4. Filtering papers by abstract
#print('4. Filtering papers by abstract...')
#retrieve.filter_papers(keywords, file_name, to)


# 5. Getting papers to check based on semantic analysis
#print('5. Getting to check papers...')
#semantic_analyser.get_to_check_papers(keywords, file_name, to)

# 6. Manual filtering of papers to check
#print('6. Manual filtering of papers to check...')
#manual.manual_filter_by_abstract(file_name, to)


# 7. Manual filtering by full paper
#print('7. Manual filtering by full paper...')
#manual.manual_filter_by_full_text(file_name, to)
#manual.remove_repeated(file_name, to)


# 8. Snowballing
#print('8. Snowballing...')
#retrieve.get_citations(file_name, to)

# 9. Filtering citation papers by abstract
#print('9. Filtering citation papers by abstract...')
#retrieve.filter_papers(keywords, './papers/citations_papers.csv', 'filtered_citations_papers.csv')

# 10. Getting citations papers to check based on semantic analysis
#print('10. Getting to check citations papers...')
#semantic_analyser.get_to_check_papers(keywords, './papers/filtered_citations_papers.csv',
#                                      'to_check_citations_papers.csv')

# 11. Manual filtering of citations papers to check
#print('11. Manual filtering of citations papers to check...')
#manual.manual_filter_by_abstract('./papers/to_check_citations_papers.csv', 'filtered_by_abstract_citations.csv')


# 12. Manual filtering by full citations paper
#print('12. Manual filtering by full paper...')
#manual.manual_filter_by_full_text('./papers/filtered_by_abstract_citations.csv', 'filtered_by_full_text_citations.csv')


# 13. Plot results
print('13. Plotting results...')
util.plot()
