from analysis import util
from analysis import retrieve
from analysis import semantic_analyser
from analysis import manual


# 1. Reading search parameters
print('1. Reading parameters file...')
domains, interests, keywords, synonyms, fields, types, databases = util.read_parameters('parameters.yaml')


# 2. Getting papers from databases
#print('2. Getting all papers...')
#retrieve.get_papers(domains, interests, keywords, synonyms, fields, types)


# 3. Preprocessing papers
#print('3. Preprocessing papers...')
#retrieve.preprocess(domains, databases)


# 4. Filtering papers by abstract
#print('4. Filtering papers by abstract...')
#retrieve.filter_papers(keywords, './papers/preprocessed_papers.csv', 'filtered_papers.csv')


# 5. Getting papers to check based on semantic analysis
#print('5. Getting to check papers...')
#semantic_analyser.get_to_check_papers(keywords, './papers/filtered_papers.csv', 'to_check_papers.csv')

# 6. Manual filtering of papers to check
#print('6. Manual filtering of papers to check...')
#manual.manual_filter_by_abstract('./papers/to_check_papers.csv', 'filtered_by_abstract.csv')


# 7. Manual filtering by full paper
#print('7. Manual filtering by full paper...')
#manual.manual_filter_by_full_text('./papers/filtered_by_abstract.csv', 'filtered_by_full_text.csv')
#manual.remove_repeated('./papers/filtered_by_full_text.csv')


# 8. Snowballing
#print('8. Snowballing...')
#retrieve.get_citations()

# 9. Filtering citation papers by abstract
#print('9. Filtering citation papers by abstract...')
#retrieve.filter_papers(keywords, './papers/citations_papers.csv', 'filtered_citations_papers.csv')

# 10. Getting citations papers to check based on semantic analysis
#print('10. Getting to check citations papers...')
#semantic_analyser.get_to_check_papers(keywords, './papers/filtered_citations_papers.csv',
#                                      'to_check_citations_papers.csv')

# 11. Manual filtering of citations papers to check
print('11. Manual filtering of citations papers to check...')
manual.manual_filter_by_abstract('./papers/to_check_citations_papers.csv', 'filtered_by_abstract_citations.csv')


# 12. Manual filtering by full citations paper
print('12. Manual filtering by full paper...')
manual.manual_filter_by_full_text('./papers/filtered_by_abstract_citations.csv', 'filtered_by_full_text_citations.csv')
manual.remove_repeated('./papers/filtered_by_full_text_citations.csv')

# 13. Merge final papers with citations papers
print('13. Merging final papers with citations papers...')


# 14. Plot results
print('14. Plotting results...')
util.plot()
