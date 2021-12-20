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
#retrieve.filter_papers(keywords)


# 5. Getting papers to check based on semantic analysis
#print('5. Getting to check papers...')
#semantic_analyser.get_to_check_papers(keywords)

# 6. Manual filtering of papers to check
#print('6. Manual filtering of papers to check...')
#manual.manual_filter()

# 7. Plot results
print('7. Plotting results...')
util.plot()
