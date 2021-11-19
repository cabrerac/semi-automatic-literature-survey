from analysis import util
from analysis import retrieve
from analysis import semantic_analyser


# 1. Reading search parameters
print('Reading parameters file...')
domains, interests, keywords, synonyms, fields, types, databases = util.read_parameters('parameters.yaml')


# 2. Getting papers from databases
"""for domain in domains:
    print('Getting all papers for domain: ' + domain + '...')
    retrieve.get_papers(domain, interests, [], synonyms, fields, types)"""


# 3. Preprocessing papers
#retrieve.preprocess(domains, databases)


# 4. Filtering papers by abstract
#retrieve.filter_papers('abstract', keywords)


# 5. Get sentences from abstracts
sentences_abstract = semantic_analyser.get_sentences(keywords)


#6. Get papers to check based on network dependency
semantic_analyser.get_to_check_papers(sentences_abstract)

#7. Plot results
util.plot()