from clients import arxiv
from clients import ieeexplore
import yaml

with open(r'parameters.yaml') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    parameters = yaml.load(file, Loader=yaml.FullLoader)
domains = parameters['domains']
interests = parameters['interests']
keywords = parameters['keywords']
fields = parameters['fields']
types = parameters['types']
synonyms = {}
for domain in domains:
    synonyms[domain] = parameters[domain]
for interest in interests:
    synonyms[interest] = parameters[interest]

#ieee_explore_papers = ieeexplore.get_papers(domains, interests, keywords, synonyms, fields, types)
#print(ieee_explore_papers)
#arxiv_papers = arxiv.get_papers(domains, interests, keywords, synonyms, fields, types)
#print(arxiv_papers)



