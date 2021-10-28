from .xplore.xploreapi import XPLORE


def get_papers(domains, interests, keywords, synonyms, fields):
    a_fields = []
    query = create_query(domains, interests, keywords, synonyms, a_fields)
    client = XPLORE('api_access_key')
    client.abstractText(query)
    papers = client.callAPI()
    return papers


def create_query(domains, interests, keywords, synonyms, a_fields):
    return ''



