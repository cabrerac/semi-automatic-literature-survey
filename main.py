import urllib.request
import xmltodict, json

domains = ['self-driving cars']
interests = ['machine learning', 'artificial intelligence']
keywords = ['deployment', 'real world deployment']
synonyms = {'self-driving cars': ['autonomous car', 'driverless car', 'unmanned car']}

for domain in domains:
    title = '%28ti:' + domain
    abstract = '%28abs' + domain
    for synonym in synonyms[domain]:
        title = title + '+OR+ti:' + synonym
        abstract = abstract + '+OR+abs:' + synonym
    title = title + '%29+AND+%28ti:'
    abstract = abstract + '%29+AND+%28ti:'
    for interest in interests:
        title = title + interest + '+OR+ti:'
        abstract = abstract + interest + '+OR+abs:'
    title = title + '%29'
    abstract = abstract + '%29'
    title = title.replace('+OR+ti:%29','%29')
    abstract = abstract.replace('+OR+abs:%29', '%29')
    for keyword in keywords:
        title = title + '+AND+ti:' + keyword
        abstract = abstract + '+AND+abs:' + keyword
        search_query = '%28' + title + '%29+OR+%28' + abstract + '%29'
        search_query = search_query.replace(' ', '%20')
        print(search_query)
        # Getting papers from arXiv
        url = 'http://export.arxiv.org/api/query?search_query='+search_query
        data = urllib.request.urlopen(url)
        print(data.read().decode('utf-8'))



