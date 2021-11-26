from .apis.generic import Generic
from os.path import exists

#https://dl.acm.org/action/doSearch?fillQuickSearch=false&target=advanced&expand=dl&
#field1=AllField&text1=%22autonomous+vehicles%22+or+%22driverless+car%22&
#field2=AllField&text2=%22machine+learning%22+or+%22artificial+intelligence%22&field3=AllField&text3=%22deployment%22+or+%22deploy%22

#https://dl.acm.org/action/doSearch?fillQuickSearch=false&target=advanced&expand=dl&
#AllField=AllField%3A%28%22autonomous+vehicles%22+or+%22driverless+car%22%29+AND+AllField%3A%28%22machine+learning%22+or+%22artificial+intelligence%22%29+AND+AllField%3A%28%22real+world%22+AND+%22deploy%22+or+%22real+world%22+AND+%22deployment%22%29

client = Generic()
max = 50
start = 0
database = 'acm'
api_url = 'https://dl.acm.org/action/doSearch?fillQuickSearch=false&target=advanced&expand=dl&' \
          'AllField=AllField%3A<q1>+AND+AllField%3A<q2>+AND+AllField%3A<q3>&pageSize=<size>&startPage=<start>'
fr = 'utf-8'
def get_papers(domain, interests, keywords, synonyms, fields, types):
    file_name = 'domains/' + domain.lower().replace(' ', '_') + '_' + database + '.csv'
    if not exists('./papers/' + file_name):
        parameters = {'domains': [domain], 'interests': interests, 'keywords': keywords, 'synonyms': synonyms,
                      'types': types}
        queries = create_request(parameters)
        req = api_url.replace('<q1>', queries[0]).replace('<q2>', queries[1]).replace('<q3>', queries[2])
        req = req.replace('<size>', str(max)).replace('<start>', str(start))
        print(req)
        result = client.request(req, 'retrieve', {})
        print(result)



def create_request(parameters):
    req = []
    query = client.default_query(parameters)
    queries = query.split('%29+AND+%28')
    for q in queries:
        q = q.replace('%28%28', '').replace('%29%29', '')
        q = q.replace('<field>:', '')
        q = '%28' + q + '%29'
        req.append(q)
    return req
