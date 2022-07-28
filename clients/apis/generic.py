import urllib.request
import urllib.parse
import urllib.error
import urllib
import time
import json
import requests
import re


class Generic:
    def request(self, query, method, data):
        request_result = None
        time.sleep(1)
        if method == 'post':
            try:
                headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
                data = json.dumps(data)
                request_result = requests.post(query, data=data, headers=headers)
            except urllib.error.HTTPError as ex:
                request_result = {'exception': str(ex)}
        if method == 'get':
            try:
                request_result = urllib.request.urlopen(query).read().decode('utf-8')
            except urllib.error.HTTPError as ex:
                request_result = {'exception': str(ex)}
        if method == 'retrieve':
            try:
                req = urllib.request.Request(query, headers={'User-Agent': 'Mozilla/5.0'})
                request_result = urllib.request.urlopen(req).read()
            except urllib.error.HTTPError as ex:
                request_result = {'exception': str(ex)}
        return request_result

    def default_query(self, parameters):
        query = parameters['query'].replace('(', '%28').replace(')', '%29').replace("'", "")
        words = re.split(' & | Â¦ ', query)
        for word in words:
            word = word.replace('%29', '').replace('%28', '')
            synonyms = parameters['synonyms']
            query_parameter = ''
            if word in synonyms.keys():
                word_synonyms = synonyms[word]
                query_parameter = query_parameter + '<field>:%22' + word + '%22'
                for synonym in word_synonyms:
                    query_parameter = query_parameter + '+OR+<field>:%22' + synonym + '%22'
                query_parameter = '%28' + query_parameter + '%29'
                query = query.replace(word, query_parameter)
            else:
                query_parameter = query_parameter + '<field>:%22' + word + '%22'
                query = query.replace(word, query_parameter)
        query = query.replace(' & ', '+AND+').replace(' Â¦ ', '+OR+').replace(' ', '+')
        query = '%28' + query + '%29'

        if 'fields' in parameters:
            qf = ''
            fields = parameters['fields']
            for field in fields:
                qf = qf + query.replace('<field>', field) + '+OR+'
            query = qf[:-4]
        return query

    def ieeexplore_query(self, parameters):
        query = parameters['query'].replace("\'", '')
        words = re.split(' & | Â¦ ', query)
        for word in words:
            word = word.replace(')', '').replace('(', '')
            synonyms = parameters['synonyms']
            query_parameter = ''
            if word in synonyms.keys():
                word_synonyms = synonyms[word]
                query_parameter = query_parameter + '"' + word + '"'
                for synonym in word_synonyms:
                    query_parameter = query_parameter + 'OR"' + synonym + '"'
                query_parameter = '(' + query_parameter + ')'
                query = query.replace(word, query_parameter)
            else:
                query_parameter = query_parameter + '"' + word + '"'
                query = query.replace(word, query_parameter)
        query = query.replace(' & ', 'AND').replace(' Â¦ ', 'OR')
        first_term = query.split('AND')[0]
        first_term = first_term.replace('(', '').replace(')', '')
        words_first_term = first_term.split('OR')
        queries = []
        for word in words_first_term:
            q = query.replace('(' + first_term + ')', word)
            queries.append(q)
        return queries

    def sciencedirect_query(self, parameters):
        domains = []
        for domain in parameters['domains']:
            domains.append(domain)
            synonyms = parameters['synonyms'][domain]
            for synonym in synonyms:
                domains.append(synonym)
        query_domains = 'ALL('
        for domain in domains:
            query_domains = query_domains + domain + ' OR '
        query_domains = query_domains + ')'
        query_domains = query_domains.replace(' OR )', ')')

        interests = []
        for interest in parameters['interests']:
            interests.append(interest)
            synonyms = parameters['synonyms'][interest]
            for synonym in synonyms:
                interests.append(synonym)
        query_interests = 'ALL('
        for interest in interests:
            query_interests = query_interests + interest + ' OR '
        query_interests = query_interests + ')'
        query_interests = query_interests.replace(' OR )', ')')
        query = query_domains + ' AND ' + query_interests
        return query

    def core_query(self, parameters):
        query = parameters['query'].replace("'", "")
        words = re.split(' & | Â¦ ', query)
        for word in words:
            word = word.replace('(', '').replace(')', '')
            synonyms = parameters['synonyms']
            query_parameter = ''
            if word in synonyms.keys():
                word_synonyms = synonyms[word]
                query_parameter = query_parameter + '"' + word + '"'
                for synonym in word_synonyms:
                    query_parameter = query_parameter + ' OR "' + synonym + '"'
                query_parameter = '<field>:(' + query_parameter + ')'
                query = query.replace(word, query_parameter)
            else:
                query_parameter = query_parameter + '"' + word + '"'
                query = query.replace(word, query_parameter)
        query = query.replace(' & ', ' AND ').replace(' Â¦ ', ' OR ')
        query = '(' + query + ')'

        if 'fields' in parameters:
            qf = ''
            fields = parameters['fields']
            for field in fields:
                qf = qf + query.replace('<field>', field) + ' OR '
            query = qf[:-4]
        query = '(subjects:(*article* OR *Article* OR *journal* OR *Journal* OR *ART* OR ' \
                '*conference* OR *CONFERENCE*)) AND (description:(NOT *thes* AND NOT *Thes* ' \
                'AND NOT *tesis* AND NOT *Tesis* AND NOT *Master* AND NOT *master*)) AND (' + query + ')'
        return query