import urllib.request
import urllib.parse
import urllib.error
import urllib
import time
import json
import requests


class Generic:
    def request(self, query, method, data):
        request_result = None
        time.sleep(1)
        if method == 'post':
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            data = json.dumps(data)
            request_result = requests.post(query, data=data, headers=headers)
        if method == 'get':
            try:
                request_result = urllib.request.urlopen(query).read().decode('utf-8')
            except urllib.error.HTTPError as ex:
                print('Exception in request ::' + str(ex))
                request_result = {}
        if method == 'retrieve':
            try:
                req = urllib.request.Request(query, headers={'User-Agent': 'Mozilla/5.0'})
                request_result = urllib.request.urlopen(req).read()
            except urllib.error.HTTPError as ex:
                print('Exception in request ::' + str(ex))
                request_result = {}
        return request_result

    def default_query(self, parameters):
        query = ''
        for parameter in parameters:
            if parameter != 'fields' and parameter != 'start' and parameter != 'page' \
                    and parameter != 'synonyms' and parameter != 'keywords' and parameter != 'types':
                parameters_list = parameters[parameter]
                query_parameter = ''
                for element in parameters_list:
                    query_parameter = query_parameter + '<field>:%22' + element + '%22'
                    if element in parameters['synonyms']:
                        synonyms = parameters['synonyms'][element]
                        for synonym in synonyms:
                            query_parameter = query_parameter + '+OR+<field>:%22' + synonym + '%22'
                    query_parameter = query_parameter + '+OR+'
                if len(parameters_list) > 0:
                    query_parameter = '%28' + query_parameter + '%29'
                    query_parameter = query_parameter.replace('+OR+%29', '%29')
                    if len(query) == 0:
                        query = query + query_parameter
                    else:
                        query = query + '+AND+' + query_parameter
        query = query.replace(' ', '+')
        query = query.replace('+AND+%28%29', '')

        if 'keywords' in parameters:
            keywords = parameters['keywords']
            query_keywords = ''
            for keyword in keywords:
                if not isinstance(keyword, str):
                    for key, terms in keyword.items():
                        for term in terms:
                            query_term = '%28<field>:%22' + key + '%22+AND+<field>:%22' + term + '%22%29'
                            query_keywords = query_keywords + query_term + '+OR+'
                else:
                    query_keywords = query_keywords + '<field>:%22' + keyword + '%22+OR+'
            if len(keywords) > 0:
                query_keywords = '%28' + query_keywords + '%29'
                query_keywords = query_keywords.replace(' ', '+')
                query_keywords = query_keywords.replace('+OR+%29', '%29')
                query = query + '+AND+' + query_keywords
                query = '%28' + query + '%29'


        if 'fields' in parameters:
            qf = ''
            fields = parameters['fields']
            for field in fields:
                qf = qf + query.replace('<field>', field) + '+OR+'
            query = qf[:-4]
        return query

    def ieeexplore_query(self, parameters, first_parameter):
        queries = []
        first_fields = []
        for first_field in parameters[first_parameter]:
            first_fields.append(first_field)
            if first_field in parameters['synonyms']:
                synonyms = parameters['synonyms'][first_field]
                for synonym in synonyms:
                    first_fields.append(synonym)
        for first_field in first_fields:
            query = '"' + first_field + '"'
            for parameter in parameters:
                if parameter != first_parameter and parameter != 'fields' \
                        and parameter != 'start' and parameter != 'page' and parameter != 'synonyms' \
                        and parameter != 'keywords' and parameter != 'types':
                    parameters_list = parameters[parameter]
                    query_parameter = ''
                    for element in parameters_list:
                        query_parameter = query_parameter + '"' + element + '"OR'
                        if element in parameters['synonyms']:
                            synonyms = parameters['synonyms'][element]
                            for synonym in synonyms:
                                query_parameter = query_parameter + '"' + synonym + '"OR'
                    if len(query_parameter) > 0:
                        query = query + 'AND(' + query_parameter + ')'
                    query = query.replace('OR)', ')')
            if 'keywords' in parameters:
                keywords = parameters['keywords']
                query_keywords = ''
                for keyword in keywords:
                    if not isinstance(keyword, str):
                        for key, terms in keyword.items():
                            for key, terms in keyword.items():
                                for term in terms:
                                    query_term = '("' + key + '"AND"' + term + '")'
                                    query_keywords = query_keywords + query_term + 'OR'
                    else:
                        query_keywords = query_keywords + '<field>:%22' + keyword + '%22+OR+'
                if len(keywords) > 0:
                    query_keywords = '(' + query_keywords + ')'
                    query_keywords = query_keywords.replace('OR)', ')')
                    query = query + ' AND ' + query_keywords
            queries.append(query)
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
        query = ''
        for parameter in parameters:
            if parameter != 'fields' and parameter != 'start' and parameter != 'page' \
                    and parameter != 'synonyms' and parameter != 'keywords' and parameter != 'types':
                parameters_list = parameters[parameter]
                query_parameter = ''
                for element in parameters_list:
                    query_parameter = query_parameter + '\"' + element + '\"'
                    if element in parameters['synonyms']:
                        synonyms = parameters['synonyms'][element]
                        for synonym in synonyms:
                            query_parameter = query_parameter + ' OR \"' + synonym + '\"'
                    query_parameter = query_parameter + ' OR '
                if len(parameters_list) > 0:
                    query_parameter = '<field>:(' + query_parameter + ')'
                    query_parameter = query_parameter.replace(' OR )', ')')
                    if len(query) == 0:
                        query = query + query_parameter
                    else:
                        query = query + ' AND ' + query_parameter
        query = query.replace(' AND ()', '')

        if 'keywords' in parameters:
            keywords = parameters['keywords']
            query_keywords = ''
            for keyword in keywords:
                if not isinstance(keyword, str):
                    for key, terms in keyword.items():
                        for term in terms:
                            query_term = '(\"' + key + '\" AND \"' + term + '\")'
                            query_keywords = query_keywords + query_term + ' OR '
                else:
                    query_keywords = query_keywords + '(\"' + keyword + '\") OR '
            if len(keywords)>0:
                query_keywords = '<field>:(' + query_keywords + ')'
                query_keywords = query_keywords.replace(' OR )', ')')
                query = query + ' AND ' + query_keywords
                query = '(' + query + ')'

        if 'fields' in parameters:
            qf = ''
            fields = parameters['fields']
            for field in fields:
                qf = qf + query.replace('<field>', field) + ' OR '
            query = qf[:-4]

        query = '(language.code(NOT ru NOT es)) AND (subjects:(NOT *thes* AND NOT *Thes* AND NOT *tesis* ' \
                'AND NOT *Tesis* AND NOT *Master* AND NOT *master*)) AND (' + query + ')'
        return query

    def project_academic_query(self, parameters):
        query = ''
        for parameter in parameters:
            if parameter != 'fields' and parameter != 'start' and parameter != 'page' \
                    and parameter != 'synonyms' and parameter != 'keywords' and parameter != 'types':
                parameters_list = parameters[parameter]
                elements = []
                for element in parameters_list:
                    elements.append(element)
                    if element in parameters['synonyms']:
                        synonyms = parameters['synonyms'][element]
                        for synonym in synonyms:
                            elements.append(synonym)
                query_elements = ''
                for element in elements:
                    els = element.replace('-', ' ').split(' ')
                    query_element = ''
                    for el in els:
                        query_element = query_element + "AW='" + el + "',"
                    query_element = 'AND(' + query_element + ')'
                    query_element = query_element.replace(',)', ')')
                    query_elements = query_elements + query_element + ','
                if len(parameters_list) > 0:
                    query_elements = 'OR(' + query_elements + ')'
                    query_elements = query_elements.replace(',)', ')')
                    query = query + query_elements + ','
        query = 'AND(' + query + ')'
        query = query.replace(',)', ')')
        return query

    def get_proxy(self, file_name):
        proxy = ''
        text_file = open(file_name, "r")
        proxies = text_file.readlines()
        text_file.close()
        index = 0
        while proxy == '' and index < len(proxies):
            try:
                proxy = 'https://' + proxies[index]
                proxy = proxy.replace('\n', '')
                req = urllib.request.Request('https://www.google.com')
                req.set_proxy(proxy, 'https')
                response = urllib.request.urlopen(req)
            except IOError as ex:
                proxy = ''
            index = index + 1
        index = 0
        while proxy == '' and index < len(proxies):
            try:
                proxy = 'http://' + proxies[index]
                proxy = proxy.replace('\n', '')
                req = urllib.request.Request('https://www.google.com')
                req.set_proxy(proxy, 'http')
                response = urllib.request.urlopen(req)
            except IOError  as ex:
                proxy = ''
            index = index + 1
        if proxy == '':
            print("No proxy selected")
        else:
            print("Selected proxy: " + proxy)
        return proxy
