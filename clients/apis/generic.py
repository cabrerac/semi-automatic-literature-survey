import urllib.request
import urllib.parse
import urllib.error
import time


class Generic:
    def request(self, query, method, data):
        request_result = None
        time.sleep(1)
        if method == 'post':
            data = urllib.parse.urlencode(data).encode()
            req = urllib.request.Request(query, data=data)
            request_result = req.urlopen(req)
        if method == 'get':
            try:
                request_result = urllib.request.urlopen(query).read().decode('utf-8')
            except urllib.error.HTTPError:
                #print('Exception in request...')
                request_result = {}
        return request_result

    def default_query(self, parameters):
        query = ''
        for parameter in parameters:
            if parameter != 'fields' and parameter != 'start' and parameter != 'page' \
                    and parameter != 'synonyms' != parameter != 'types':
                parameters_list = parameters[parameter]
                query_parameter = ''
                for element in parameters_list:
                    query_parameter = query_parameter + '<field>:%22' + element + '%22'
                    if element in parameters['synonyms']:
                        synonyms = parameters['synonyms'][element]
                        for synonym in synonyms:
                            query_parameter = query_parameter + '+OR+<field>:%22' + synonym + '%22'
                    query_parameter = query_parameter + '+OR+'
                query_parameter = '%28' + query_parameter + '%29'
                query_parameter = query_parameter.replace('+OR+%29', '%29')
                if len(query) == 0:
                    query = query + query_parameter
                else:
                    query = query + '+AND+' + query_parameter
        query = '%28' + query + '%29'
        query = query.replace(' ', '+')
        query = query.replace('+AND+%28%29', '')
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
                if parameter != first_parameter and parameter != 'keywords' and parameter != 'fields' \
                        and parameter != 'start' and parameter != 'page' and parameter != 'synonyms' \
                        and parameter != 'types':
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
            queries.append(query)
        return queries
