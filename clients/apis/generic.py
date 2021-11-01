import urllib.request
import urllib.parse
import urllib.error


class Generic:
    def request(self, query, method, data):
        request_result = None
        if method == 'post':
            data = urllib.parse.urlencode(data).encode()
            req = urllib.request.Request(query, data=data)
            request_result = req.urlopen(req)
        if method == 'get':
            try:
                request_result = urllib.request.urlopen(query).read().decode('utf-8')
            except urllib.error.HTTPError:
                print('Exception in request...')
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
        first_fields = parameters[first_parameter]
        for first_field in first_fields:
            query = '"' + first_field + '"'
            for parameter in parameters:
                if parameter != first_parameter and parameter != 'fields' and parameter != 'start' and parameter != 'page' \
                        and parameter != 'synonyms' != parameter != 'types':
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
        if len(queries) == 1 and first_parameter == 'domains':
            first_fields = parameters[first_parameter]
            for first_field in first_fields:
                synonyms = parameters['synonyms'][first_field]
                for synonym in synonyms:
                    queries[0] = queries[0] + 'OR"' + synonym + '"'
                    #query = '"' + synonym + '"'
                    #queries.append(query)
        return queries

    def save(self, file_name, papers):
        with open('./papers/' + file_name, 'a', newline='', encoding='utf-8') as f:
            papers.to_csv(f, encoding='utf-8', index=False, header=f.tell() == 0)

    def filterByField(self, papers, field, keywords):
        filtered_papers = []
        for keyword in keywords:
            if len(filtered_papers) == 0:
                filtered_papers = papers[papers[field].str.contains(keyword)]
            else:
                filtered_papers = filtered_papers.append(papers[papers[field].str.contains(keyword)])
        if len(filtered_papers) > 0:
            filtered_papers = filtered_papers.drop_duplicates(subset=['doi'])
        return filtered_papers
