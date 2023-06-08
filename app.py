import streamlit as st
import yaml

# page settings
st.set_page_config(page_title="Literature Survey")
st.title("Literature survey")
st.markdown("Enter the information required")

with open('empty.yaml', 'r') as file:
    params = yaml.safe_load(file)

# sidebar (user input form)
st.sidebar.title("Parameters")
queries = st.sidebar.text_area("Queries")
syntactic_filters = st.sidebar.text_area("Syntactic filters", value="")
synonyms = st.sidebar.text_area("Synonyms", value="")
databases = st.sidebar.text_area("List of databases", value="arxiv\nspringer\nieeexplore\nsciencedirect\ncore\nsemantic_scholar")
search_date = st.sidebar.date_input("Search date (YYYY-mm-dd)")
folder_name = st.sidebar.text_input("Folder name")

run_button = st.sidebar.button("Run")

# a function to save the user input into a .yaml file
def save_yaml(params, filename):
    with open(filename, 'w') as file:
        yaml.dump(params, file)

if run_button:
    # Process queries
    query_list = queries.split('\n')
    processed_queries = []
    for query in query_list:
        processed_queries.append(f"{query}: \"{query}\"")

    params['queries'] = processed_queries

    # Process syntactic filters
    filters_list = [filter.strip() for filter in syntactic_filters.split('\n') if filter.strip()]
    params['syntactic_filters'] = filters_list

    # Process synonyms
    synonym_list = synonyms.split('\n')
    synonym_dict = {}
    for query in query_list:
        synonym_dict[query] = [query] + [synonym for synonym in synonym_list if synonym]
    params.update(synonym_dict)

    # Process databases
    params['databases'] = databases.split('\n')

    # Process search date
    params['search_date'] = search_date

    # Process folder name
    params['folder_name'] = folder_name

    save_yaml(params, 'user_input.yaml')
    st.success("App executed successfully")