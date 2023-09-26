# SaLS: Semi-automatic Literature Survey

This project implements SaLS: a semi-automatic tool to survey research papers based on the systematic methodology proposed by Kitchenham et al.[1, 2]. The goal of this project is to semi-automate the research papers survey process while providing a framework to enable surveys reproducibility and evolution. 

SaLS automatically retrives papers metadata based on queries that users provide according. These queries are used to consume the search APIs exposed by the most popular research papers repositories in different domains. Currently, SaLS retrieves papers information from the following repositories:

- [IEEE Xplore](https://ieeexplore.ieee.org/Xplore/home.jsp)
- [Springer Nature](https://www.springernature.com/gp)
- [Scopus](https://www.elsevier.com/en-gb/solutions/scopus)
- [Semantic Scholar](https://www.semanticscholar.org)
- [CORE](https://core.ac.uk)
- [arXiv](https://arxiv.org)

The retrieved metadata includes paper identifier (e.g., doi), publisher, publication date, title, url, and abstract.

SaLS merges papers information from different repositories, and then applies customised syntactic and semantic filters (i.e., Lbl2Vec)[3] to reduce the search space of papers according to users' interests.

Once automatic filters are applied, the tool prompts the title and abstract of the paper in a centralised interface where users can decide if the paper should be included or not in the review (i.e., papers filtered by abstract). The URL of the papers that passed the filter by abstract is then prompted in the last filter, which requires the user to skim the full paper and decide if it is included or no.

Then, the tool applies the snowballing step by retriving the metadata of the works that cited the selected papers in the last step (i.e., papers filtered by skimming the full text), and applies the automatic and semi-automatic filters on the citing papers. 

The final list of papers is composed by the cited papers that passed the first round of filters, and the citing papers that passed the second round of filters (i.e., snowballing).

# Requirements

Some of the APIs provided by the repositories require an access key to be consumed. You should request a key to each repository you want to include in your search. Each respository has its own steps to apply for a key as follows:

- [IEEE Xplore](https://developer.ieee.org/getting_started)
- [Springer Nature](https://dev.springernature.com/docs)
- [Scopus](https://dev.elsevier.com/)
- [CORE](https://core.ac.uk/services/api)

Alternatively, you can use the tool for requesting papers from arXiv and Semantic Scholar which are open and do not need an access key.

# How to run it?

The following instructions were tested on the Windows Subsystem for Linux ([WSL](https://docs.microsoft.com/en-us/windows/wsl/install)) and an Ubuntu machine with Python 3.8.

1. Clone this repository

```
git clone https://github.com/cabrerac/semi-automatic-literature-survey.git
```
```
cd semi-automatic-literature-survey/
```

2. Create and activate virtual environment 

```
python -m venv venv
```
```
source venv/bin/activate
```

3. Install requirements

```
pip install wheel
pip install  -r requirements.txt
```

4. Install language package for spacy

```
python -m spacy download en_core_web_sm
```

5. Download nltk resources

```
python -c "import nltk; nltk.download('wordnet')"
```
```
python -c "import nltk; nltk.download('omw-1.4')"
```

6. Create a file `./config.json` that will store the API access keys for the repositories you want to use. The file should have the following format:

```
 {
  "api_access_core": 'CORE_API_ACCESS_KEY',
  "api_access_ieee": 'IEEE_API_ACCESS_KEY',
  "api_access_springer": 'SPRINGER_API_ACCESS_KEY',
  "api_access_elsevier": 'ELSEVIER_API_ACCESS_KEY'
}
```

7. Run the main passing the search parameters file. For example:

```
python main.py parameters_ar.yaml
```

A simple self-explanatory example of a search parameters file can be found in `./parameters_ar.yaml`. Alternatively, a more complex one including semantic filters can be found in `./parameters_doa.yaml`

# References

[1] Barbara Kitchenham and Pearl Brereton. 2013. A systematic review of systematic review process research in software engineering. Information and Software Technology 55, 12 (2013), 2049–2075. https://doi.org/10.1016/j.infsof.2013.07.010

[2] Barbara Kitchenham and Stuart Charters. 2007. Guidelines for performing Systematic Literature Reviews in Software Engineering. Technical Report EBSE 2007-001. Keele University and Durham University Joint Report. https://www.elsevier.com/__data/promis_misc/525444systematicreviewsguide.pdf

[3] Tim Schopf, Daniel Braun, and Florian Matthes. 2021. Lbl2Vec: An Embedding-based Approach for Unsupervised Document Retrieval on Predefined Topics. In Proceedings of the 17th International Conference on Web Information Systems and Technologies - WEBIST,. 124–132. https://doi.org/10.5220/0010710300003058
