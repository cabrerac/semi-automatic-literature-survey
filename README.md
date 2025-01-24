# SaLS: Semi-automatic Literature Survey

This project implements SaLS: a semi-automatic tool to survey research papers based on the systematic methodology proposed by Kitchenham et al.[1, 2]. The goal of this project is to semi-automate the research papers survey process while providing a framework to enable surveys reproducibility and evolution. Two SaLS use cases are:

- Cabrera, Christian, et al. *The Systems Engineering Approach in Times of Large Language Models.* Proceedings of the 58th Hawaii international conference on system sciences (2025) (To Appear). [Paper.](https://arxiv.org/abs/2411.09050v1) [Code.](https://github.com/cabrerac/semi-automatic-literature-survey/tree/sys-llms-survey)
- Cabrera, Christian, et al. *Real-world machine learning systems: A survey from a data-oriented architecture perspective.* arXiv preprint arXiv:2302.04810 (2023) (Under Review) [Paper.](https://arxiv.org/abs/2302.04810) [Code.](https://github.com/cabrerac/semi-automatic-literature-survey/tree/doa-survey)

SaLS automatically retrives papers metadata based on queries that users provide. These queries are used to consume the search APIs exposed by the most popular research papers repositories in different domains. Currently, SaLS retrieves papers information from the following repositories:

- [IEEE Xplore](https://ieeexplore.ieee.org/Xplore/home.jsp)
- [Springer Nature](https://www.springernature.com/gp)
- [Scopus](https://www.elsevier.com/en-gb/solutions/scopus)
- [Semantic Scholar](https://www.semanticscholar.org)
- [CORE](https://core.ac.uk)
- [arXiv](https://arxiv.org)

The retrieved metadata includes paper identifier (e.g., doi), publisher, publication date, title, url, and abstract.

SaLS merges papers information from different repositories, and then applies customised syntactic and semantic filters (i.e., semantic search)[3] to reduce the search space of papers according to users' interests.

Once automatic filters are applied, the tool prompts the title and abstract of the paper in a centralised interface where users can decide if the paper should be included or not in the review (i.e., papers filtered by abstract). The URL of the papers that passed the filter by abstract is then prompted in the last filter, which requires the user to skim the full paper and decide if it is included or no.

Then, the tool applies the snowballing step by retriving the metadata of the works that cited the selected papers in the last step (i.e., papers filtered by skimming the full text), and applies the automatic and semi-automatic filters on the citing papers. 

The final list of papers is composed by the cited papers that passed the first round of filters, and the citing papers that passed the second round of filters (i.e., snowballing).

# Requirements

Some of the APIs provided by the repositories require an access key to be consumed. You should request a key to each repository you want to include in your search. Each respository has its own steps to apply for a key as follows:

- [IEEE Xplore](https://developer.ieee.org/getting_started)
- [Springer Nature](https://dev.springernature.com/docs)
- [Scopus](https://dev.elsevier.com/)
- [CORE](https://core.ac.uk/services/api)
- [Semantic Scholar](https://www.semanticscholar.org/product/api/tutorial)

Alternatively, you can use the tool for requesting papers from arXiv or semantic scholar which are open and do not need an access key. SaLS does not have control over the maintenance of the APIs. If an API produces an error, you can see the details in the log files. We recommend to stop using the API that produces errors for a while.

# How to run it?

The following instructions were tested on:
- A Windows machine (i.e., Windows PowerShell) with Python 3.10.11.
- Windows Subsystem for Linux ([WSL](https://docs.microsoft.com/en-us/windows/wsl/install)) with Python 3.8.
- An Ubuntu machine with Python 3.8.

1. Clone this repository

```
git clone https://github.com/cabrerac/semi-automatic-literature-survey.git
```
```
cd semi-automatic-literature-survey/
```

2. Create and activate virtual environment 

For Linux distributions
```
python -m venv venv
```
```
source venv/bin/activate
```

For Windows
```
python -m venv ./venv
```
```
./venv/Scripts/activate
```

3. Install requirements

```
pip install -r requirements.txt
```

4. Install language package for spacy

```
python -m spacy download en_core_web_sm
```

5. Create a file `./config.json` that will store the API access keys for the repositories you want to use. The file should have the following format:

```
 {
  "api_access_core": "CORE_API_ACCESS_KEY",
  "api_access_ieee": "IEEE_API_ACCESS_KEY",
  "api_access_springer": "SPRINGER_API_ACCESS_KEY",
  "api_access_elsevier": "ELSEVIER_API_ACCESS_KEY"
}
```
Ignore this step if you are testing the tool with arXiv. Also, you should only add the access keys of the repositories you want to use.

6. Run the main passing the search parameters file. For example:

```
python main.py parameters_ar.yaml
```

A simple self-explanatory example of a search parameters file can be found in `./parameters_ar.yaml`. Alternatively, a parameters file including syntactic and semantic filters can be found in `./parameters_sys.yaml`

A description of the semi-automatic methodology applied in a survey can be found in the paper ["Real-world Machine Learning Systems: A survey from a Data-Oriented Architecture Perspective"](https://arxiv.org/abs/2302.04810) [4].

# References

[1] Barbara Kitchenham and Pearl Brereton. 2013. A systematic review of systematic review process research in software engineering. Information and Software Technology 55, 12 (2013), 2049–2075. https://doi.org/10.1016/j.infsof.2013.07.010

[2] Barbara Kitchenham and Stuart Charters. 2007. Guidelines for performing Systematic Literature Reviews in Software Engineering. Technical Report EBSE 2007-001. Keele University and Durham University Joint Report. https://www.elsevier.com/__data/promis_misc/525444systematicreviewsguide.pdf

[3] SBERT.net Sentence Transformers. 2024. Semantic Search [Available online](https://www.sbert.net/examples/applications/semantic-search/README.html)

[4] Christian Cabrera, Andrei Paleyes, Pierre Thodoroff, and Neil D. Lawrence. 2023. Real-world Machine Learning Systems: A survey from a Data-Oriented Architecture Perspective. arXiv preprint arXiv:2302.04810. [Available online](https://arxiv.org/abs/2302.04810)
