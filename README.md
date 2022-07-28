# SaLS: Semi-automatic Literature Survey

This project implements SaLS: a semi-automatic program to survey research papers based on the systematic methodology proposed by Kitchenham et al.[1, 2]. The goal of this project is to semi-automate the research papers survey process while providing a framework to enable surveys reproducibility and evolution. 

SaLS automatically retrives papers metadata based on queries that users provide according to these interests. These queries are used to consume the search APIs exposed by the most popular research papers repositories in the domain of computer science, engineering and information systems. Currently, SaLS retrieves papers information from the following repositories:

- [IEEE Xplore](https://developer.ieee.org/)
- [Springer Nature](https://dev.springernature.com/)
- [Science Direct](https://www.elsevier.com/solutions/sciencedirect/librarian-resource-center/api)
- [Semantic Scholar](https://www.semanticscholar.org/product/api)
- [CORE](https://core.ac.uk/services/api)
- [arXiv](https://arxiv.org/help/api/)

The retrieved metadata includes paper identifier (e.g., doi), publisher, publication date, title, url, and abstract.

SaLS merges papers information from different repositories, and then applies customised syntactic and semantic filters to reduce the search space of papers according to users' interests.


[1] Barbara Kitchenham and Pearl Brereton. 2013. A systematic review of systematic review process research in software engineering. Information and Software Technology 55, 12 (2013), 2049–2075. https://doi.org/10.1016/j.infsof.2013.07.010

[2] Barbara Kitchenham and Stuart Charters. 2007. Guidelines for performing Systematic Literature Reviews in Software Engineering. Technical Report EBSE 2007-001. Keele University and Durham University Joint Report. https://www.elsevier.com/__data/promis_misc/525444systematicreviewsguide.pdf
