# SaLS Quick Start Guide

## Get Started in 5 Minutes

This guide will help you run your first literature search with SaLS in just a few minutes.

## Prerequisites

- Python 3.8 or higher
- Internet connection
- Basic understanding of YAML files

## Step 1: Setup (2 minutes)

### 1.1 Clone and Install
```bash
git clone https://github.com/cabrerac/semi-automatic-literature-survey.git
cd semi-automatic-literature-survey
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### 1.2 Test Installation
```bash
python -c "from util import util; print('✅ SaLS installed successfully!')"
```

## Step 2: Create Your First Configuration (2 minutes)

### 2.1 Copy a Template
```bash
cp templates/basic_search_template.yaml my_first_search.yaml
```

### 2.2 Edit the Configuration
Open `my_first_search.yaml` and modify these lines:

```yaml
queries:
  - your_topic: "'your research topic' & 'key concept'"

search_date: 2024-12-15        # Today's date
folder_name: my_first_search    # Your project name
```

**Example for machine learning research**:
```yaml
queries:
  - deep learning: "'deep learning' & 'computer vision'"

search_date: 2024-12-15
folder_name: deep_learning_cv
```

## Step 3: Run Your First Search (1 minute)

```bash
python main.py my_first_search.yaml
```

## What Happens Next?

1. **Paper Retrieval**: SaLS searches selected databases
2. **Preprocessing**: Papers are cleaned and deduplicated
3. **Semantic Filtering**: AI-powered relevance scoring (if configured)
4. **Manual Review**: You review abstracts and full papers
5. **Results**: Final paper list saved to `./papers/` folder

## Expected Output

```
0. Retrieving papers from the databases...
✅ Retrieved 150 papers from arxiv
✅ Retrieved 89 papers from semantic_scholar

1. Preprocessing papers...
✅ Preprocessing results can be found at: 1_preprocessed_papers.csv

2. Manual filtering by abstract...
[Interactive review process starts]
```

## Common First-Time Issues

### Issue: "Configuration validation failed"
**Solution**: SaLS will show exactly what's wrong and how to fix it
- **Critical errors** (🔴) must be fixed before continuing
- **Warnings** (🟡) allow the pipeline to continue with defaults
- Follow the provided examples to fix issues quickly

### Issue: "No papers found"
**Solution**: Try broader queries or different databases

### Issue: "API key required"
**Solution**: Use only `arxiv` and `semantic_scholar` (no API key needed)

### Issue: Missing optional fields
**Solution**: SaLS automatically provides sensible defaults:
- Missing databases → defaults to open databases
- Missing search_date → defaults to current date
- Missing folder_name → defaults to filename-based

## Next Steps

1. **Review Results**: Check the generated CSV files
2. **Refine Queries**: Adjust based on initial results
3. **Add Filters**: Use syntactic and semantic filters
4. **Expand Databases**: Add commercial databases with API keys

## Need Help?

- **Configuration Guide**: `docs/configuration_guide.md`
- **Templates**: `templates/` directory
- **Examples**: `parameters_ar.yaml` (working example)

## Quick Configuration Examples

### Simple Search
```yaml
queries:
  - ai: "'artificial intelligence'"
databases:
  - arxiv
  - semantic_scholar
search_date: 2024-12-15
folder_name: ai_search
```

### Focused Search
```yaml
queries:
  - ml_edge: "'machine learning' & 'edge computing'"
databases:
  - arxiv
  - semantic_scholar
start_date: 2020-01-01
end_date: 2024-12-31
search_date: 2024-12-15
folder_name: ml_edge_search
```

### Advanced Search
```yaml
queries:
  - systems: "'systems engineering' & ('AI' | 'machine learning')"
databases:
  - arxiv
  - semantic_scholar
  - springer
syntactic_filters:
  - systems
  - engineering
semantic_filters:
  - ai_systems: "Research on AI and machine learning in systems engineering contexts"
search_date: 2024-12-15
folder_name: ai_systems_search
```

## Success Checklist

- [ ] SaLS runs without errors
- [ ] Papers are retrieved from databases
- [ ] Results are saved to CSV files
- [ ] You can review and filter papers
- [ ] Final paper list is generated

## Congratulations!

You've successfully completed your first literature search with SaLS. The system is now ready for your research needs.

**Tip**: Start with simple searches and gradually add complexity as you become familiar with the system.
