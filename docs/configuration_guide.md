# SaLS Configuration Guide

## Overview

This guide provides comprehensive information about configuring SaLS (Semi-automatic Literature Survey) for your research needs. SaLS uses YAML configuration files to define search parameters, filters, and database selections.

## Table of Contents

1. [Basic Configuration Structure](#basic-configuration-structure)
2. [Required Parameters](#required-parameters)
3. [Optional Parameters](#optional-parameters)
4. [Query Syntax](#query-syntax)
5. [Database Configuration](#database-configuration)
6. [Filtering Options](#filtering-options)
7. [Best Practices](#best-practices)
8. [Troubleshooting](#troubleshooting)
9. [Configuration Templates](#configuration-templates)

## Basic Configuration Structure

A SaLS configuration file is a YAML document with the following structure:

```yaml
# Required parameters
queries: [...]
databases: [...]
search_date: "YYYY-MM-DD"
folder_name: "your_search_name"

# Optional parameters
start_date: "YYYY-MM-DD"
end_date: "YYYY-MM-DD"
syntactic_filters: [...]
semantic_filters: [...]
synonyms: {...}
```

## Required Parameters

### queries
**Type**: List of dictionaries  
**Description**: Defines your search queries using boolean expressions  
**Format**: `[{query_name: "boolean_expression"}]`

**Example**:
```yaml
queries:
  - machine learning: "'machine learning' & 'edge computing'"
  - systems engineering: "'systems engineering' | 'SE'"
```

**Best Practices**:
- Use descriptive names for your queries
- Use quotes around multi-word terms
- Combine related concepts with OR operators
- Use AND operators to narrow down results

### databases
**Type**: List of strings  
**Description**: Specifies which databases to search  
**Available Options**: `arxiv`, `semantic_scholar`, `springer`, `ieeexplore`, `scopus`, `core`, `crossref`, `europe_pmc`, `pubmed`, `openalex`

**Example**:
```yaml
databases:
  - arxiv                    # Open access, no API key needed
  - semantic_scholar        # Open access, no API key needed
  - springer                # Commercial, requires API key
```

**Note**: Some databases require API keys in `config.json`. See [Database Configuration](#database-configuration) for details.

### search_date
**Type**: String (YYYY-MM-DD format)  
**Description**: Date when the search was performed (for organization purposes)  
**Example**: `search_date: 2024-12-15`

### folder_name
**Type**: String  
**Description**: Name of the folder where results will be stored  
**Example**: `folder_name: my_literature_search`

## Optional Parameters

### start_date and end_date
**Type**: String (YYYY-MM-DD format)  
**Description**: Date range to limit search results  
**Example**:
```yaml
start_date: 2020-01-01      # Papers from 2020 onwards
end_date: 2024-12-31        # Papers until end of 2024
```

**Benefits**:
- Reduces search time
- Focuses on recent research
- Improves relevance for time-sensitive topics

### synonyms
**Type**: Dictionary  
**Description**: Defines synonyms for query expansion to increase search coverage  
**Format**: `{term: [synonym1, synonym2, ...]}`

**Example**:
```yaml
machine learning:
  - ml
  - deep learning
  - neural networks
  - supervised learning
```

**Best Practices**:
- Include abbreviations and alternative names
- Add related concepts and terminology
- Use domain-specific synonyms

### syntactic_filters
**Type**: List of strings  
**Description**: Terms that must appear in paper content (AND logic)  
**Example**:
```yaml
syntactic_filters:
  - edge computing
  - distributed systems
  - performance
```

**Use Cases**:
- Filtering out irrelevant papers early
- Ensuring specific concepts are covered
- Improving result relevance

### semantic_filters
**Type**: List of dictionaries  
**Description**: AI-powered similarity matching using detailed descriptions  
**Format**: `[{filter_name: "detailed_description"}]`

**Example**:
```yaml
semantic_filters:
  - edge computing: "Research on edge computing, fog computing, and distributed edge systems including resource management, placement strategies, and performance optimization"
  - ml systems: "Papers about machine learning systems in production environments including deployment, monitoring, scaling, and operational challenges"
```

**Best Practices**:
- Be specific and descriptive
- Include key concepts and requirements
- Focus on what you're looking for, not what you want to exclude

## Query Syntax

SaLS supports a flexible boolean query syntax with the following operators:

### Basic Operators
- `&` or `AND` - AND operator (both terms must be present)
- `|` or `OR` - OR operator (either term can be present)
- `&&` or `||` - Alternative syntax for AND/OR

### Advanced Features
- **Parentheses**: Group expressions for complex logic
- **Quotes**: Preserve multi-word terms as phrases
- **Legacy Support**: `¦` character for OR operations

### Examples

**Simple AND**:
```yaml
queries:
  - basic: "'machine learning' & 'edge computing'"
```

**Complex Boolean Expression**:
```yaml
queries:
  - complex: "'machine learning' & ('edge computing' | 'fog computing') & ('performance' | 'optimization')"
```

**Grouped Logic**:
```yaml
queries:
  - grouped: "('deep learning' | 'neural networks') & ('computer vision' | 'image processing')"
```

## Database Configuration

### Open Access Databases (No API Key Required)
- **arXiv**: Excellent for recent preprints and open access papers
- **Semantic Scholar**: Good for citation analysis and impact assessment

### Commercial Databases (API Key Required)
- **Springer Nature**: High-quality journals and books
- **IEEE Xplore**: Excellent for engineering and computer science
- **Scopus**: Comprehensive coverage across all disciplines
- **CORE**: Open access repository aggregator

### API Key Setup
1. Create a `config.json` file in the project root
2. Add your API keys:
```json
{
  "api_access_springer": "YOUR_SPRINGER_API_KEY",
  "api_access_ieee": "YOUR_IEEE_API_KEY",
  "api_access_elsevier": "YOUR_SCOPUS_API_KEY",
  "api_access_core": "YOUR_CORE_API_KEY"
}
```

**Note**: Only add keys for databases you plan to use.

## Filtering Options

### Two-Stage Filtering Process

1. **Syntactic Filtering**: Basic text matching using your specified terms
2. **Semantic Filtering**: AI-powered similarity matching using BERT models

### Filtering Strategy

**For High Precision (Fewer, More Relevant Results)**:
- Use more specific queries
- Add more syntactic filters
- Use date ranges to focus on recent work

**For High Recall (More Results, May Include Less Relevant)**:
- Use broader queries with OR operators
- Fewer syntactic filters
- No date restrictions

## Best Practices

### 1. Start Simple
- Begin with basic queries
- Add complexity gradually
- Test with open databases first

### 2. Query Design
- Use specific terminology from your field
- Include synonyms and abbreviations
- Balance between precision and recall

### 3. Database Selection
- Start with open databases (arxiv, semantic_scholar)
- Add commercial databases for comprehensive coverage
- Consider field-specific database strengths

### 4. Filtering Strategy
- Use syntactic filters for precision
- Use semantic filters for recall
- Iterate based on initial results

### 5. Date Management
- Set reasonable date ranges for your research area
- Consider field evolution speed
- Balance between recency and comprehensiveness

## Troubleshooting and Error Recovery

### Configuration Error Recovery

SaLS now provides intelligent error recovery that helps you fix configuration issues quickly and continue with your research.

#### Error Severity Levels

**🔴 Critical Errors** - Pipeline cannot continue
- Missing or invalid queries (required for search)
- Malformed query syntax
- These must be fixed before the pipeline can run

**🟡 Warnings** - Pipeline can continue with defaults
- Missing databases (defaults to open databases)
- Missing search_date (defaults to current date)
- Missing folder_name (defaults to filename-based)
- Invalid date formats (defaults to reasonable values)
- Missing filters (defaults to empty lists)

#### Automatic Fallbacks

When warnings are detected, SaLS automatically applies sensible defaults:

```yaml
# If databases are missing, SaLS uses:
databases: [arxiv, semantic_scholar]

# If search_date is missing, SaLS uses:
search_date: [current date]

# If folder_name is missing, SaLS uses:
folder_name: [filename without .yaml extension]

# If filters are missing, SaLS uses:
syntactic_filters: []
semantic_filters: []
```

#### Recovery Suggestions

For each issue, SaLS provides:
- **Clear description** of what's wrong
- **Specific fix** instructions
- **Working examples** to copy-paste
- **Default values** that will be used

### Common Issues and Solutions

#### Configuration Validation Errors
**Problem**: Configuration validation fails with specific error messages  
**Solution**: Follow the error message guidance and check:
- YAML syntax (proper indentation)
- Required field formats
- Date format (YYYY-MM-DD)
- Database name spelling

**Recovery**: SaLS will show exactly what's wrong and how to fix it

#### Missing Required Fields
**Problem**: Critical fields like queries are missing  
**Solution**: Add the missing sections following the provided examples

**Recovery**: SaLS prevents pipeline execution and guides you to add required fields

#### Missing Optional Fields
**Problem**: Optional fields like databases or search_date are missing  
**Solution**: Either add them or let SaLS use sensible defaults

**Recovery**: SaLS continues with defaults and shows what was applied

#### Invalid Date Formats
**Problem**: Dates are in wrong format (e.g., 2020/01/01)  
**Solution**: Use YYYY-MM-DD format (e.g., 2020-01-01)

**Recovery**: SaLS suggests the correct format and provides examples

#### Invalid Database Names
**Problem**: Unknown database specified  
**Solution**: Use only valid database names from the supported list

**Recovery**: SaLS shows all valid databases and continues with valid ones

#### Too Many Results
**Problem**: Search returns too many papers  
**Solutions**:
- Add more specific terms to queries
- Use syntactic filters
- Set date ranges
- Use more specific semantic filter descriptions

#### Too Few Results
**Problem**: Search returns too few papers  
**Solutions**:
- Broaden queries with OR operators
- Add synonyms
- Remove overly restrictive filters
- Check date ranges

#### API Errors
**Problem**: Commercial database searches fail  
**Solutions**:
- Verify API keys in `config.json`
- Check API key validity
- Use open databases as fallback
- Check rate limiting

#### Semantic Filtering Issues
**Problem**: Semantic filters don't work as expected  
**Solutions**:
- Make descriptions more specific and detailed
- Include key concepts and requirements
- Focus on what you want, not what you want to exclude

### Error Message Examples

#### Critical Error (Pipeline Stops)
```
Configuration error: 'queries' section is missing in config.yaml

🔴 CRITICAL ERRORS - Pipeline cannot continue:

❌ Missing queries section
   Fix: Add a queries section with your search terms
   Example:
queries:
  - augmented reality: "'augmented reality' & 'edge'"
  - machine learning: "'machine learning' & 'systems'"
```

#### Warnings (Pipeline Continues with Defaults)
```
Configuration validation completed with warnings:
Configuration warning: 'databases' section is missing
Configuration warning: 'search_date' is missing

🟡 WARNINGS - Pipeline will continue with defaults where possible:

⚠️  Missing databases section
   Fix: Add databases section or use default open databases
   Default: ['arxiv', 'semantic_scholar']
   Example:
databases:
  - arxiv                    # Open access, no API key needed
  - semantic_scholar        # Open access, no API key needed

⚠️  Missing search_date
   Fix: Add search_date or use current date
   Default: current date
   Example:
search_date: 2024-12-15
```

### Best Practices for Error Recovery

1. **Start with the error messages** - they provide specific guidance
2. **Fix critical errors first** - these prevent the pipeline from running
3. **Review warnings** - understand what defaults will be applied
4. **Use the provided examples** - copy-paste working configurations
5. **Test incrementally** - fix one issue at a time
6. **Let SaLS help** - use the automatic fallbacks when appropriate

### Getting Help with Configuration Issues

If you encounter persistent issues:

1. **Check the error messages** - they provide specific guidance
2. **Review the configuration guide** - covers common scenarios
3. **Use the templates** - working examples to build upon
4. **Start simple** - add complexity gradually
5. **Test with open databases** - no API key requirements

## Configuration Templates

SaLS provides several configuration templates to get you started:

### Basic Template
- **File**: `templates/basic_search_template.yaml`
- **Use Case**: Simple literature searches
- **Features**: Basic queries, synonyms, open databases

### Advanced Template
- **File**: `templates/advanced_research_template.yaml`
- **Use Case**: Complex research projects, systematic reviews
- **Features**: All SaLS features, comprehensive examples

### Machine Learning Template
- **File**: `templates/machine_learning_template.yaml`
- **Use Case**: ML/AI research
- **Features**: ML-specific terminology, subfield examples

### Using Templates
1. Copy the appropriate template file
2. Rename it to your project
3. Modify the values according to your research needs
4. Update the `search_date` and `folder_name`
5. Test with a small search first

## Getting Help

If you encounter issues:

1. **Check the error messages** - they provide specific guidance
2. **Review the configuration guide** - covers common scenarios
3. **Use the templates** - working examples to build upon
4. **Start simple** - add complexity gradually
5. **Test with open databases** - no API key requirements

## Advanced Configuration

### Custom Fields and Types
```yaml
# Advanced users can customize search fields and types
fields: ['title', 'abstract', 'keywords', 'full_text']
types: ['conferences', 'journals', 'preprints', 'reports']
```

### Performance Optimization
- Use date ranges to limit search scope
- Start with fewer databases and add more as needed
- Use syntactic filters to reduce processing time
- Test queries with small date ranges first

---

*This guide covers the essential configuration options for SaLS. For more advanced usage, refer to the code documentation and examples in the templates directory.*
