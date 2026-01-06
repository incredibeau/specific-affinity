# Specific Affinity

A generalized entity resolution and record linkage framework using text similarity matching, blocking keys, and connected components analysis.

## Overview

Specific Affinity is designed to solve the problem of matching and clustering records based on text similarity. Common use cases include:

- **Vendor/Merchant matching**: Grouping transactions from the same vendor
- **Customer deduplication**: Finding duplicate customer records
- **Product matching**: Linking similar products across catalogs
- **Address standardization**: Grouping addresses that refer to the same location

## How It Works

The framework uses a 5-step pipeline:

### 1. Create Prime Table
Build a reference table by clustering similar records from historical data:
- Tokenize text fields into blocking keys
- Calculate TF-IDF-style weights for each token
- Self-join records on shared tokens
- Score pairs based on weighted token overlap
- Identify connected components to form clusters

### 2. Make Inference
Match new records against the Prime Table:
- Tokenize the new record
- Join against the Prime Table's blocking keys
- Score and rank potential matches
- Assign to the best matching cluster (if above threshold)

### 3. Unassigned Records Cleanup
Handle records that couldn't be matched:
- Extract unassigned records
- Apply the same clustering methodology
- Create new clusters from similar unassigned records
- Integrate new clusters into the Prime Table

### 4. Record Categorization (Optional)
Classify records based on patterns (when numeric/date fields are present):
- Analyze frequency patterns
- Examine amount/value consistency
- Categorize as subscription, recurring, or one-time

### 5. Quality Assurance
Validate assignments and manage the system:
- Analyze similarity score distributions
- Check cluster consistency
- Identify potential false matches
- Manage blocking keys

## Project Structure

```
specific-affinity/
├── README.md
├── python/
│   ├── config.py           # Configuration settings
│   ├── prime_table.py      # Step 1: Create Prime Table
│   ├── inference.py        # Step 2: Make Inference
│   ├── cleanup.py          # Step 3: Unassigned Records Cleanup
│   ├── categorization.py   # Step 4: Record Categorization
│   ├── qa.py               # Step 5: Quality Assurance
│   └── main.py             # Orchestrator script
├── sql/
│   ├── 01_create_prime_table.sql
│   ├── 02_make_inference.sql
│   ├── 03_unassigned_cleanup.sql
│   ├── 04_categorization.sql
│   └── 05_qa.sql
└── examples/
    └── example_usage.py    # Example with sample data
```

## Requirements

- Python 3.8+
- DuckDB

Install dependencies:
```bash
pip install duckdb pandas
```

## Quick Start

### Using Python

```python
from python.main import SpecificAffinity

# Initialize with your configuration
sa = SpecificAffinity(
    db_path="my_data.duckdb",
    text_field="description",      # The text field to match on
    id_field="record_id",          # Unique identifier field
    source_table="raw_records",    # Your source data table
    similarity_threshold=0.5       # Minimum similarity for matches
)

# Run the full pipeline
sa.run_pipeline()

# Or run individual steps
sa.create_prime_table()
sa.make_inference(new_records_table="new_data")
sa.cleanup_unassigned()
sa.run_qa()
```

### Using SQL

Execute the SQL scripts in order against your DuckDB database:
```bash
duckdb my_data.duckdb < sql/01_create_prime_table.sql
duckdb my_data.duckdb < sql/02_make_inference.sql
# ... etc
```

## Configuration

Key parameters to tune:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `text_field` | Required | The text column to use for matching |
| `id_field` | Required | Unique identifier column |
| `similarity_threshold` | 0.5 | Minimum score to consider a match |
| `stop_words` | Common English | Words to exclude from tokenization |
| `min_token_length` | 2 | Minimum characters for a valid token |

## Algorithm Details

### Tokenization
Text is normalized and split into tokens:
1. Convert to lowercase
2. Remove special characters
3. Split on whitespace
4. Filter stop words and short tokens

### Weight Calculation
Tokens are weighted using inverse document frequency:
```
weight = log(avg_frequency / token_frequency)
normalized_weight = (weight - min) / (max - min)
```

Rare tokens get higher weights, common tokens get lower weights.

### Similarity Scoring
Pairs are scored by summing weights of shared tokens:
```
similarity = sum(weight for each shared token)
```

### Connected Components
Graph-based clustering using recursive CTEs:
- Each record starts in its own cluster
- Records sharing high-scoring matches are merged
- Minimum cluster ID propagates through the graph

## Best Practices

1. **Data Quality**: Clean and normalize your data before processing
2. **Threshold Tuning**: Start with 0.5, adjust based on precision/recall needs
3. **Stop Words**: Add domain-specific stop words for better results
4. **Incremental Updates**: Use inference for new records rather than rebuilding
5. **Regular QA**: Monitor cluster consistency and similarity distributions

## License

MIT License
