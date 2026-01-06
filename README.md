# Specific Affinity

A generalized entity resolution and record linkage framework using text similarity matching, blocking keys, and connected components analysis.

## Overview

Specific Affinity is designed to solve the problem of matching and clustering records based on text similarity. Common use cases include:

- **Vendor/Merchant matching**: Grouping transactions from the same vendor
- **Customer deduplication**: Finding duplicate customer records
- **Product matching**: Linking similar products across catalogs
- **Address standardization**: Grouping addresses that refer to the same location

**Supported Platforms & Languages:**
- Python + DuckDB (local/embedded)
- Python + Snowflake (cloud data warehouse)
- R with dplyr + data.table

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
├── requirements.txt
├── python/                      # Python + DuckDB implementation
│   ├── config.py
│   ├── prime_table.py
│   ├── inference.py
│   ├── cleanup.py
│   ├── categorization.py
│   ├── qa.py
│   └── main.py
├── sql/                         # DuckDB SQL scripts
│   ├── 01_create_prime_table.sql
│   ├── 02_make_inference.sql
│   ├── 03_unassigned_cleanup.sql
│   ├── 04_categorization.sql
│   └── 05_qa.sql
├── snowflake/                   # Snowflake implementation
│   ├── snowflake_matcher.py
│   ├── matching_between_tables.sql
│   └── example_usage.py
├── R/                           # R implementation
│   ├── specific_affinity.R     # Main functions (dplyr + data.table)
│   └── example_usage.R         # Usage examples
└── examples/
    └── example_usage.py        # Python example with sample data
```

## Requirements

### Python
```bash
pip install -r requirements.txt

# Or individually:
pip install duckdb pandas                    # For DuckDB
pip install snowflake-connector-python       # For Snowflake
```

### R
```r
install.packages(c("dplyr", "data.table", "stringr"))
```

---

## R Quick Start

The R implementation uses `dplyr` for readable data manipulation and `data.table` for high-performance operations.

### Match Two Tables

```r
source("R/specific_affinity.R")

# Reference table (master vendor list)
master_vendors <- data.frame(
  vendor_id = c("V001", "V002", "V003"),
  vendor_name = c("Netflix Inc", "Spotify Technology", "Amazon Web Services")
)

# Records to match
incoming_data <- data.frame(
  record_id = c("R001", "R002", "R003"),
  merchant_name = c("NETFLIX STREAMING", "Spotify Premium", "Unknown Vendor")
)

# Match tables
results <- match_tables(
  table_a = master_vendors,
  table_b = incoming_data,
  id_col_a = "vendor_id",
  id_col_b = "record_id",
  text_col_a = "vendor_name",
  text_col_b = "merchant_name",
  similarity_threshold = 0.4
)

# View results
print(results$results)
print(results$stats)
```

### Cluster Similar Records (Create Prime Table)

```r
source("R/specific_affinity.R")

# Transaction data with vendor variations
transactions <- data.frame(
  id = c("T001", "T002", "T003", "T004"),
  memo = c("NETFLIX.COM", "NETFLIX SUBSCRIPTION",
           "SPOTIFY USA", "SPOTIFY PREMIUM")
)

# Create clusters
result <- create_prime_table(
  df = transactions,
  id_col = "id",
  text_col = "memo",
  similarity_threshold = 0.4
)

# View clustered data
print(result$prime_table)
print(result$stats)
```

### Match a Single Record

```r
source("R/specific_affinity.R")

# Build reference data structures
reference <- data.frame(
  id = c("V001", "V002"),
  name = c("Netflix Inc", "Spotify Technology")
)

blocking_keys <- create_blocking_keys(reference, "id", "name")
weights <- calculate_weights(blocking_keys)

# Match a single text value
result <- match_single(
  text = "NETFLIX PAYMENT",
  blocking_keys = blocking_keys,
  weights = weights,
  reference_df = reference,
  id_col = "id",
  text_col = "name",
  similarity_threshold = 0.3
)

print(result)
```

### Using with dplyr Pipelines

```r
library(dplyr)
source("R/specific_affinity.R")

# Match tables and analyze with dplyr
results <- match_tables(table_a, table_b,
                        "id_a", "id_b", "text_a", "text_b",
                        similarity_threshold = 0.5)

# Analyze results
results$results %>%
  as_tibble() %>%
  group_by(match_status) %>%
  summarise(
    count = n(),
    avg_score = mean(similarity_score, na.rm = TRUE)
  )

# Filter to matched only
matched <- results$results %>%
  as_tibble() %>%
  filter(match_status == "MATCHED") %>%
  select(table_b_id, matched_table_a_id, similarity_score)
```

### Custom Stop Words in R

```r
source("R/specific_affinity.R")

# Add domain-specific stop words
custom_stops <- c(
  DEFAULT_STOP_WORDS,
  "services", "solutions", "international", "group", "company"
)

results <- match_tables(
  table_a, table_b,
  "id_a", "id_b", "text_a", "text_b",
  similarity_threshold = 0.5,
  stop_words = custom_stops
)
```

### R Function Reference

| Function | Description |
|----------|-------------|
| `create_prime_table()` | Cluster similar records into groups |
| `match_tables()` | Match records from table B against table A |
| `match_single()` | Match a single text value against reference data |
| `create_blocking_keys()` | Tokenize text into blocking keys |
| `calculate_weights()` | Calculate TF-IDF style token weights |
| `find_connected_components()` | Group related records into clusters |

---

## Python + DuckDB Quick Start

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

---

## Snowflake Quick Start

The Snowflake implementation is optimized for matching records between two tables (e.g., matching incoming data against a master reference table).

### Using Python

```python
from snowflake.snowflake_matcher import match_snowflake_tables

results = match_snowflake_tables(
    # Connection
    account="your_account.us-east-1",
    user="your_user",
    password="your_password",
    warehouse="COMPUTE_WH",
    database="MY_DB",
    schema="PUBLIC",

    # Table A: Reference/master table (match AGAINST this)
    table_a="MASTER_VENDORS",
    id_field_a="VENDOR_ID",
    text_field_a="VENDOR_NAME",

    # Table B: Records to match (find matches FOR these)
    table_b="INCOMING_TRANSACTIONS",
    id_field_b="TRANSACTION_ID",
    text_field_b="MERCHANT_NAME",

    # Options
    similarity_threshold=0.5,
    results_table="MATCH_RESULTS"
)

print(f"Matched {results['matched']} of {results['total_records']} records")
```

### Using the Class (More Control)

```python
from snowflake.snowflake_matcher import SnowflakeMatcher, MatchConfig

# Initialize with credentials
matcher = SnowflakeMatcher(
    account="your_account.us-east-1",
    user="your_user",
    password="your_password",  # Or use authenticator="externalbrowser" for SSO
    warehouse="COMPUTE_WH",
    database="MY_DB",
    schema="PUBLIC"
)

# Add custom stop words for your domain
matcher.add_stop_words({"services", "solutions", "group", "international"})

# Configure the match
config = MatchConfig(
    table_a="MASTER_VENDORS",
    id_field_a="VENDOR_ID",
    text_field_a="VENDOR_NAME",
    table_b="INCOMING_DATA",
    id_field_b="RECORD_ID",
    text_field_b="MERCHANT_NAME",
    similarity_threshold=0.5,
    results_table="VENDOR_MATCHES"
)

# Run matching
with matcher:
    results = matcher.match_tables(config)

    # Get results as pandas DataFrame
    df = matcher.get_results_df(config)
    df.to_csv("matches.csv", index=False)
```

### Using SQL Only

Run `snowflake/matching_between_tables.sql` directly in Snowflake after replacing these placeholders:

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `{{DATABASE}}` | Your database | `MY_DB` |
| `{{SCHEMA}}` | Your schema | `PUBLIC` |
| `{{TABLE_A}}` | Reference table | `MASTER_VENDORS` |
| `{{TABLE_B}}` | Table to match | `INCOMING_DATA` |
| `{{ID_FIELD_A}}` | ID column in Table A | `VENDOR_ID` |
| `{{ID_FIELD_B}}` | ID column in Table B | `RECORD_ID` |
| `{{TEXT_FIELD_A}}` | Text column in Table A | `VENDOR_NAME` |
| `{{TEXT_FIELD_B}}` | Text column in Table B | `MERCHANT_NAME` |
| `{{SIMILARITY_THRESHOLD}}` | Minimum match score | `0.5` |

### Snowflake Output Schema

Results are written to your specified results table:

| Column | Description |
|--------|-------------|
| `table_b_id` | ID from the records you're matching |
| `table_b_text` | Original text from Table B |
| `matched_table_a_id` | Matched ID from Table A (NULL if unmatched) |
| `matched_table_a_text` | Matched text from Table A |
| `similarity_score` | Match score (higher = better match) |
| `match_status` | `'MATCHED'` or `'UNMATCHED'` |

### Snowflake Authentication Options

```python
# Option 1: Password
matcher = SnowflakeMatcher(
    account="...", user="...", password="your_password", ...
)

# Option 2: SSO (opens browser)
matcher = SnowflakeMatcher(
    account="...", user="your_email@company.com",
    authenticator="externalbrowser", ...
)

# Option 3: Environment variables
import os
matcher = SnowflakeMatcher(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    ...
)
```

---

## Configuration

Key parameters to tune:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `text_field` / `text_col` | Required | The text column to use for matching |
| `id_field` / `id_col` | Required | Unique identifier column |
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
Graph-based clustering using iterative label propagation:
- Each record starts in its own cluster
- Records sharing high-scoring matches are merged
- Minimum cluster ID propagates through the graph

## Tuning Tips

| Issue | Solution |
|-------|----------|
| Too few matches | Lower `similarity_threshold` (try 0.3-0.4) |
| Too many false matches | Raise `similarity_threshold` (try 0.6-0.7) |
| Common words causing bad matches | Add domain-specific stop words |
| Short text fields | Lower `min_token_length` to 1 |
| Need to match on multiple fields | Concatenate fields before matching |

## Best Practices

1. **Data Quality**: Clean and normalize your data before processing
2. **Threshold Tuning**: Start with 0.5, adjust based on precision/recall needs
3. **Stop Words**: Add domain-specific stop words for better results
4. **Incremental Updates**: Use inference for new records rather than rebuilding
5. **Regular QA**: Monitor cluster consistency and similarity distributions

## License

MIT License
