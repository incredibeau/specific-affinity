"""
Example: Matching Two Tables in Snowflake

This script demonstrates how to use the Specific Affinity framework
to match records between two Snowflake tables.
"""

from snowflake_matcher import SnowflakeMatcher, MatchConfig, match_snowflake_tables


# =============================================================================
# EXAMPLE 1: Using the SnowflakeMatcher class
# =============================================================================

def example_with_class():
    """Example using the SnowflakeMatcher class for more control."""

    # Initialize the matcher with your Snowflake credentials
    matcher = SnowflakeMatcher(
        account="your_account.us-east-1",    # e.g., "xy12345.us-east-1"
        user="your_username",
        password="your_password",             # Or use authenticator="externalbrowser"
        warehouse="your_warehouse",           # e.g., "COMPUTE_WH"
        database="your_database",
        schema="your_schema"                  # e.g., "PUBLIC"
    )

    # Add custom stop words for your domain
    matcher.add_stop_words({"services", "solutions", "group", "international"})

    # Define the matching configuration
    config = MatchConfig(
        # Reference table (what you're matching AGAINST)
        table_a="MASTER_VENDORS",
        id_field_a="VENDOR_ID",
        text_field_a="VENDOR_NAME",

        # Table to match (records you want to find matches for)
        table_b="INCOMING_TRANSACTIONS",
        id_field_b="TRANSACTION_ID",
        text_field_b="MERCHANT_NAME",

        # Matching parameters
        similarity_threshold=0.5,  # Adjust based on your data (0.3-0.7 typical)

        # Output table name
        results_table="VENDOR_MATCH_RESULTS"
    )

    # Run the matching
    with matcher:
        results = matcher.match_tables(config)

        # Get results as DataFrame
        df = matcher.get_results_df(config)
        print(df.head(20))

        # Export to CSV
        df.to_csv("match_results.csv", index=False)

    return results


# =============================================================================
# EXAMPLE 2: Using the quick function
# =============================================================================

def example_quick_function():
    """Example using the convenience function for simple matching."""

    results = match_snowflake_tables(
        # Snowflake connection
        account="your_account.us-east-1",
        user="your_username",
        password="your_password",
        warehouse="your_warehouse",
        database="your_database",
        schema="your_schema",

        # Table A: Reference/Master table
        table_a="MASTER_CUSTOMERS",
        id_field_a="CUSTOMER_ID",
        text_field_a="CUSTOMER_NAME",

        # Table B: Records to match
        table_b="NEW_LEADS",
        id_field_b="LEAD_ID",
        text_field_b="COMPANY_NAME",

        # Options
        similarity_threshold=0.5,
        results_table="CUSTOMER_MATCH_RESULTS"
    )

    print(f"Match rate: {results['match_rate_pct']}%")
    return results


# =============================================================================
# EXAMPLE 3: Using environment variables for credentials
# =============================================================================

def example_with_env_vars():
    """Example using environment variables for secure credential handling."""
    import os

    matcher = SnowflakeMatcher(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
    )

    config = MatchConfig(
        table_a="REFERENCE_TABLE",
        id_field_a="ID",
        text_field_a="NAME",
        table_b="RECORDS_TO_MATCH",
        id_field_b="ID",
        text_field_b="NAME",
        similarity_threshold=0.5
    )

    with matcher:
        return matcher.match_tables(config)


# =============================================================================
# EXAMPLE 4: Using SSO/Browser authentication
# =============================================================================

def example_with_sso():
    """Example using SSO authentication (opens browser)."""

    matcher = SnowflakeMatcher(
        account="your_account.us-east-1",
        user="your_email@company.com",
        authenticator="externalbrowser",  # Opens browser for SSO
        warehouse="your_warehouse",
        database="your_database",
        schema="your_schema"
    )

    config = MatchConfig(
        table_a="TABLE_A",
        id_field_a="ID",
        text_field_a="DESCRIPTION",
        table_b="TABLE_B",
        id_field_b="ID",
        text_field_b="DESCRIPTION"
    )

    with matcher:
        return matcher.match_tables(config)


# =============================================================================
# SQL-Only Approach (if you prefer running SQL directly)
# =============================================================================

SQL_TEMPLATE = """
-- =============================================================================
-- Run this SQL directly in Snowflake after replacing the placeholders
-- =============================================================================

-- Step 1: Create stop words
CREATE OR REPLACE TABLE stop_words AS
SELECT column1 AS word FROM VALUES
('a'),('an'),('and'),('the'),('of'),('to'),('in'),('for'),('is'),('on'),
('that'),('by'),('this'),('with'),('or'),('as'),('be'),('are'),('from'),
('inc'),('llc'),('corp'),('ltd'),('co'),('company');

-- Step 2: Tokenize Table A
CREATE OR REPLACE TABLE table_a_tokens AS
WITH tokenized AS (
    SELECT
        {id_field_a} AS record_id,
        TRIM(t.value) AS token
    FROM {table_a},
    LATERAL SPLIT_TO_TABLE(
        LOWER(REGEXP_REPLACE({text_field_a}, '[^a-zA-Z0-9\\\\s]', ' ')),
        ' '
    ) t
)
SELECT DISTINCT record_id, token
FROM tokenized
WHERE LENGTH(token) >= 2 AND token NOT IN (SELECT word FROM stop_words);

-- Step 3: Calculate weights
CREATE OR REPLACE TABLE token_weights AS
WITH counts AS (
    SELECT token, COUNT(*) AS freq,
           (SELECT COUNT(DISTINCT record_id) FROM table_a_tokens) AS total
    FROM table_a_tokens GROUP BY token
)
SELECT token,
       ROUND((LN((SELECT AVG(freq::FLOAT/total) FROM counts) / (freq::FLOAT/total))
              - MIN(LN((SELECT AVG(freq::FLOAT/total) FROM counts) / (freq::FLOAT/total))) OVER())
             / NULLIF(MAX(LN((SELECT AVG(freq::FLOAT/total) FROM counts) / (freq::FLOAT/total))) OVER()
                      - MIN(LN((SELECT AVG(freq::FLOAT/total) FROM counts) / (freq::FLOAT/total))) OVER(), 0), 4) AS weight
FROM counts WHERE freq > 0;

-- Step 4: Tokenize Table B
CREATE OR REPLACE TABLE table_b_tokens AS
WITH tokenized AS (
    SELECT
        {id_field_b} AS record_id,
        TRIM(t.value) AS token
    FROM {table_b},
    LATERAL SPLIT_TO_TABLE(
        LOWER(REGEXP_REPLACE({text_field_b}, '[^a-zA-Z0-9\\\\s]', ' ')),
        ' '
    ) t
)
SELECT DISTINCT record_id, token
FROM tokenized
WHERE LENGTH(token) >= 2 AND token NOT IN (SELECT word FROM stop_words);

-- Step 5: Find and rank matches
CREATE OR REPLACE TABLE {results_table} AS
WITH candidates AS (
    SELECT bt.record_id AS b_id, at.record_id AS a_id, SUM(w.weight) AS score
    FROM table_b_tokens bt
    JOIN table_a_tokens at ON bt.token = at.token
    JOIN token_weights w ON bt.token = w.token
    GROUP BY bt.record_id, at.record_id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY b_id ORDER BY score DESC) AS rn
    FROM candidates
)
SELECT
    b.{id_field_b} AS table_b_id,
    b.{text_field_b} AS table_b_text,
    a.{id_field_a} AS matched_table_a_id,
    a.{text_field_a} AS matched_table_a_text,
    r.score AS similarity_score,
    CASE WHEN r.score >= {threshold} THEN 'MATCHED' ELSE 'UNMATCHED' END AS match_status
FROM {table_b} b
LEFT JOIN ranked r ON b.{id_field_b} = r.b_id AND r.rn = 1 AND r.score >= {threshold}
LEFT JOIN {table_a} a ON r.a_id = a.{id_field_a};

-- View results
SELECT * FROM {results_table} ORDER BY similarity_score DESC NULLS LAST;
"""

def generate_sql(
    table_a: str,
    id_field_a: str,
    text_field_a: str,
    table_b: str,
    id_field_b: str,
    text_field_b: str,
    threshold: float = 0.5,
    results_table: str = "match_results"
) -> str:
    """Generate SQL script for matching (to run directly in Snowflake)."""
    return SQL_TEMPLATE.format(
        table_a=table_a,
        id_field_a=id_field_a,
        text_field_a=text_field_a,
        table_b=table_b,
        id_field_b=id_field_b,
        text_field_b=text_field_b,
        threshold=threshold,
        results_table=results_table
    )


if __name__ == "__main__":
    # Generate SQL for manual execution
    sql = generate_sql(
        table_a="MASTER_VENDORS",
        id_field_a="VENDOR_ID",
        text_field_a="VENDOR_NAME",
        table_b="INCOMING_DATA",
        id_field_b="RECORD_ID",
        text_field_b="MERCHANT_NAME",
        threshold=0.5,
        results_table="VENDOR_MATCHES"
    )

    print(sql)

    # Or run one of the examples:
    # example_with_class()
    # example_quick_function()
