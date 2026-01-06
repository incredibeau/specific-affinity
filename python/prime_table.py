"""
Step 1: Create Prime Table

This module creates the foundational clustering of records based on text similarity.
It tokenizes text fields, calculates weights, performs self-joins, and identifies
connected components to form clusters.
"""

import duckdb
from typing import Dict, Any
from .config import Config


def create_stop_words_table(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """Create and populate the stop words table."""
    print("Creating stop words table...")

    con.execute("CREATE TABLE IF NOT EXISTS stop_words (word VARCHAR PRIMARY KEY)")

    # Clear existing and insert configured stop words
    con.execute("DELETE FROM stop_words")

    for word in config.stop_words:
        con.execute("INSERT OR IGNORE INTO stop_words (word) VALUES (?)", [word])

    count = con.execute("SELECT COUNT(*) FROM stop_words").fetchone()[0]
    print(f"Stop words table created with {count} words.")


def create_normalized_text_view(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """Create a view with normalized text for tokenization."""
    print("Creating normalized text view...")

    con.execute(f"""
    CREATE OR REPLACE VIEW normalized_text AS
    SELECT
        {config.id_field} AS record_id,
        {config.text_field} AS original_text,
        regexp_replace(LOWER({config.text_field}), '[^a-zA-Z0-9\\s]', ' ', 'g') AS normalized_text
    FROM {config.source_table}
    WHERE {config.text_field} IS NOT NULL AND LENGTH(TRIM({config.text_field})) > 0
    """)


def create_blocking_keys(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Tokenize text into blocking keys."""
    print("Creating blocking keys...")

    con.execute(f"""
    CREATE OR REPLACE TABLE {config.blocking_keys_table} AS
    WITH tokenized AS (
        SELECT
            record_id,
            unnest(string_split(normalized_text, ' ')) AS token
        FROM normalized_text
    )
    SELECT DISTINCT
        record_id,
        token
    FROM tokenized
    WHERE
        token IS NOT NULL
        AND LENGTH(token) >= {config.min_token_length}
        AND token NOT IN (SELECT word FROM stop_words)
    """)

    count = con.execute(f"SELECT COUNT(*) FROM {config.blocking_keys_table}").fetchone()[0]
    print(f"Created {count} blocking keys.")
    return count


def calculate_weights(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Calculate TF-IDF style weights for tokens."""
    print("Calculating token weights...")

    con.execute(f"""
    CREATE OR REPLACE TABLE {config.weights_table} AS
    WITH token_counts AS (
        SELECT
            token,
            COUNT(*) AS freq,
            (SELECT COUNT(DISTINCT record_id) FROM {config.blocking_keys_table}) AS total_records
        FROM {config.blocking_keys_table}
        GROUP BY token
    ),
    weight_calc AS (
        SELECT
            token,
            freq,
            total_records,
            freq::DOUBLE / total_records AS raw_frequency,
            (SELECT AVG(freq::DOUBLE / total_records) FROM token_counts) /
                NULLIF(freq::DOUBLE / total_records, 0) AS weight
        FROM token_counts
    ),
    log_weights AS (
        SELECT
            token,
            LN(weight) AS log_weight
        FROM weight_calc
        WHERE token != '' AND weight > 0
    ),
    normalized AS (
        SELECT
            token,
            (log_weight - MIN(log_weight) OVER()) /
            NULLIF((MAX(log_weight) OVER() - MIN(log_weight) OVER()), 0) AS normalized_weight
        FROM log_weights
    )
    SELECT
        token,
        ROUND(COALESCE(normalized_weight, 0), 4) AS weight
    FROM normalized
    WHERE normalized_weight IS NOT NULL
    """)

    count = con.execute(f"SELECT COUNT(*) FROM {config.weights_table}").fetchone()[0]
    print(f"Calculated weights for {count} unique tokens.")
    return count


def find_candidate_pairs(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Self-join to find candidate pairs based on shared tokens."""
    print("Finding candidate pairs...")

    con.execute(f"""
    CREATE OR REPLACE TABLE candidate_pairs AS
    SELECT
        bk1.record_id AS record_id_1,
        bk2.record_id AS record_id_2,
        SUM(tw.weight) AS similarity_score
    FROM
        {config.blocking_keys_table} bk1
    JOIN
        {config.blocking_keys_table} bk2 ON bk1.token = bk2.token
    JOIN
        {config.weights_table} tw ON bk1.token = tw.token
    WHERE
        bk1.record_id < bk2.record_id
    GROUP BY
        bk1.record_id, bk2.record_id
    """)

    count = con.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]
    print(f"Found {count} candidate pairs.")
    return count


def filter_by_threshold(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Filter pairs by similarity threshold."""
    print(f"Filtering pairs with threshold >= {config.similarity_threshold}...")

    con.execute(f"""
    CREATE OR REPLACE TABLE filtered_pairs AS
    SELECT
        record_id_1,
        record_id_2,
        similarity_score
    FROM candidate_pairs
    WHERE similarity_score >= {config.similarity_threshold}
    """)

    count = con.execute("SELECT COUNT(*) FROM filtered_pairs").fetchone()[0]
    print(f"Kept {count} pairs above threshold.")
    return count


def identify_connected_components(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Use connected components algorithm to form clusters."""
    print("Identifying connected components...")

    con.execute(f"""
    CREATE OR REPLACE TABLE {config.clusters_table} AS
    WITH RECURSIVE connected_components AS (
        -- Base case: every record starts in its own cluster
        SELECT
            record_id_1 AS node,
            record_id_1 AS cluster_id
        FROM filtered_pairs
        UNION
        SELECT
            record_id_2 AS node,
            record_id_2 AS cluster_id
        FROM filtered_pairs
        UNION
        -- Recursive: propagate minimum cluster_id
        SELECT
            fp.record_id_2 AS node,
            LEAST(cc.cluster_id, fp.record_id_2) AS cluster_id
        FROM connected_components cc
        JOIN filtered_pairs fp ON cc.node = fp.record_id_1
        WHERE cc.cluster_id <> LEAST(cc.cluster_id, fp.record_id_2)
        UNION
        SELECT
            fp.record_id_1 AS node,
            LEAST(cc.cluster_id, fp.record_id_1) AS cluster_id
        FROM connected_components cc
        JOIN filtered_pairs fp ON cc.node = fp.record_id_2
        WHERE cc.cluster_id <> LEAST(cc.cluster_id, fp.record_id_1)
    )
    SELECT
        node AS record_id,
        MIN(cluster_id) AS cluster_id
    FROM connected_components
    GROUP BY node
    ORDER BY cluster_id, record_id
    """)

    count = con.execute(f"SELECT COUNT(DISTINCT cluster_id) FROM {config.clusters_table}").fetchone()[0]
    print(f"Created {count} clusters.")
    return count


def create_prime_table(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """Create the final prime table with cluster assignments."""
    print("Creating prime table...")

    con.execute(f"""
    CREATE OR REPLACE TABLE {config.prime_table} AS
    SELECT
        s.*,
        rc.cluster_id
    FROM {config.source_table} s
    LEFT JOIN {config.clusters_table} rc ON s.{config.id_field} = rc.record_id
    """)

    total = con.execute(f"SELECT COUNT(*) FROM {config.prime_table}").fetchone()[0]
    clustered = con.execute(f"SELECT COUNT(*) FROM {config.prime_table} WHERE cluster_id IS NOT NULL").fetchone()[0]
    print(f"Prime table created: {clustered}/{total} records clustered.")


def get_summary_stats(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """Get summary statistics for the prime table."""
    result = con.execute(f"""
    SELECT
        COUNT(DISTINCT cluster_id) AS total_clusters,
        COUNT(*) AS total_records,
        COUNT(*) FILTER (WHERE cluster_id IS NOT NULL) AS clustered_records,
        COUNT(*) FILTER (WHERE cluster_id IS NULL) AS unclustered_records
    FROM {config.prime_table}
    """).fetchone()

    avg_size = con.execute(f"""
    SELECT ROUND(AVG(cluster_size), 2)
    FROM (
        SELECT cluster_id, COUNT(*) AS cluster_size
        FROM {config.prime_table}
        WHERE cluster_id IS NOT NULL
        GROUP BY cluster_id
    ) t
    """).fetchone()[0]

    return {
        "total_clusters": result[0],
        "total_records": result[1],
        "clustered_records": result[2],
        "unclustered_records": result[3],
        "avg_cluster_size": avg_size
    }


def run_step_1(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """
    Run the complete Step 1: Create Prime Table.

    Args:
        con: DuckDB connection
        config: Configuration object

    Returns:
        Dictionary with summary statistics
    """
    print("\n" + "=" * 60)
    print("STEP 1: CREATE PRIME TABLE")
    print("=" * 60 + "\n")

    create_stop_words_table(con, config)
    create_normalized_text_view(con, config)
    create_blocking_keys(con, config)
    calculate_weights(con, config)
    find_candidate_pairs(con, config)
    filter_by_threshold(con, config)
    identify_connected_components(con, config)
    create_prime_table(con, config)

    stats = get_summary_stats(con, config)

    print("\n" + "-" * 40)
    print("SUMMARY:")
    print(f"  Total records: {stats['total_records']}")
    print(f"  Clustered records: {stats['clustered_records']}")
    print(f"  Unclustered records: {stats['unclustered_records']}")
    print(f"  Total clusters: {stats['total_clusters']}")
    print(f"  Avg cluster size: {stats['avg_cluster_size']}")
    print("-" * 40 + "\n")

    return stats
