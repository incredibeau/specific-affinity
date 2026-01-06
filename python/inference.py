"""
Step 2: Make Inference

This module matches new records against the existing Prime Table to identify
their cluster assignment.
"""

import duckdb
from typing import Dict, Any, Optional
from .config import Config


def tokenize_new_records(
    con: duckdb.DuckDBPyConnection,
    config: Config,
    new_records_table: str
) -> int:
    """Tokenize new records for matching."""
    print(f"Tokenizing new records from {new_records_table}...")

    con.execute(f"""
    CREATE OR REPLACE TABLE new_record_tokens AS
    WITH normalized AS (
        SELECT
            {config.id_field} AS record_id,
            regexp_replace(LOWER({config.text_field}), '[^a-zA-Z0-9\\s]', ' ', 'g') AS normalized_text
        FROM {new_records_table}
        WHERE {config.text_field} IS NOT NULL AND LENGTH(TRIM({config.text_field})) > 0
    ),
    tokenized AS (
        SELECT
            record_id,
            unnest(string_split(normalized_text, ' ')) AS token
        FROM normalized
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

    count = con.execute("SELECT COUNT(DISTINCT record_id) FROM new_record_tokens").fetchone()[0]
    print(f"Tokenized {count} new records.")
    return count


def find_candidates(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Find candidate matches against the prime table."""
    print("Finding candidate matches...")

    con.execute(f"""
    CREATE OR REPLACE TABLE inference_candidates AS
    SELECT
        nrt.record_id AS query_id,
        bk.record_id AS prime_record_id,
        pt.cluster_id AS prime_cluster_id,
        SUM(tw.weight) AS similarity_score
    FROM
        new_record_tokens nrt
    JOIN
        {config.blocking_keys_table} bk ON nrt.token = bk.token
    JOIN
        {config.weights_table} tw ON nrt.token = tw.token
    JOIN
        {config.prime_table} pt ON bk.record_id = pt.{config.id_field}
    WHERE
        pt.cluster_id IS NOT NULL
    GROUP BY
        nrt.record_id, bk.record_id, pt.cluster_id
    """)

    count = con.execute("SELECT COUNT(*) FROM inference_candidates").fetchone()[0]
    print(f"Found {count} candidate matches.")
    return count


def rank_matches(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """Rank candidate matches by similarity score."""
    print("Ranking matches...")

    con.execute("""
    CREATE OR REPLACE TABLE ranked_matches AS
    SELECT
        query_id,
        prime_record_id,
        prime_cluster_id,
        similarity_score,
        ROW_NUMBER() OVER (
            PARTITION BY query_id
            ORDER BY similarity_score DESC
        ) AS match_rank
    FROM inference_candidates
    """)


def create_inferred_matches(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Create table of inferred matches above threshold."""
    print(f"Creating inferred matches (threshold >= {config.similarity_threshold})...")

    con.execute(f"""
    CREATE OR REPLACE TABLE {config.inferred_matches_table} AS
    SELECT
        query_id,
        prime_record_id AS matched_record_id,
        prime_cluster_id AS assigned_cluster_id,
        similarity_score,
        match_rank
    FROM ranked_matches
    WHERE
        match_rank = 1
        AND similarity_score >= {config.similarity_threshold}
    """)

    count = con.execute(f"SELECT COUNT(*) FROM {config.inferred_matches_table}").fetchone()[0]
    print(f"Created {count} inferred matches.")
    return count


def create_inference_results(
    con: duckdb.DuckDBPyConnection,
    config: Config,
    new_records_table: str
) -> None:
    """Create the final inference results table."""
    print("Creating inference results...")

    con.execute(f"""
    CREATE OR REPLACE TABLE inference_results AS
    SELECT
        nr.*,
        im.assigned_cluster_id,
        im.matched_record_id,
        im.similarity_score,
        CASE
            WHEN im.assigned_cluster_id IS NOT NULL THEN 'matched'
            ELSE 'unmatched'
        END AS match_status
    FROM {new_records_table} nr
    LEFT JOIN {config.inferred_matches_table} im ON nr.{config.id_field} = im.query_id
    """)


def get_inference_stats(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get summary statistics for inference results."""
    result = con.execute("""
    SELECT
        COUNT(*) AS total_records,
        COUNT(*) FILTER (WHERE match_status = 'matched') AS matched,
        COUNT(*) FILTER (WHERE match_status = 'unmatched') AS unmatched,
        ROUND(AVG(similarity_score) FILTER (WHERE match_status = 'matched'), 4) AS avg_similarity
    FROM inference_results
    """).fetchone()

    match_rate = result[1] * 100.0 / result[0] if result[0] > 0 else 0

    return {
        "total_records": result[0],
        "matched": result[1],
        "unmatched": result[2],
        "match_rate_pct": round(match_rate, 2),
        "avg_similarity": result[3]
    }


def run_step_2(
    con: duckdb.DuckDBPyConnection,
    config: Config,
    new_records_table: str
) -> Dict[str, Any]:
    """
    Run the complete Step 2: Make Inference.

    Args:
        con: DuckDB connection
        config: Configuration object
        new_records_table: Name of table containing new records to match

    Returns:
        Dictionary with summary statistics
    """
    print("\n" + "=" * 60)
    print("STEP 2: MAKE INFERENCE")
    print("=" * 60 + "\n")

    tokenize_new_records(con, config, new_records_table)
    find_candidates(con, config)
    rank_matches(con, config)
    create_inferred_matches(con, config)
    create_inference_results(con, config, new_records_table)

    stats = get_inference_stats(con)

    print("\n" + "-" * 40)
    print("SUMMARY:")
    print(f"  Total new records: {stats['total_records']}")
    print(f"  Matched: {stats['matched']} ({stats['match_rate_pct']}%)")
    print(f"  Unmatched: {stats['unmatched']}")
    print(f"  Avg similarity: {stats['avg_similarity']}")
    print("-" * 40 + "\n")

    return stats


def infer_single_record(
    con: duckdb.DuckDBPyConnection,
    config: Config,
    text_value: str,
    record_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Match a single text value against the prime table.

    Args:
        con: DuckDB connection
        config: Configuration object
        text_value: The text to match
        record_id: Optional ID for the record

    Returns:
        Dictionary with match results
    """
    import re

    # Normalize and tokenize
    normalized = re.sub(r'[^a-zA-Z0-9\s]', ' ', text_value.lower())
    tokens = [t for t in normalized.split() if len(t) >= config.min_token_length and t not in config.stop_words]

    if not tokens:
        return {"matched": False, "reason": "No valid tokens"}

    # Build query to find matches
    token_list = ", ".join(f"'{t}'" for t in tokens)

    result = con.execute(f"""
    WITH query_tokens AS (
        SELECT unnest([{token_list}]) AS token
    ),
    matches AS (
        SELECT
            pt.{config.id_field} AS matched_record_id,
            pt.cluster_id,
            pt.{config.text_field} AS matched_text,
            SUM(tw.weight) AS similarity_score
        FROM query_tokens qt
        JOIN {config.blocking_keys_table} bk ON qt.token = bk.token
        JOIN {config.weights_table} tw ON qt.token = tw.token
        JOIN {config.prime_table} pt ON bk.record_id = pt.{config.id_field}
        WHERE pt.cluster_id IS NOT NULL
        GROUP BY pt.{config.id_field}, pt.cluster_id, pt.{config.text_field}
    )
    SELECT *
    FROM matches
    WHERE similarity_score >= {config.similarity_threshold}
    ORDER BY similarity_score DESC
    LIMIT 5
    """).fetchall()

    if not result:
        return {"matched": False, "reason": "No matches above threshold"}

    top_match = result[0]
    return {
        "matched": True,
        "assigned_cluster_id": top_match[1],
        "matched_record_id": top_match[0],
        "matched_text": top_match[2],
        "similarity_score": top_match[3],
        "all_candidates": [
            {
                "record_id": r[0],
                "cluster_id": r[1],
                "text": r[2],
                "score": r[3]
            }
            for r in result
        ]
    }
