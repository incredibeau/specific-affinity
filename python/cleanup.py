"""
Step 3: Unassigned Records Cleanup

This module handles records that couldn't be matched during inference by
applying the same clustering methodology to find matches among the unassigned
records themselves.
"""

import duckdb
from typing import Dict, Any
from .config import Config


def identify_unassigned(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Identify records that weren't matched during inference."""
    print("Identifying unassigned records...")

    con.execute(f"""
    CREATE OR REPLACE TABLE {config.unassigned_table} AS
    SELECT *
    FROM inference_results
    WHERE match_status = 'unmatched'
    """)

    count = con.execute(f"SELECT COUNT(*) FROM {config.unassigned_table}").fetchone()[0]
    print(f"Found {count} unassigned records.")
    return count


def create_unassigned_blocking_keys(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Create blocking keys for unassigned records."""
    print("Creating blocking keys for unassigned records...")

    con.execute(f"""
    CREATE OR REPLACE TABLE unassigned_blocking_keys AS
    WITH normalized AS (
        SELECT
            {config.id_field} AS record_id,
            regexp_replace(LOWER({config.text_field}), '[^a-zA-Z0-9\\s]', ' ', 'g') AS normalized_text
        FROM {config.unassigned_table}
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

    count = con.execute("SELECT COUNT(*) FROM unassigned_blocking_keys").fetchone()[0]
    print(f"Created {count} blocking keys.")
    return count


def calculate_unassigned_weights(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Calculate weights for unassigned record tokens."""
    print("Calculating weights for unassigned tokens...")

    con.execute("""
    CREATE OR REPLACE TABLE unassigned_weights AS
    WITH token_counts AS (
        SELECT
            token,
            COUNT(*) AS freq,
            (SELECT COUNT(DISTINCT record_id) FROM unassigned_blocking_keys) AS total_records
        FROM unassigned_blocking_keys
        GROUP BY token
    ),
    weight_calc AS (
        SELECT
            token,
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

    count = con.execute("SELECT COUNT(*) FROM unassigned_weights").fetchone()[0]
    print(f"Calculated weights for {count} tokens.")
    return count


def find_unassigned_pairs(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Self-join unassigned records to find potential matches."""
    print("Finding potential matches among unassigned records...")

    con.execute("""
    CREATE OR REPLACE TABLE unassigned_candidate_pairs AS
    SELECT
        bk1.record_id AS record_id_1,
        bk2.record_id AS record_id_2,
        SUM(uw.weight) AS similarity_score
    FROM
        unassigned_blocking_keys bk1
    JOIN
        unassigned_blocking_keys bk2 ON bk1.token = bk2.token
    JOIN
        unassigned_weights uw ON bk1.token = uw.token
    WHERE
        bk1.record_id < bk2.record_id
    GROUP BY
        bk1.record_id, bk2.record_id
    """)

    count = con.execute("SELECT COUNT(*) FROM unassigned_candidate_pairs").fetchone()[0]
    print(f"Found {count} candidate pairs.")
    return count


def filter_unassigned_pairs(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Filter unassigned pairs by threshold."""
    print(f"Filtering pairs with threshold >= {config.similarity_threshold}...")

    con.execute(f"""
    CREATE OR REPLACE TABLE unassigned_filtered_pairs AS
    SELECT
        record_id_1,
        record_id_2,
        similarity_score
    FROM unassigned_candidate_pairs
    WHERE similarity_score >= {config.similarity_threshold}
    """)

    count = con.execute("SELECT COUNT(*) FROM unassigned_filtered_pairs").fetchone()[0]
    print(f"Kept {count} pairs above threshold.")
    return count


def identify_unassigned_components(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Identify connected components among unassigned records."""
    print("Identifying connected components...")

    # Check if there are any pairs to process
    pair_count = con.execute("SELECT COUNT(*) FROM unassigned_filtered_pairs").fetchone()[0]
    if pair_count == 0:
        print("No pairs to cluster.")
        con.execute("CREATE OR REPLACE TABLE unassigned_clusters AS SELECT NULL::VARCHAR AS record_id, NULL::VARCHAR AS cluster_id WHERE 1=0")
        return 0

    con.execute("""
    CREATE OR REPLACE TABLE unassigned_clusters AS
    WITH RECURSIVE connected_components AS (
        SELECT
            record_id_1 AS node,
            record_id_1 AS cluster_id
        FROM unassigned_filtered_pairs
        UNION
        SELECT
            record_id_2 AS node,
            record_id_2 AS cluster_id
        FROM unassigned_filtered_pairs
        UNION
        SELECT
            fp.record_id_2 AS node,
            LEAST(cc.cluster_id, fp.record_id_2) AS cluster_id
        FROM connected_components cc
        JOIN unassigned_filtered_pairs fp ON cc.node = fp.record_id_1
        WHERE cc.cluster_id <> LEAST(cc.cluster_id, fp.record_id_2)
        UNION
        SELECT
            fp.record_id_1 AS node,
            LEAST(cc.cluster_id, fp.record_id_1) AS cluster_id
        FROM connected_components cc
        JOIN unassigned_filtered_pairs fp ON cc.node = fp.record_id_2
        WHERE cc.cluster_id <> LEAST(cc.cluster_id, fp.record_id_1)
    )
    SELECT
        node AS record_id,
        MIN(cluster_id) AS cluster_id
    FROM connected_components
    GROUP BY node
    ORDER BY cluster_id, record_id
    """)

    count = con.execute("SELECT COUNT(DISTINCT cluster_id) FROM unassigned_clusters").fetchone()[0]
    print(f"Created {count} new clusters.")
    return count


def generate_new_cluster_ids(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """Generate new cluster IDs that don't conflict with existing ones."""
    print("Generating new cluster IDs...")

    con.execute(f"""
    CREATE OR REPLACE TABLE new_clusters AS
    WITH max_existing AS (
        SELECT COALESCE(MAX(cluster_id), '0') AS max_id FROM {config.prime_table}
    ),
    cluster_mapping AS (
        SELECT DISTINCT
            cluster_id AS old_cluster_id,
            'NEW_' || ROW_NUMBER() OVER (ORDER BY cluster_id) AS new_cluster_id
        FROM unassigned_clusters
    )
    SELECT
        uc.record_id,
        cm.new_cluster_id AS cluster_id
    FROM unassigned_clusters uc
    JOIN cluster_mapping cm ON uc.cluster_id = cm.old_cluster_id
    """)


def update_prime_table(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Update the prime table with newly clustered records."""
    print("Updating prime table with new clusters...")

    # Count new clusters
    new_count = con.execute("SELECT COUNT(*) FROM new_clusters").fetchone()[0]

    if new_count > 0:
        # Update existing records in prime table
        con.execute(f"""
        UPDATE {config.prime_table}
        SET cluster_id = nc.cluster_id
        FROM new_clusters nc
        WHERE {config.prime_table}.{config.id_field} = nc.record_id
        """)

    print(f"Updated {new_count} records with new cluster assignments.")
    return new_count


def get_cleanup_stats(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """Get summary statistics for the cleanup process."""
    unassigned = con.execute(f"SELECT COUNT(*) FROM {config.unassigned_table}").fetchone()[0]
    newly_clustered = con.execute("SELECT COUNT(*) FROM new_clusters").fetchone()[0]
    new_clusters = con.execute("SELECT COUNT(DISTINCT cluster_id) FROM new_clusters").fetchone()[0]

    return {
        "total_unassigned": unassigned,
        "newly_clustered": newly_clustered,
        "new_clusters_created": new_clusters,
        "still_unassigned": unassigned - newly_clustered
    }


def run_step_3(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """
    Run the complete Step 3: Unassigned Records Cleanup.

    Args:
        con: DuckDB connection
        config: Configuration object

    Returns:
        Dictionary with summary statistics
    """
    print("\n" + "=" * 60)
    print("STEP 3: UNASSIGNED RECORDS CLEANUP")
    print("=" * 60 + "\n")

    unassigned_count = identify_unassigned(con, config)

    if unassigned_count == 0:
        print("No unassigned records to process.")
        return {
            "total_unassigned": 0,
            "newly_clustered": 0,
            "new_clusters_created": 0,
            "still_unassigned": 0
        }

    create_unassigned_blocking_keys(con, config)
    calculate_unassigned_weights(con, config)
    find_unassigned_pairs(con, config)
    filter_unassigned_pairs(con, config)
    identify_unassigned_components(con, config)
    generate_new_cluster_ids(con, config)
    update_prime_table(con, config)

    stats = get_cleanup_stats(con, config)

    print("\n" + "-" * 40)
    print("SUMMARY:")
    print(f"  Total unassigned: {stats['total_unassigned']}")
    print(f"  Newly clustered: {stats['newly_clustered']}")
    print(f"  New clusters created: {stats['new_clusters_created']}")
    print(f"  Still unassigned: {stats['still_unassigned']}")
    print("-" * 40 + "\n")

    return stats
