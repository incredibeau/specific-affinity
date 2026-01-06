"""
Step 4: Record Categorization (Optional)

This module categorizes records based on patterns in numeric and date fields.
Useful for transaction data to identify subscriptions, recurring, and one-time items.
"""

import duckdb
from typing import Dict, Any, Optional
from .config import Config


def create_cluster_records_view(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """Create a view of records organized by cluster."""
    print("Creating cluster records view...")

    group_field = config.group_field or f"'default'"

    con.execute(f"""
    CREATE OR REPLACE VIEW cluster_records AS
    SELECT
        cluster_id,
        {config.id_field} AS record_id,
        {config.amount_field} AS amount,
        {config.date_field} AS record_date,
        {config.text_field} AS text_value,
        {group_field} AS group_id
    FROM {config.prime_table}
    WHERE cluster_id IS NOT NULL
    ORDER BY cluster_id, {group_field}, {config.date_field}
    """)


def analyze_cluster_metadata(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Analyze patterns within each cluster."""
    print("Analyzing cluster patterns...")

    con.execute("""
    CREATE OR REPLACE TABLE cluster_metadata AS
    WITH cluster_stats AS (
        SELECT
            cluster_id,
            COUNT(*) AS record_count,
            COUNT(DISTINCT group_id) AS group_count,
            MIN(record_date) AS first_date,
            MAX(record_date) AS last_date,
            MIN(amount) AS min_amount,
            MAX(amount) AS max_amount,
            AVG(amount) AS avg_amount,
            STDDEV(amount) AS stddev_amount
        FROM cluster_records
        GROUP BY cluster_id
    ),
    date_frequency AS (
        SELECT
            cluster_id,
            group_id,
            record_date,
            LAG(record_date) OVER (PARTITION BY cluster_id, group_id ORDER BY record_date) AS prev_date,
            DATEDIFF('day',
                LAG(record_date) OVER (PARTITION BY cluster_id, group_id ORDER BY record_date),
                record_date
            ) AS days_between
        FROM cluster_records
    ),
    frequency_stats AS (
        SELECT
            cluster_id,
            AVG(days_between) AS avg_days_between,
            STDDEV(days_between) AS stddev_days_between,
            MODE() WITHIN GROUP (ORDER BY days_between) AS most_common_frequency
        FROM date_frequency
        WHERE days_between IS NOT NULL AND days_between > 0
        GROUP BY cluster_id
    )
    SELECT
        cs.*,
        fs.avg_days_between,
        fs.stddev_days_between,
        fs.most_common_frequency,
        CASE
            WHEN cs.stddev_amount IS NULL OR cs.avg_amount = 0 THEN 0
            ELSE cs.stddev_amount / cs.avg_amount
        END AS amount_cv,
        CASE
            WHEN fs.stddev_days_between IS NULL OR fs.avg_days_between = 0 THEN 0
            ELSE fs.stddev_days_between / fs.avg_days_between
        END AS date_cv
    FROM cluster_stats cs
    LEFT JOIN frequency_stats fs ON cs.cluster_id = fs.cluster_id
    """)

    count = con.execute("SELECT COUNT(*) FROM cluster_metadata").fetchone()[0]
    print(f"Analyzed {count} clusters.")
    return count


def determine_cluster_types(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """Determine the type of each cluster based on patterns."""
    print("Determining cluster types...")

    con.execute("""
    CREATE OR REPLACE TABLE cluster_types AS
    SELECT
        cluster_id,
        record_count,
        group_count,
        avg_amount,
        most_common_frequency,
        amount_cv,
        date_cv,
        CASE
            -- Subscription: Regular frequency AND consistent amounts
            WHEN most_common_frequency BETWEEN 7 AND 35
                AND (amount_cv < 0.1 OR record_count < 3)
                AND date_cv < 0.3
            THEN 'subscription'

            -- Recurring: Regular frequency BUT varying amounts
            WHEN most_common_frequency BETWEEN 7 AND 35
                AND date_cv < 0.3
            THEN 'recurring'

            -- One-time: No regular pattern
            ELSE 'one-time'
        END AS cluster_type
    FROM cluster_metadata
    """)


def categorize_individual_records(con: duckdb.DuckDBPyConnection, config: Config) -> int:
    """Categorize individual records based on cluster patterns."""
    print("Categorizing individual records...")

    con.execute(f"""
    CREATE OR REPLACE TABLE categorized_records AS
    WITH record_analysis AS (
        SELECT
            cr.record_id,
            cr.cluster_id,
            cr.group_id,
            cr.amount,
            cr.record_date,
            ct.cluster_type AS default_type,
            ct.most_common_frequency,
            LAG(cr.record_date) OVER (
                PARTITION BY cr.cluster_id, cr.group_id
                ORDER BY cr.record_date
            ) AS prev_date,
            LAG(cr.amount) OVER (
                PARTITION BY cr.cluster_id, cr.group_id
                ORDER BY cr.record_date
            ) AS prev_amount,
            DATEDIFF('day',
                LAG(cr.record_date) OVER (
                    PARTITION BY cr.cluster_id, cr.group_id
                    ORDER BY cr.record_date
                ),
                cr.record_date
            ) AS days_between
        FROM cluster_records cr
        JOIN cluster_types ct ON cr.cluster_id = ct.cluster_id
    )
    SELECT
        record_id,
        cluster_id,
        group_id,
        amount,
        record_date,
        days_between,
        CASE
            -- First record: use cluster default
            WHEN prev_date IS NULL THEN default_type

            -- Subscription: regular timing + same amount
            WHEN days_between IS NOT NULL
                AND ABS(days_between - most_common_frequency) <= {config.date_threshold_days}
                AND (
                    amount = prev_amount
                    OR ABS(amount - prev_amount) * 100.0 / NULLIF(ABS(prev_amount), 0) <= {config.amount_threshold_pct}
                )
            THEN 'subscription'

            -- Recurring: regular timing + different amount
            WHEN days_between IS NOT NULL
                AND ABS(days_between - most_common_frequency) <= {config.date_threshold_days}
            THEN 'recurring'

            -- One-time: irregular
            ELSE 'one-time'
        END AS record_type
    FROM record_analysis
    """)

    count = con.execute("SELECT COUNT(*) FROM categorized_records").fetchone()[0]
    print(f"Categorized {count} records.")
    return count


def create_categorized_prime_table(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """Create prime table with categorization."""
    print("Creating categorized prime table...")

    con.execute(f"""
    CREATE OR REPLACE TABLE {config.prime_table}_categorized AS
    SELECT
        pt.*,
        cr.record_type,
        ct.cluster_type
    FROM {config.prime_table} pt
    LEFT JOIN categorized_records cr ON pt.{config.id_field} = cr.record_id
    LEFT JOIN cluster_types ct ON pt.cluster_id = ct.cluster_id
    """)


def get_categorization_stats(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get summary statistics for categorization."""
    record_stats = con.execute("""
    SELECT
        record_type,
        COUNT(*) AS count
    FROM categorized_records
    GROUP BY record_type
    ORDER BY count DESC
    """).fetchall()

    cluster_stats = con.execute("""
    SELECT
        cluster_type,
        COUNT(*) AS cluster_count,
        SUM(record_count) AS total_records
    FROM cluster_types
    JOIN cluster_metadata USING (cluster_id)
    GROUP BY cluster_type
    """).fetchall()

    return {
        "record_types": {r[0]: r[1] for r in record_stats},
        "cluster_types": {c[0]: {"clusters": c[1], "records": c[2]} for c in cluster_stats}
    }


def run_step_4(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """
    Run the complete Step 4: Record Categorization.

    Args:
        con: DuckDB connection
        config: Configuration object (must have amount_field and date_field set)

    Returns:
        Dictionary with summary statistics
    """
    print("\n" + "=" * 60)
    print("STEP 4: RECORD CATEGORIZATION")
    print("=" * 60 + "\n")

    # Check required fields
    if not config.amount_field or not config.date_field:
        print("WARNING: amount_field and date_field are required for categorization.")
        print("Skipping categorization step.")
        return {"skipped": True, "reason": "Missing required fields"}

    create_cluster_records_view(con, config)
    analyze_cluster_metadata(con, config)
    determine_cluster_types(con, config)
    categorize_individual_records(con, config)
    create_categorized_prime_table(con, config)

    stats = get_categorization_stats(con)

    print("\n" + "-" * 40)
    print("SUMMARY:")
    print("\nRecord Types:")
    for rtype, count in stats["record_types"].items():
        print(f"  {rtype}: {count}")
    print("\nCluster Types:")
    for ctype, data in stats["cluster_types"].items():
        print(f"  {ctype}: {data['clusters']} clusters, {data['records']} records")
    print("-" * 40 + "\n")

    return stats
