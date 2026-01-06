"""
Step 5: Quality Assurance

This module provides validation and analysis tools to ensure the quality
of cluster assignments and identify potential issues.
"""

import duckdb
from typing import Dict, Any, List
from .config import Config


def analyze_similarity_distribution(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Analyze the distribution of similarity scores."""
    print("Analyzing similarity score distribution...")

    stats = con.execute("""
    SELECT
        MIN(similarity_score) AS min_score,
        MAX(similarity_score) AS max_score,
        ROUND(AVG(similarity_score), 4) AS avg_score,
        ROUND(STDDEV(similarity_score), 4) AS stddev_score,
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY similarity_score), 4) AS q1,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY similarity_score), 4) AS median,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY similarity_score), 4) AS q3,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY similarity_score), 4) AS p95
    FROM filtered_pairs
    """).fetchone()

    histogram = con.execute("""
    SELECT
        FLOOR(similarity_score * 10) / 10 AS bucket,
        COUNT(*) AS count
    FROM filtered_pairs
    GROUP BY FLOOR(similarity_score * 10) / 10
    ORDER BY bucket
    """).fetchall()

    return {
        "statistics": {
            "min": stats[0],
            "max": stats[1],
            "avg": stats[2],
            "stddev": stats[3],
            "q1": stats[4],
            "median": stats[5],
            "q3": stats[6],
            "p95": stats[7]
        },
        "histogram": {f"{h[0]:.1f}": h[1] for h in histogram}
    }


def analyze_cluster_sizes(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """Analyze the distribution of cluster sizes."""
    print("Analyzing cluster size distribution...")

    distribution = con.execute(f"""
    SELECT
        CASE
            WHEN cluster_size = 1 THEN 'Singleton (1)'
            WHEN cluster_size = 2 THEN 'Pair (2)'
            WHEN cluster_size BETWEEN 3 AND 5 THEN 'Small (3-5)'
            WHEN cluster_size BETWEEN 6 AND 10 THEN 'Medium (6-10)'
            WHEN cluster_size BETWEEN 11 AND 50 THEN 'Large (11-50)'
            ELSE 'Very Large (50+)'
        END AS size_category,
        COUNT(*) AS cluster_count,
        SUM(cluster_size) AS total_records
    FROM (
        SELECT cluster_id, COUNT(*) AS cluster_size
        FROM {config.prime_table}
        WHERE cluster_id IS NOT NULL
        GROUP BY cluster_id
    ) t
    GROUP BY size_category
    ORDER BY MIN(cluster_size)
    """).fetchall()

    return {
        cat: {"clusters": count, "records": records}
        for cat, count, records in distribution
    }


def check_cluster_consistency(con: duckdb.DuckDBPyConnection, config: Config) -> List[Dict[str, Any]]:
    """Find clusters with high text variation (potential false positives)."""
    print("Checking cluster consistency...")

    issues = con.execute(f"""
    WITH cluster_text_stats AS (
        SELECT
            cluster_id,
            COUNT(*) AS record_count,
            COUNT(DISTINCT {config.text_field}) AS unique_texts,
            MIN(LENGTH({config.text_field})) AS min_length,
            MAX(LENGTH({config.text_field})) AS max_length
        FROM {config.prime_table}
        WHERE cluster_id IS NOT NULL
        GROUP BY cluster_id
    )
    SELECT
        cluster_id,
        record_count,
        unique_texts,
        ROUND(unique_texts * 100.0 / record_count, 2) AS diversity_pct,
        max_length - min_length AS length_variance
    FROM cluster_text_stats
    WHERE unique_texts > 1
    ORDER BY diversity_pct DESC
    LIMIT 20
    """).fetchall()

    return [
        {
            "cluster_id": row[0],
            "record_count": row[1],
            "unique_texts": row[2],
            "diversity_pct": row[3],
            "length_variance": row[4]
        }
        for row in issues
    ]


def analyze_token_weights(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """Analyze token weights to identify potential stop words."""
    print("Analyzing token weights...")

    # Most distinctive (highest weight)
    distinctive = con.execute(f"""
    SELECT
        token,
        weight,
        (SELECT COUNT(*) FROM {config.blocking_keys_table} WHERE token = tw.token) AS frequency
    FROM {config.weights_table} tw
    ORDER BY weight DESC
    LIMIT 15
    """).fetchall()

    # Most common (lowest weight) - potential stop words
    common = con.execute(f"""
    SELECT
        token,
        weight,
        (SELECT COUNT(*) FROM {config.blocking_keys_table} WHERE token = tw.token) AS frequency
    FROM {config.weights_table} tw
    ORDER BY weight ASC
    LIMIT 15
    """).fetchall()

    # Candidates for stop words (very high frequency)
    stop_word_candidates = con.execute(f"""
    SELECT
        token,
        COUNT(*) AS frequency,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT record_id) FROM {config.blocking_keys_table}), 2) AS pct
    FROM {config.blocking_keys_table}
    GROUP BY token
    HAVING COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT record_id) FROM {config.blocking_keys_table}) > 10
    ORDER BY frequency DESC
    """).fetchall()

    return {
        "most_distinctive": [{"token": t, "weight": w, "frequency": f} for t, w, f in distinctive],
        "most_common": [{"token": t, "weight": w, "frequency": f} for t, w, f in common],
        "stop_word_candidates": [{"token": t, "frequency": f, "pct": p} for t, f, p in stop_word_candidates]
    }


def analyze_unclustered(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """Analyze records that weren't clustered."""
    print("Analyzing unclustered records...")

    stats = con.execute(f"""
    SELECT
        COUNT(*) AS unclustered_count,
        (SELECT COUNT(*) FROM {config.prime_table}) AS total_count
    FROM {config.prime_table}
    WHERE cluster_id IS NULL
    """).fetchone()

    samples = con.execute(f"""
    SELECT
        {config.id_field},
        {config.text_field},
        LENGTH({config.text_field}) AS text_length
    FROM {config.prime_table}
    WHERE cluster_id IS NULL
    ORDER BY LENGTH({config.text_field}) DESC
    LIMIT 10
    """).fetchall()

    return {
        "unclustered_count": stats[0],
        "total_count": stats[1],
        "unclustered_pct": round(stats[0] * 100.0 / stats[1], 2) if stats[1] > 0 else 0,
        "samples": [
            {"id": s[0], "text": s[1], "length": s[2]}
            for s in samples
        ]
    }


def find_near_threshold_pairs(con: duckdb.DuckDBPyConnection, config: Config) -> List[Dict[str, Any]]:
    """Find pairs just below threshold that might be matches."""
    print("Finding near-threshold pairs...")

    threshold = config.similarity_threshold

    pairs = con.execute(f"""
    SELECT
        cp.record_id_1,
        cp.record_id_2,
        cp.similarity_score,
        pt1.{config.text_field} AS text_1,
        pt2.{config.text_field} AS text_2
    FROM candidate_pairs cp
    JOIN {config.prime_table} pt1 ON cp.record_id_1 = pt1.{config.id_field}
    JOIN {config.prime_table} pt2 ON cp.record_id_2 = pt2.{config.id_field}
    WHERE
        cp.similarity_score < {threshold}
        AND cp.similarity_score >= {threshold * 0.8}
    ORDER BY cp.similarity_score DESC
    LIMIT 15
    """).fetchall()

    return [
        {
            "record_id_1": p[0],
            "record_id_2": p[1],
            "score": p[2],
            "text_1": p[3],
            "text_2": p[4]
        }
        for p in pairs
    ]


def get_overall_metrics(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """Get overall quality metrics."""
    print("Calculating overall metrics...")

    metrics = {}

    metrics["total_records"] = con.execute(f"SELECT COUNT(*) FROM {config.prime_table}").fetchone()[0]
    metrics["clustered_records"] = con.execute(f"SELECT COUNT(*) FROM {config.prime_table} WHERE cluster_id IS NOT NULL").fetchone()[0]
    metrics["total_clusters"] = con.execute(f"SELECT COUNT(DISTINCT cluster_id) FROM {config.prime_table}").fetchone()[0]

    avg_size = con.execute(f"""
    SELECT ROUND(AVG(cluster_size), 2)
    FROM (
        SELECT cluster_id, COUNT(*) AS cluster_size
        FROM {config.prime_table} WHERE cluster_id IS NOT NULL
        GROUP BY cluster_id
    ) t
    """).fetchone()[0]
    metrics["avg_cluster_size"] = avg_size

    metrics["total_pairs_evaluated"] = con.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]
    metrics["pairs_above_threshold"] = con.execute("SELECT COUNT(*) FROM filtered_pairs").fetchone()[0]
    metrics["unique_tokens"] = con.execute(f"SELECT COUNT(*) FROM {config.weights_table}").fetchone()[0]

    return metrics


def run_step_5(con: duckdb.DuckDBPyConnection, config: Config) -> Dict[str, Any]:
    """
    Run the complete Step 5: Quality Assurance.

    Args:
        con: DuckDB connection
        config: Configuration object

    Returns:
        Dictionary with all QA results
    """
    print("\n" + "=" * 60)
    print("STEP 5: QUALITY ASSURANCE")
    print("=" * 60 + "\n")

    results = {}

    results["overall_metrics"] = get_overall_metrics(con, config)
    results["similarity_distribution"] = analyze_similarity_distribution(con)
    results["cluster_sizes"] = analyze_cluster_sizes(con, config)
    results["consistency_issues"] = check_cluster_consistency(con, config)
    results["token_analysis"] = analyze_token_weights(con, config)
    results["unclustered_analysis"] = analyze_unclustered(con, config)
    results["near_threshold_pairs"] = find_near_threshold_pairs(con, config)

    print("\n" + "-" * 40)
    print("SUMMARY:")
    print(f"\nOverall Metrics:")
    for key, value in results["overall_metrics"].items():
        print(f"  {key}: {value}")

    print(f"\nSimilarity Score Stats:")
    stats = results["similarity_distribution"]["statistics"]
    print(f"  Range: {stats['min']} - {stats['max']}")
    print(f"  Median: {stats['median']}, Avg: {stats['avg']}")

    print(f"\nUnclustered Records:")
    unclustered = results["unclustered_analysis"]
    print(f"  {unclustered['unclustered_count']} / {unclustered['total_count']} ({unclustered['unclustered_pct']}%)")

    print(f"\nPotential Issues:")
    print(f"  Clusters with high diversity: {len(results['consistency_issues'])}")
    print(f"  Stop word candidates: {len(results['token_analysis']['stop_word_candidates'])}")
    print(f"  Near-threshold pairs: {len(results['near_threshold_pairs'])}")
    print("-" * 40 + "\n")

    return results


def print_detailed_report(results: Dict[str, Any]) -> None:
    """Print a detailed QA report."""
    print("\n" + "=" * 60)
    print("DETAILED QA REPORT")
    print("=" * 60)

    print("\n--- SIMILARITY SCORE HISTOGRAM ---")
    histogram = results["similarity_distribution"]["histogram"]
    max_count = max(histogram.values()) if histogram else 1
    for bucket, count in sorted(histogram.items()):
        bar = "*" * int(count * 40 / max_count)
        print(f"  {bucket}: {count:6d} {bar}")

    print("\n--- CLUSTER SIZE DISTRIBUTION ---")
    for category, data in results["cluster_sizes"].items():
        print(f"  {category}: {data['clusters']} clusters, {data['records']} records")

    print("\n--- POTENTIAL STOP WORDS ---")
    for token in results["token_analysis"]["stop_word_candidates"][:10]:
        print(f"  '{token['token']}': {token['frequency']} occurrences ({token['pct']}%)")

    if results["consistency_issues"]:
        print("\n--- HIGH DIVERSITY CLUSTERS (Review Needed) ---")
        for issue in results["consistency_issues"][:5]:
            print(f"  Cluster {issue['cluster_id']}: {issue['unique_texts']}/{issue['record_count']} unique ({issue['diversity_pct']}%)")

    if results["near_threshold_pairs"]:
        print("\n--- NEAR-THRESHOLD PAIRS (Potential Matches) ---")
        for pair in results["near_threshold_pairs"][:5]:
            print(f"  Score {pair['score']:.4f}:")
            print(f"    '{pair['text_1'][:50]}...'")
            print(f"    '{pair['text_2'][:50]}...'")
