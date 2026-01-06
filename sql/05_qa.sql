-- =============================================================================
-- Step 5: Quality Assurance
-- =============================================================================
-- This script provides validation queries and analysis tools to ensure
-- the quality of cluster assignments and identify potential issues.
--
-- Prerequisites:
--   - Prime Table created with cluster assignments
--   - All previous steps completed
--
-- Configuration (replace these placeholders):
--   - {{ID_FIELD}}: Unique identifier column name
--   - {{TEXT_FIELD}}: Text column to use for matching
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 5.1 Similarity Score Distribution Analysis
-- -----------------------------------------------------------------------------

-- Distribution statistics
SELECT
    'Filtered Pairs' AS source,
    MIN(similarity_score) AS min_score,
    MAX(similarity_score) AS max_score,
    ROUND(AVG(similarity_score), 4) AS avg_score,
    ROUND(STDDEV(similarity_score), 4) AS stddev_score,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY similarity_score), 4) AS q1,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY similarity_score), 4) AS median,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY similarity_score), 4) AS q3,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY similarity_score), 4) AS p95
FROM filtered_pairs;

-- Histogram of similarity scores
SELECT
    FLOOR(similarity_score * 10) / 10 AS score_bucket,
    COUNT(*) AS pair_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    REPEAT('*', (COUNT(*) * 50 / MAX(COUNT(*)) OVER ())::INT) AS histogram
FROM filtered_pairs
GROUP BY FLOOR(similarity_score * 10) / 10
ORDER BY score_bucket;

-- -----------------------------------------------------------------------------
-- 5.2 Cluster Size Distribution
-- -----------------------------------------------------------------------------
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
    SUM(cluster_size) AS total_records,
    ROUND(AVG(cluster_size), 2) AS avg_size
FROM (
    SELECT
        cluster_id,
        COUNT(*) AS cluster_size
    FROM prime_table
    WHERE cluster_id IS NOT NULL
    GROUP BY cluster_id
) t
GROUP BY size_category
ORDER BY MIN(cluster_size);

-- -----------------------------------------------------------------------------
-- 5.3 Cluster Consistency Check
-- -----------------------------------------------------------------------------

-- Find clusters with high text variation (potential false positives)
WITH cluster_text_stats AS (
    SELECT
        cluster_id,
        COUNT(*) AS record_count,
        COUNT(DISTINCT {{TEXT_FIELD}}) AS unique_texts,
        MIN(LENGTH({{TEXT_FIELD}})) AS min_text_length,
        MAX(LENGTH({{TEXT_FIELD}})) AS max_text_length,
        MAX(LENGTH({{TEXT_FIELD}})) - MIN(LENGTH({{TEXT_FIELD}})) AS length_variance
    FROM prime_table
    WHERE cluster_id IS NOT NULL
    GROUP BY cluster_id
)
SELECT
    cluster_id,
    record_count,
    unique_texts,
    ROUND(unique_texts * 100.0 / record_count, 2) AS text_diversity_pct,
    length_variance
FROM cluster_text_stats
WHERE unique_texts > 1
ORDER BY text_diversity_pct DESC
LIMIT 20;

-- Sample records from clusters with high diversity
WITH diverse_clusters AS (
    SELECT cluster_id
    FROM (
        SELECT
            cluster_id,
            COUNT(*) AS record_count,
            COUNT(DISTINCT {{TEXT_FIELD}}) AS unique_texts
        FROM prime_table
        WHERE cluster_id IS NOT NULL
        GROUP BY cluster_id
        HAVING COUNT(DISTINCT {{TEXT_FIELD}}) > 1
    ) t
    ORDER BY unique_texts * 1.0 / record_count DESC
    LIMIT 5
)
SELECT
    pt.cluster_id,
    pt.{{ID_FIELD}},
    pt.{{TEXT_FIELD}}
FROM prime_table pt
JOIN diverse_clusters dc ON pt.cluster_id = dc.cluster_id
ORDER BY pt.cluster_id, pt.{{TEXT_FIELD}};

-- -----------------------------------------------------------------------------
-- 5.4 Token Weight Analysis
-- -----------------------------------------------------------------------------

-- Most distinctive tokens (highest weights)
SELECT
    token,
    weight,
    (SELECT COUNT(*) FROM blocking_keys WHERE token = tw.token) AS frequency
FROM token_weights tw
ORDER BY weight DESC
LIMIT 20;

-- Most common tokens (lowest weights)
SELECT
    token,
    weight,
    (SELECT COUNT(*) FROM blocking_keys WHERE token = tw.token) AS frequency
FROM token_weights tw
ORDER BY weight ASC
LIMIT 20;

-- Tokens that might need to be added to stop words
SELECT
    token,
    COUNT(*) AS frequency,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT record_id) FROM blocking_keys), 2) AS pct_of_records
FROM blocking_keys
GROUP BY token
HAVING COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT record_id) FROM blocking_keys) > 10
ORDER BY frequency DESC;

-- -----------------------------------------------------------------------------
-- 5.5 Unclustered Records Analysis
-- -----------------------------------------------------------------------------

-- Records that didn't get clustered
SELECT
    COUNT(*) AS unclustered_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM prime_table), 2) AS pct_of_total
FROM prime_table
WHERE cluster_id IS NULL;

-- Sample unclustered records
SELECT
    {{ID_FIELD}},
    {{TEXT_FIELD}},
    LENGTH({{TEXT_FIELD}}) AS text_length
FROM prime_table
WHERE cluster_id IS NULL
ORDER BY LENGTH({{TEXT_FIELD}}) DESC
LIMIT 20;

-- Token coverage for unclustered records
WITH unclustered_tokens AS (
    SELECT DISTINCT token
    FROM blocking_keys bk
    JOIN prime_table pt ON bk.record_id = pt.{{ID_FIELD}}
    WHERE pt.cluster_id IS NULL
)
SELECT
    COUNT(*) AS unique_tokens,
    (SELECT COUNT(*) FROM unclustered_tokens WHERE token IN (SELECT token FROM token_weights)) AS tokens_with_weights
FROM unclustered_tokens;

-- -----------------------------------------------------------------------------
-- 5.6 Potential Missed Matches (False Negatives)
-- -----------------------------------------------------------------------------

-- Find pairs just below threshold that might be matches
SELECT
    record_id_1,
    record_id_2,
    similarity_score,
    pt1.{{TEXT_FIELD}} AS text_1,
    pt2.{{TEXT_FIELD}} AS text_2
FROM candidate_pairs cp
JOIN prime_table pt1 ON cp.record_id_1 = pt1.{{ID_FIELD}}
JOIN prime_table pt2 ON cp.record_id_2 = pt2.{{ID_FIELD}}
WHERE
    similarity_score < (SELECT MIN(similarity_score) FROM filtered_pairs)
    AND similarity_score > (SELECT MIN(similarity_score) FROM filtered_pairs) * 0.8
ORDER BY similarity_score DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 5.7 Overall Quality Metrics
-- -----------------------------------------------------------------------------
SELECT
    'Total Records' AS metric,
    COUNT(*)::VARCHAR AS value
FROM prime_table
UNION ALL
SELECT
    'Clustered Records',
    COUNT(*)::VARCHAR
FROM prime_table WHERE cluster_id IS NOT NULL
UNION ALL
SELECT
    'Total Clusters',
    COUNT(DISTINCT cluster_id)::VARCHAR
FROM prime_table
UNION ALL
SELECT
    'Avg Cluster Size',
    ROUND(AVG(cluster_size), 2)::VARCHAR
FROM (
    SELECT cluster_id, COUNT(*) AS cluster_size
    FROM prime_table WHERE cluster_id IS NOT NULL
    GROUP BY cluster_id
) t
UNION ALL
SELECT
    'Total Pairs Evaluated',
    COUNT(*)::VARCHAR
FROM candidate_pairs
UNION ALL
SELECT
    'Pairs Above Threshold',
    COUNT(*)::VARCHAR
FROM filtered_pairs
UNION ALL
SELECT
    'Unique Tokens',
    COUNT(*)::VARCHAR
FROM token_weights;
