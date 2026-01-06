-- =============================================================================
-- Step 3: Unassigned Records Cleanup
-- =============================================================================
-- This script handles records that couldn't be matched during inference.
-- It applies the same clustering methodology to find matches among the
-- unassigned records themselves, creating new clusters where appropriate.
--
-- Prerequisites:
--   - Prime Table created (Step 1)
--   - Inference completed (Step 2)
--   - inference_results table exists
--
-- Configuration (replace these placeholders):
--   - {{ID_FIELD}}: Unique identifier column name
--   - {{TEXT_FIELD}}: Text column to use for matching
--   - {{SIMILARITY_THRESHOLD}}: Minimum score for matches (e.g., 0.5)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1 Identify Unassigned Records
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE unassigned_records AS
SELECT *
FROM inference_results
WHERE match_status = 'unmatched';

-- Get count of unassigned records
SELECT COUNT(*) AS unassigned_count FROM unassigned_records;

-- -----------------------------------------------------------------------------
-- 3.2 Create Blocking Keys for Unassigned Records
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE unassigned_blocking_keys AS
WITH normalized AS (
    SELECT
        {{ID_FIELD}} AS record_id,
        regexp_replace(LOWER({{TEXT_FIELD}}), '[^a-zA-Z0-9\s]', ' ', 'g') AS normalized_text
    FROM unassigned_records
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
    AND LENGTH(token) >= 2
    AND token NOT IN (SELECT word FROM stop_words);

-- -----------------------------------------------------------------------------
-- 3.3 Calculate Weights for Unassigned Records
-- -----------------------------------------------------------------------------
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
WHERE normalized_weight IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 3.4 Self-Join Unassigned Records
-- -----------------------------------------------------------------------------
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
    bk1.record_id, bk2.record_id;

-- -----------------------------------------------------------------------------
-- 3.5 Filter by Threshold
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE unassigned_filtered_pairs AS
SELECT
    record_id_1,
    record_id_2,
    similarity_score
FROM unassigned_candidate_pairs
WHERE similarity_score >= {{SIMILARITY_THRESHOLD}};

-- -----------------------------------------------------------------------------
-- 3.6 Identify Connected Components for Unassigned
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE unassigned_clusters AS
WITH RECURSIVE connected_components AS (
    -- Base case
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
    -- Recursive propagation
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
ORDER BY cluster_id, record_id;

-- -----------------------------------------------------------------------------
-- 3.7 Generate New Cluster IDs (Avoid Conflicts)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE new_clusters AS
WITH max_existing AS (
    SELECT COALESCE(MAX(cluster_id), 0) AS max_id FROM prime_table
),
cluster_mapping AS (
    SELECT DISTINCT
        cluster_id AS old_cluster_id,
        ROW_NUMBER() OVER (ORDER BY cluster_id) + (SELECT max_id FROM max_existing) AS new_cluster_id
    FROM unassigned_clusters
)
SELECT
    uc.record_id,
    cm.new_cluster_id AS cluster_id
FROM unassigned_clusters uc
JOIN cluster_mapping cm ON uc.cluster_id = cm.old_cluster_id;

-- -----------------------------------------------------------------------------
-- 3.8 Update Prime Table with New Clusters
-- -----------------------------------------------------------------------------
-- Option A: Create updated prime table
CREATE OR REPLACE TABLE prime_table_updated AS
SELECT
    pt.*,
    COALESCE(pt.cluster_id, nc.cluster_id) AS updated_cluster_id
FROM prime_table pt
LEFT JOIN new_clusters nc ON pt.{{ID_FIELD}} = nc.record_id;

-- Option B: Insert new records with clusters
INSERT INTO prime_table
SELECT
    ur.*,
    nc.cluster_id
FROM unassigned_records ur
JOIN new_clusters nc ON ur.{{ID_FIELD}} = nc.record_id
WHERE ur.{{ID_FIELD}} NOT IN (SELECT {{ID_FIELD}} FROM prime_table);

-- -----------------------------------------------------------------------------
-- 3.9 Summary Statistics
-- -----------------------------------------------------------------------------
SELECT
    (SELECT COUNT(*) FROM unassigned_records) AS total_unassigned,
    COUNT(DISTINCT cluster_id) AS new_clusters_created,
    COUNT(*) AS records_newly_clustered,
    ROUND(AVG(cluster_size), 2) AS avg_new_cluster_size
FROM (
    SELECT
        cluster_id,
        COUNT(*) OVER (PARTITION BY cluster_id) AS cluster_size
    FROM new_clusters
) t;
