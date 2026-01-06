-- =============================================================================
-- Step 2: Make Inference
-- =============================================================================
-- This script matches new records against the existing Prime Table to identify
-- their cluster assignment. It uses the same tokenization and weighting as
-- the Prime Table creation.
--
-- Prerequisites:
--   - Prime Table created (Step 1)
--   - blocking_keys table exists
--   - token_weights table exists
--   - A new_records table with records to match
--
-- Configuration (replace these placeholders):
--   - {{NEW_RECORDS_TABLE}}: Table containing new records to match
--   - {{ID_FIELD}}: Unique identifier column name
--   - {{TEXT_FIELD}}: Text column to use for matching
--   - {{SIMILARITY_THRESHOLD}}: Minimum score for matches (e.g., 0.5)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 Tokenize New Records
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE new_record_tokens AS
WITH normalized AS (
    SELECT
        {{ID_FIELD}} AS record_id,
        regexp_replace(LOWER({{TEXT_FIELD}}), '[^a-zA-Z0-9\s]', ' ', 'g') AS normalized_text
    FROM {{NEW_RECORDS_TABLE}}
    WHERE {{TEXT_FIELD}} IS NOT NULL AND LENGTH(TRIM({{TEXT_FIELD}})) > 0
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
-- 2.2 Find Candidate Matches Against Prime Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE inference_candidates AS
SELECT
    nrt.record_id AS query_id,
    bk.record_id AS prime_record_id,
    pt.cluster_id AS prime_cluster_id,
    SUM(tw.weight) AS similarity_score
FROM
    new_record_tokens nrt
JOIN
    blocking_keys bk ON nrt.token = bk.token
JOIN
    token_weights tw ON nrt.token = tw.token
JOIN
    prime_table pt ON bk.record_id = pt.{{ID_FIELD}}
WHERE
    pt.cluster_id IS NOT NULL  -- Only match against clustered records
GROUP BY
    nrt.record_id, bk.record_id, pt.cluster_id;

-- -----------------------------------------------------------------------------
-- 2.3 Rank Matches and Select Best
-- -----------------------------------------------------------------------------
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
FROM inference_candidates;

-- -----------------------------------------------------------------------------
-- 2.4 Create Inferred Matches (Above Threshold)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE inferred_matches AS
SELECT
    query_id,
    prime_record_id AS matched_record_id,
    prime_cluster_id AS assigned_cluster_id,
    similarity_score,
    match_rank
FROM ranked_matches
WHERE
    match_rank = 1
    AND similarity_score >= {{SIMILARITY_THRESHOLD}};

-- -----------------------------------------------------------------------------
-- 2.5 Create Inference Results Table
-- -----------------------------------------------------------------------------
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
FROM {{NEW_RECORDS_TABLE}} nr
LEFT JOIN inferred_matches im ON nr.{{ID_FIELD}} = im.query_id;

-- -----------------------------------------------------------------------------
-- 2.6 Summary Statistics
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_new_records,
    COUNT(*) FILTER (WHERE match_status = 'matched') AS matched_records,
    COUNT(*) FILTER (WHERE match_status = 'unmatched') AS unmatched_records,
    ROUND(
        COUNT(*) FILTER (WHERE match_status = 'matched') * 100.0 / COUNT(*),
        2
    ) AS match_rate_pct,
    ROUND(AVG(similarity_score) FILTER (WHERE match_status = 'matched'), 4) AS avg_similarity
FROM inference_results;

-- -----------------------------------------------------------------------------
-- 2.7 Show Top Matches for Review
-- -----------------------------------------------------------------------------
SELECT
    ir.{{ID_FIELD}},
    ir.{{TEXT_FIELD}} AS new_record_text,
    pt.{{TEXT_FIELD}} AS matched_text,
    ir.assigned_cluster_id,
    ir.similarity_score
FROM inference_results ir
JOIN prime_table pt ON ir.matched_record_id = pt.{{ID_FIELD}}
WHERE ir.match_status = 'matched'
ORDER BY ir.similarity_score DESC
LIMIT 20;
