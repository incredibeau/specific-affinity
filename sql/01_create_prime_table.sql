-- =============================================================================
-- Step 1: Create Prime Table
-- =============================================================================
-- This script creates the foundational clustering of records based on text
-- similarity. It tokenizes text fields, calculates weights, performs self-joins,
-- and identifies connected components to form clusters.
--
-- Prerequisites:
--   - A source table with at least: id_field, text_field
--   - A stop_words table with a 'word' column
--
-- Configuration (replace these placeholders):
--   - {{SOURCE_TABLE}}: Your source data table name
--   - {{ID_FIELD}}: Unique identifier column name
--   - {{TEXT_FIELD}}: Text column to use for matching
--   - {{SIMILARITY_THRESHOLD}}: Minimum score for matches (e.g., 0.5)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1 Create Stop Words Table (if not exists)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stop_words (word VARCHAR PRIMARY KEY);

-- Insert common stop words (run once)
INSERT OR IGNORE INTO stop_words (word) VALUES
    ('a'), ('an'), ('and'), ('are'), ('as'), ('at'), ('be'), ('by'),
    ('for'), ('from'), ('has'), ('he'), ('in'), ('is'), ('it'), ('its'),
    ('of'), ('on'), ('or'), ('that'), ('the'), ('to'), ('was'), ('were'),
    ('will'), ('with'), ('this'), ('but'), ('they'), ('have'), ('had'),
    ('what'), ('when'), ('where'), ('who'), ('which'), ('why'), ('how'),
    ('all'), ('each'), ('every'), ('both'), ('few'), ('more'), ('most'),
    ('other'), ('some'), ('such'), ('no'), ('nor'), ('not'), ('only'),
    ('own'), ('same'), ('so'), ('than'), ('too'), ('very'), ('just'),
    ('can'), ('should'), ('now'), ('inc'), ('llc'), ('corp'), ('ltd'),
    ('co'), ('www'), ('com'), ('net'), ('org');

-- -----------------------------------------------------------------------------
-- 1.2 Create Normalized Text View
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW normalized_text AS
SELECT
    {{ID_FIELD}} AS record_id,
    {{TEXT_FIELD}} AS original_text,
    regexp_replace(LOWER({{TEXT_FIELD}}), '[^a-zA-Z0-9\s]', ' ', 'g') AS normalized_text
FROM {{SOURCE_TABLE}}
WHERE {{TEXT_FIELD}} IS NOT NULL AND LENGTH(TRIM({{TEXT_FIELD}})) > 0;

-- -----------------------------------------------------------------------------
-- 1.3 Create Blocking Keys (Tokenization)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE blocking_keys AS
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
    AND LENGTH(token) >= 2
    AND token NOT IN (SELECT word FROM stop_words);

-- -----------------------------------------------------------------------------
-- 1.4 Calculate Token Weights (Inverse Frequency)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE token_weights AS
WITH token_counts AS (
    SELECT
        token,
        COUNT(*) AS freq,
        (SELECT COUNT(DISTINCT record_id) FROM blocking_keys) AS total_records
    FROM blocking_keys
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
WHERE normalized_weight IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 1.5 Self-Join to Find Candidate Pairs
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE candidate_pairs AS
SELECT
    bk1.record_id AS record_id_1,
    bk2.record_id AS record_id_2,
    SUM(tw.weight) AS similarity_score
FROM
    blocking_keys bk1
JOIN
    blocking_keys bk2 ON bk1.token = bk2.token
JOIN
    token_weights tw ON bk1.token = tw.token
WHERE
    bk1.record_id < bk2.record_id  -- Avoid duplicate pairs and self-matches
GROUP BY
    bk1.record_id, bk2.record_id;

-- -----------------------------------------------------------------------------
-- 1.6 Filter by Similarity Threshold
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE filtered_pairs AS
SELECT
    record_id_1,
    record_id_2,
    similarity_score
FROM candidate_pairs
WHERE similarity_score >= {{SIMILARITY_THRESHOLD}};

-- -----------------------------------------------------------------------------
-- 1.7 Identify Connected Components (Clustering)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE record_clusters AS
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
    -- Recursive: propagate minimum cluster_id across connections
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
ORDER BY cluster_id, record_id;

-- -----------------------------------------------------------------------------
-- 1.8 Create Prime Table (Final Output)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE prime_table AS
SELECT
    s.*,
    rc.cluster_id
FROM {{SOURCE_TABLE}} s
LEFT JOIN record_clusters rc ON s.{{ID_FIELD}} = rc.record_id;

-- -----------------------------------------------------------------------------
-- 1.9 Summary Statistics
-- -----------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT cluster_id) AS total_clusters,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE cluster_id IS NOT NULL) AS clustered_records,
    COUNT(*) FILTER (WHERE cluster_id IS NULL) AS unclustered_records,
    ROUND(AVG(cluster_size), 2) AS avg_cluster_size
FROM (
    SELECT
        cluster_id,
        COUNT(*) OVER (PARTITION BY cluster_id) AS cluster_size
    FROM prime_table
    WHERE cluster_id IS NOT NULL
) t;
