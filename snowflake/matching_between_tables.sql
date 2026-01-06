-- =============================================================================
-- Specific Affinity: Match Records Between Two Tables (Snowflake)
-- =============================================================================
-- This script matches records from TABLE_B against TABLE_A using text similarity.
--
-- CONFIGURATION - Replace these placeholders:
--   {{DATABASE}}          : Your database name
--   {{SCHEMA}}            : Your schema name
--   {{TABLE_A}}           : Reference/master table name
--   {{TABLE_B}}           : Table with records to match
--   {{ID_FIELD_A}}        : Unique ID column in Table A
--   {{ID_FIELD_B}}        : Unique ID column in Table B
--   {{TEXT_FIELD_A}}      : Text column to match in Table A
--   {{TEXT_FIELD_B}}      : Text column to match in Table B
--   {{SIMILARITY_THRESHOLD}} : Minimum score for matches (e.g., 0.5)
-- =============================================================================

USE DATABASE {{DATABASE}};
USE SCHEMA {{SCHEMA}};

-- -----------------------------------------------------------------------------
-- 1. Create Stop Words Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE stop_words (word VARCHAR);

INSERT INTO stop_words (word) VALUES
    ('a'), ('an'), ('and'), ('are'), ('as'), ('at'), ('be'), ('by'),
    ('for'), ('from'), ('has'), ('he'), ('in'), ('is'), ('it'), ('its'),
    ('of'), ('on'), ('or'), ('that'), ('the'), ('to'), ('was'), ('were'),
    ('will'), ('with'), ('this'), ('but'), ('they'), ('have'), ('had'),
    ('what'), ('when'), ('where'), ('who'), ('which'), ('why'), ('how'),
    ('all'), ('each'), ('every'), ('both'), ('few'), ('more'), ('most'),
    ('other'), ('some'), ('such'), ('no'), ('nor'), ('not'), ('only'),
    ('own'), ('same'), ('so'), ('than'), ('too'), ('very'), ('just'),
    ('can'), ('should'), ('now'), ('inc'), ('llc'), ('corp'), ('ltd'),
    ('co'), ('www'), ('com'), ('net'), ('org'), ('company');

-- -----------------------------------------------------------------------------
-- 2. Tokenize Table A (Reference Table)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE table_a_blocking_keys AS
WITH normalized AS (
    SELECT
        {{ID_FIELD_A}} AS record_id,
        {{TEXT_FIELD_A}} AS original_text,
        LOWER(REGEXP_REPLACE({{TEXT_FIELD_A}}, '[^a-zA-Z0-9\\s]', ' ')) AS normalized_text
    FROM {{TABLE_A}}
    WHERE {{TEXT_FIELD_A}} IS NOT NULL AND LENGTH(TRIM({{TEXT_FIELD_A}})) > 0
),
tokenized AS (
    SELECT
        n.record_id,
        TRIM(t.value) AS token
    FROM normalized n,
    LATERAL SPLIT_TO_TABLE(n.normalized_text, ' ') t
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
-- 3. Calculate Token Weights for Table A
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE table_a_weights AS
WITH token_counts AS (
    SELECT
        token,
        COUNT(*) AS freq,
        (SELECT COUNT(DISTINCT record_id) FROM table_a_blocking_keys) AS total_records
    FROM table_a_blocking_keys
    GROUP BY token
),
weight_calc AS (
    SELECT
        token,
        freq,
        total_records,
        freq / total_records::FLOAT AS raw_frequency,
        (SELECT AVG(freq / total_records::FLOAT) FROM token_counts) /
            NULLIF(freq / total_records::FLOAT, 0) AS weight
    FROM token_counts
),
log_weights AS (
    SELECT
        token,
        LN(weight) AS log_weight
    FROM weight_calc
    WHERE token != '' AND weight > 0
),
weight_stats AS (
    SELECT
        MIN(log_weight) AS min_weight,
        MAX(log_weight) AS max_weight
    FROM log_weights
)
SELECT
    lw.token,
    ROUND(
        CASE
            WHEN ws.max_weight = ws.min_weight THEN 0.5
            ELSE (lw.log_weight - ws.min_weight) / NULLIF(ws.max_weight - ws.min_weight, 0)
        END,
        4
    ) AS weight
FROM log_weights lw
CROSS JOIN weight_stats ws
WHERE lw.log_weight IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 4. Tokenize Table B (Records to Match)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE table_b_tokens AS
WITH normalized AS (
    SELECT
        {{ID_FIELD_B}} AS record_id,
        {{TEXT_FIELD_B}} AS original_text,
        LOWER(REGEXP_REPLACE({{TEXT_FIELD_B}}, '[^a-zA-Z0-9\\s]', ' ')) AS normalized_text
    FROM {{TABLE_B}}
    WHERE {{TEXT_FIELD_B}} IS NOT NULL AND LENGTH(TRIM({{TEXT_FIELD_B}})) > 0
),
tokenized AS (
    SELECT
        n.record_id,
        TRIM(t.value) AS token
    FROM normalized n,
    LATERAL SPLIT_TO_TABLE(n.normalized_text, ' ') t
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
-- 5. Find Candidate Matches
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE candidate_matches AS
SELECT
    bt.record_id AS table_b_id,
    ak.record_id AS table_a_id,
    SUM(aw.weight) AS similarity_score
FROM
    table_b_tokens bt
INNER JOIN
    table_a_blocking_keys ak ON bt.token = ak.token
INNER JOIN
    table_a_weights aw ON bt.token = aw.token
GROUP BY
    bt.record_id, ak.record_id;

-- -----------------------------------------------------------------------------
-- 6. Rank and Filter Matches
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ranked_matches AS
SELECT
    table_b_id,
    table_a_id,
    similarity_score,
    ROW_NUMBER() OVER (
        PARTITION BY table_b_id
        ORDER BY similarity_score DESC
    ) AS match_rank
FROM candidate_matches;

-- -----------------------------------------------------------------------------
-- 7. Final Match Results
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE match_results AS
SELECT
    b.{{ID_FIELD_B}} AS table_b_id,
    b.{{TEXT_FIELD_B}} AS table_b_text,
    a.{{ID_FIELD_A}} AS matched_table_a_id,
    a.{{TEXT_FIELD_A}} AS matched_table_a_text,
    rm.similarity_score,
    CASE
        WHEN rm.similarity_score >= {{SIMILARITY_THRESHOLD}} THEN 'MATCHED'
        ELSE 'UNMATCHED'
    END AS match_status
FROM {{TABLE_B}} b
LEFT JOIN ranked_matches rm
    ON b.{{ID_FIELD_B}} = rm.table_b_id
    AND rm.match_rank = 1
    AND rm.similarity_score >= {{SIMILARITY_THRESHOLD}}
LEFT JOIN {{TABLE_A}} a
    ON rm.table_a_id = a.{{ID_FIELD_A}};

-- -----------------------------------------------------------------------------
-- 8. Summary Statistics
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN match_status = 'MATCHED' THEN 1 ELSE 0 END) AS matched_count,
    SUM(CASE WHEN match_status = 'UNMATCHED' THEN 1 ELSE 0 END) AS unmatched_count,
    ROUND(SUM(CASE WHEN match_status = 'MATCHED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS match_rate_pct,
    ROUND(AVG(CASE WHEN match_status = 'MATCHED' THEN similarity_score END), 4) AS avg_match_score
FROM match_results;

-- -----------------------------------------------------------------------------
-- 9. View Results
-- -----------------------------------------------------------------------------
SELECT * FROM match_results ORDER BY similarity_score DESC NULLS LAST;
