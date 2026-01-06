-- =============================================================================
-- Step 4: Record Categorization (Optional)
-- =============================================================================
-- This script categorizes records based on patterns in numeric and date fields.
-- It's particularly useful for transaction data where you want to identify:
--   - Subscriptions: Regular dates + consistent amounts
--   - Recurring: Regular dates + varying amounts
--   - One-time: No regular pattern
--
-- Prerequisites:
--   - Prime Table created with cluster assignments
--   - Records have date and amount fields
--
-- Configuration (replace these placeholders):
--   - {{ID_FIELD}}: Unique identifier column name
--   - {{TEXT_FIELD}}: Text column (for reference)
--   - {{DATE_FIELD}}: Date column name
--   - {{AMOUNT_FIELD}}: Numeric amount column name
--   - {{GROUP_FIELD}}: Optional grouping field (e.g., customer_id)
--   - {{AMOUNT_THRESHOLD_PCT}}: Percentage for "same" amount (e.g., 5)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 Create Cluster Transactions View
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW cluster_records AS
SELECT
    cluster_id,
    {{ID_FIELD}} AS record_id,
    {{AMOUNT_FIELD}} AS amount,
    {{DATE_FIELD}} AS record_date,
    {{TEXT_FIELD}} AS text_value,
    {{GROUP_FIELD}} AS group_id
FROM prime_table
WHERE cluster_id IS NOT NULL
ORDER BY cluster_id, {{GROUP_FIELD}}, {{DATE_FIELD}};

-- -----------------------------------------------------------------------------
-- 4.2 Analyze Cluster Patterns
-- -----------------------------------------------------------------------------
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
    END AS amount_coefficient_of_variation,
    CASE
        WHEN fs.stddev_days_between IS NULL OR fs.avg_days_between = 0 THEN 0
        ELSE fs.stddev_days_between / fs.avg_days_between
    END AS date_coefficient_of_variation
FROM cluster_stats cs
LEFT JOIN frequency_stats fs ON cs.cluster_id = fs.cluster_id;

-- -----------------------------------------------------------------------------
-- 4.3 Determine Cluster Types
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE cluster_types AS
SELECT
    cluster_id,
    record_count,
    group_count,
    avg_amount,
    most_common_frequency,
    amount_coefficient_of_variation,
    date_coefficient_of_variation,
    CASE
        -- Subscription: Regular frequency AND consistent amounts
        WHEN most_common_frequency BETWEEN 7 AND 35
            AND (amount_coefficient_of_variation < 0.1 OR record_count < 3)
            AND date_coefficient_of_variation < 0.3
        THEN 'subscription'

        -- Recurring: Regular frequency BUT varying amounts
        WHEN most_common_frequency BETWEEN 7 AND 35
            AND date_coefficient_of_variation < 0.3
        THEN 'recurring'

        -- One-time: No regular pattern
        ELSE 'one-time'
    END AS cluster_type
FROM cluster_metadata;

-- -----------------------------------------------------------------------------
-- 4.4 Categorize Individual Records
-- -----------------------------------------------------------------------------
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
        -- First record for this group: use cluster default
        WHEN prev_date IS NULL THEN default_type

        -- Subscription: regular timing + same amount
        WHEN days_between IS NOT NULL
            AND ABS(days_between - most_common_frequency) <= 3
            AND (
                amount = prev_amount
                OR ABS(amount - prev_amount) * 100.0 / NULLIF(ABS(prev_amount), 0) <= {{AMOUNT_THRESHOLD_PCT}}
            )
        THEN 'subscription'

        -- Recurring: regular timing + different amount
        WHEN days_between IS NOT NULL
            AND ABS(days_between - most_common_frequency) <= 3
        THEN 'recurring'

        -- One-time: irregular
        ELSE 'one-time'
    END AS record_type
FROM record_analysis;

-- -----------------------------------------------------------------------------
-- 4.5 Update Prime Table with Categorization
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE prime_table_categorized AS
SELECT
    pt.*,
    cr.record_type,
    ct.cluster_type
FROM prime_table pt
LEFT JOIN categorized_records cr ON pt.{{ID_FIELD}} = cr.record_id
LEFT JOIN cluster_types ct ON pt.cluster_id = ct.cluster_id;

-- -----------------------------------------------------------------------------
-- 4.6 Summary Statistics
-- -----------------------------------------------------------------------------
SELECT
    record_type,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM categorized_records
GROUP BY record_type
ORDER BY record_count DESC;

-- Cluster type distribution
SELECT
    cluster_type,
    COUNT(*) AS cluster_count,
    SUM(record_count) AS total_records,
    ROUND(AVG(avg_amount), 2) AS avg_amount,
    ROUND(AVG(most_common_frequency), 1) AS avg_frequency_days
FROM cluster_types
JOIN cluster_metadata USING (cluster_id)
GROUP BY cluster_type
ORDER BY cluster_count DESC;
