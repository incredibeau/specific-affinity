"""
Specific Affinity: Snowflake Implementation

This module provides entity matching between two tables in Snowflake.
"""

import snowflake.connector
from typing import Dict, Any, Optional, Set, List
from dataclasses import dataclass


# Default stop words
DEFAULT_STOP_WORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "has", "he", "in", "is", "it", "its", "of", "on", "or", "that",
    "the", "to", "was", "were", "will", "with", "this", "but", "they",
    "have", "had", "what", "when", "where", "who", "which", "why", "how",
    "all", "each", "every", "both", "few", "more", "most", "other",
    "some", "such", "no", "nor", "not", "only", "own", "same", "so",
    "than", "too", "very", "just", "can", "should", "now", "inc", "llc",
    "corp", "ltd", "co", "www", "com", "net", "org", "company"
}


@dataclass
class MatchConfig:
    """Configuration for matching between two tables."""
    # Table A (reference/master)
    table_a: str
    id_field_a: str
    text_field_a: str

    # Table B (records to match)
    table_b: str
    id_field_b: str
    text_field_b: str

    # Matching parameters
    similarity_threshold: float = 0.5
    min_token_length: int = 2

    # Output table names
    results_table: str = "match_results"


class SnowflakeMatcher:
    """
    Match records between two Snowflake tables using text similarity.

    Example:
        matcher = SnowflakeMatcher(
            account="your_account",
            user="your_user",
            password="your_password",
            warehouse="your_warehouse",
            database="your_database",
            schema="your_schema"
        )

        config = MatchConfig(
            table_a="master_vendors",
            id_field_a="vendor_id",
            text_field_a="vendor_name",
            table_b="incoming_vendors",
            id_field_b="record_id",
            text_field_b="vendor_name",
            similarity_threshold=0.5
        )

        results = matcher.match_tables(config)
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: Optional[str] = None,
        warehouse: str = None,
        database: str = None,
        schema: str = None,
        role: Optional[str] = None,
        authenticator: Optional[str] = None,
        private_key_path: Optional[str] = None,
        stop_words: Optional[Set[str]] = None
    ):
        """
        Initialize Snowflake connection.

        Args:
            account: Snowflake account identifier
            user: Username
            password: Password (or use authenticator/private_key)
            warehouse: Warehouse name
            database: Database name
            schema: Schema name
            role: Optional role
            authenticator: Optional authenticator (e.g., 'externalbrowser')
            private_key_path: Optional path to private key file
            stop_words: Custom stop words set
        """
        self.connection_params = {
            "account": account,
            "user": user,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
        }

        if password:
            self.connection_params["password"] = password
        if role:
            self.connection_params["role"] = role
        if authenticator:
            self.connection_params["authenticator"] = authenticator

        self.stop_words = stop_words or DEFAULT_STOP_WORDS.copy()
        self.con = None

    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """Establish connection to Snowflake."""
        if self.con is None:
            self.con = snowflake.connector.connect(**self.connection_params)
        return self.con

    def close(self) -> None:
        """Close the connection."""
        if self.con:
            self.con.close()
            self.con = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _execute(self, sql: str) -> List[tuple]:
        """Execute SQL and return results."""
        cur = self.con.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    def _execute_many(self, statements: List[str]) -> None:
        """Execute multiple SQL statements."""
        cur = self.con.cursor()
        try:
            for sql in statements:
                cur.execute(sql)
        finally:
            cur.close()

    def _create_stop_words_table(self) -> None:
        """Create and populate stop words table."""
        print("Creating stop words table...")

        self._execute("CREATE OR REPLACE TABLE _sa_stop_words (word VARCHAR)")

        # Insert in batches
        values = ", ".join(f"('{word}')" for word in self.stop_words)
        self._execute(f"INSERT INTO _sa_stop_words (word) VALUES {values}")

    def _tokenize_table_a(self, config: MatchConfig) -> int:
        """Tokenize the reference table."""
        print(f"Tokenizing reference table: {config.table_a}...")

        sql = f"""
        CREATE OR REPLACE TABLE _sa_table_a_tokens AS
        WITH normalized AS (
            SELECT
                {config.id_field_a} AS record_id,
                LOWER(REGEXP_REPLACE({config.text_field_a}, '[^a-zA-Z0-9\\\\s]', ' ')) AS normalized_text
            FROM {config.table_a}
            WHERE {config.text_field_a} IS NOT NULL
              AND LENGTH(TRIM({config.text_field_a})) > 0
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
            AND LENGTH(token) >= {config.min_token_length}
            AND token NOT IN (SELECT word FROM _sa_stop_words)
        """
        self._execute(sql)

        count = self._execute("SELECT COUNT(*) FROM _sa_table_a_tokens")[0][0]
        print(f"  Created {count} tokens from Table A")
        return count

    def _calculate_weights(self) -> int:
        """Calculate token weights using inverse frequency."""
        print("Calculating token weights...")

        sql = """
        CREATE OR REPLACE TABLE _sa_weights AS
        WITH token_counts AS (
            SELECT
                token,
                COUNT(*) AS freq,
                (SELECT COUNT(DISTINCT record_id) FROM _sa_table_a_tokens) AS total_records
            FROM _sa_table_a_tokens
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
        WHERE lw.log_weight IS NOT NULL
        """
        self._execute(sql)

        count = self._execute("SELECT COUNT(*) FROM _sa_weights")[0][0]
        print(f"  Calculated weights for {count} unique tokens")
        return count

    def _tokenize_table_b(self, config: MatchConfig) -> int:
        """Tokenize the table to match."""
        print(f"Tokenizing match table: {config.table_b}...")

        sql = f"""
        CREATE OR REPLACE TABLE _sa_table_b_tokens AS
        WITH normalized AS (
            SELECT
                {config.id_field_b} AS record_id,
                LOWER(REGEXP_REPLACE({config.text_field_b}, '[^a-zA-Z0-9\\\\s]', ' ')) AS normalized_text
            FROM {config.table_b}
            WHERE {config.text_field_b} IS NOT NULL
              AND LENGTH(TRIM({config.text_field_b})) > 0
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
            AND LENGTH(token) >= {config.min_token_length}
            AND token NOT IN (SELECT word FROM _sa_stop_words)
        """
        self._execute(sql)

        count = self._execute("SELECT COUNT(DISTINCT record_id) FROM _sa_table_b_tokens")[0][0]
        print(f"  Tokenized {count} records from Table B")
        return count

    def _find_matches(self, config: MatchConfig) -> int:
        """Find candidate matches between tables."""
        print("Finding candidate matches...")

        sql = """
        CREATE OR REPLACE TABLE _sa_candidates AS
        SELECT
            bt.record_id AS table_b_id,
            at.record_id AS table_a_id,
            SUM(w.weight) AS similarity_score
        FROM
            _sa_table_b_tokens bt
        INNER JOIN
            _sa_table_a_tokens at ON bt.token = at.token
        INNER JOIN
            _sa_weights w ON bt.token = w.token
        GROUP BY
            bt.record_id, at.record_id
        """
        self._execute(sql)

        count = self._execute("SELECT COUNT(*) FROM _sa_candidates")[0][0]
        print(f"  Found {count} candidate pairs")
        return count

    def _rank_and_filter(self, config: MatchConfig) -> None:
        """Rank matches and create final results."""
        print(f"Ranking matches (threshold >= {config.similarity_threshold})...")

        # Rank matches
        self._execute("""
        CREATE OR REPLACE TABLE _sa_ranked AS
        SELECT
            table_b_id,
            table_a_id,
            similarity_score,
            ROW_NUMBER() OVER (
                PARTITION BY table_b_id
                ORDER BY similarity_score DESC
            ) AS match_rank
        FROM _sa_candidates
        """)

        # Create final results
        sql = f"""
        CREATE OR REPLACE TABLE {config.results_table} AS
        SELECT
            b.{config.id_field_b} AS table_b_id,
            b.{config.text_field_b} AS table_b_text,
            a.{config.id_field_a} AS matched_table_a_id,
            a.{config.text_field_a} AS matched_table_a_text,
            r.similarity_score,
            CASE
                WHEN r.similarity_score >= {config.similarity_threshold} THEN 'MATCHED'
                ELSE 'UNMATCHED'
            END AS match_status
        FROM {config.table_b} b
        LEFT JOIN _sa_ranked r
            ON b.{config.id_field_b} = r.table_b_id
            AND r.match_rank = 1
            AND r.similarity_score >= {config.similarity_threshold}
        LEFT JOIN {config.table_a} a
            ON r.table_a_id = a.{config.id_field_a}
        """
        self._execute(sql)

    def _cleanup_temp_tables(self) -> None:
        """Remove temporary tables."""
        print("Cleaning up temporary tables...")
        tables = [
            "_sa_stop_words",
            "_sa_table_a_tokens",
            "_sa_table_b_tokens",
            "_sa_weights",
            "_sa_candidates",
            "_sa_ranked"
        ]
        for table in tables:
            try:
                self._execute(f"DROP TABLE IF EXISTS {table}")
            except:
                pass

    def match_tables(
        self,
        config: MatchConfig,
        cleanup: bool = True
    ) -> Dict[str, Any]:
        """
        Match records from Table B against Table A.

        Args:
            config: MatchConfig with table and field names
            cleanup: Whether to remove temporary tables after matching

        Returns:
            Dictionary with match statistics
        """
        print("\n" + "=" * 60)
        print("SPECIFIC AFFINITY - SNOWFLAKE TABLE MATCHING")
        print("=" * 60)
        print(f"\nMatching: {config.table_b} -> {config.table_a}")
        print(f"Threshold: {config.similarity_threshold}\n")

        self.connect()

        try:
            # Run matching pipeline
            self._create_stop_words_table()
            self._tokenize_table_a(config)
            self._calculate_weights()
            self._tokenize_table_b(config)
            self._find_matches(config)
            self._rank_and_filter(config)

            # Get statistics
            stats = self._execute(f"""
            SELECT
                COUNT(*) AS total_records,
                SUM(CASE WHEN match_status = 'MATCHED' THEN 1 ELSE 0 END) AS matched,
                SUM(CASE WHEN match_status = 'UNMATCHED' THEN 1 ELSE 0 END) AS unmatched,
                ROUND(AVG(CASE WHEN match_status = 'MATCHED' THEN similarity_score END), 4) AS avg_score
            FROM {config.results_table}
            """)[0]

            results = {
                "total_records": stats[0],
                "matched": stats[1],
                "unmatched": stats[2],
                "match_rate_pct": round(stats[1] * 100.0 / stats[0], 2) if stats[0] > 0 else 0,
                "avg_similarity": stats[3],
                "results_table": config.results_table
            }

            print("\n" + "-" * 40)
            print("RESULTS:")
            print(f"  Total records:  {results['total_records']}")
            print(f"  Matched:        {results['matched']} ({results['match_rate_pct']}%)")
            print(f"  Unmatched:      {results['unmatched']}")
            print(f"  Avg similarity: {results['avg_similarity']}")
            print(f"  Results table:  {config.results_table}")
            print("-" * 40 + "\n")

            if cleanup:
                self._cleanup_temp_tables()

            return results

        except Exception as e:
            print(f"Error: {e}")
            raise

    def get_results_df(self, config: MatchConfig):
        """Get results as a pandas DataFrame."""
        try:
            import pandas as pd
            cur = self.con.cursor()
            cur.execute(f"SELECT * FROM {config.results_table} ORDER BY similarity_score DESC NULLS LAST")
            df = cur.fetch_pandas_all()
            cur.close()
            return df
        except ImportError:
            raise ImportError("pandas is required for get_results_df()")

    def add_stop_words(self, words: Set[str]) -> None:
        """Add custom stop words."""
        self.stop_words.update(words)


# =============================================================================
# Convenience function for quick matching
# =============================================================================

def match_snowflake_tables(
    # Connection params
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
    schema: str,
    # Table A (reference)
    table_a: str,
    id_field_a: str,
    text_field_a: str,
    # Table B (to match)
    table_b: str,
    id_field_b: str,
    text_field_b: str,
    # Options
    similarity_threshold: float = 0.5,
    results_table: str = "match_results"
) -> Dict[str, Any]:
    """
    Quick function to match two Snowflake tables.

    Example:
        results = match_snowflake_tables(
            account="xy12345.us-east-1",
            user="my_user",
            password="my_password",
            warehouse="COMPUTE_WH",
            database="MY_DB",
            schema="PUBLIC",
            table_a="master_vendors",
            id_field_a="vendor_id",
            text_field_a="vendor_name",
            table_b="incoming_data",
            id_field_b="record_id",
            text_field_b="vendor_name",
            similarity_threshold=0.5
        )
    """
    config = MatchConfig(
        table_a=table_a,
        id_field_a=id_field_a,
        text_field_a=text_field_a,
        table_b=table_b,
        id_field_b=id_field_b,
        text_field_b=text_field_b,
        similarity_threshold=similarity_threshold,
        results_table=results_table
    )

    matcher = SnowflakeMatcher(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    with matcher:
        return matcher.match_tables(config)
