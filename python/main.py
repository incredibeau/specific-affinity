"""
Specific Affinity - Main Orchestrator

This module provides the main interface for running the entity resolution pipeline.
"""

import duckdb
from typing import Dict, Any, Optional, Set
from pathlib import Path

from .config import Config
from .prime_table import run_step_1
from .inference import run_step_2, infer_single_record
from .cleanup import run_step_3
from .categorization import run_step_4
from .qa import run_step_5, print_detailed_report


class SpecificAffinity:
    """
    Main class for the Specific Affinity entity resolution framework.

    This class provides a high-level interface for:
    - Creating a prime table from source data
    - Matching new records against the prime table
    - Cleaning up unassigned records
    - Categorizing records (optional)
    - Running quality assurance checks

    Example:
        sa = SpecificAffinity(
            db_path="data.duckdb",
            text_field="description",
            id_field="id",
            source_table="products"
        )
        sa.run_pipeline()
    """

    def __init__(
        self,
        db_path: str,
        text_field: str,
        id_field: str,
        source_table: str,
        similarity_threshold: float = 0.5,
        stop_words: Optional[Set[str]] = None,
        min_token_length: int = 2,
        amount_field: Optional[str] = None,
        date_field: Optional[str] = None,
        group_field: Optional[str] = None
    ):
        """
        Initialize the Specific Affinity framework.

        Args:
            db_path: Path to the DuckDB database file
            text_field: Name of the text column to use for matching
            id_field: Name of the unique identifier column
            source_table: Name of the source data table
            similarity_threshold: Minimum similarity score for matches (0-1)
            stop_words: Set of words to exclude from tokenization (optional)
            min_token_length: Minimum character length for valid tokens
            amount_field: Optional numeric field for categorization
            date_field: Optional date field for categorization
            group_field: Optional field to group records
        """
        self.config = Config(
            db_path=db_path,
            text_field=text_field,
            id_field=id_field,
            source_table=source_table,
            similarity_threshold=similarity_threshold,
            min_token_length=min_token_length,
            amount_field=amount_field,
            date_field=date_field,
            group_field=group_field
        )

        if stop_words:
            self.config.add_stop_words(stop_words)

        self.con = None
        self._results = {}

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connect to the DuckDB database."""
        if self.con is None:
            self.con = duckdb.connect(self.config.db_path)
        return self.con

    def close(self) -> None:
        """Close the database connection."""
        if self.con is not None:
            self.con.close()
            self.con = None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def create_prime_table(self) -> Dict[str, Any]:
        """
        Step 1: Create the prime table by clustering similar records.

        Returns:
            Dictionary with summary statistics
        """
        con = self.connect()
        result = run_step_1(con, self.config)
        self._results["step_1"] = result
        return result

    def make_inference(self, new_records_table: str) -> Dict[str, Any]:
        """
        Step 2: Match new records against the prime table.

        Args:
            new_records_table: Name of table containing new records

        Returns:
            Dictionary with match statistics
        """
        con = self.connect()
        result = run_step_2(con, self.config, new_records_table)
        self._results["step_2"] = result
        return result

    def cleanup_unassigned(self) -> Dict[str, Any]:
        """
        Step 3: Cluster unassigned records among themselves.

        Returns:
            Dictionary with cleanup statistics
        """
        con = self.connect()
        result = run_step_3(con, self.config)
        self._results["step_3"] = result
        return result

    def categorize_records(self) -> Dict[str, Any]:
        """
        Step 4: Categorize records by pattern (subscription, recurring, one-time).

        Requires amount_field and date_field to be configured.

        Returns:
            Dictionary with categorization statistics
        """
        con = self.connect()
        result = run_step_4(con, self.config)
        self._results["step_4"] = result
        return result

    def run_qa(self, detailed: bool = False) -> Dict[str, Any]:
        """
        Step 5: Run quality assurance checks.

        Args:
            detailed: If True, print a detailed report

        Returns:
            Dictionary with QA results
        """
        con = self.connect()
        result = run_step_5(con, self.config)
        self._results["step_5"] = result

        if detailed:
            print_detailed_report(result)

        return result

    def run_pipeline(
        self,
        new_records_table: Optional[str] = None,
        include_categorization: bool = False,
        run_qa: bool = True
    ) -> Dict[str, Any]:
        """
        Run the complete pipeline.

        Args:
            new_records_table: Optional table of new records to match (Step 2)
            include_categorization: Whether to run categorization (Step 4)
            run_qa: Whether to run QA checks (Step 5)

        Returns:
            Dictionary with all results
        """
        print("\n" + "#" * 60)
        print("# SPECIFIC AFFINITY - ENTITY RESOLUTION PIPELINE")
        print("#" * 60 + "\n")

        # Step 1: Create Prime Table
        self.create_prime_table()

        # Step 2: Make Inference (if new records provided)
        if new_records_table:
            self.make_inference(new_records_table)

            # Step 3: Cleanup Unassigned
            self.cleanup_unassigned()

        # Step 4: Categorization (optional)
        if include_categorization and self.config.amount_field and self.config.date_field:
            self.categorize_records()

        # Step 5: Quality Assurance
        if run_qa:
            self.run_qa()

        print("\n" + "#" * 60)
        print("# PIPELINE COMPLETE")
        print("#" * 60 + "\n")

        return self._results

    def match_text(self, text: str) -> Dict[str, Any]:
        """
        Match a single text value against the prime table.

        Args:
            text: The text to match

        Returns:
            Dictionary with match results
        """
        con = self.connect()
        return infer_single_record(con, self.config, text)

    def get_cluster(self, cluster_id: str) -> list:
        """
        Get all records in a specific cluster.

        Args:
            cluster_id: The cluster ID to retrieve

        Returns:
            List of records in the cluster
        """
        con = self.connect()
        result = con.execute(f"""
        SELECT *
        FROM {self.config.prime_table}
        WHERE cluster_id = ?
        """, [cluster_id]).fetchall()
        return result

    def get_cluster_sample(self, cluster_id: str, limit: int = 5) -> list:
        """
        Get a sample of records from a cluster.

        Args:
            cluster_id: The cluster ID to sample
            limit: Maximum number of records to return

        Returns:
            List of sample records
        """
        con = self.connect()
        result = con.execute(f"""
        SELECT {self.config.id_field}, {self.config.text_field}
        FROM {self.config.prime_table}
        WHERE cluster_id = ?
        LIMIT ?
        """, [cluster_id, limit]).fetchall()
        return result

    def add_stop_words(self, words: Set[str]) -> None:
        """Add words to the stop words list."""
        self.config.add_stop_words(words)

    def set_threshold(self, threshold: float) -> None:
        """Update the similarity threshold."""
        self.config.similarity_threshold = threshold

    @property
    def results(self) -> Dict[str, Any]:
        """Get all pipeline results."""
        return self._results


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Specific Affinity Entity Resolution")
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument("--table", required=True, help="Source table name")
    parser.add_argument("--text-field", required=True, help="Text column name")
    parser.add_argument("--id-field", required=True, help="ID column name")
    parser.add_argument("--threshold", type=float, default=0.5, help="Similarity threshold")
    parser.add_argument("--new-records", help="Table of new records to match")
    parser.add_argument("--qa", action="store_true", help="Run QA checks")

    args = parser.parse_args()

    sa = SpecificAffinity(
        db_path=args.db,
        text_field=args.text_field,
        id_field=args.id_field,
        source_table=args.table,
        similarity_threshold=args.threshold
    )

    with sa:
        sa.run_pipeline(
            new_records_table=args.new_records,
            run_qa=args.qa
        )


if __name__ == "__main__":
    main()
