"""
Configuration module for Specific Affinity framework.

This module defines the configuration dataclass and default settings
for the entity resolution pipeline.
"""

from dataclasses import dataclass, field
from typing import Optional, Set


# Default English stop words
DEFAULT_STOP_WORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "has", "he", "in", "is", "it", "its", "of", "on", "or", "that",
    "the", "to", "was", "were", "will", "with", "this", "but", "they",
    "have", "had", "what", "when", "where", "who", "which", "why", "how",
    "all", "each", "every", "both", "few", "more", "most", "other",
    "some", "such", "no", "nor", "not", "only", "own", "same", "so",
    "than", "too", "very", "just", "can", "should", "now", "inc", "llc",
    "corp", "ltd", "co", "www", "com", "net", "org"
}


@dataclass
class Config:
    """
    Configuration for the Specific Affinity entity resolution pipeline.

    Attributes:
        db_path: Path to the DuckDB database file
        text_field: Name of the text column to use for matching
        id_field: Name of the unique identifier column
        source_table: Name of the source data table
        similarity_threshold: Minimum similarity score to consider a match (0-1)
        stop_words: Set of words to exclude from tokenization
        min_token_length: Minimum character length for valid tokens
        amount_field: Optional numeric field for categorization
        date_field: Optional date field for categorization
        group_field: Optional field to group records (e.g., customer_id)
    """

    # Required fields
    db_path: str
    text_field: str
    id_field: str
    source_table: str

    # Matching parameters
    similarity_threshold: float = 0.5
    stop_words: Set[str] = field(default_factory=lambda: DEFAULT_STOP_WORDS.copy())
    min_token_length: int = 2

    # Optional fields for categorization
    amount_field: Optional[str] = None
    date_field: Optional[str] = None
    group_field: Optional[str] = None

    # Categorization thresholds
    amount_threshold_pct: float = 5.0  # Percentage difference for "same" amount
    date_threshold_days: int = 3       # Days tolerance for regular frequency

    # Table names (auto-generated from source_table if not specified)
    prime_table: str = ""
    blocking_keys_table: str = ""
    weights_table: str = ""
    clusters_table: str = ""
    inferred_matches_table: str = ""
    unassigned_table: str = ""

    def __post_init__(self):
        """Set default table names based on source_table."""
        prefix = self.source_table.replace(".", "_")

        if not self.prime_table:
            self.prime_table = f"{prefix}_prime"
        if not self.blocking_keys_table:
            self.blocking_keys_table = f"{prefix}_blocking_keys"
        if not self.weights_table:
            self.weights_table = f"{prefix}_weights"
        if not self.clusters_table:
            self.clusters_table = f"{prefix}_clusters"
        if not self.inferred_matches_table:
            self.inferred_matches_table = f"{prefix}_inferred_matches"
        if not self.unassigned_table:
            self.unassigned_table = f"{prefix}_unassigned"

    def add_stop_words(self, words: Set[str]) -> None:
        """Add additional stop words to the set."""
        self.stop_words.update(words)

    def remove_stop_words(self, words: Set[str]) -> None:
        """Remove words from the stop words set."""
        self.stop_words -= words
