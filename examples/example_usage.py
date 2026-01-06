"""
Example: Using Specific Affinity for Entity Resolution

This example demonstrates how to use the Specific Affinity framework
to cluster similar records and match new records against existing clusters.

The example uses synthetic transaction data to show vendor matching.
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from python.main import SpecificAffinity


def create_sample_data(con: duckdb.DuckDBPyConnection) -> None:
    """Create sample transaction data for demonstration."""
    print("Creating sample data...")

    # Create transactions table
    con.execute("""
    CREATE OR REPLACE TABLE transactions (
        transaction_id VARCHAR PRIMARY KEY,
        memo VARCHAR,
        amount DECIMAL(10,2),
        transaction_date DATE,
        customer_id VARCHAR
    )
    """)

    # Insert sample transactions with various vendor name variations
    sample_data = [
        # Netflix variations
        ("t001", "NETFLIX.COM", 15.99, "2024-01-15", "c001"),
        ("t002", "NETFLIX SUBSCRIPTION", 15.99, "2024-02-15", "c001"),
        ("t003", "NETFLIX.COM/ACCT", 15.99, "2024-03-15", "c001"),
        ("t004", "NETFLIX INC", 15.99, "2024-01-20", "c002"),
        ("t005", "NETFLIX STREAMING", 15.99, "2024-02-20", "c002"),

        # Spotify variations
        ("t006", "SPOTIFY USA", 9.99, "2024-01-10", "c001"),
        ("t007", "SPOTIFY PREMIUM", 9.99, "2024-02-10", "c001"),
        ("t008", "SPOTIFY.COM", 9.99, "2024-03-10", "c001"),
        ("t009", "SPOTIFY SUBSCRIPTION", 9.99, "2024-01-12", "c003"),

        # Amazon variations
        ("t010", "AMAZON.COM", 47.50, "2024-01-05", "c001"),
        ("t011", "AMZN MKTP US", 23.99, "2024-01-18", "c001"),
        ("t012", "AMAZON PRIME", 14.99, "2024-02-01", "c001"),
        ("t013", "AMAZON.COM AMZN", 89.99, "2024-02-15", "c002"),
        ("t014", "AMZN DIGITAL", 4.99, "2024-02-20", "c002"),

        # Uber variations
        ("t015", "UBER TRIP", 24.50, "2024-01-08", "c001"),
        ("t016", "UBER *EATS", 32.00, "2024-01-15", "c001"),
        ("t017", "UBER TECHNOLOGIES", 18.75, "2024-01-22", "c002"),
        ("t018", "UBER RIDE", 15.00, "2024-02-05", "c002"),

        # Starbucks variations
        ("t019", "STARBUCKS STORE 1234", 5.75, "2024-01-03", "c001"),
        ("t020", "STARBUCKS COFFEE", 6.25, "2024-01-10", "c001"),
        ("t021", "STARBUCKS #5678", 4.50, "2024-01-17", "c002"),
        ("t022", "STARBUCKS MOBILE", 7.00, "2024-01-24", "c003"),

        # Target variations
        ("t023", "TARGET STORE", 156.78, "2024-01-12", "c001"),
        ("t024", "TARGET.COM", 45.00, "2024-01-25", "c001"),
        ("t025", "TARGET #1234", 89.99, "2024-02-08", "c002"),

        # One-off transactions (should remain unclustered or form small clusters)
        ("t026", "LOCAL PIZZA SHOP", 25.00, "2024-01-20", "c001"),
        ("t027", "JOE'S GARAGE AUTO", 450.00, "2024-01-22", "c001"),
        ("t028", "CITY PARKING LOT", 15.00, "2024-01-25", "c002"),
        ("t029", "DOWNTOWN DELI", 12.50, "2024-02-01", "c003"),
        ("t030", "HARDWARE STORE INC", 67.89, "2024-02-05", "c001"),
    ]

    con.executemany("""
    INSERT INTO transactions (transaction_id, memo, amount, transaction_date, customer_id)
    VALUES (?, ?, ?, ?, ?)
    """, sample_data)

    print(f"Created {len(sample_data)} sample transactions.")


def create_new_records(con: duckdb.DuckDBPyConnection) -> None:
    """Create new records to match against the prime table."""
    print("Creating new records for inference...")

    con.execute("""
    CREATE OR REPLACE TABLE new_transactions (
        transaction_id VARCHAR PRIMARY KEY,
        memo VARCHAR,
        amount DECIMAL(10,2),
        transaction_date DATE,
        customer_id VARCHAR
    )
    """)

    new_data = [
        # Should match existing clusters
        ("n001", "NETFLIX BILLING", 15.99, "2024-04-15", "c004"),
        ("n002", "SPOTIFY MONTHLY", 9.99, "2024-04-10", "c004"),
        ("n003", "AMZN MARKETPLACE", 33.50, "2024-04-12", "c004"),
        ("n004", "UBER *TRIP", 22.00, "2024-04-08", "c004"),
        ("n005", "STARBUCKS APP", 8.50, "2024-04-05", "c004"),

        # May or may not match
        ("n006", "PRIME VIDEO", 8.99, "2024-04-01", "c004"),  # Might match Amazon
        ("n007", "SBUX REWARDS", 5.00, "2024-04-03", "c004"),  # Might match Starbucks

        # Should not match (new vendors)
        ("n008", "GROCERY MART", 125.00, "2024-04-07", "c004"),
        ("n009", "ELECTRIC COMPANY", 89.50, "2024-04-15", "c004"),
        ("n010", "WATER UTILITY", 45.00, "2024-04-15", "c004"),
    ]

    con.executemany("""
    INSERT INTO new_transactions (transaction_id, memo, amount, transaction_date, customer_id)
    VALUES (?, ?, ?, ?, ?)
    """, new_data)

    print(f"Created {len(new_data)} new transactions for matching.")


def run_basic_example():
    """Run a basic example of the Specific Affinity pipeline."""
    print("\n" + "=" * 70)
    print("EXAMPLE: Basic Entity Resolution Pipeline")
    print("=" * 70 + "\n")

    db_path = "/tmp/specific_affinity_example.duckdb"

    # Initialize the framework
    sa = SpecificAffinity(
        db_path=db_path,
        text_field="memo",
        id_field="transaction_id",
        source_table="transactions",
        similarity_threshold=0.4
    )

    with sa:
        # Create sample data
        create_sample_data(sa.con)
        create_new_records(sa.con)

        # Run the complete pipeline
        results = sa.run_pipeline(
            new_records_table="new_transactions",
            run_qa=True
        )

        # Show some cluster examples
        print("\n" + "=" * 50)
        print("CLUSTER EXAMPLES")
        print("=" * 50)

        clusters = sa.con.execute(f"""
        SELECT DISTINCT cluster_id
        FROM {sa.config.prime_table}
        WHERE cluster_id IS NOT NULL
        LIMIT 5
        """).fetchall()

        for (cluster_id,) in clusters:
            print(f"\nCluster: {cluster_id}")
            samples = sa.get_cluster_sample(cluster_id, limit=5)
            for record_id, memo in samples:
                print(f"  - [{record_id}] {memo}")

        # Test single record matching
        print("\n" + "=" * 50)
        print("SINGLE RECORD MATCHING")
        print("=" * 50)

        test_texts = [
            "NETFLIX PAYMENT",
            "SPOTIFY FAMILY PLAN",
            "RANDOM NEW VENDOR",
            "AMAZON SHOPPING"
        ]

        for text in test_texts:
            result = sa.match_text(text)
            print(f"\nQuery: '{text}'")
            if result.get("matched"):
                print(f"  Matched to cluster: {result['assigned_cluster_id']}")
                print(f"  Best match: '{result['matched_text']}'")
                print(f"  Score: {result['similarity_score']:.4f}")
            else:
                print(f"  No match found: {result.get('reason', 'Unknown')}")

    print("\n" + "=" * 70)
    print("EXAMPLE COMPLETE")
    print("=" * 70 + "\n")


def run_categorization_example():
    """Run an example with transaction categorization."""
    print("\n" + "=" * 70)
    print("EXAMPLE: Transaction Categorization")
    print("=" * 70 + "\n")

    db_path = "/tmp/specific_affinity_categorization.duckdb"

    # Initialize with categorization fields
    sa = SpecificAffinity(
        db_path=db_path,
        text_field="memo",
        id_field="transaction_id",
        source_table="transactions",
        similarity_threshold=0.4,
        amount_field="amount",
        date_field="transaction_date",
        group_field="customer_id"
    )

    with sa:
        # Create sample data
        create_sample_data(sa.con)

        # Run pipeline with categorization
        results = sa.run_pipeline(
            include_categorization=True,
            run_qa=True
        )

        # Show categorization results
        if "step_4" in results and not results["step_4"].get("skipped"):
            print("\n" + "=" * 50)
            print("CATEGORIZATION RESULTS")
            print("=" * 50)

            cat_results = sa.con.execute("""
            SELECT
                cluster_type,
                record_type,
                COUNT(*) as count
            FROM transactions_prime_categorized
            WHERE cluster_id IS NOT NULL
            GROUP BY cluster_type, record_type
            ORDER BY count DESC
            """).fetchall()

            for cluster_type, record_type, count in cat_results:
                print(f"  {cluster_type or 'N/A'} / {record_type or 'N/A'}: {count} records")

    print("\n" + "=" * 70)
    print("CATEGORIZATION EXAMPLE COMPLETE")
    print("=" * 70 + "\n")


def run_custom_stop_words_example():
    """Example showing how to customize stop words."""
    print("\n" + "=" * 70)
    print("EXAMPLE: Custom Stop Words")
    print("=" * 70 + "\n")

    db_path = "/tmp/specific_affinity_stopwords.duckdb"

    # Initialize with custom stop words
    custom_stops = {"store", "shop", "inc", "llc", "corp", "payment", "billing"}

    sa = SpecificAffinity(
        db_path=db_path,
        text_field="memo",
        id_field="transaction_id",
        source_table="transactions",
        similarity_threshold=0.4,
        stop_words=custom_stops
    )

    with sa:
        # Create sample data
        create_sample_data(sa.con)

        # Add more stop words dynamically
        sa.add_stop_words({"monthly", "subscription", "account"})

        # Run Step 1 only
        sa.create_prime_table()

        # Show token weights (custom stop words should be excluded)
        print("\nTop weighted tokens (custom stop words excluded):")
        tokens = sa.con.execute(f"""
        SELECT token, weight
        FROM {sa.config.weights_table}
        ORDER BY weight DESC
        LIMIT 10
        """).fetchall()

        for token, weight in tokens:
            print(f"  {token}: {weight:.4f}")

    print("\n" + "=" * 70)
    print("CUSTOM STOP WORDS EXAMPLE COMPLETE")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Specific Affinity Examples")
    parser.add_argument(
        "--example",
        choices=["basic", "categorization", "stopwords", "all"],
        default="basic",
        help="Which example to run"
    )

    args = parser.parse_args()

    if args.example == "basic" or args.example == "all":
        run_basic_example()

    if args.example == "categorization" or args.example == "all":
        run_categorization_example()

    if args.example == "stopwords" or args.example == "all":
        run_custom_stop_words_example()
