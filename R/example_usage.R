# =============================================================================
# Specific Affinity: R Examples
# =============================================================================
# This script demonstrates how to use the Specific Affinity framework in R
# for entity resolution and table matching.
# =============================================================================

# Load required packages
library(dplyr)
library(data.table)

# Source the main module
source("specific_affinity.R")

# =============================================================================
# EXAMPLE 1: Match Two Tables (Vendor Matching)
# =============================================================================

example_match_tables <- function() {
  cat("\n", strrep("=", 70), "\n")
  cat("EXAMPLE 1: Matching Two Tables\n")
  cat(strrep("=", 70), "\n\n")

  # Create sample reference data (master vendor list)
  master_vendors <- data.frame(
    vendor_id = c("V001", "V002", "V003", "V004", "V005"),
    vendor_name = c(
      "Netflix Inc",
      "Spotify Technology",
      "Amazon Web Services",
      "Microsoft Corporation",
      "Google Cloud Platform"
    ),
    stringsAsFactors = FALSE
  )

  # Create sample data to match (incoming transactions)
  incoming_data <- data.frame(
    record_id = c("R001", "R002", "R003", "R004", "R005", "R006"),
    merchant_name = c(
      "NETFLIX STREAMING",
      "Spotify Premium",
      "AWS Amazon",
      "MSFT Microsoft",
      "Unknown Vendor XYZ",
      "GOOGLE CLOUD"
    ),
    stringsAsFactors = FALSE
  )

  cat("Master Vendors:\n")
  print(master_vendors)
  cat("\nIncoming Data to Match:\n")
  print(incoming_data)

  # Run matching
  results <- match_tables(
    table_a = master_vendors,
    table_b = incoming_data,
    id_col_a = "vendor_id",
    id_col_b = "record_id",
    text_col_a = "vendor_name",
    text_col_b = "merchant_name",
    similarity_threshold = 0.4
  )

  # View results
  cat("\nMatch Results:\n")
  print(results$results[, .(table_b_id, merchant_name, matched_table_a_id,
                            matched_text, similarity_score, match_status)])

  return(results)
}

# =============================================================================
# EXAMPLE 2: Cluster Similar Records (Prime Table)
# =============================================================================

example_create_clusters <- function() {
  cat("\n", strrep("=", 70), "\n")
  cat("EXAMPLE 2: Clustering Similar Records\n")
  cat(strrep("=", 70), "\n\n")

  # Create sample transaction data with vendor name variations
  transactions <- data.frame(
    transaction_id = paste0("T", sprintf("%03d", 1:20)),
    memo = c(
      # Netflix variations
      "NETFLIX.COM", "NETFLIX SUBSCRIPTION", "NETFLIX INC", "NETFLIX STREAMING",
      # Spotify variations
      "SPOTIFY USA", "SPOTIFY PREMIUM", "SPOTIFY.COM",
      # Amazon variations
      "AMAZON.COM", "AMZN MKTP US", "AMAZON PRIME", "AMZN DIGITAL",
      # Uber variations
      "UBER TRIP", "UBER *EATS", "UBER TECHNOLOGIES",
      # Starbucks variations
      "STARBUCKS STORE 1234", "STARBUCKS COFFEE", "STARBUCKS #5678",
      # One-off transactions
      "LOCAL PIZZA SHOP", "JOE'S AUTO GARAGE", "RANDOM STORE"
    ),
    amount = c(15.99, 15.99, 15.99, 15.99,
               9.99, 9.99, 9.99,
               47.50, 23.99, 14.99, 4.99,
               24.50, 32.00, 18.75,
               5.75, 6.25, 4.50,
               25.00, 450.00, 15.00),
    stringsAsFactors = FALSE
  )

  cat("Sample Transactions:\n")
  print(transactions)

  # Create prime table (cluster similar records)
  result <- create_prime_table(
    df = transactions,
    id_col = "transaction_id",
    text_col = "memo",
    similarity_threshold = 0.4
  )

  # View clusters
  cat("\nClustered Results:\n")
  clustered <- result$prime_table[!is.na(cluster_id), .(transaction_id, memo, cluster_id)]
  clustered <- clustered[order(cluster_id)]
  print(clustered)

  # Show cluster summary
  cat("\nCluster Summary:\n")
  cluster_summary <- result$prime_table[!is.na(cluster_id),
                                         .(records = .N,
                                           sample_memo = first(memo)),
                                         by = cluster_id][order(-records)]
  print(cluster_summary)

  return(result)
}

# =============================================================================
# EXAMPLE 3: Match a Single Record
# =============================================================================

example_match_single <- function() {
  cat("\n", strrep("=", 70), "\n")
  cat("EXAMPLE 3: Match a Single Record\n")
  cat(strrep("=", 70), "\n\n")

  # Create reference data
  reference <- data.frame(
    id = c("V001", "V002", "V003", "V004", "V005"),
    name = c(
      "Netflix Inc",
      "Spotify Technology",
      "Amazon Web Services",
      "Microsoft Corporation",
      "Starbucks Coffee Company"
    ),
    stringsAsFactors = FALSE
  )

  # Create blocking keys and weights from reference
  blocking_keys <- create_blocking_keys(reference, "id", "name")
  weights <- calculate_weights(blocking_keys)

  # Test matching single records
  test_texts <- c(
    "NETFLIX PAYMENT",
    "Spotify Music Premium",
    "AMZN AWS",
    "Random Unknown Company"
  )

  for (text in test_texts) {
    cat(sprintf("\nQuery: '%s'\n", text))

    result <- match_single(
      text = text,
      blocking_keys = blocking_keys,
      weights = weights,
      reference_df = reference,
      id_col = "id",
      text_col = "name",
      similarity_threshold = 0.3,
      top_n = 3
    )

    if (!is.na(result$matched_id[1])) {
      print(result[, .(matched_id, matched_text, similarity_score, match_status)])
    } else {
      cat("  No matches found\n")
    }
  }
}

# =============================================================================
# EXAMPLE 4: Using dplyr Workflow
# =============================================================================

example_dplyr_workflow <- function() {
  cat("\n", strrep("=", 70), "\n")
  cat("EXAMPLE 4: dplyr Integration\n")
  cat(strrep("=", 70), "\n\n")

  # Create sample data
  vendors <- tibble(
    vendor_id = paste0("V", 1:5),
    vendor_name = c("Apple Inc", "Google LLC", "Microsoft Corp",
                    "Amazon.com Inc", "Facebook Meta")
  )

  transactions <- tibble(
    txn_id = paste0("T", 1:8),
    merchant = c("APPLE STORE", "GOOGLE PLAY", "MSFT OFFICE",
                 "AMZN SHOPPING", "META PLATFORMS", "APPLE MUSIC",
                 "UNKNOWN VENDOR", "RANDOM SHOP")
  )

  # Match and join back with dplyr
  match_result <- match_tables(
    table_a = vendors,
    table_b = transactions,
    id_col_a = "vendor_id",
    id_col_b = "txn_id",
    text_col_a = "vendor_name",
    text_col_b = "merchant",
    similarity_threshold = 0.3
  )

  # Use dplyr to analyze results
  summary_by_status <- match_result$results %>%
    as_tibble() %>%
    group_by(match_status) %>%
    summarise(
      count = n(),
      avg_score = mean(similarity_score, na.rm = TRUE),
      .groups = "drop"
    )

  cat("Summary by Match Status:\n")
  print(summary_by_status)

  # Find matched transactions with vendor info
  matched_transactions <- match_result$results %>%
    as_tibble() %>%
    filter(match_status == "MATCHED") %>%
    select(txn_id, merchant, matched_table_a_id, matched_text, similarity_score)

  cat("\nMatched Transactions:\n")
  print(matched_transactions)

  return(match_result)
}

# =============================================================================
# EXAMPLE 5: Custom Stop Words
# =============================================================================

example_custom_stop_words <- function() {
  cat("\n", strrep("=", 70), "\n")
  cat("EXAMPLE 5: Custom Stop Words\n")
  cat(strrep("=", 70), "\n\n")

  # Data with common words that might cause false matches
  companies <- data.frame(
    id = c("C001", "C002", "C003"),
    name = c(
      "International Business Solutions Inc",
      "Global Services Group LLC",
      "Technology Solutions Company"
    ),
    stringsAsFactors = FALSE
  )

  incoming <- data.frame(
    id = c("I001", "I002"),
    name = c(
      "International Technology Corp",
      "Business Group Inc"
    ),
    stringsAsFactors = FALSE
  )

  # Custom stop words for business domain
  custom_stop_words <- c(
    DEFAULT_STOP_WORDS,
    "international", "global", "group", "solutions",
    "services", "technology", "business", "company"
  )

  cat("Without custom stop words:\n")
  result1 <- match_tables(
    companies, incoming,
    "id", "id", "name", "name",
    similarity_threshold = 0.3
  )
  print(result1$results[, .(id = table_b_id, name, matched_table_a_id, similarity_score)])

  cat("\nWith custom stop words:\n")
  result2 <- match_tables(
    companies, incoming,
    "id", "id", "name", "name",
    similarity_threshold = 0.3,
    stop_words = custom_stop_words
  )
  print(result2$results[, .(id = table_b_id, name, matched_table_a_id, similarity_score)])
}

# =============================================================================
# Run Examples
# =============================================================================

run_all_examples <- function() {
  example_match_tables()
  example_create_clusters()
  example_match_single()
  example_dplyr_workflow()
  example_custom_stop_words()

  cat("\n", strrep("=", 70), "\n")
  cat("ALL EXAMPLES COMPLETE\n")
  cat(strrep("=", 70), "\n")
}

# Uncomment to run:
# run_all_examples()

# Or run individual examples:
# example_match_tables()
# example_create_clusters()
# example_match_single()
