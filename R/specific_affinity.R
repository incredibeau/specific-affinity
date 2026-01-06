# =============================================================================
# Specific Affinity: Entity Resolution Framework in R
# =============================================================================
# This module provides entity matching and clustering using text similarity.
# Uses dplyr for data manipulation and data.table for performance.
# =============================================================================

library(dplyr)
library(data.table)
library(stringr)

# -----------------------------------------------------------------------------
# Default Stop Words
# -----------------------------------------------------------------------------
DEFAULT_STOP_WORDS <- c(

"a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
"has", "he", "in", "is", "it", "its", "of", "on", "or", "that",
"the", "to", "was", "were", "will", "with", "this", "but", "they",
"have", "had", "what", "when", "where", "who", "which", "why", "how",
"all", "each", "every", "both", "few", "more", "most", "other",
"some", "such", "no", "nor", "not", "only", "own", "same", "so",
"than", "too", "very", "just", "can", "should", "now", "inc", "llc",
"corp", "ltd", "co", "www", "com", "net", "org", "company"
)

# -----------------------------------------------------------------------------
# Tokenization Functions
# -----------------------------------------------------------------------------

#' Normalize and tokenize text
#'
#' @param text Character vector of text to tokenize
#' @param min_token_length Minimum token length to keep
#' @param stop_words Character vector of stop words to remove
#' @return List of character vectors (tokens for each input)
tokenize_text <- function(text, min_token_length = 2, stop_words = DEFAULT_STOP_WORDS) {
  # Normalize: lowercase and remove special characters
  normalized <- text %>%
    tolower() %>%
    str_replace_all("[^a-z0-9\\s]", " ") %>%
    str_squish()

  # Split into tokens and filter
  tokens <- str_split(normalized, "\\s+")

  lapply(tokens, function(t) {
    t <- t[nchar(t) >= min_token_length]
    t <- t[!t %in% stop_words]
    unique(t)
  })
}

#' Create blocking keys data.table from a data frame
#'
#' @param df Data frame with id and text columns
#' @param id_col Name of the ID column
#' @param text_col Name of the text column
#' @param min_token_length Minimum token length
#' @param stop_words Stop words to exclude
#' @return data.table with record_id and token columns
create_blocking_keys <- function(df, id_col, text_col,
                                  min_token_length = 2,
                                  stop_words = DEFAULT_STOP_WORDS) {
  message("Creating blocking keys...")

  # Tokenize all text
  tokens_list <- tokenize_text(df[[text_col]], min_token_length, stop_words)

  # Create data.table with record_id and tokens
  blocking_keys <- rbindlist(lapply(seq_along(tokens_list), function(i) {
    if (length(tokens_list[[i]]) > 0) {
      data.table(
        record_id = df[[id_col]][i],
        token = tokens_list[[i]]
      )
    } else {
      NULL
    }
  }))

  # Remove duplicates
  blocking_keys <- unique(blocking_keys)

  message(sprintf("  Created %d blocking keys from %d records",
                  nrow(blocking_keys), nrow(df)))

  return(blocking_keys)
}

# -----------------------------------------------------------------------------
# Weight Calculation
# -----------------------------------------------------------------------------

#' Calculate TF-IDF style weights for tokens
#'
#' @param blocking_keys data.table with record_id and token columns
#' @return data.table with token and weight columns
calculate_weights <- function(blocking_keys) {
  message("Calculating token weights...")

  # Convert to data.table if needed
  blocking_keys <- as.data.table(blocking_keys)

  # Count token frequencies
  total_records <- blocking_keys[, uniqueN(record_id)]

  token_counts <- blocking_keys[, .(freq = .N), by = token]
  token_counts[, raw_frequency := freq / total_records]

  # Calculate average frequency
  avg_freq <- mean(token_counts$raw_frequency)

  # Calculate weights (inverse frequency)
  token_counts[, weight := avg_freq / raw_frequency]
  token_counts[, log_weight := log(weight)]

  # Normalize weights to 0-1 range
  min_weight <- min(token_counts$log_weight, na.rm = TRUE)
  max_weight <- max(token_counts$log_weight, na.rm = TRUE)

  if (max_weight > min_weight) {
    token_counts[, normalized_weight := (log_weight - min_weight) / (max_weight - min_weight)]
  } else {
    token_counts[, normalized_weight := 0.5]
  }

  # Return clean weights table
  weights <- token_counts[, .(token, weight = round(normalized_weight, 4))]

  message(sprintf("  Calculated weights for %d unique tokens", nrow(weights)))

  return(weights)
}

# -----------------------------------------------------------------------------
# Matching Functions
# -----------------------------------------------------------------------------

#' Find candidate pairs using blocking keys (self-join for clustering)
#'
#' @param blocking_keys data.table with record_id and token columns
#' @param weights data.table with token and weight columns
#' @return data.table with record_id_1, record_id_2, similarity_score
find_candidate_pairs <- function(blocking_keys, weights) {
  message("Finding candidate pairs...")

  # Convert to data.table
  blocking_keys <- as.data.table(blocking_keys)
  weights <- as.data.table(weights)

  # Join blocking keys with weights
  bk_weighted <- merge(blocking_keys, weights, by = "token", all.x = FALSE)

  # Self-join on token to find pairs
  setkey(bk_weighted, token)

  pairs <- bk_weighted[bk_weighted,
                       on = "token",
                       allow.cartesian = TRUE,
                       nomatch = 0L][record_id < i.record_id]

  # Sum weights for each pair
  candidate_pairs <- pairs[, .(similarity_score = sum(weight)),
                           by = .(record_id_1 = record_id, record_id_2 = i.record_id)]

  message(sprintf("  Found %d candidate pairs", nrow(candidate_pairs)))

  return(candidate_pairs)
}

#' Find matches between two tables
#'
#' @param blocking_keys_a data.table - blocking keys for reference table
#' @param blocking_keys_b data.table - blocking keys for table to match
#' @param weights data.table - token weights (from reference table)
#' @return data.table with table_b_id, table_a_id, similarity_score
find_matches_between_tables <- function(blocking_keys_a, blocking_keys_b, weights) {
  message("Finding matches between tables...")

  # Convert to data.table
  blocking_keys_a <- as.data.table(blocking_keys_a)
  blocking_keys_b <- as.data.table(blocking_keys_b)
  weights <- as.data.table(weights)

  # Add weights to both tables
  bk_a <- merge(blocking_keys_a, weights, by = "token", all.x = FALSE)
  bk_b <- merge(blocking_keys_b, weights, by = "token", all.x = FALSE)

  # Join on token
  setkey(bk_a, token)
  setkey(bk_b, token)

  matches <- bk_a[bk_b, on = "token", allow.cartesian = TRUE, nomatch = 0L]

  # Sum weights for each pair
  candidate_matches <- matches[, .(similarity_score = sum(weight)),
                               by = .(table_a_id = record_id, table_b_id = i.record_id)]

  message(sprintf("  Found %d candidate matches", nrow(candidate_matches)))

  return(candidate_matches)
}

# -----------------------------------------------------------------------------
# Connected Components (Clustering)
# -----------------------------------------------------------------------------

#' Find connected components using iterative label propagation
#'
#' @param pairs data.table with record_id_1 and record_id_2 columns
#' @return data.table with record_id and cluster_id columns
find_connected_components <- function(pairs) {
  message("Finding connected components...")

  if (nrow(pairs) == 0) {
    message("  No pairs to cluster")
    return(data.table(record_id = character(), cluster_id = character()))
  }

  # Get all unique nodes
  nodes <- unique(c(pairs$record_id_1, pairs$record_id_2))

  # Initialize each node in its own cluster
  clusters <- data.table(
    record_id = nodes,
    cluster_id = nodes
  )
  setkey(clusters, record_id)

  # Iteratively propagate minimum cluster_id
  changed <- TRUE
  iteration <- 0

  while (changed && iteration < 100) {
    iteration <- iteration + 1
    changed <- FALSE

    # For each pair, propagate the minimum cluster_id
    for (i in 1:nrow(pairs)) {
      id1 <- pairs$record_id_1[i]
      id2 <- pairs$record_id_2[i]

      cluster1 <- clusters[record_id == id1, cluster_id]
      cluster2 <- clusters[record_id == id2, cluster_id]

      min_cluster <- min(cluster1, cluster2)

      if (cluster1 != min_cluster || cluster2 != min_cluster) {
        # Update all nodes in both clusters to the minimum
        old_clusters <- c(cluster1, cluster2)
        clusters[cluster_id %in% old_clusters, cluster_id := min_cluster]
        changed <- TRUE
      }
    }
  }

  message(sprintf("  Found %d clusters in %d iterations",
                  uniqueN(clusters$cluster_id), iteration))

  return(clusters)
}

# -----------------------------------------------------------------------------
# Main Matching Functions
# -----------------------------------------------------------------------------

#' Create a prime table by clustering similar records
#'
#' @param df Data frame to cluster
#' @param id_col Name of the ID column
#' @param text_col Name of the text column
#' @param similarity_threshold Minimum similarity score for matches
#' @param min_token_length Minimum token length
#' @param stop_words Stop words to exclude
#' @return List with prime_table, blocking_keys, weights, and stats
create_prime_table <- function(df, id_col, text_col,
                                similarity_threshold = 0.5,
                                min_token_length = 2,
                                stop_words = DEFAULT_STOP_WORDS) {

  message("\n", paste(rep("=", 60), collapse = ""))
  message("CREATING PRIME TABLE")
  message(paste(rep("=", 60), collapse = ""), "\n")

  # Create blocking keys
  blocking_keys <- create_blocking_keys(df, id_col, text_col,
                                        min_token_length, stop_words)

  # Calculate weights
  weights <- calculate_weights(blocking_keys)

  # Find candidate pairs
  candidate_pairs <- find_candidate_pairs(blocking_keys, weights)

  # Filter by threshold
  message(sprintf("Filtering pairs with threshold >= %.2f...", similarity_threshold))
  filtered_pairs <- candidate_pairs[similarity_score >= similarity_threshold]
  message(sprintf("  Kept %d pairs above threshold", nrow(filtered_pairs)))

  # Find connected components
  clusters <- find_connected_components(filtered_pairs)

  # Create prime table
  prime_table <- as.data.table(df)
  prime_table <- merge(prime_table, clusters,
                       by.x = id_col, by.y = "record_id",
                       all.x = TRUE)

  # Calculate stats
  stats <- list(
    total_records = nrow(prime_table),
    clustered_records = sum(!is.na(prime_table$cluster_id)),
    unclustered_records = sum(is.na(prime_table$cluster_id)),
    total_clusters = uniqueN(prime_table$cluster_id, na.rm = TRUE),
    avg_cluster_size = if (any(!is.na(prime_table$cluster_id))) {
      mean(table(prime_table$cluster_id[!is.na(prime_table$cluster_id)]))
    } else NA
  )

  message("\n", paste(rep("-", 40), collapse = ""))
  message("SUMMARY:")
  message(sprintf("  Total records: %d", stats$total_records))
  message(sprintf("  Clustered: %d", stats$clustered_records))
  message(sprintf("  Unclustered: %d", stats$unclustered_records))
  message(sprintf("  Total clusters: %d", stats$total_clusters))
  message(sprintf("  Avg cluster size: %.2f", stats$avg_cluster_size))
  message(paste(rep("-", 40), collapse = ""), "\n")

  return(list(
    prime_table = prime_table,
    blocking_keys = blocking_keys,
    weights = weights,
    filtered_pairs = filtered_pairs,
    stats = stats
  ))
}

#' Match records from table B against table A
#'
#' @param table_a Reference data frame
#' @param table_b Data frame to match
#' @param id_col_a ID column name in table A
#' @param id_col_b ID column name in table B
#' @param text_col_a Text column name in table A
#' @param text_col_b Text column name in table B
#' @param similarity_threshold Minimum similarity score
#' @param min_token_length Minimum token length
#' @param stop_words Stop words to exclude
#' @return List with results, stats, and intermediate data
match_tables <- function(table_a, table_b,
                         id_col_a, id_col_b,
                         text_col_a, text_col_b,
                         similarity_threshold = 0.5,
                         min_token_length = 2,
                         stop_words = DEFAULT_STOP_WORDS) {

  message("\n", paste(rep("=", 60), collapse = ""))
  message("MATCHING TABLES")
  message(paste(rep("=", 60), collapse = ""), "\n")

  # Create blocking keys for both tables
  blocking_keys_a <- create_blocking_keys(table_a, id_col_a, text_col_a,
                                          min_token_length, stop_words)
  blocking_keys_b <- create_blocking_keys(table_b, id_col_b, text_col_b,
                                          min_token_length, stop_words)

  # Calculate weights from table A (reference)
  weights <- calculate_weights(blocking_keys_a)

  # Find matches
  candidate_matches <- find_matches_between_tables(blocking_keys_a,
                                                    blocking_keys_b,
                                                    weights)

  # Rank matches for each table_b record
  message("Ranking matches...")
  candidate_matches <- candidate_matches[order(table_b_id, -similarity_score)]
  candidate_matches[, rank := seq_len(.N), by = table_b_id]

  # Keep best match above threshold
  best_matches <- candidate_matches[rank == 1 & similarity_score >= similarity_threshold]

  # Create results table
  results <- as.data.table(table_b)
  setnames(results, id_col_b, "table_b_id")

  results <- merge(results,
                   best_matches[, .(table_b_id, matched_table_a_id = table_a_id, similarity_score)],
                   by = "table_b_id",
                   all.x = TRUE)

  # Add matched text from table A
  table_a_lookup <- as.data.table(table_a)[, c(id_col_a, text_col_a), with = FALSE]
  setnames(table_a_lookup, c("matched_table_a_id", "matched_text"))

  results <- merge(results, table_a_lookup, by = "matched_table_a_id", all.x = TRUE)

  # Add match status
  results[, match_status := ifelse(is.na(matched_table_a_id), "UNMATCHED", "MATCHED")]

  # Calculate stats
  stats <- list(
    total_records = nrow(results),
    matched = sum(results$match_status == "MATCHED"),
    unmatched = sum(results$match_status == "UNMATCHED"),
    match_rate_pct = round(sum(results$match_status == "MATCHED") / nrow(results) * 100, 2),
    avg_similarity = mean(results$similarity_score[results$match_status == "MATCHED"], na.rm = TRUE)
  )

  message("\n", paste(rep("-", 40), collapse = ""))
  message("SUMMARY:")
  message(sprintf("  Total records: %d", stats$total_records))
  message(sprintf("  Matched: %d (%.1f%%)", stats$matched, stats$match_rate_pct))
  message(sprintf("  Unmatched: %d", stats$unmatched))
  message(sprintf("  Avg similarity: %.4f", stats$avg_similarity))
  message(paste(rep("-", 40), collapse = ""), "\n")

  return(list(
    results = results,
    candidate_matches = candidate_matches,
    blocking_keys_a = blocking_keys_a,
    blocking_keys_b = blocking_keys_b,
    weights = weights,
    stats = stats
  ))
}

#' Match a single text value against a reference table
#'
#' @param text Text to match
#' @param blocking_keys Reference blocking keys
#' @param weights Token weights
#' @param reference_df Reference data frame (for retrieving matched text)
#' @param id_col ID column name in reference
#' @param text_col Text column name in reference
#' @param similarity_threshold Minimum similarity
#' @param top_n Number of top matches to return
#' @return data.table with top matches
match_single <- function(text, blocking_keys, weights, reference_df,
                         id_col, text_col,
                         similarity_threshold = 0.5,
                         top_n = 5,
                         stop_words = DEFAULT_STOP_WORDS) {

  # Tokenize the input
  tokens <- tokenize_text(text, stop_words = stop_words)[[1]]

  if (length(tokens) == 0) {
    return(data.table(
      matched_id = NA,
      matched_text = NA,
      similarity_score = NA,
      match_status = "NO_TOKENS"
    ))
  }

  # Create blocking keys for input
  query_keys <- data.table(record_id = "query", token = tokens)

  # Find matches
  blocking_keys <- as.data.table(blocking_keys)
  weights <- as.data.table(weights)

  bk_weighted <- merge(blocking_keys, weights, by = "token")
  query_weighted <- merge(query_keys, weights, by = "token")

  matches <- merge(bk_weighted, query_weighted, by = "token", allow.cartesian = TRUE)

  if (nrow(matches) == 0) {
    return(data.table(
      matched_id = NA,
      matched_text = NA,
      similarity_score = NA,
      match_status = "NO_MATCHES"
    ))
  }

  # Sum weights
  scores <- matches[, .(similarity_score = sum(weight.x)), by = record_id]
  scores <- scores[order(-similarity_score)][1:min(top_n, nrow(scores))]

  # Add text from reference
  ref_lookup <- as.data.table(reference_df)[, c(id_col, text_col), with = FALSE]
  setnames(ref_lookup, c("record_id", "matched_text"))

  scores <- merge(scores, ref_lookup, by = "record_id")
  setnames(scores, "record_id", "matched_id")

  scores[, match_status := ifelse(similarity_score >= similarity_threshold, "MATCHED", "BELOW_THRESHOLD")]

  return(scores)
}
