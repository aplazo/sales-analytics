# ============================================================================
# BigQuery Connection Utilities for Causal Impact Analysis
# ============================================================================
# This file provides helper functions for connecting to BigQuery and 
# fetching loan data for the Steve Madden Credit Booster analysis.
# ============================================================================

# Required packages
required_packages <- c("bigrquery", "DBI", "dplyr", "tidyr", "lubridate")

# Install missing packages
install_if_missing <- function(packages) {
  new_packages <- packages[!(packages %in% installed.packages()[, "Package"])]
  if (length(new_packages) > 0) {
    install.packages(new_packages, repos = "https://cloud.r-project.org/")
  }
}

install_if_missing(required_packages)

# Load packages
library(bigrquery)
library(DBI)
library(dplyr)
library(tidyr)
library(lubridate)

# ============================================================================
# Configuration
# ============================================================================

#' Set BigQuery Project ID
#' @param project_id Your Google Cloud project ID
#' @return The project ID (invisibly)
set_bq_project <- function(project_id) {
  options(bigquery.project = project_id)
  invisible(project_id)
}

#' Get BigQuery Project ID
#' @return The configured project ID
get_bq_project <- function() {
  getOption("bigquery.project")
}

# ============================================================================
# Authentication
# ============================================================================

#' Authenticate with BigQuery using OAuth
#' @param email Optional email to use for authentication
#' @param use_oob Use out-of-band auth (works better in Jupyter notebooks)
#' @param cache Whether to cache credentials
#' @return Invisible NULL
authenticate_bq <- function(email = NULL, use_oob = TRUE, cache = TRUE) {
  # For Jupyter notebooks, use_oob = TRUE works better
  # It will print a URL that you can open in your browser
  # Then paste the authorization code back
  
  tryCatch({
    # Try with gargle options for OOB
    if (use_oob) {
      options(gargle_oob_default = TRUE)
    }
    
    bq_auth(email = email, cache = cache)
    message("Successfully authenticated with BigQuery")
  }, error = function(e) {
    message("Standard auth failed. Trying alternative methods...")
    message("Error was: ", e$message)
    
    # Provide manual instructions
    message("\n=== MANUAL AUTHENTICATION STEPS ===")
    message("1. Run this in your R console (not notebook):")
    message("   library(bigrquery)")
    message("   bq_auth()")
    message("2. Complete the browser authentication")
    message("3. Return to this notebook and run the cells again")
    message("===================================\n")
    
    stop("Authentication failed. See instructions above.")
  })
  
  invisible(NULL)
}

#' Check if authenticated
#' @return TRUE if authenticated, FALSE otherwise
is_bq_authenticated <- function() {
  tryCatch({
    bq_has_token()
  }, error = function(e) {
    FALSE
  })
}

# ============================================================================
# Data Fetching
# ============================================================================

#' Build the loan data query
#' @param start_date Start date for the query (character or Date)
#' @param end_date End date for the query (character or Date, default: current_date - 23 days)
#' @param exclude_merchants Vector of merchant IDs to exclude from control group (e.g., merchants with credit boosters)
#' @return SQL query string
build_loan_query <- function(start_date = "2025-01-01", end_date = NULL, exclude_merchants = NULL) {
  
  # Format dates
  start_date <- as.character(as.Date(start_date))
  
  if (is.null(end_date)) {
    end_date_clause <- "date_sub(current_date('Mexico/General'), interval 23 day)"
  } else {
    end_date <- as.character(as.Date(end_date))
    end_date_clause <- sprintf('"%s"', end_date)
  }
  
  # Build exclusion clause for merchants with credit boosters
  # Note: We exclude from the raw query to avoid contaminating control group
  # Steve Madden (4896) is handled separately via holding_id = 9
  if (!is.null(exclude_merchants) && length(exclude_merchants) > 0) {
    # Exclude from control group only (holding_id != 9 means not Steve Madden)
    exclude_clause <- sprintf("AND (holding_id = 9 OR merchant_id NOT IN (%s))", 
                              paste(exclude_merchants, collapse = ", "))
  } else {
    exclude_clause <- ""
  }
  
  query <- sprintf('
SELECT
  loan_request_date_local,
  CASE WHEN holding_id = 9 THEN "Steve Madden (4896)" ELSE merchant_name_with_id END AS merchant_id,
  CASE WHEN holding_id = 9 THEN "treatment" ELSE "control" END AS merchant_assignment,
  COUNTIF(loan_is_originated_not_canceled) AS loans_originated,
  SUM(loan_net_profit_proxy_at_22_dob_predicted) AS npp,
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(loan_delinquent_principal_at_22_dob_predicted > 0),
      COUNTIF(loan_is_originated_not_canceled)
    ), 3) AS loan_dq_ratio
FROM `analytics.obt_loan`
WHERE TRUE
  AND loan_request_date_local >= "%s"
  AND loan_request_date_local <= %s
  AND merchant_business_line = "BNPL"
  AND merchant_channel = "Online"
  %s
GROUP BY ALL
ORDER BY loan_request_date_local DESC
', start_date, end_date_clause, exclude_clause)
  
  return(query)
}

#' Fetch loan data from BigQuery
#' @param start_date Start date for the query
#' @param end_date End date for the query (optional)
#' @param project_id BigQuery project ID (uses default if not specified)
#' @param exclude_merchants Vector of merchant IDs to exclude from control group
#' @return Data frame with loan data
fetch_loan_data <- function(start_date = "2025-01-01", end_date = NULL, project_id = NULL, exclude_merchants = NULL) {
  
  # Use default project if not specified
  if (is.null(project_id)) {
    project_id <- get_bq_project()
  }
  
  if (is.null(project_id)) {
    stop("No project ID specified. Use set_bq_project() to set a default.")
  }
  
  # Check authentication
  if (!is_bq_authenticated()) {
    message("Not authenticated. Initiating OAuth flow...")
    authenticate_bq()
  }
  
  # Build and execute query (excluding specified merchants from control)
  query <- build_loan_query(start_date, end_date, exclude_merchants)
  
  if (!is.null(exclude_merchants) && length(exclude_merchants) > 0) {
    message(sprintf("Excluding %d merchants with credit boosters from control group", 
                    length(exclude_merchants)))
  }
  
  message("Fetching data from BigQuery...")
  
  data <- bq_project_query(project_id, query) %>%
    bq_table_download()
  
  # Convert date column
  data$loan_request_date_local <- as.Date(data$loan_request_date_local)
  
  # Fix encoding for character columns (Spanish accents, etc.)
  char_cols <- sapply(data, is.character)
  if (any(char_cols)) {
    data[char_cols] <- lapply(data[char_cols], function(x) {
      # Ensure UTF-8 encoding
      x <- enc2utf8(x)
      # Replace any problematic characters
      iconv(x, from = "UTF-8", to = "UTF-8", sub = "")
    })
  }
  
  message(sprintf("Fetched %d rows of data", nrow(data)))
  
  return(as.data.frame(data))
}

# ============================================================================
# Data Transformation
# ============================================================================

#' Pivot data to wide format for CausalImpact
#' @param data Data frame from fetch_loan_data()
#' @param metric Column name of the metric to analyze ("loans_originated", "npp", or "loan_dq_ratio")
#' @param treatment_id Merchant ID for treatment group (default: 4896 for Steve Madden)
#' @return Wide-format data frame with date as rows and merchants as columns
pivot_for_causal_impact <- function(data, metric = "loans_originated", treatment_id = 4896) {
  
  # Validate metric
  valid_metrics <- c("loans_originated", "npp", "loan_dq_ratio")
  if (!metric %in% valid_metrics) {
    stop(sprintf("Invalid metric. Choose from: %s", paste(valid_metrics, collapse = ", ")))
  }
  
  # Sanitize merchant_id to remove special characters before pivoting
  data <- data %>%
    mutate(merchant_id_clean = sanitize_colnames(as.character(merchant_id)))
  
  # Aggregate by date and merchant
  agg_data <- data %>%
    group_by(loan_request_date_local, merchant_id_clean) %>%
    summarise(
      value = sum(!!sym(metric), na.rm = TRUE),
      .groups = "drop"
    )
  
  # Pivot to wide format
  wide_data <- agg_data %>%
    pivot_wider(
      id_cols = loan_request_date_local,
      names_from = merchant_id_clean,
      values_from = value,
      names_prefix = "merchant_",
      values_fill = 0
    ) %>%
    arrange(loan_request_date_local)
  
  # Ensure all column names are valid R names
  names(wide_data) <- make.names(names(wide_data), unique = TRUE)
  
  # Rename treatment column for clarity
  treatment_col_clean <- paste0("merchant_", sanitize_colnames(as.character(treatment_id)))
  if (treatment_col_clean %in% names(wide_data)) {
    names(wide_data)[names(wide_data) == treatment_col_clean] <- "steve_madden"
  }
  
  return(as.data.frame(wide_data))
}

#' Get treatment (Steve Madden) data
#' @param data Data frame from fetch_loan_data()
#' @param metric Column name of the metric
#' @return Data frame with only treatment data
get_treatment_data <- function(data, metric = "loans_originated") {
  data %>%
    filter(merchant_assignment == "treatment") %>%
    group_by(loan_request_date_local) %>%
    summarise(
      value = sum(!!sym(metric), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(loan_request_date_local)
}

#' Get control merchants data
#' @param data Data frame from fetch_loan_data()
#' @param metric Column name of the metric
#' @return Data frame with control merchants data in wide format
get_control_data <- function(data, metric = "loans_originated") {
  result <- data %>%
    filter(merchant_assignment == "control") %>%
    # Sanitize merchant_id to remove special characters before pivoting
    mutate(merchant_id_clean = sanitize_colnames(as.character(merchant_id))) %>%
    group_by(loan_request_date_local, merchant_id_clean) %>%
    summarise(
      value = sum(!!sym(metric), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    pivot_wider(
      id_cols = loan_request_date_local,
      names_from = merchant_id_clean,
      values_from = value,
      names_prefix = "merchant_",
      values_fill = 0
    ) %>%
    arrange(loan_request_date_local)
  
  # Ensure all column names are valid R names
  names(result) <- make.names(names(result), unique = TRUE)
  
  return(result)
}

# ============================================================================
# Utility Functions
# ============================================================================

#' Sanitize strings for use as column names (removes accents and special chars)
#' @param x Character vector to sanitize
#' @return Sanitized character vector safe for column names
sanitize_colnames <- function(x) {
  x <- as.character(x)
  
  # Use iconv to transliterate accented characters to ASCII
  # This handles á->a, é->e, ñ->n, etc. automatically
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT", sub = "")
  
  # If iconv fails (returns NA), try alternative approach
  na_idx <- is.na(x)
  if (any(na_idx)) {
    # For failed conversions, just remove non-ASCII characters
    original <- as.character(x)
    x[na_idx] <- gsub("[^\x01-\x7F]", "", original[na_idx])
  }
  
  # Replace any remaining non-alphanumeric chars (except underscore) with underscore
  x <- gsub("[^a-zA-Z0-9_]", "_", x)
  
  # Remove multiple consecutive underscores
  x <- gsub("_+", "_", x)
  
  # Remove leading/trailing underscores
  x <- gsub("^_|_$", "", x)
  
  # Handle empty strings
  x[x == ""] <- "unknown"
  
  return(x)
}

#' Fill missing dates in time series
#' @param data Data frame with date column
#' @param date_col Name of date column
#' @param fill_value Value to fill for missing dates (default: 0)
#' @return Data frame with complete date sequence
fill_missing_dates <- function(data, date_col = "loan_request_date_local", fill_value = 0) {
  
  date_range <- seq(
    min(data[[date_col]]),
    max(data[[date_col]]),
    by = "day"
  )
  
  complete_dates <- data.frame(date = date_range)
  names(complete_dates) <- date_col
  
  result <- complete_dates %>%
    left_join(data, by = date_col)
  
  # Fill NAs with fill_value for numeric columns
  numeric_cols <- sapply(result, is.numeric)
  result[, numeric_cols][is.na(result[, numeric_cols])] <- fill_value
  
  return(result)
}

#' Summary of data quality
#' @param data Data frame to check
#' @return Summary statistics
data_quality_summary <- function(data) {
  list(
    n_rows = nrow(data),
    n_cols = ncol(data),
    date_range = range(data$loan_request_date_local),
    n_merchants = length(unique(data$merchant_id)),
    n_treatment = sum(data$merchant_assignment == "treatment"),
    n_control = sum(data$merchant_assignment == "control"),
    missing_values = colSums(is.na(data))
  )
}

message("BigQuery helpers loaded successfully!")

