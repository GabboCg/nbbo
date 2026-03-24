#!/usr/bin/env Rscript
# ======================================================== #
#
#                     Download NBBO TAQ
#
#                 Gabriel E. Cabrera-Guzmán
#                The University of Manchester
#
#                       Spring, 2026
#
#                https://gcabrerag.rbind.io
#
# ------------------------------ #
# email: gabriel.cabreraguzman@postgrad.manchester.ac.uk
# ======================================================== #

# Load packages
library(tidyverse)  
library(DBI)        
library(RPostgres)   
library(data.table)  

# Create connection
# Set WRDS_USER and WRDS_PASSWORD in ~/.Renviron or as environment variables
wrds <- dbConnect(
    Postgres(),
    host = "wrds-pgdata.wharton.upenn.edu",
    dbname = "wrds",
    port = 9737,
    sslmode = "require",
    user = Sys.getenv("WRDS_USER"),
    password = Sys.getenv("WRDS_PASSWORD")
)

# ==========================================
#                 Parameters
# ------------------------------------------

# Helpers
source("R/taq-cleaning.R")

# This data was delivered by Lancaster University Management School (LUMS)
# Remove consecutive duplicate NBBO states? set TRUE/FALSE
flag_remove_dupe_nbbo_states <- FALSE

# Symbols and dates to pull
SYMS  <- c("AAPL")
DATES <- c("2023-05-04")  # YYYY-MM-DD

nbbo_tbls <- paste0("complete_nbbo_", to_yyyymmdd(DATES))
cqm_tbls  <- paste0("cqm_",          to_yyyymmdd(DATES))
ctm_tbls  <- paste0("ctm_",          to_yyyymmdd(DATES))

# ==========================================
#   Retrieve Daily NBBO / QUOTES / TRADES
# ------------------------------------------

# Pull data from WRDS using dbplyr (lazy SQL evaluation)
# Filter:
#   - desired ticker
#   - common shares only (sym_suffix blank or NULL)
#   - wide intraday window (04:00–20:00)

res <- tryCatch({
    
    nbbo_sql <- tbl(wrds, dbplyr::in_schema("taqmsec", nbbo_tbls)) |>
        filter(
            sym_root == SYMS,
            is.na(sym_suffix) | sym_suffix == "",
            time_m >= "04:00:00" & time_m <= "20:00:00"
        )
    
    cqm_sql <- tbl(wrds, dbplyr::in_schema("taqmsec", cqm_tbls)) |>
        filter(
            sym_root == SYMS,
            is.na(sym_suffix) | sym_suffix == "",
            time_m >= "04:00:00" & time_m <= "20:00:00"
        )
    
    ctm_sql <- tbl(wrds, dbplyr::in_schema("taqmsec", ctm_tbls)) |>
        filter(
            sym_root == SYMS,
            is.na(sym_suffix) | sym_suffix == "",
            time_m >= "04:00:00" & time_m <= "20:00:00"
        )
    
    list(
        DailyNBBO  = nbbo_sql |> collect() |> as.data.table(),
        DailyQuote = cqm_sql  |> collect() |> as.data.table(),
        DailyTrade = ctm_sql  |> collect() |> as.data.table()
    )
    
}, error = function(e) {
    
    message("Error at row ", j, " (", SYMS, " / ", DATES, "): ", e$message)
    NULL
    
})

DailyNBBO  <- res$DailyNBBO
DailyQuote <- res$DailyQuote
DailyTrade <- res$DailyTrade

# Step 2–4: Clean NBBO (regular strict; pre/post relaxed)
NBBO2 <- clean_nbbo_sessioned(DailyNBBO, flag_remove_dupes = flag_remove_dupe_nbbo_states)

# Step 5: Clean Quotes (regular strict; pre/post relaxed)
quoteAB_clean <- clean_quotes_sessioned(DailyQuote)

# Step 6: Clean Trades (regular base; pre/post extra tr_scon exclusions)
trade2 <- clean_trades_sessioned(DailyTrade)

# Step 7: Build Official Complete NBBO
OfficialCompleteNBBO <- build_official_complete_nbbo(NBBO2, quoteAB_clean)

# Step 8: Match Trades To Prior NBBO (rolling join, stable)
TradesandCorrespondingNBBO <- match_trades_to_nbbo(OfficialCompleteNBBO, trade2)

# Step 9: Buy/Sell Classification
BuySellIndicators <- classify_buysell(TradesandCorrespondingNBBO)

# Final output formatting (match your existing output)
clean_complete_nbbo <- BuySellIndicators |>
    as_tibble() |>
    rename(
        DATE = date,
        TIME_M = time_m,
        SYM_ROOT = sym_root,
        EX = ex,
        SIZE = size,
        PRICE = price
    ) |>
    select(
        DATE, TIME_M, SYM_ROOT, EX, SIZE, PRICE,
        NBO, NBB, NBOqty, NBBqty, midpoint,
        BuySellLR, BuySellEMO, BuySellCLNV
    ) |>
    mutate(
        # Store TIME_M numeric like your previous code (seconds since midnight)
        TIME_M = as.numeric(hms::as_hms(TIME_M)),
        # Convert shares back to round-lots in output like you had (divide by 100)
        NBOqty = NBOqty / 100,
        NBBqty = NBBqty / 100
    )

# Export
write_csv(clean_complete_nbbo, "AAPL_20230504.csv")
    