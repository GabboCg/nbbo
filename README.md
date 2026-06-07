# NBBO: National Best Bid and Offer Tick-by-Tick

[![R](https://img.shields.io/badge/R-%3E%3D4.1-276DC3?logo=r&logoColor=white)](https://www.r-project.org/)
[![WRDS](https://img.shields.io/badge/Data-WRDS-003366)](https://wrds-www.wharton.upenn.edu/)

An R implementation that downloads and cleans millisecond TAQ data from WRDS and signs each trade as a buy or sell using three standard algorithms (LR, EMO, CLNV).

## Overview

This R pipeline pulls tick-by-tick TAQ millisecond data from WRDS, cleans quotes and trades following Holden & Jacobsen (2014, JF), reconstructs the official NBBO, and matches each trade to the prevailing quote. It then signs trades as buys or sells using three standard algorithms (LR, EMO, CLNV). Pre- and post-market hours are handled with the relaxed rules from Grégoire & Martineau (2022, JAR).

## Data Source

Data is pulled from WRDS (`taqmsec` schema, PostgreSQL). Requires a WRDS account with TAQ millisecond access.

Three table types per trading day:
- `complete_nbbo_YYYYMMDD` — National Best Bid and Offer
- `cqm_YYYYMMDD` — Consolidated Quote (millisecond)
- `ctm_YYYYMMDD` — Consolidated Trade (millisecond)

## Methodology

Cleaning and trade classification are based on Holden & Jacobsen (2014, JF). The Holden & Jacobsen (2014, JF) methodology targets regular trading hours (09:30–16:00 ET). For pre/post (after-hours) trading, we follow the four adaptations suggested by Grégoire & Martineau (2022, JAR), as suggested in their repository at [vgreg/earnings_news_jar](https://github.com/vgreg/earnings_news_jar):

1. Add closing quote condition (`C`) to valid quote types
2. Preserve empty/withdrawn quotes rather than deleting them (better to keep potentially problematic updates than discard them in thin markets)
3. Skip the spread outlier rule (step 3) — wide spreads are normal outside regular hours
4. Retain crossed quotes without filtering

## Usage

Add your WRDS credentials to `~/.Renviron` (never commit this file):

```
WRDS_USER=your_username
WRDS_PASSWORD=your_password
```

Then set tickers and dates in `load.R` and run:

```r
source("load.R")
```

```r
# Parameters to configure
SYMS  <- c("AAPL")
DATES <- c("2023-05-04")               # YYYY-MM-DD
flag_remove_dupe_nbbo_states <- FALSE  # remove consecutive duplicate NBBO states?
```

Output is written as a CSV (`<SYM>_<YYYYMMDD>.csv`). See `AAPL_20230504.csv` for a sample.

## Processing Pipeline

| Step | Function | Description |
|------|----------|-------------|
| 2–4 | `clean_nbbo_sessioned()` | Filter quote conditions; invalidate bad sides; outlier rule (regular session only) |
| 5 | `clean_quotes_sessioned()` | Strict filters for regular hours; relaxed (+ cond `C`) for pre/post |
| 6 | `clean_trades_sessioned()` | Require `tr_corr == "00"`; exclude extra `tr_scond` codes in pre/post |
| 7 | `build_official_complete_nbbo()` | Merge cleaned NBBO with eligible CQM quotes; deduplicate by timestamp |
| 8 | `match_trades_to_nbbo()` | Rolling join attaching the most recent prior NBBO to each trade |
| 9 | `classify_buysell()` | LR (Lee-Ready), EMO (Ellis-Michaely-O'Hara), CLNV (Chakrabarty et al.) |

Sessions: `pre` (< 09:30 ET), `regular` (09:30–16:00), `post` (> 16:00). Cleaning rules are stricter during regular hours.

## Output Columns

| Column | Description |
|--------|-------------|
| `DATE` | Trade date |
| `TIME_M` | Seconds since midnight |
| `SYM_ROOT` | Ticker symbol |
| `EX` | Exchange code |
| `SIZE` | Trade size (shares) |
| `PRICE` | Trade price |
| `NBO` / `NBB` | National Best Offer / Bid at trade time |
| `NBOqty` / `NBBqty` | Quoted sizes (round-lots) |
| `midpoint` | Quote midpoint |
| `BuySellLR` | Lee-Ready: `1` buy, `-1` sell |
| `BuySellEMO` | Ellis-Michaely-O'Hara: `1` buy, `-1` sell |
| `BuySellCLNV` | Chakrabarty et al.: `1` buy, `-1` sell |

## Dependencies

```r
install.packages(c("tidyverse", "DBI", "RPostgres", "data.table", "hms", "dbplyr"))
```

## References

- Holden, C. W., & Jacobsen, S. (2014). Liquidity Measurement Problems in Fast, Competitive Markets: Expensive and Cheap Solutions. *The Journal of Finance*, 69(4), 1747–1785. 
- Grégoire, V., & Martineau, C. (2022). How is Earnings News Transmitted to Stock Prices? *Journal of Accounting Research*, 60(1), 261–297. 
