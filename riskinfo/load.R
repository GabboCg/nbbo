# ======================================================== #
#
#                      Project Loader
#
#                 Gabriel E. Cabrera Guzmán
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
library(DBI)
library(RPostgres)
library(dplyr)
library(tidyr)
library(lubridate)
library(broom)
library(bizdays)

# Connection
wrds <- dbConnect(
  RPostgres::Postgres(),
  host     = "wrds-pgdata.wharton.upenn.edu",
  port     = 9737L,
  dbname   = "wrds",
  sslmode  = "require",
  user     = Sys.getenv("WRDS_USER"),
  password = Sys.getenv("WRDS_PASSWORD")
)

# Sample period
beg_date <- as.Date("2010-01-01")
end_date  <- as.Date("2022-12-31")

# IV measurement window (trading days relative to event)
# Pre-event: Smith (2021) uses -2; alternatives: -1, 0
# Post-event: Smith (2021) uses +1; alternatives: +2
td_pre  <- -2L
td_post <-  1L

# Run pipeline — produces smithsoJAR.csv
source("riskinfo.R")

dbDisconnect(wrds)
