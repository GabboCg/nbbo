# Modify dates format
to_yyyymmdd <- function(d) gsub("-", "", d)

# Label each record as pre / regular / post (ET cutoffs)
label_session_dt <- function(dt, time_col = "time_m", open = "09:30:00", close = "16:00:00") {
    
    OPEN  <- hms::as_hms(open)
    CLOSE <- hms::as_hms(close)
    
    if (!inherits(dt[[time_col]], "hms")) {
        
        dt[, (time_col) := hms::as_hms(get(time_col))]
        
    }
    
    dt[, session := data.table::fcase(
        get(time_col) <  OPEN, "pre",
        get(time_col) <= CLOSE, "regular",
        default = "post"
    )]
    
    dt
    
}

# Create numeric seconds since midnight for stable joins / comparisons
add_time_sec <- function(dt, time_col = "time_m") {
    
    if (!inherits(dt[[time_col]], "hms")) dt[, (time_col) := hms::as_hms(get(time_col))]
    
    dt[, time_sec := as.numeric(get(time_col))]
    
    dt
    
}

# NBBO cleaning (session dependent)
clean_nbbo_sessioned <- function(DailyNBBO, flag_remove_dupes = FALSE) {
    
    NBBO2 <- copy(DailyNBBO)
    
    # Ensure session + time_sec exist
    NBBO2 <- label_session_dt(NBBO2, "time_m")
    NBBO2 <- add_time_sec(NBBO2, "time_m")
    
    # Quote conditions:
    # - Regular: A B H O R W
    # - Pre/Post: A B H O R W + C
    NBBO2 <- NBBO2[
        (session == "regular" & qu_cond %in% c("A", "B", "H", "O", "R", "W")) |
        (session != "regular" & qu_cond %in% c("A", "B", "H", "O", "R", "W", "C"))
    ]
    
    # Regular-only deletion of "both sides invalid" rows (do NOT do pre/post)
    NBBO2 <- NBBO2[
        session != "regular" |
            (
                !((best_ask <= 0 | is.na(best_ask)) & (best_bid <= 0 | is.na(best_bid))) &
                !((best_asksizeshares <= 0 | is.na(best_asksizeshares)) & (best_bidsizeshares <= 0 | is.na(best_bidsizeshares)))
            )
    ]
    
    # Compute spread/midpoint using raw values (may be invalid; will be NA'd below)
    NBBO2[, `:=`(
        spread   = best_ask - best_bid,
        midpoint = (best_ask + best_bid) / 2
    )]
    
    # Invalidate ask side (all sessions): do NOT delete rows
    NBBO2[
        best_ask <= 0 | is.na(best_ask) | best_asksizeshares <= 0 | is.na(best_asksizeshares),
        `:=`(best_ask = NA_real_, best_asksizeshares = NA_real_)
    ]
    
    # Invalidate bid side (all sessions): do NOT delete rows
    NBBO2[
        best_bid <= 0 | is.na(best_bid) | best_bidsizeshares <= 0 | is.na(best_bidsizeshares),
        `:=`(best_bid = NA_real_, best_bidsizeshares = NA_real_)
    ]
    
    # Convert sizes to shares
    NBBO2[, `:=`(
        best_bidsize_shares = best_bidsizeshares * 100,
        best_asksize_shares = best_asksizeshares * 100
    )]
    
    # Step 3 outlier rule: REGULAR ONLY (skip pre/post)
    setorder(NBBO2, sym_root, date, time_sec)
    
    NBBO2[, lmid := shift(midpoint, 1L), by = .(sym_root, date)]
    NBBO2[, `:=`(lm25 = lmid - 2.5, lp25 = lmid + 2.5)]
    
    NBBO2[
        session == "regular" & spread > 5 & !is.na(lm25) & !is.na(best_bid) & best_bid < lm25,
        `:=`(best_bid = NA_real_, best_bidsize_shares = NA_real_)
    ]
    NBBO2[
        session == "regular" & spread > 5 & !is.na(lp25) & !is.na(best_ask) & best_ask > lp25,
        `:=`(best_ask = NA_real_, best_asksize_shares = NA_real_)
    ]
    
    # Keep only necessary cols (retain session + time_sec)
    NBBO2 <- NBBO2[, .(
        date, time_m, time_sec, sym_root, session,
        best_bid, best_bidsize_shares,
        best_ask, best_asksize_shares
    )]
    
    # Optional: remove consecutive duplicate NBBO states
    if (isTRUE(flag_remove_dupes)) {
        
        setorder(NBBO2, sym_root, date, time_sec)
        NBBO2 <- NBBO2[
            sym_root != shift(sym_root) |
                date != shift(date) |
                best_ask != shift(best_ask) |
                best_bid != shift(best_bid) |
                best_asksize_shares != shift(best_asksize_shares) |
                best_bidsize_shares != shift(best_bidsize_shares)
        ]
        
    } else {
        
        cat("\tDuplicate NBBO states not removed\n")
        
    }
    
    NBBO2
    
}

# Quote cleaning (session dependent)
clean_quotes_sessioned <- function(DailyQuote) {
    
    quoteAB <- copy(DailyQuote)
    quoteAB <- label_session_dt(quoteAB, "time_m")
    quoteAB <- add_time_sec(quoteAB, "time_m")
    quoteAB[, spread := ask - bid]
    
    # Regular: strict filters 
    quote_regular <- quoteAB[
        session == "regular" &
            qu_cond %in% c("A","B","H","O","R","W") &
            bid <= ask &
            spread <= 5 &
            ask  > 0 & !is.na(ask) &
            asksiz > 0 & !is.na(asksiz) &
            bid  > 0 & !is.na(bid) &
            bidsiz > 0 & !is.na(bidsiz)
    ]
    
    # Pre/Post: relaxed filters, include C, do NOT delete crossed/wide/withdrawn/etc
    quote_extended <- quoteAB[
        session != "regular" &
        qu_cond %in% c("A", "B", "H", "O", "R", "W", "C")
    ]
    
    rbindlist(list(quote_regular, quote_extended), use.names = TRUE, fill = TRUE)
    
}

# Trade cleaning (session dependent)
clean_trades_sessioned <- function(DailyTrade) {
    
    trade2 <- copy(DailyTrade)
    trade2 <- label_session_dt(trade2, "time_m")
    trade2 <- add_time_sec(trade2, "time_m")
    
    # Base filters
    trade2 <- trade2[tr_corr == "00" & price > 0]
    
    # Pre/Post only: exclude specific sale condition codes (keep 'T')
    trade2 <- trade2[
        session == "regular" | !(tr_scond %in% c("L", "P", "U", "Z", "4"))
    ]
    
    trade2
    
}

# Build Official Complete NBBO table
build_official_complete_nbbo <- function(NBBO2, quoteAB_clean) {
    
    # Eligible quote feed observations
    quoteAB2 <- quoteAB_clean[
        (qu_source == "C" & natbbo_ind == "1") | (qu_source == "N" & natbbo_ind == "4")
    ][, .(
        date, time_m, time_sec, sym_root, session,
        best_bid = bid,
        best_ask = ask,
        best_bidsize_shares = bidsiz * 100,
        best_asksize_shares = asksiz * 100
    )]
    
    # Invalidate bad sides (do not delete rows)
    quoteAB2[
        best_ask <= 0 | is.na(best_ask) | best_asksize_shares <= 0 | is.na(best_asksize_shares),
        `:=`(best_ask = NA_real_, best_asksize_shares = NA_real_)
    ]
    
    quoteAB2[
        best_bid <= 0 | is.na(best_bid) | best_bidsize_shares <= 0 | is.na(best_bidsize_shares),
        `:=`(best_bid = NA_real_, best_bidsize_shares = NA_real_)
    ]
    
    # Stack
    OfficialCompleteNBBO <- rbindlist(list(
        NBBO2[, .(date, time_m, time_sec, sym_root, session, best_bid, best_ask, best_bidsize_shares, best_asksize_shares)],
        quoteAB2
    ), use.names = TRUE, fill = TRUE)
    
    # Sort + remove duplicate timestamps 
    setorder(OfficialCompleteNBBO, sym_root, date, time_sec)
    OfficialCompleteNBBO <- OfficialCompleteNBBO[, .SD[1L], by = .(sym_root, date, time_sec)]  
    
    OfficialCompleteNBBO
    
}

# Match trades to prior NBBO (rolling join)
match_trades_to_nbbo <- function(OfficialCompleteNBBO, trade2) {
    
    # Shift NBBO by 1 nanosecond to replicate SAS T-1 matching
    OfficialCompleteNBBO[, type := "Q"]
    OfficialCompleteNBBO[, time_m := time_m + 1e-9]
    
    setkey(OfficialCompleteNBBO, sym_root, date, time_m)
    setkey(trade2,               sym_root, date, time_m)
    
    OfficialCompleteNBBO <- OfficialCompleteNBBO |>
        mutate(time_m = hms::as_hms(time_m))
    
    # Rolling join attaches most recent prior quote to each trade
    TradesandCorrespondingNBBO <- OfficialCompleteNBBO[trade2, roll = TRUE]
    
    # Rename quote variables to match SAS retained variables
    TradesandCorrespondingNBBO[, `:=`(
        type  = "T",
        QTime = time_m,
        NBO   = best_ask,
        NBB   = best_bid,
        NBOqty = best_asksize_shares,
        NBBqty = best_bidsize_shares
    )]
    
    # Compute midpoint and lock/cross indicators
    TradesandCorrespondingNBBO[, `:=`(
        midpoint = (best_ask + best_bid) / 2,
        lock  = fifelse(best_ask == best_bid, 1L, 0L),
        cross = fifelse(best_ask <  best_bid, 1L, 0L)
    )]
    
    TradesandCorrespondingNBBO
    
}

# Buy/Sell classification
classify_buysell <- function(dt_trades_nbbo) {
    
    BuySellIndicators <- copy(dt_trades_nbbo)
    
    # Tick test: compare to previous trade price (within sym/date)
    # Use trade sequence if available to order
    if (!("tr_seqnum" %in% names(BuySellIndicators))) {
        
        BuySellIndicators[, tr_seqnum := .I]
        
    }
    
    setorder(BuySellIndicators, sym_root, date, time_sec, tr_seqnum)
    
    BuySellIndicators[, direction := price - shift(price), by = .(sym_root, date)]
    BuySellIndicators[, direction := fifelse(rowid(sym_root, date) == 1L, NA_real_, direction)]
    
    # Carry forward last non-zero direction
    BuySellIndicators[, direction2 := direction]
    BuySellIndicators[direction2 == 0, direction2 := NA_real_]
    BuySellIndicators[, direction2 := nafill(direction2, type = "locf"), by = .(sym_root, date)]
    
    # Initial classification via tick test
    BuySellIndicators[, `:=`(
        BuySellLR   = fifelse(direction2 > 0,  1L, fifelse(direction2 < 0, -1L, NA_integer_)),
        BuySellEMO  = fifelse(direction2 > 0,  1L, fifelse(direction2 < 0, -1L, NA_integer_)),
        BuySellCLNV = fifelse(direction2 > 0,  1L, fifelse(direction2 < 0, -1L, NA_integer_))
    )]
    
    # Refine using midpoint and quote-based rules (only when not locked/crossed and midpoint exists)
    BuySellIndicators[lock == 0 & cross == 0 & !is.na(midpoint) & price > midpoint, BuySellLR :=  1L]
    BuySellIndicators[lock == 0 & cross == 0 & !is.na(midpoint) & price < midpoint, BuySellLR := -1L]
    
    BuySellIndicators[lock == 0 & cross == 0 & !is.na(NBO) & price == NBO, BuySellEMO :=  1L]
    BuySellIndicators[lock == 0 & cross == 0 & !is.na(NBB) & price == NBB, BuySellEMO := -1L]
    
    BuySellIndicators[, ofr30 := NBO - 0.3 * (NBO - NBB)]
    BuySellIndicators[, bid30 := NBB + 0.3 * (NBO - NBB)]
    
    BuySellIndicators[
        lock == 0 & cross == 0 & !is.na(NBO) & !is.na(NBB) & price <= NBO & price >= ofr30,
        BuySellCLNV := 1L
    ]
    
    BuySellIndicators[
        lock == 0 & cross == 0 & !is.na(NBO) & !is.na(NBB) & price <= bid30 & price >= NBB,
        BuySellCLNV := -1L
    ]
    
    BuySellIndicators
    
}
