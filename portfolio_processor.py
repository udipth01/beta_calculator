# portfolio_processor.py

import pandas as pd
import yfinance as yf
from datetime import date

from beta_engine import get_stock_beta, load_index_series
from isin_master import get_isin_symbol_map
from mf_master import find_scheme_by_isin
from mf_beta_engine import get_mf_beta, get_latest_nav, get_nav_on_date


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def get_latest_price(symbol: str):
    data = yf.download(
        f"{symbol}.NS",
        period="5d",
        auto_adjust=True,
        progress=False,
        threads=False
    )
    if data is None or data.empty:
        return None
    return float(data["Close"].iloc[-1].item())


def _safe_float(val):
    try:
        if val is None or pd.isna(val):
            return 0.0
        if isinstance(val, str):
            val = val.replace(",", "").strip()
        return float(val)
    except Exception:
        return 0.0


# -------------------------------------------------
# Main Processor
# -------------------------------------------------

def process_portfolio(df: pd.DataFrame, valuation_date: date | None = None):
    """
    FINAL LOGIC (IMPORTANT):

    For each ISIN:
      Final Quantity =
          (Sum of explicit QTY across files)
        + (Sum of AMOUNT converted to QTY using price/NAV)

    This ensures:
      - Multiple broker files merge correctly
      - MF amount-only files are respected
      - Equity QTY + MF AMOUNT both contribute
    """

    df = df.copy()
    df.columns = df.columns.str.strip().str.upper()

    if "VALUE" in df.columns and "AMOUNT" not in df.columns:
        df["AMOUNT"] = df["VALUE"]

    for col in ["QTY", "AMOUNT"]:
        if col not in df.columns:
            df[col] = 0.0

    df["QTY"] = df["QTY"].apply(_safe_float)
    df["AMOUNT"] = df["AMOUNT"].apply(_safe_float)

    # -------------------------------------------------
    # Load masters once
    # -------------------------------------------------
    isin_symbol_map = get_isin_symbol_map()
    index_series = load_index_series()

    records = []

    # -------------------------------------------------
    # Process per ISIN (AFTER merge)
    # -------------------------------------------------
    grouped = df.groupby("ISIN", as_index=False).agg({
        "QTY": "sum",
        "AMOUNT": "sum"
    })

    for row in grouped.itertuples(index=False):
        isin = str(row.ISIN).strip().upper()
        base_qty = row.QTY
        amount = row.AMOUNT

        # ------------------- EQUITY -------------------
        if isin.startswith("INE"):
            symbol = isin_symbol_map.get(isin)
            if not symbol:
                records.append({"ISIN": isin, "ERROR": "Equity ISIN not found"})
                continue

            price = get_latest_price(symbol)
            if price is None:
                records.append({
                    "ISIN": isin,
                    "TYPE": "EQUITY",
                    "SYMBOL": symbol,
                    "ERROR": "Price fetch failed"
                })
                continue

            derived_qty = amount / price if amount > 0 else 0.0
            final_qty = base_qty + derived_qty
            value = final_qty * price

            beta = get_stock_beta(symbol, index_series)

            records.append({
                "ISIN": isin,
                "TYPE": "EQUITY",
                "SYMBOL": symbol,
                "QTY": round(final_qty, 6),
                "PRICE": round(price, 2),
                "VALUE": round(value, 2),
                "BETA": round(beta, 6)
            })

        # ------------------- MUTUAL FUND -------------------
        elif isin.startswith("INF"):
            scheme_code, scheme_name = find_scheme_by_isin(isin)
            if not scheme_code:
                records.append({"ISIN": isin, "ERROR": "MF ISIN not found"})
                continue

            if valuation_date:
                nav = get_nav_on_date(scheme_code, valuation_date)
            else:
                nav = get_latest_nav(scheme_code)

            if nav is None:
                records.append({
                    "ISIN": isin,
                    "TYPE": "MF",
                    "SYMBOL": scheme_name,
                    "ERROR": "NAV fetch failed"
                })
                continue

            derived_qty = amount / nav if amount > 0 else 0.0
            final_qty = base_qty + derived_qty
            value = final_qty * nav

            beta = get_mf_beta(scheme_code)

            records.append({
                "ISIN": isin,
                "TYPE": "MF",
                "SYMBOL": scheme_name,
                "QTY": round(final_qty, 6),
                "PRICE": round(nav, 4),
                "VALUE": round(value, 2),
                "BETA": round(beta, 6)
            })

        # ------------------- UNKNOWN -------------------
        else:
            records.append({"ISIN": isin, "ERROR": "Unknown ISIN type"})

    # -------------------------------------------------
    # Portfolio aggregation
    # -------------------------------------------------
    result_df = pd.DataFrame(records)

    if result_df.empty or not {"VALUE", "BETA"}.issubset(result_df.columns):
        return {
            "portfolio_beta": None,
            "total_value": 0,
            "details": records,
            "error": "No valid securities"
        }

    valid_df = result_df[
        result_df["VALUE"].notna() & result_df["BETA"].notna()
    ]

    total_value = valid_df["VALUE"].sum()

    if total_value <= 0:
        return {
            "portfolio_beta": None,
            "total_value": 0,
            "details": records,
            "error": "Total portfolio value is zero"
        }

    valid_df["WEIGHT"] = valid_df["VALUE"] / total_value
    valid_df["WEIGHTED_BETA"] = valid_df["WEIGHT"] * valid_df["BETA"]

    portfolio_beta = valid_df["WEIGHTED_BETA"].sum()

    return {
        "portfolio_beta": round(portfolio_beta, 6),
        "total_value": round(total_value, 2),
        "details": records
    }
