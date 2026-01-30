# portfolio_processor.py

import pandas as pd
import yfinance as yf
from datetime import date

from beta_engine import get_stock_beta, load_index_series
from isin_master import get_isin_symbol_map
from mf_master import find_scheme_by_isin
from mf_beta_engine import get_mf_beta, get_latest_nav, get_nav_on_date


# -------------------------------
# Helpers
# -------------------------------

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
    return float(data["Close"].iloc[-1])


def _safe_float(val):
    try:
        if val is None or pd.isna(val):
            return None

        if isinstance(val, str):
            # Remove commas & spaces (Indian / US formats)
            val = val.replace(",", "").strip()

        return float(val)
    except Exception:
        return None


# -------------------------------
# Main Processor
# -------------------------------

def process_portfolio(df: pd.DataFrame, valuation_date: date | None = None):
    """
    Supported input columns:
      ISIN (mandatory)
      QTY (optional)
      AMOUNT / VALUE (optional)

    Rules:
      - If QTY present and > 0 → use QTY
      - Else if AMOUNT present → derive QTY
      - If both present → QTY wins
      - Equity AMOUNT → latest market price
      - MF AMOUNT → valuation_date if provided, else latest NAV
    """

    df = df.copy()
    df.columns = df.columns.str.strip().str.upper()

    if "VALUE" in df.columns and "AMOUNT" not in df.columns:
        df["AMOUNT"] = df["VALUE"]

    for col in ["QTY", "AMOUNT"]:
        if col not in df.columns:
            df[col] = None

    # -------------------------------
    # Aggregate duplicate ISIN rows
    # -------------------------------
    agg_map = {}

    if "QTY" in df.columns:
        agg_map["QTY"] = "sum"
    if "AMOUNT" in df.columns:
        agg_map["AMOUNT"] = "sum"

    df = df.groupby("ISIN", as_index=False).agg(agg_map)
    # If QTY exists for an ISIN, ignore AMOUNT
    df.loc[df["QTY"].notna() & (df["QTY"] > 0), "AMOUNT"] = None



    # -------------------------------
    # Load masters & index
    # -------------------------------
    isin_symbol_map = get_isin_symbol_map()
    index_series = load_index_series()

    records = []

    # -------------------------------
    # Row-wise processing
    # -------------------------------
    for row in df.itertuples(index=False):
        isin = str(row.ISIN).strip().upper()
        qty_input = _safe_float(row.QTY)
        amt_input = _safe_float(row.AMOUNT)

        # ---------- EQUITY ----------
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
                    "ERROR": "Equity price fetch failed"
                })
                continue

            if qty_input is not None and qty_input > 0:
                final_qty = qty_input
            elif amt_input is not None and amt_input > 0:
                final_qty = amt_input / price
            else:
                records.append({
                    "ISIN": isin,
                    "TYPE": "EQUITY",
                    "SYMBOL": symbol,
                    "ERROR": "Neither QTY nor AMOUNT provided"
                })
                continue

            value = final_qty * price
            beta = get_stock_beta(symbol, index_series)

            records.append({
                "ISIN": isin,
                "TYPE": "EQUITY",
                "SYMBOL": symbol,
                "QTY": round(final_qty, 6),
                "PRICE": round(price, 2),
                "VALUE": round(value, 2),
                "BETA": round(beta, 6) if beta == beta else None
            })

        # ---------- MUTUAL FUND ----------
        elif isin.startswith("INF"):
            scheme_code, scheme_name = find_scheme_by_isin(isin)
            if not scheme_code:
                records.append({"ISIN": isin, "ERROR": "MF ISIN not found"})
                continue

            # Resolve NAV
            if qty_input is not None and qty_input > 0:
                nav = get_latest_nav(scheme_code)
            elif amt_input is not None and amt_input > 0:
                if valuation_date:
                    nav = get_nav_on_date(scheme_code, valuation_date)
                else:
                    nav = get_latest_nav(scheme_code)
            else:
                records.append({
                    "ISIN": isin,
                    "TYPE": "MF",
                    "SYMBOL": scheme_name,
                    "ERROR": "Neither QTY nor AMOUNT provided"
                })
                continue

            if nav is None:
                records.append({
                    "ISIN": isin,
                    "TYPE": "MF",
                    "SYMBOL": scheme_name,
                    "ERROR": "NAV fetch failed"
                })
                continue

            final_qty = qty_input if (qty_input is not None and qty_input > 0) else amt_input / nav
            value = final_qty * nav
            beta = get_mf_beta(scheme_code)

            records.append({
                "ISIN": isin,
                "TYPE": "MF",
                "SYMBOL": scheme_name,
                "QTY": round(final_qty, 6),
                "PRICE": round(nav, 4),
                "VALUE": round(value, 2),
                "BETA": round(beta, 6) if beta == beta else None
            })

        # ---------- UNKNOWN ----------
        else:
            records.append({"ISIN": isin, "ERROR": "Unknown ISIN type"})

    # -------------------------------
    # Portfolio aggregation (SAFE)
    # -------------------------------
    result_df = pd.DataFrame(records)

    if not {"VALUE", "BETA"}.issubset(result_df.columns):
        return {
            "portfolio_beta": None,
            "total_value": 0,
            "details": records,
            "error": "No valid securities with VALUE and BETA"
        }

    valid_df = result_df[
        result_df["VALUE"].notna() & result_df["BETA"].notna()
    ]

    if valid_df.empty:
        return {
            "portfolio_beta": None,
            "total_value": 0,
            "details": records,
            "error": "No valid securities for portfolio beta"
        }

    valid_df["WEIGHT"] = valid_df["VALUE"] / valid_df["VALUE"].sum()
    valid_df["WEIGHTED_BETA"] = valid_df["WEIGHT"] * valid_df["BETA"]

    portfolio_beta = valid_df["WEIGHTED_BETA"].sum()

    return {
        "portfolio_beta": round(portfolio_beta, 6),
        "total_value": round(valid_df["VALUE"].sum(), 2),
        "details": records
    }
