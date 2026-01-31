import re
import pandas as pd

ISIN_REGEX = re.compile(r"^IN[EF][A-Z0-9]{9}$")

def looks_like_header(row: pd.Series) -> bool:
    cells = row.astype(str).str.upper().str.strip()

    has_isin_label = any(c == "ISIN" for c in cells)

    has_value_label = any(
        any(k in c for k in [
            "AMOUNT", "VALUE", "MARKET VALUE",
            "NO. OF UNITS", "UNITS", "QTY", "QUANTITY"
        ])
        for c in cells
    )
    # print(f"\n===== AFTER NORMALIZATION: {has_isin_label} and {has_value_label} =====")

    return has_isin_label and has_value_label




HEADER_KEYWORDS = [
    "ISIN",
    "UNIT",
    "UNITS",
    "NO. OF UNITS",
    "QUANTITY",
    "QTY",
    "MARKET VALUE",
    "INVESTED VALUE",
    "CURRENT VALUE",
    "AMOUNT",
    "VALUE"
]

def find_header_row(df: pd.DataFrame) -> int | None:
    for i in range(min(len(df), 50)):
        row = df.iloc[i]
        if looks_like_header(row):
            return i
        # print(f"\n===== AFTER NORMALIZATION: {row.tolist()} =====")

    return None


def rebuild_table_from_header(df: pd.DataFrame) -> pd.DataFrame:
    header_row = find_header_row(df)

    if header_row is None:
        raise ValueError("Could not detect table header row")

    new_header = df.iloc[header_row].astype(str).str.strip()
    data_df = df.iloc[header_row + 1:].copy()
    data_df.columns = new_header
    data_df = data_df.dropna(how="all")

    return data_df

def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.astype(str)

    # ---------- ISIN ----------
    isin_col = None
    for col in df.columns:
        if "ISIN" in col.upper():
            isin_col = col
            break

    if not isin_col:
        raise ValueError("ISIN column not found")

    df["ISIN"] = df[isin_col].astype(str).str.strip()

    # ---------- QTY ----------
    qty_col = None
    for col in df.columns:
        if any(k in col.upper() for k in ["UNIT", "QTY", "QUANTITY"]):
            qty_col = col
            break

    df["QTY"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0.0) if qty_col else 0.0

    # ---------- AMOUNT ----------
    amt_col = None
    for col in df.columns:
        if any(k in col.upper() for k in ["MARKET VALUE", "INVESTED VALUE", "AMOUNT", "VALUE"]):
            amt_col = col
            break

    df["AMOUNT"] = pd.to_numeric(df[amt_col], errors="coerce").fillna(0.0) if amt_col else 0.0

    return df[["ISIN", "QTY", "AMOUNT"]]

def normalize_broker_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = df.columns.astype(str).str.upper()

    # -----------------------------------------
    # CASE 1: Already a clean table → skip rebuild
    # -----------------------------------------
    if any("ISIN" in c for c in df.columns):
        print("✅ Detected clean table, skipping header rebuild")
        df = canonicalize_columns(df)

    # -----------------------------------------
    # CASE 2: Broker-style sheet → rebuild header
    # -----------------------------------------
    else:
        print("🔍 Detected broker-style sheet, rebuilding header")
        df = rebuild_table_from_header(df)
        df = canonicalize_columns(df)

    # -----------------------------------------
    # Keep only valid ISIN rows
    # -----------------------------------------
    df = df[df["ISIN"].str.match(r"^(INE|INF)[A-Z0-9]{9}$", na=False)]

    return df