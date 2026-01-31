# app.py
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import date
import pandas as pd
import io
from typing import List

from helpers import canonicalize_columns
from helpers import normalize_broker_dataframe
from portfolio_processor import process_portfolio

app = FastAPI(title="Portfolio Beta Calculator")

# Static + templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# -------------------------------
# Website
# -------------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


# -------------------------------
# API (MULTI-FILE)
# -------------------------------
@app.post("/portfolio/beta")
async def calculate_beta(
    files: List[UploadFile] = File(...),
    valuation_date: date | None = Query(
        None,
        description="Valuation date for MF AMOUNT-based inputs"
    )
):
    if not files or len(files) == 0:
        raise HTTPException(400, "At least one file is required")

    dfs = []

    for file in files:
        if not file.filename.lower().endswith((".csv", ".xlsx")):
            raise HTTPException(
                400,
                f"Unsupported file type: {file.filename}"
            )

        content = await file.read()

        try:
            if file.filename.lower().endswith(".csv"):
                df = pd.read_csv(io.StringIO(content.decode()))
            else:
                df = pd.read_excel(io.BytesIO(content), header=None)
        except Exception as e:
            raise HTTPException(
                400,
                f"Invalid file {file.filename}: {e}"
            )
        df = normalize_broker_dataframe(df)
        df = canonicalize_columns(df)
        # DEBUG SNAPSHOT (TEMPORARY)
        print(f"\n===== AFTER NORMALIZATION: {file.filename} =====")
        print(df.head(20))
        print(df.dtypes)
        print("Non-null counts:")
        print(df.notna().sum())
        print("=============================================\n")

        # Normalize columns
        df.columns = df.columns.str.strip().str.upper()

        # VALUE alias
        if "VALUE" in df.columns and "AMOUNT" not in df.columns:
            df["AMOUNT"] = df["VALUE"]

        if "ISIN" not in df.columns:
            raise HTTPException(
                400,
                f"{file.filename} is missing ISIN column"
            )

        if not (("QTY" in df.columns) or ("AMOUNT" in df.columns)):
            raise HTTPException(
                400,
                f"{file.filename} must contain QTY or AMOUNT"
            )

        dfs.append(df)

    # -------------------------------
    # MERGE ALL FILES
    # -------------------------------
    merged_df = pd.concat(dfs, ignore_index=True)

    # -------------------------------
    # PROCESS PORTFOLIO
    # -------------------------------
    result = process_portfolio(merged_df, valuation_date)

    if not result or result.get("portfolio_beta") is None:
        raise HTTPException(
            400,
            result.get("error", "Unable to calculate portfolio beta")
        )

    return result

@app.post("/debug/normalize")
async def debug_normalize(file: UploadFile = File(...)):
    content = await file.read()
    df = pd.read_excel(io.BytesIO(content))
    df = normalize_broker_dataframe(df)
    return df.head(50).to_dict(orient="records")
