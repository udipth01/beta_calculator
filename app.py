from fastapi import FastAPI, UploadFile, File, HTTPException, Query
import pandas as pd
import io
from typing import List, Optional
from datetime import date

from portfolio_processor import process_portfolio

app = FastAPI(title="Portfolio Beta API")


@app.post("/portfolio/beta")
async def calculate_beta(
    files: List[UploadFile] = File(...),
    valuation_date: Optional[date] = Query(
        None,
        description="Valuation date for MF amount-based portfolios (YYYY-MM-DD)"
    )
):
    if not files:
        raise HTTPException(status_code=400, detail="At least one file is required")

    dfs = []

    # -------------------------------
    # Read & normalize each file
    # -------------------------------
    for file in files:
        if not file.filename.lower().endswith((".csv", ".xlsx")):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type: {file.filename}"
            )

        content = await file.read()

        try:
            if file.filename.lower().endswith(".csv"):
                df = pd.read_csv(io.StringIO(content.decode()))
            else:
                df = pd.read_excel(io.BytesIO(content))
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to read {file.filename}: {e}"
            )

        # Normalize column names
        df.columns = df.columns.str.strip().str.upper()

        # VALUE → AMOUNT alias
        if "VALUE" in df.columns and "AMOUNT" not in df.columns:
            df["AMOUNT"] = df["VALUE"]

        if "ISIN" not in df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"{file.filename} missing ISIN column"
            )

        dfs.append(df)

    # -------------------------------
    # Merge all files
    # -------------------------------
    merged_df = pd.concat(dfs, ignore_index=True)

    # -------------------------------
    # Basic validation
    # -------------------------------
    if not (("QTY" in merged_df.columns) or ("AMOUNT" in merged_df.columns)):
        raise HTTPException(
            status_code=400,
            detail="Combined files must contain QTY or AMOUNT column"
        )

    # -------------------------------
    # Process portfolio
    # -------------------------------
    result = process_portfolio(merged_df, valuation_date)

    if not result or result.get("portfolio_beta") is None:
        raise HTTPException(
            status_code=400,
            detail=result.get("error", "Unable to calculate portfolio beta")
        )

    return result
