from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import date
import pandas as pd
import io

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
# API
# -------------------------------
@app.post("/portfolio/beta")
async def calculate_beta(
    file: UploadFile = File(...),
    valuation_date: date | None = Query(None)
):
    if not file.filename.lower().endswith((".csv", ".xlsx")):
        raise HTTPException(400, "Upload CSV or Excel")

    content = await file.read()

    try:
        if file.filename.lower().endswith(".csv"):
            df = pd.read_csv(io.StringIO(content.decode()))
        else:
            df = pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"Invalid file: {e}")

    df.columns = df.columns.str.strip().str.upper()

    if "VALUE" in df.columns and "AMOUNT" not in df.columns:
        df["AMOUNT"] = df["VALUE"]

    if "ISIN" not in df.columns:
        raise HTTPException(400, "ISIN column missing")

    if not (("QTY" in df.columns) or ("AMOUNT" in df.columns)):
        raise HTTPException(400, "Need QTY or AMOUNT column")

    result = process_portfolio(df, valuation_date)

    if not result or result["portfolio_beta"] is None:
        raise HTTPException(400, result.get("error", "Unable to calculate beta"))

    return result
