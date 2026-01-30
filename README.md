# Beta Calculator API

FastAPI-based backend to calculate:
- Equity beta
- Mutual fund beta
- Portfolio weighted beta

## Features
- Upload CSV/XLSX portfolio
- Supports Equity + Mutual Funds
- ISIN-based mapping
- Amount or Quantity input
- Optional valuation date for MF

## Run locally
```bash
uvicorn app:app --reload
