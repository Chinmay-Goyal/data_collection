#!/usr/bin/env python3
"""
Download history of NSE indices from niftyindices.com and build combined,
portfolio-ready files. INCREMENTAL: if the daily price CSV already exists, only
data after its last Date (minus a small overlap) is downloaded and appended.

The site only returns ~1 year per request, so this script loops year by year
from the start date to today and stitches the chunks together.

Output (6 CSVs, one column per index, datetime index):
    <PREFIX>_daily_price.csv      <PREFIX>_daily_returns.csv
    <PREFIX>_weekly_price.csv     <PREFIX>_weekly_returns.csv
    <PREFIX>_monthly_price.csv    <PREFIX>_monthly_returns.csv

"price" = the index level (TRI by default), "returns" = simple % change
(0.01 = 1%). Weekly/monthly prices are the LAST available value of the
week (ending Friday) / month, dated on the actual last trading day.
Column names are "<index name>_<SUFFIX>", e.g. "NIFTY 50_TRI".

How updating works:
  * The daily price CSV is the source of truth. Its last Date decides where
    the download starts (minus OVERLAP_DAYS, so recent values get refreshed).
  * New data is merged into it (new values win on overlapping dates).
  * Weekly/monthly prices and all returns files are then REBUILT from the full
    merged daily data, so every file stays in sync (including a partially
    completed week/month, which gets corrected on the next run).
  * An index in your list that is not yet a column in the CSV gets its full
    history downloaded from START_YEAR.

Usage:
    python download_returns_nse.py                      # update using settings below
    python download_returns_nse.py --full               # ignore existing CSV, rebuild all
    python download_returns_nse.py --overlap 10         # re-fetch last 10 days too
    python download_returns_nse.py "NIFTY 50" "NIFTY 100=<CODE>"   # override list

Requires:  pip install pandas requests
Recommended (helps if NSE blocks plain requests):  pip install curl_cffi
"""

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# Prefer curl_cffi (impersonates Chrome's TLS fingerprint); fall back to requests.
try:
    from curl_cffi import requests as http
    SESSION_KWARGS = {"impersonate": "chrome"}
    CLIENT = "curl_cffi (Chrome impersonation)"
except ImportError:
    import requests as http
    SESSION_KWARGS = {}
    CLIENT = "requests"

# =============================== SETTINGS ===================================
DATA_TYPE = "tri"        # "tri" = Total Return Index, "price" = price index (Close)
SUFFIX = "TRI"           # appended to every column name: "<index>_<SUFFIX>"
OUT_DIR = r"C:\Users\chinm\Dev\data_collection\Data"   # output folder
PREFIX = "nifty_tri"     # output file prefix
START_YEAR = 1995        # first year to try for NEW indices (years with no data are skipped)
PAUSE = 1.0              # seconds between requests (be polite to the server)
OVERLAP_DAYS = 3         # re-download this many days before the last CSV date
                         # (catches late/revised values; 0 = start the day after)

# Which column to use as the index level. None = auto-detect (prints what it chose).
# For TRI the site returns both a Total Return and a Net Total Return column;
# set e.g. VALUE_COLUMN = "NTR_Value" (exact key printed by the script) to use NTR.
VALUE_COLUMN = None

# Format: "FULL INDEX NAME" or "FULL INDEX NAME=CODE"
DEFAULT_INDICES = [
    'NIFTY MIDCAP150 MOMENTUM 50', 'NIFTY ALPHA 50', 'NIFTY MICROCAP 250',
    'NIFTY200 ALPHA 30', 'NIFTY500 MOMENTUM 50', 'NIFTY MIDCAP 50',
    'NIFTY MIDSMALLCAP400 MOMENTUM QUALITY 100', 'NIFTY MIDCAP 150',
    'NIFTY200 VALUE 30', 'NIFTY SMALLCAP250 MOMENTUM QUALITY 100',
    'NIFTY MIDCAP SELECT', 'NIFTY500 MULTICAP MOMENTUM QUALITY 50',
    'NIFTY500 VALUE 50', 'NIFTY200 MOMENTUM 30', 'NIFTY MIDSMALLCAP 400',
    'NIFTY MIDCAP 100', 'NIFTY ALPHA QUALITY VALUE LOW-VOLATILITY 30',
    'NIFTY SMALLCAP250 QUALITY 50', 'NIFTY100 ALPHA 30',
    'NIFTY500 LARGEMIDSMALL EQUAL-CAP WEIGHTED', 'NIFTY LARGEMIDCAP 250',
    'NIFTY SMALLCAP 250', 'NIFTY500 MULTICAP 50:25:25', 'NIFTY50 VALUE 20',
    'NIFTY500 EQUAL WEIGHT', 'NIFTY MIDCAP150 QUALITY 50',
    'NIFTY ALPHA LOW-VOLATILITY 30', 'NIFTY ALPHA QUALITY LOW-VOLATILITY 30',
    'NIFTY NEXT 50', 'NIFTY DIVIDEND OPPORTUNITIES 50', 'NIFTY TOTAL MARKET',
    'NIFTY 500', 'NIFTY SMALLCAP 100', 'NIFTY50 EQUAL WEIGHT',
    'NIFTY100 EQUAL WEIGHT', 'NIFTY 200', 'NIFTY100 LOW VOLATILITY 30',
    'NIFTY LOW VOLATILITY 50', 'NIFTY TOP 10 EQUAL WEIGHT', 'NIFTY SMALLCAP 50',
    'NIFTY 100', 'NIFTY200 QUALITY 30', 'NIFTY 50', 'NIFTY HIGH BETA 50',
    'NIFTY100 QUALITY 30', 'NIFTY QUALITY LOW-VOLATILITY 30',
    'NIFTY GROWTH SECTORS 15',
]

# Known internal codes (the 'name' value from the website's own requests).
# If an index isn't listed, the script tries the full name as the code.
INDEX_CODES = {
    "NIFTY MIDCAP150 MOMENTUM 50": "NIFTYM150MOMNTM50",
}
# ============================================================================

BASE = "https://www.niftyindices.com"
PAGE_URL = f"{BASE}/reports/historical-data"

ENDPOINTS = {
    "tri": f"{BASE}/BackPage/getTotalReturnIndexString",          # verified
    "price": f"{BASE}/BackPage/getHistoricaldatatabletoString",   # NOT verified
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE,
    "Referer": PAGE_URL,
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# pandas >= 2.2 renamed month-end alias "M" -> "ME"
_pv = tuple(int(x) for x in pd.__version__.split(".")[:2])
MONTH_RULE = "ME" if _pv >= (2, 2) else "M"
WEEK_RULE = "W-FRI"

DAILY_PRICE_PATH = Path(OUT_DIR) / f"{PREFIX}_daily_price.csv"


class FetchError(Exception):
    pass


# ----------------------------- downloading ----------------------------------
def make_session():
    s = http.Session(**SESSION_KWARGS)
    s.headers.update(HEADERS)
    r = s.get(PAGE_URL, timeout=30)  # pick up cookies the site expects
    print(f"Client: {CLIENT} | warm-up GET status: {r.status_code}")
    return s


def parse_index_arg(arg: str):
    """'NAME=CODE' -> (NAME, CODE); 'NAME' -> (NAME, known code or None)."""
    if "=" in arg:
        name, code = arg.rsplit("=", 1)
        return name.strip(), code.strip()
    name = arg.strip()
    return name, INDEX_CODES.get(name.upper()) or INDEX_CODES.get(name)


def build_payload(code, index_name, start: date, end: date) -> str:
    inner = (
        "{'name':'%s','startDate':'%s','endDate':'%s','indexName':'%s'}"
        % (code, start.strftime("%d-%b-%Y"), end.strftime("%d-%b-%Y"), index_name)
    )
    return json.dumps({"cinfo": inner})


def fetch_chunk(session, kind, code, index_name, start: date, end: date, retries=3):
    body = build_payload(code, index_name, start, end)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = session.post(ENDPOINTS[kind], data=body, timeout=60)
            text = r.text
            try:
                outer = json.loads(text)
            except ValueError:
                raise FetchError(
                    f"Server did not return JSON.\n"
                    f"      HTTP status : {r.status_code}\n"
                    f"      Content-Type: {r.headers.get('Content-Type')}\n"
                    f"      Body starts : {text[:300]!r}"
                )
            inner = outer.get("d", "") if isinstance(outer, dict) else outer
            rows = json.loads(inner) if isinstance(inner, str) and inner else (inner or [])
            return pd.DataFrame(rows)
        except FetchError as e:
            last_err = e
        except Exception as e:
            last_err = FetchError(f"{type(e).__name__}: {e}")
        time.sleep(2 * attempt)
    raise last_err


def pick_value_column(df: pd.DataFrame, kind: str):
    """Choose the column holding the index level."""
    cols = [c for c in df.columns if "date" not in c.lower()
            and c.lower() not in ("requestnumber", "indexname", "index name", "index_name")]
    if VALUE_COLUMN and VALUE_COLUMN in df.columns:
        return VALUE_COLUMN
    if kind == "price":
        for c in cols:
            if c.lower() == "close":
                return c
    # TRI: prefer the plain Total Return column, not Net Total Return
    non_net = [c for c in cols if "net" not in c.lower() and "ntr" not in c.lower()]
    for c in non_net:
        if "total" in c.lower() or "tri" in c.lower():
            return c
    if non_net:
        return non_net[0]
    if cols:
        return cols[0]
    raise FetchError(f"Could not find a value column in {list(df.columns)}")


def to_series(raw: pd.DataFrame, kind: str, label: str, first=False) -> pd.Series:
    date_col = next(c for c in raw.columns if "date" in c.lower())
    val_col = pick_value_column(raw, kind)
    if first:
        print(f"  Columns returned: {list(raw.columns)} -> using '{val_col}'")
    dates = pd.to_datetime(raw[date_col], format="mixed", dayfirst=True, errors="coerce")
    vals = pd.to_numeric(raw[val_col].astype(str).str.replace(",", ""), errors="coerce")
    s = pd.Series(vals.values, index=dates, name=label).dropna()
    s = s[~s.index.isna()]
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s


def year_chunks(start: date, end: date):
    """Split [start, end] into pieces that don't cross a calendar year."""
    cur = start
    while cur <= end:
        stop = min(date(cur.year, 12, 31), end)
        yield cur, stop
        cur = stop + timedelta(days=1)


def download_index(session, index_name, code, kind, label, start: date, first):
    """Download [start, today] for one index.

    Returns a Series (possibly empty if the site had no rows in the range),
    or None if a request failed.
    """
    print(f"\n== {index_name} [{kind}] from {start} (code: {code or 'NOT SET - using full name'}) ==")
    code = code or index_name
    end = date.today()
    frames = []
    for c_start, c_end in year_chunks(start, end):
        try:
            df = fetch_chunk(session, kind, code, index_name, c_start, c_end)
        except FetchError as e:
            print(f"  {c_start} -> {c_end}: FAILED\n      {e}")
            print("  Skipping this index.")
            return None
        print(f"  {c_start} -> {c_end}: {len(df)} rows")
        if not df.empty:
            frames.append(df)
        time.sleep(PAUSE)

    if not frames:
        return pd.Series(dtype=float, name=label)
    return to_series(pd.concat(frames, ignore_index=True), kind, label, first=first)


# ------------------------------ existing data -------------------------------
def load_existing():
    """Load the existing daily price CSV (or None if it doesn't exist)."""
    if not DAILY_PRICE_PATH.exists():
        return None
    df = pd.read_csv(DAILY_PRICE_PATH, index_col=0, parse_dates=True)
    df = df[~df.index.isna()]
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df.index.name = "Date"
    return df


# ------------------------------ transforming --------------------------------
def to_period_end(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Last available value in each period, dated on the last actual trading day."""
    out = df.resample(rule).last()
    last_day = df.index.to_series().resample(rule).max()
    mask = last_day.notna().values
    out = out[mask]
    out.index = pd.DatetimeIndex(last_day[mask].values, name="Date")
    return out


def to_returns(prices: pd.DataFrame) -> pd.DataFrame:
    # fill_method=None: don't bridge gaps/NaNs when an index hasn't started yet
    return prices.pct_change(fill_method=None).dropna(how="all")


def save_all(daily: pd.DataFrame):
    out = Path(OUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    prices = {
        "daily": daily,
        "weekly": to_period_end(daily, WEEK_RULE),
        "monthly": to_period_end(daily, MONTH_RULE),
    }
    paths = []
    for freq, px in prices.items():
        px.index.name = "Date"
        p_price = out / f"{PREFIX}_{freq}_price.csv"
        p_ret = out / f"{PREFIX}_{freq}_returns.csv"
        px.to_csv(p_price)
        ret = to_returns(px)
        ret.index.name = "Date"
        ret.to_csv(p_ret)
        paths += [p_price, p_ret]
        print(f"  {freq:8s}: {len(px):6d} price rows, {len(ret):6d} return rows")
    print("\nSaved:")
    for p in paths:
        print(f"  {p}")


# --------------------------------- main -------------------------------------
def main():
    p = argparse.ArgumentParser(
        description="Download/update NSE index history; build daily/weekly/monthly price and return CSVs",
        epilog='Index format: "FULL NAME" or "FULL NAME=CODE".',
    )
    p.add_argument("indices", nargs="*", default=DEFAULT_INDICES,
                   help='Index names, quoted; optionally "NAME=CODE"')
    p.add_argument("--full", action="store_true",
                   help="Ignore the existing daily CSV and re-download everything")
    p.add_argument("--overlap", type=int, default=OVERLAP_DAYS,
                   help=f"Days before the CSV's last date to re-fetch (default {OVERLAP_DAYS})")
    args = p.parse_args()

    existing = None if args.full else load_existing()
    if existing is not None and not existing.empty:
        last_date = existing.index.max().date()
        incr_start = last_date - timedelta(days=max(args.overlap, 0))
        print(f"Existing file: {DAILY_PRICE_PATH}  ({existing.shape[1]} indices, "
              f"{len(existing)} days, last date {last_date})")
        print(f"Incremental mode: fetching from {incr_start} onward for existing indices.")
    else:
        existing = None
        last_date = incr_start = None
        print("No existing daily file (or --full given): downloading full history.")

    try:
        session = make_session()
    except Exception as e:
        sys.exit(f"Could not reach {PAGE_URL}: {e}")

    series, failed, empty_new = [], [], []
    for i, arg in enumerate(args.indices):
        name, code = parse_index_arg(arg)
        label = f"{name}_{SUFFIX}"
        is_new = existing is None or label not in existing.columns
        start = date(START_YEAR, 1, 1) if is_new else incr_start
        if existing is not None and is_new:
            print(f"\n[{name}] not in existing CSV -> downloading full history.")

        s = download_index(session, name, code, DATA_TYPE, label, start, first=(i == 0))
        if s is None:
            failed.append(name)
        elif s.empty:
            if is_new:
                empty_new.append(name)
                print("  No data for any year. Likely the internal code is wrong/missing")
                print("  (use \"FULL NAME=CODE\" or add it to INDEX_CODES).")
            else:
                print("  No new rows (already up to date).")
        else:
            print(f"  -> {len(s)} points, {s.index.min().date()} to {s.index.max().date()}")
            series.append(s)

    # ---- merge new data into existing ----
    new_df = pd.concat(series, axis=1).sort_index() if series else pd.DataFrame()

    if existing is None:
        if new_df.empty:
            sys.exit("\nNo data downloaded for any index.")
        daily = new_df
    else:
        # new values take precedence on overlapping dates; existing fills the rest
        daily = new_df.combine_first(existing) if not new_df.empty else existing.copy()
        cols = list(existing.columns) + [c for c in daily.columns if c not in existing.columns]
        daily = daily[cols].sort_index()
        added = daily.index.difference(existing.index)
        print(f"\nNew dates added: {len(added)}"
              + (f" ({added.min().date()} to {added.max().date()})" if len(added) else ""))

    daily.index.name = "Date"
    print(f"Combined: {daily.shape[1]} indices, {len(daily)} days, "
          f"last date {daily.index.max().date()}")
    save_all(daily)

    problems = failed + empty_new
    if problems:
        print(f"\nNo data / failed for {len(problems)} index(es): {problems}")
        if existing is not None and failed:
            print("(Failed indices keep their old data; re-run to retry them.)")


if __name__ == "__main__":
    main()

    """
    Usage:
    # Update existing data (incremental)
    uv run python -m data_collection.download_returns_nse

    # Full rebuild (ignore existing CSV)
    uv run python -m data_collection.download_returns_nse --full

    # Custom overlap
    uv run python -m data_collection.download_returns_nse --overlap 10

    # Override index list
    uv run python -m data_collection.download_returns_nse "NIFTY 50" "NIFTY 100=MYCODE"
    """