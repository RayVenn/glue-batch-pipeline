"""
Generate synthetic equity trade data (CSV), with a small amount of
deliberately dirty rows so the ETL job has something to clean.

Standard library only. Output is deterministic per date (seeded).

    python generate_data.py --start 2026-10-01 --days 3 --rows 20000
    -> data/raw/trades/dt=2026-10-01/trades_2026-10-01.csv ...
    -> data/raw/securities/securities.csv
"""
import argparse
import csv
import io
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SECURITIES = [
    # symbol, company_name, sector, start price
    ("AAPL", "Apple Inc.", "Technology", 230.0),
    ("MSFT", "Microsoft Corp.", "Technology", 420.0),
    ("NVDA", "NVIDIA Corp.", "Technology", 130.0),
    ("AMZN", "Amazon.com Inc.", "Consumer Discretionary", 190.0),
    ("TSLA", "Tesla Inc.", "Consumer Discretionary", 250.0),
    ("JPM", "JPMorgan Chase & Co.", "Financials", 215.0),
    ("GS", "Goldman Sachs Group Inc.", "Financials", 520.0),
    ("XOM", "Exxon Mobil Corp.", "Energy", 115.0),
    ("CVX", "Chevron Corp.", "Energy", 150.0),
    ("JNJ", "Johnson & Johnson", "Health Care", 160.0),
    ("PFE", "Pfizer Inc.", "Health Care", 29.0),
    ("KO", "Coca-Cola Co.", "Consumer Staples", 70.0),
]
VENUES = ["XNYS", "XNAS", "ARCX", "BATS", "IEXG"]
TRADE_HEADER = ["trade_id", "ts", "symbol", "side", "price", "qty", "venue"]

# Fraction of rows that get each kind of defect.
DIRTY = {
    "missing_price": 0.003,
    "negative_qty": 0.002,
    "messy_symbol": 0.003,    # " aapl " -> cleaned, NOT rejected
    "unknown_symbol": 0.001,
    "bad_side": 0.001,
    "bad_timestamp": 0.001,
    "duplicate": 0.004,
}


def securities_csv() -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["symbol", "company_name", "sector"])
    for sym, name, sector, _ in SECURITIES:
        w.writerow([sym, name, sector])
    return buf.getvalue()


def trades_csv(day: date, rows: int) -> str:
    rng = random.Random(day.toordinal())
    # Daily open price: random walk from the base price, seeded by date.
    prices = {s[0]: s[3] * (1 + rng.gauss(0, 0.02)) for s in SECURITIES}
    weights = [1 / (i + 1) for i in range(len(SECURITIES))]  # some names trade more
    open_t = datetime(day.year, day.month, day.day, 9, 30)
    session_ms = int(6.5 * 3600 * 1000)
    offsets = sorted(rng.randrange(session_ms) for _ in range(rows))

    out, emitted = [], []
    for i, off in enumerate(offsets):
        sym = rng.choices(SECURITIES, weights=weights)[0][0]
        prices[sym] *= 1 + rng.gauss(0, 0.0004)
        ts = open_t + timedelta(milliseconds=off)
        row = [
            f"{day:%Y%m%d}-{i:07d}",
            ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{ts.microsecond // 1000:03d}",
            sym,
            rng.choice(["BUY", "SELL"]),
            f"{prices[sym]:.2f}",
            str(rng.choice([1, 5, 10, 25, 50, 100, 100, 200, 300, 500, 1000])),
            rng.choice(VENUES),
        ]
        r = rng.random()
        acc = 0.0
        for defect, p in DIRTY.items():
            acc += p
            if r < acc:
                if defect == "missing_price":
                    row[4] = ""
                elif defect == "negative_qty":
                    row[5] = str(-int(row[5]))
                elif defect == "messy_symbol":
                    row[2] = f" {row[2].lower()} "
                elif defect == "unknown_symbol":
                    row[2] = "ZZZZ"
                elif defect == "bad_side":
                    row[3] = "HOLD"
                elif defect == "bad_timestamp":
                    row[1] = "not-a-timestamp"
                elif defect == "duplicate" and emitted:
                    out.append(list(rng.choice(emitted)))  # replay an earlier row
                break
        out.append(row)
        emitted.append(row)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(TRADE_HEADER)
    w.writerows(out)
    return buf.getvalue()


def write_local(start: date, days: int, rows: int, root: Path = Path("data")) -> list[Path]:
    paths = []
    sec = root / "raw" / "securities" / "securities.csv"
    sec.parent.mkdir(parents=True, exist_ok=True)
    sec.write_text(securities_csv())
    paths.append(sec)
    for d in trading_days(start, days):
        p = root / "raw" / "trades" / f"dt={d:%Y-%m-%d}" / f"trades_{d:%Y-%m-%d}.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(trades_csv(d, rows))
        paths.append(p)
    return paths


def trading_days(start: date, n: int) -> list[date]:
    """n weekdays starting at `start` (weekends skipped)."""
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-10-01", help="first trade date (YYYY-MM-DD)")
    ap.add_argument("--days", type=int, default=3, help="number of weekdays to generate")
    ap.add_argument("--rows", type=int, default=20000, help="trades per day")
    a = ap.parse_args()
    for p in write_local(date.fromisoformat(a.start), a.days, a.rows):
        print(f"wrote {p}  ({p.stat().st_size / 1024:.0f} KiB)")
