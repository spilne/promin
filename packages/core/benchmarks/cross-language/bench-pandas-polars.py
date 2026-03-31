# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "polars"]
# ///

"""Cross-language benchmark: Pandas vs Polars on 1M row CSV."""

import time
import pandas as pd
import polars as pl

CSV_PATH = "/tmp/benchmark_1m.csv"
WARMUP = 1
RUNS = 5

def bench(name, fn):
    # Warmup
    for _ in range(WARMUP):
        fn()

    times = []
    for _ in range(RUNS):
        start = time.perf_counter()
        result = fn()
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)

    avg = sum(times) / len(times)
    min_t = min(times)
    print(f"  {name}: {avg:.1f}ms avg, {min_t:.1f}ms min")
    return avg

print("=" * 60)
print("Pandas")
print("=" * 60)

print("\n--- CSV Load ---")
bench("pd.read_csv", lambda: pd.read_csv(CSV_PATH))
pdf = pd.read_csv(CSV_PATH)

print("\n--- Filter (revenue > 5000) ---")
bench("pandas filter", lambda: pdf[pdf["revenue"] > 5000])

print("\n--- GroupBy + Sum ---")
bench("pandas groupby", lambda: pdf.groupby("region")["revenue"].sum())

print("\n--- GroupBy + Multiple Agg ---")
bench("pandas multi-agg", lambda: pdf.groupby("region").agg({"revenue": "sum", "quantity": "mean", "score": "max"}))

print("\n--- Sort + Limit 10 ---")
bench("pandas sort+head", lambda: pdf.sort_values("revenue", ascending=False).head(10))

print("\n--- Filter + GroupBy + Sort ---")
bench("pandas chained", lambda: (
    pdf[pdf["status"] == "active"]
    .groupby("region")["revenue"]
    .sum()
    .sort_values(ascending=False)
))

print("\n--- Distinct regions ---")
bench("pandas unique", lambda: pdf["region"].unique())

print()
print("=" * 60)
print("Polars")
print("=" * 60)

print("\n--- CSV Load ---")
bench("pl.read_csv", lambda: pl.read_csv(CSV_PATH))
plf = pl.read_csv(CSV_PATH)

print("\n--- Filter (revenue > 5000) ---")
bench("polars filter", lambda: plf.filter(pl.col("revenue") > 5000))

print("\n--- GroupBy + Sum ---")
bench("polars groupby", lambda: plf.group_by("region").agg(pl.col("revenue").sum()))

print("\n--- GroupBy + Multiple Agg ---")
bench("polars multi-agg", lambda: plf.group_by("region").agg([
    pl.col("revenue").sum(),
    pl.col("quantity").mean(),
    pl.col("score").max(),
]))

print("\n--- Sort + Limit 10 ---")
bench("polars sort+head", lambda: plf.sort("revenue", descending=True).head(10))

print("\n--- Filter + GroupBy + Sort ---")
bench("polars chained", lambda: (
    plf.filter(pl.col("status") == "active")
    .group_by("region")
    .agg(pl.col("revenue").sum())
    .sort("revenue", descending=True)
))

print("\n--- Distinct regions ---")
bench("polars unique", lambda: plf.select("region").unique())
