# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "polars"]
# ///
import time, json, pandas as pd, polars as pl

CSV = "/tmp/benchmark_1m.csv"
W, R = 2, 5

def b(fn):
    for _ in range(W): fn()
    ts = []
    for _ in range(R):
        s = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - s) * 1000)
    return sum(ts) / len(ts)

pdf = pd.read_csv(CSV)
plf = pl.read_csv(CSV)

pandas = [
    {"name": "CSV Load", "avgMs": b(lambda: pd.read_csv(CSV))},
    {"name": "Filter (revenue > 5000)", "avgMs": b(lambda: pdf[pdf["revenue"] > 5000])},
    {"name": "GroupBy + Sum", "avgMs": b(lambda: pdf.groupby("region")["revenue"].sum())},
    {"name": "Sort (full)", "avgMs": b(lambda: pdf.sort_values("revenue", ascending=False))},
    {"name": "Sort + Limit 10", "avgMs": b(lambda: pdf.sort_values("revenue", ascending=False).head(10))},
    {"name": "Chained (filter+groupBy+sort)", "avgMs": b(lambda: pdf[pdf["status"] == "active"].groupby("region")["revenue"].sum().sort_values(ascending=False))},
    {"name": "Distinct", "avgMs": b(lambda: pdf["region"].unique())},
]

polars = [
    {"name": "CSV Load", "avgMs": b(lambda: pl.read_csv(CSV))},
    {"name": "Filter (revenue > 5000)", "avgMs": b(lambda: plf.filter(pl.col("revenue") > 5000))},
    {"name": "GroupBy + Sum", "avgMs": b(lambda: plf.group_by("region").agg(pl.col("revenue").sum()))},
    {"name": "Sort (full)", "avgMs": b(lambda: plf.sort("revenue", descending=True))},
    {"name": "Sort + Limit 10", "avgMs": b(lambda: plf.sort("revenue", descending=True).head(10))},
    {"name": "Chained (filter+groupBy+sort)", "avgMs": b(lambda: plf.filter(pl.col("status") == "active").group_by("region").agg(pl.col("revenue").sum()).sort("revenue", descending=True))},
    {"name": "Distinct", "avgMs": b(lambda: plf.select("region").unique())},
]

print(json.dumps({"pandas": pandas, "polars": polars}))
