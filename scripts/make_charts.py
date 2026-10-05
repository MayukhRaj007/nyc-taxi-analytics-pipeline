"""Render the README charts from the dbt marts (read-only).

Usage: python scripts/make_charts.py [output_dir]

Reads marts.mart_daily_revenue, marts.mart_zone_performance and
marts.mart_hourly_demand, writes three PNGs and prints the numbers behind them,
so every figure in the README can be traced to a real query result.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

DB_PATH = os.environ.get("DUCKDB_PATH", "warehouse/taxi.duckdb")
OUT_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("docs/images")

# One hue for one series (identity never depends on color alone: titles and
# direct labels carry the meaning). Ink colors are neutral and high-contrast.
ACCENT = "#1F5FBF"
INK = "#1B1F24"
MUTED = "#5B6470"
GRID = "#E3E6EA"
SURFACE = "#FFFFFF"


def style(ax, title: str, subtitle: str) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=MUTED, length=0, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.text(0, 1.14, title, transform=ax.transAxes, fontsize=13, fontweight="bold", color=INK)
    ax.text(0, 1.065, subtitle, transform=ax.transAxes, fontsize=9, color=MUTED)


def save(fig, name: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / name
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    print(f"wrote {path}")


def daily_revenue(con) -> None:
    rows = con.execute(
        "SELECT pickup_date, total_revenue::DOUBLE / 1e6 FROM marts.mart_daily_revenue ORDER BY pickup_date"
    ).fetchall()
    dates, rev = [r[0] for r in rows], [r[1] for r in rows]
    hi, lo = max(range(len(rev)), key=rev.__getitem__), min(range(len(rev)), key=rev.__getitem__)

    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.plot(dates, rev, color=ACCENT, linewidth=2)
    ax.fill_between(dates, rev, color=ACCENT, alpha=0.08)
    ax.set_ylim(0, max(rev) * 1.18)
    ax.set_xlim(dates[0], dates[-1])
    ax.yaxis.set_major_formatter(lambda v, _: f"${v:.1f}M")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO, interval=2))
    style(
        ax,
        "Daily taxi revenue follows a weekly rhythm",
        f"Yellow taxi total fares per pickup day, {dates[0]:%b %d} to {dates[-1]:%b %d, %Y}",
    )
    for i, label, dy in ((hi, "peak", 10), (lo, "low", -18)):
        ax.plot(dates[i], rev[i], "o", color=ACCENT, markersize=7, markeredgecolor=SURFACE, markeredgewidth=2)
        ax.annotate(
            f"{label}: ${rev[i]:.2f}M\n{dates[i]:%a %b %d}",
            (dates[i], rev[i]),
            textcoords="offset points",
            xytext=(8, dy),
            fontsize=9,
            color=INK,
        )
    save(fig, "daily_revenue.png")
    print(f"  days={len(rev)} total=${sum(rev):.1f}M peak={dates[hi]} ${rev[hi]:.2f}M low={dates[lo]} ${rev[lo]:.2f}M")


def busiest_zones(con) -> None:
    rows = con.execute(
        "SELECT zone, borough, trip_count FROM marts.mart_zone_performance ORDER BY trip_count DESC LIMIT 10"
    ).fetchall()[::-1]
    names = [f"{z} ({b})" for z, b, _ in rows]
    counts = [c / 1e3 for *_, c in rows]

    fig, ax = plt.subplots(figsize=(9, 4.6))
    bars = ax.barh(names, counts, color=ACCENT, height=0.62)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:,.0f}k")
    ax.set_xlim(0, max(counts) * 1.15)
    style(ax, "Midtown and the Upper East Side dominate pickups", "Ten busiest pickup zones by number of trips")
    ax.grid(axis="y", visible=False)
    for bar, value in zip(bars, counts, strict=True):
        ax.text(
            value + max(counts) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{value:,.0f}k",
            va="center",
            fontsize=9,
            color=INK,
        )
    save(fig, "busiest_zones.png")
    for z, b, c in rows[::-1]:
        print(f"  {z} ({b}): {c:,}")


def tip_by_hour(con) -> None:
    rows = con.execute(
        "SELECT pickup_hour, weighted_tip_pct FROM marts.mart_hourly_demand ORDER BY pickup_hour"
    ).fetchall()
    hours, tip = [r[0] for r in rows], [r[1] for r in rows]
    hi, lo = max(range(24), key=tip.__getitem__), min(range(24), key=tip.__getitem__)

    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.plot(
        hours, tip, color=ACCENT, linewidth=2, marker="o", markersize=5, markeredgecolor=SURFACE, markeredgewidth=1.5
    )
    ax.set_xticks(range(0, 24, 3))
    ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 3)])
    ax.set_ylim(min(tip) - 2, max(tip) + 3)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
    style(
        ax,
        "Tip percentage by hour of day",
        "Total tips / total metered fares, card-paid trips only (cash tips are not recorded)",
    )
    for i, dy in ((hi, 10), (lo, -18)):
        ax.annotate(
            f"{hours[i]:02d}:00  {tip[i]:.1f}%",
            (hours[i], tip[i]),
            textcoords="offset points",
            xytext=(8, dy),
            fontsize=9,
            color=INK,
        )
    save(fig, "tip_pct_by_hour.png")
    print(f"  highest {hours[hi]:02d}:00 = {tip[hi]:.2f}%, lowest {hours[lo]:02d}:00 = {tip[lo]:.2f}%")


def main() -> None:
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        daily_revenue(con)
        busiest_zones(con)
        tip_by_hour(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
