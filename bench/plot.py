#!/usr/bin/env python3
"""Plots and a summary table from the SOTERIA benchmark CSV.

Usage: bench/plot.py results.csv [--out DIR] [--scale S]

For each SOTERIA runner (gramine-direct, gramine-sgx) and scale in the CSV:
  runtime-<runner>-scale-<s>.png   training time per algorithm (one panel each,
                                   median with min-max whiskers), vanilla vs SML-1 vs SML-2
  overhead-<runner>-scale-<s>.png  median time relative to vanilla (1 = no overhead)
  summary.md                       the same numbers, plus model quality
Warm-up repetitions are left out. Vanilla rows (runner "native") are the
baseline for every runner. Needs only python3 and matplotlib.
"""
import argparse
import collections
import csv
import os
import statistics
import sys

MODES = ["vanilla", "sml1", "sml2"]
LABELS = {"vanilla": "Vanilla Spark", "sml1": "SOTERIA SML-1", "sml2": "SOTERIA SML-2"}
COLORS = {"vanilla": "#2a78d6", "sml1": "#eb6834", "sml2": "#1baf7a"}  # categorical slots 1-3
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def load(path):
    with open(path, newline="") as f:
        rows = [r for r in csv.DictReader(f) if r["warmup"].strip().lower() != "true"]
    # Rows without input_partitions were written before every mode read the
    # same input splits; their SML-1 and SML-2 models are not comparable.
    old = [r for r in rows if not (r.get("input_partitions") or "").strip()]
    if old:
        print(f"skipping {len(old)} rows written before input splits were fixed; "
              "rerun those modes (see bench/README.md)", file=sys.stderr)
        rows = [r for r in rows if r not in old]
    if not rows:
        sys.exit(f"{path}: no measured (non-warm-up) rows")
    return rows


def group(rows):
    """(scale, runner-or-vanilla, algo, mode) -> list of seconds, and quality per key."""
    times, quality = collections.defaultdict(list), collections.defaultdict(list)
    for r in rows:
        key = (r["scale"], r["runner"] if r["mode"] != "vanilla" else "native", r["algo"], r["mode"])
        times[key].append(float(r["train_s"]))
        quality[key].append((r["metric"], float(r["quality"])))
    return times, quality


def style(ax):
    ax.set_facecolor("white")
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelsize=8)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def runtime_figure(plt, algos, stats, title, path):
    cols = min(4, len(algos))
    rows = (len(algos) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(3.2 * cols, 2.8 * rows + 0.6), squeeze=False)
    for ax in axes.flat[len(algos):]:
        ax.set_visible(False)
    for ax, algo in zip(axes.flat, algos):
        style(ax)
        present = [m for m in MODES if (algo, m) in stats]
        for i, m in enumerate(present):
            med, lo, hi = stats[(algo, m)]
            ax.bar(i, med, width=0.72, color=COLORS[m], edgecolor="white", linewidth=2)
            ax.errorbar(i, med, yerr=[[med - lo], [hi - med]], color=MUTED, capsize=3, linewidth=1)
            ax.annotate(f"{med:.1f}s", (i, hi), textcoords="offset points", xytext=(0, 3),
                        ha="center", fontsize=7, color=INK)
        ax.set_xticks(range(len(present)))
        ax.set_xticklabels([m.upper() if m != "vanilla" else "vanilla" for m in present], fontsize=8)
        ax.set_title(algo, fontsize=10, color=INK, loc="left")
        ax.set_ylim(0, max(stats[(algo, m)][2] for m in present) * 1.2)
    axes.flat[0].set_ylabel("training time (s)", fontsize=8, color=MUTED)
    handles = [plt.Rectangle((0, 0), 1, 1, color=COLORS[m]) for m in MODES]
    fig.legend(handles, [LABELS[m] for m in MODES], loc="upper right", ncol=3, frameon=False, fontsize=8)
    fig.suptitle(title, x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(path, dpi=150)
    plt.close(fig)


def overhead_figure(plt, algos, stats, title, path):
    modes = [m for m in ("sml1", "sml2") if any((a, m) in stats for a in algos)]
    algos = [a for a in algos if (a, "vanilla") in stats and any((a, m) in stats for m in modes)]
    if not algos or not modes:
        return False
    fig, ax = plt.subplots(figsize=(1.1 * len(algos) + 2, 3.4))
    style(ax)
    width = 0.8 / len(modes)
    for j, m in enumerate(modes):
        for i, a in enumerate(algos):
            if (a, m) not in stats:
                continue
            x = i + (j - (len(modes) - 1) / 2) * width
            factor = stats[(a, m)][0] / stats[(a, "vanilla")][0]
            ax.bar(x, factor, width=width, color=COLORS[m], edgecolor="white", linewidth=2,
                   label=LABELS[m] if i == 0 else None)
            ax.annotate(f"{factor:.1f}x", (x, factor), textcoords="offset points", xytext=(0, 3),
                        ha="center", fontsize=7, color=INK)
    ax.axhline(1.0, color=MUTED, linewidth=1, linestyle="--")
    ax.set_xticks(range(len(algos)))
    ax.set_xticklabels(algos, fontsize=8)
    ax.set_ylabel("median time / vanilla", fontsize=8, color=MUTED)
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    ax.set_title(title, fontsize=11, color=INK, loc="left")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return True


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("csv")
    p.add_argument("--out", default="plots")
    p.add_argument("--scale", help="only this scale (default: every scale in the CSV)")
    a = p.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        sys.exit("matplotlib is missing (Rocky: sudo dnf install python3-matplotlib; Ubuntu: sudo apt install python3-matplotlib)")

    times, quality = group(load(a.csv))
    os.makedirs(a.out, exist_ok=True)
    scales = sorted({k[0] for k in times if a.scale is None or k[0] == a.scale}, key=float)
    runners = sorted({k[1] for k in times if k[1] != "native"}) or ["native"]
    lines = ["# SOTERIA benchmark summary", "",
             "Median training time over the measured repetitions (min-max in brackets); "
             "overhead is relative to vanilla Spark.", ""]
    written = []
    for scale in scales:
        for runner in runners:
            stats, qual = {}, {}
            for (s, r, algo, mode), secs in times.items():
                if s == scale and (r == runner or mode == "vanilla"):
                    stats[(algo, mode)] = (statistics.median(secs), min(secs), max(secs))
                    metric = quality[(s, r, algo, mode)][0][0]
                    qual[(algo, mode)] = (metric, statistics.median(q for _, q in quality[(s, r, algo, mode)]))
            if not any(m != "vanilla" for _, m in stats) and runner != "native":
                continue
            algos = sorted({algo for algo, _ in stats})
            tag = f"{runner}-scale-{scale}"
            title = f"Training time, SOTERIA under {runner}, scale {scale}"
            runtime_figure(plt, algos, stats, title, os.path.join(a.out, f"runtime-{tag}.png"))
            written.append(f"runtime-{tag}.png")
            if overhead_figure(plt, algos, stats, f"Overhead vs vanilla Spark ({runner}, scale {scale})",
                               os.path.join(a.out, f"overhead-{tag}.png")):
                written.append(f"overhead-{tag}.png")

            lines += [f"## {runner}, scale {scale}", "",
                      "| algorithm | metric | " + " | ".join(LABELS[m] for m in MODES) +
                      " | SML-1 overhead | SML-2 overhead |",
                      "|---|---|" + "---:|" * (len(MODES) + 2)]
            for algo in algos:
                metric = next((qual[(algo, m)][0] for m in MODES if (algo, m) in qual), "")
                cells = []
                for m in MODES:
                    if (algo, m) in stats:
                        med, lo, hi = stats[(algo, m)]
                        cells.append(f"{med:.2f}s [{lo:.2f}-{hi:.2f}], {qual[(algo, m)][1]:.4g}")
                    else:
                        cells.append("-")
                over = [f"{stats[(algo, m)][0] / stats[(algo, 'vanilla')][0]:.2f}x"
                        if (algo, m) in stats and (algo, "vanilla") in stats else "-" for m in ("sml1", "sml2")]
                lines.append(f"| {algo} | {metric} | " + " | ".join(cells + over) + " |")
            lines.append("")
    with open(os.path.join(a.out, "summary.md"), "w") as f:
        f.write("\n".join(lines))
    print(f"wrote {', '.join(written + ['summary.md'])} to {a.out}")


if __name__ == "__main__":
    main()
