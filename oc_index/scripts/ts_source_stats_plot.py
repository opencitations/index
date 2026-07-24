#!/usr/bin/env python3

import argparse

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Rectangle


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot source combinations matrix."
    )

    parser.add_argument(
        "csv",
        help="Input CSV file."
    )

    parser.add_argument(
        "-o",
        "--output",
        default="source_combinations.png",
        help="Output figure (default: source_combinations.png)"
    )

    parser.add_argument(
        "--keep-zero",
        action="store_true",
        help="Keep rows with count = 0."
    )

    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="Figure DPI (default: 300)."
    )

    return parser.parse_args()


def main():

    args = parse_args()

    # --------------------------------------------------
    # Labels
    # --------------------------------------------------

    labels = {
        "coci": "Crossref",
        "doci": "DataCite",
        "poci": "PubMed",
        "oroci": "OpenAire",
        "joci": "JaLC",
        "outoci": "OutCite",
        "moci": "Matilda",
    }

    order = list(labels.keys())

    # --------------------------------------------------
    # Read CSV
    # --------------------------------------------------

    df = pd.read_csv(args.csv)

    # Keep only combinations of size >= 2
    df = df[df["size"] >= 2]

    # Remove zero-count combinations unless requested
    if not args.keep_zero:
        df = df[df["count"] > 0]

    df = df.sort_values(["size", "combination"])

    # --------------------------------------------------
    # Build plotting rows
    # --------------------------------------------------

    rows = []
    groups = []

    y = 0

    for size in sorted(df["size"].unique()):

        subset = df[df["size"] == size]

        start = y

        for _, row in subset.iterrows():

            rows.append({
                "size": size,
                "count": row["count"],
                "combo": row["combination"].split("-"),
                "y": y
            })

            y += 1

        end = y

        groups.append((size, start, end))

        # blank row between groups
        y += 1

    # --------------------------------------------------
    # Figure
    # --------------------------------------------------

    fig_height = max(6, len(rows) * 0.28)

    fig, ax = plt.subplots(figsize=(12, fig_height))

    n_sources = len(order)

    # --------------------------------------------------
    # Alternating row colors
    # --------------------------------------------------

    for row in rows:

        if row["y"] % 2 == 0:

            ax.add_patch(
                Rectangle(
                    (-2.4, row["y"] - 0.5),
                    n_sources + 2.9,
                    1,
                    facecolor="#f3f6f8",
                    edgecolor="none",
                    zorder=0,
                )
            )

    # --------------------------------------------------
    # Counts
    # --------------------------------------------------

    for row in rows:

        ax.text(
            -1.15,
            row["y"],
            f"{row['count']:,}",
            ha="right",
            va="center",
            fontsize=9,
        )

    # --------------------------------------------------
    # Circles + connecting line
    # --------------------------------------------------

    for row in rows:

        xs = [
            i
            for i, source in enumerate(order)
            if source in row["combo"]
        ]

        if len(xs) > 1:

            ax.plot(
                [min(xs), max(xs)],
                [row["y"], row["y"]],
                color="black",
                linewidth=1.3,
                zorder=2,
            )

        ax.scatter(
            xs,
            [row["y"]] * len(xs),
            s=55,
            color="black",
            zorder=3,
        )

    # --------------------------------------------------
    # Group labels
    # --------------------------------------------------

    for i, (size, start, end) in enumerate(groups):

        center = (start + end - 1) / 2

        ax.text(
            -2.0,
            center,
            f"{size}\nSource\nCombo",
            rotation=90,
            ha="center",
            va="center",
            fontsize=10,
        )

        if i < len(groups) - 1:

            ax.hlines(
                end - 0.5,
                -2.4,
                n_sources - 0.5,
                color="gray",
                linestyle="dotted",
                linewidth=0.8,
            )

    # --------------------------------------------------
    # Column labels
    # --------------------------------------------------

    ax.set_xticks(range(n_sources))
    ax.set_xticklabels(
        [labels[s] for s in order],
        fontsize=12,
        fontweight="bold",
    )

    ax.xaxis.tick_top()

    # --------------------------------------------------
    # Appearance
    # --------------------------------------------------

    ax.set_xlim(-2.4, n_sources - 0.5)
    ax.set_ylim(y - 0.5, -0.5)

    ax.set_yticks([])

    ax.spines["left"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(False)

    plt.tight_layout()

    plt.savefig(
        args.output,
        dpi=args.dpi,
        bbox_inches="tight",
    )

    plt.show()


if __name__ == "__main__":
    main()
