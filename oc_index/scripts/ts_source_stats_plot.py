#!/usr/bin/env python3

import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create a source-combination matrix from a CSV."
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
    # Source names and display labels
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

    if not args.keep_zero:
        df = df[df["count"] > 0]

    df = df.sort_values(["size", "combination"])

    # --------------------------------------------------
    # Build rows
    # --------------------------------------------------

    rows = []
    group_info = []

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

        group_info.append((size, start, end))

        # blank row between groups
        y += 1

    # --------------------------------------------------
    # Figure
    # --------------------------------------------------

    fig_height = max(6, len(rows) * 0.30)

    fig, ax = plt.subplots(figsize=(12, fig_height))

    # alternating row colors
    for row in rows:

        if row["y"] % 2 == 0:

            ax.add_patch(
                Rectangle(
                    (-2.4, row["y"] - 0.5),
                    len(order) + 2.8,
                    1,
                    facecolor="#F3F6F8",
                    edgecolor="none",
                    zorder=0,
                )
            )

    # counts
    for row in rows:

        ax.text(
            -1.15,
            row["y"],
            f"{row['count']:,}",
            ha="right",
            va="center",
            fontsize=9,
        )

    # X marks
    for row in rows:

        for x, source in enumerate(order):

            if source in row["combo"]:

                ax.text(
                    x,
                    row["y"],
                    "x",
                    ha="center",
                    va="center",
                    fontsize=12,
                    fontweight="bold",
                )

    # group labels and separators
    for i, (size, start, end) in enumerate(group_info):

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

        if i < len(group_info) - 1:

            ax.hlines(
                end - 0.5,
                -2.4,
                len(order) - 0.5,
                colors="gray",
                linestyles="dotted",
            )

    # headers
    ax.set_xticks(range(len(order)))
    ax.set_xticklabels(
        [labels[s] for s in order],
        fontsize=12,
        fontweight="bold",
    )

    ax.xaxis.tick_top()

    # cosmetics
    ax.set_xlim(-2.4, len(order) - 0.5)
    ax.set_ylim(y - 0.5, -0.5)

    ax.set_yticks([])

    ax.spines["left"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(False)

    plt.tight_layout()

    plt.savefig(args.output, dpi=args.dpi, bbox_inches="tight")

    plt.show()


if __name__ == "__main__":
    main()
