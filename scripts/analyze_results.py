import os

import matplotlib.pyplot as plt
import pandas as pd
from measurements import run_measurements


def analyze_measurements():
    results = run_measurements()

    df = pd.DataFrame(
        [
            {
                "operation": r.operation,
                "database": r.database,
                "execution_time": r.execution_time,
                "result_count": r.result_count,
            }
            for r in results
        ]
    )

    os.makedirs("results", exist_ok=True)

    # Fixed aggregation with clear column names
    summary = (
        df.groupby(["operation", "database"])
        .agg(
            avg_time=("execution_time", "mean"),
            time_std=("execution_time", "std"),
            avg_results=("result_count", "mean"),
        )
        .round(4)
    )

    print("Performance Summary:")
    print(summary)

    summary.to_csv("results/performance_summary.csv")

    # Create comparison table
    comparison = create_comparison_table(summary)
    print("\nDirect Comparison:")
    print(comparison)
    comparison.to_csv("results/direct_comparison.csv")

    create_visualizations(df)

    return df, summary, comparison


def create_comparison_table(summary):
    operations = summary.index.get_level_values("operation").unique()

    comparison_data = []
    for op in operations:
        try:
            mongo_time = summary.loc[(op, "MongoDB"), "avg_time"]
            pg_time = summary.loc[(op, "PostgreSQL"), "avg_time"]

            winner = "MongoDB" if mongo_time < pg_time else "PostgreSQL"
            speedup = max(mongo_time, pg_time) / min(mongo_time, pg_time)

            comparison_data.append(
                {
                    "operation": op,
                    "mongodb_time": mongo_time,
                    "postgresql_time": pg_time,
                    "winner": winner,
                    "speedup_factor": f"{speedup:.2f}x",
                }
            )
        except KeyError:
            continue

    return pd.DataFrame(comparison_data).set_index("operation")


def plot_execution_time(df):
    plt.figure(figsize=(12, 6))
    avg_times = df.groupby(["operation", "database"])["execution_time"].mean().unstack()
    avg_times.plot(kind="bar", color=["#ff9999", "#66b3ff"])
    plt.title("Average Execution Time by Operation", fontsize=14, fontweight="bold")
    plt.ylabel("Time (seconds)")
    plt.xlabel("Operation")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Database")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("results/execution_time_comparison.png", dpi=300, bbox_inches="tight")
    plt.show()


def plot_overall_performance(df):
    plt.figure(figsize=(8, 8))
    overall_perf = df.groupby("database")["execution_time"].mean()
    colors = ["#ff9999", "#66b3ff"]
    plt.pie(
        overall_perf.values,
        labels=overall_perf.index,
        autopct="%1.1f%%",
        colors=colors,
        startangle=90,
        textprops={"fontsize": 12},
    )
    plt.title(
        "Overall Performance Distribution\n(Lower % = Faster)",
        fontsize=14,
        fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig(
        "results/overall_performance_distribution.png", dpi=300, bbox_inches="tight"
    )
    plt.show()


def create_visualizations(df):
    plot_execution_time(df)
    plot_overall_performance(df)


if __name__ == "__main__":
    df, summary, comparison = analyze_measurements()
    print(f"\nResults saved to 'results/' directory")
    print(f"- performance_summary.csv: Detailed metrics")
    print(f"- direct_comparison.csv: Head-to-head comparison")
    print(f"- performance_analysis.png: Visualizations")
