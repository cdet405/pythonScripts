import pandas as pd
import matplotlib.pyplot as plt

# import csv
data = pd.read_csv('production_data.csv')
pd.options.display.max_columns = None
# goal is 200 per day
goal = 1400
# baseline is 150 per day
base_line = 1050

"""
** not needed for weekly metrics **
# convert date format
data["Date"] = pd.to_datetime(
    data["Date"],
    format="%m/%d/%y"
)
"""

grouped_data = data.groupby(
    [
        "Product",
        "Week"
    ]
)[
    "Total Units Produced"
].sum().reset_index()
pivot_data = grouped_data.pivot(
    index="Week",
    columns="Product",
    values="Total Units Produced"
)
# plot the bar chart
pivot_data.plot(
    kind="bar",
    figsize=(
        12,
        8
    ),
    width=2
)
# add goal threshold (dotted)
plt.axhline(
    y=goal,
    linewidth=1,
    linestyle=":",
    label="Goal",
    color="g"
)
# add baseline threshold (dashed)
plt.axhline(
    y=base_line,
    linewidth=1,
    linestyle="--",
    label="Baseline",
    color="r"
)
plt.xlabel("Week")
plt.ylabel("Total Units Produced")
plt.title("Total Units Produced Weekly By Product")
plt.legend(
    bbox_to_anchor=(
        1.04,
        0.5
    ),
    loc="center left"
)
plt.savefig(
    "packetsByWeek.png",
    bbox_inches="tight"
)
