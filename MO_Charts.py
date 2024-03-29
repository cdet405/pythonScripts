"""
2023-07-21CD
Isnt Pretty But Works.
Creates MO/Production Performace KPIs
Eventually to be used in an eMail Script.
Should Modify to create the Dataframe from a googlesheet instead of CSV file. 
Possibly Add other KPIs Like Labor Cost ETC.
"""
import pandas as pd
import matplotlib.pyplot as plt

def chart(mo_type, data, timeseries):
    if mo_type == 'PACKET':
        filtered_type_data = data.loc[data['Origin'] == "PACKET"]
        goal = 200
        baseline = 150
    if mo_type == 'POUCH':
        filtered_type_data = data.loc[data['Origin'] == "POUCH"]
        goal = 200
        baseline = 150
    if mo_type == 'HAND':
        filtered_type_data = data.loc[data['Origin'] == "HAND"]
        goal = 55
        baseline = 50
    if mo_type == 'KIT':
        filtered_type_data = data.loc[data['Origin'] == "KIT"]
        goal = 55
        baseline = 50
    if mo_type == 'MISC':
        filtered_type_data = data.loc[data['Origin'] == "MISC"]
        goal = 200
        baseline = 150

    if timeseries == "DAY":
        grouped_data = filtered_type_data.groupby(["Product", "Date"])["Units per person per hour"].sum().reset_index()
        pivot_data = grouped_data.pivot(index="Date", columns="Product", values="Units per person per hour")
        x = 1
    if timeseries == "WEEK":
        grouped_data = filtered_type_data.groupby(["Product", "Week"])["Units per person per hour"].sum().reset_index()
        pivot_data = grouped_data.pivot(index="Week", columns="Product", values="Units per person per hour")
        x = 1
 
    pivot_data.plot(kind="bar", figsize=(12, 8), width=2)
    plt.axhline(y=goal * x, linewidth=1, linestyle=":", label="Goal", color="g")
    plt.axhline(y=baseline * x, linewidth=1, linestyle="--", label="Baseline", color="r")
    plt.xlabel(timeseries)
    plt.ylabel("Units Per Person Per Hour")
    plt.title(f"Units Per Person Per Hour ({mo_type}|{timeseries})")
    plt.legend(bbox_to_anchor=(1.04, 0.5), loc="center left")
    plt.savefig(f"{mo_type}By{timeseries}.png", bbox_inches="tight")
    print(f"Saved: {mo_type}By{timeseries}.png")

def main():
    base_data = pd.read_csv('baseDate.csv')
    pd.options.display.max_columns = None
    base_data["Date"] = pd.to_datetime(base_data["Date"], format="%m/%d/%y")
    df = base_data.loc[(base_data['Date'] >= '6/15/2023') & (base_data['Date'] < '7/29/2023')]
    chart(mo_type="PACKET", data=df, timeseries="DAY")
    chart(mo_type="PACKET", data=df, timeseries="WEEK")
    chart(mo_type="POUCH", data=df, timeseries="DAY")
    chart(mo_type="POUCH", data=df, timeseries="WEEK")
    chart(mo_type="KIT", data=df, timeseries="DAY")
    chart(mo_type="KIT", data=df, timeseries="WEEK")
    chart(mo_type="MISC", data=df, timeseries="DAY")
    chart(mo_type="MISC", data=df, timeseries="WEEK")
    print('done')


if __name__ == "__main__":
    main()
















