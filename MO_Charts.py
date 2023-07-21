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
    # this always printed?
    #else:
    #    print(f'ERROR: {mo_type} Not Found In Data. \nPlease Try: PACKET, POUCH, HAND, KIT or MISC')

    if timeseries == "DAY":
        grouped_data = filtered_type_data.groupby(["Product", "Date"])["Total Units Produced"].sum().reset_index()
        pivot_data = grouped_data.pivot(index="Date", columns="Product", values="Total Units Produced")
        x = 1
    if timeseries == "WEEK":
        grouped_data = filtered_type_data.groupby(["Product", "Week"])["Total Units Produced"].sum().reset_index()
        pivot_data = grouped_data.pivot(index="Week", columns="Product", values="Total Units Produced")
        x = 7
    # this always printed?
    #else:
    #    print(f'ERROR: timeseries [{timeseries}] is not Valid.\nTry DAY or WEEK')
    

    pivot_data.plot(kind="bar", figsize=(12, 8), width=2)
    plt.axhline(y=goal * x, linewidth=1, linestyle=":", label="Goal", color="g")
    plt.axhline(y=baseline * x, linewidth=1, linestyle="--", label="Baseline", color="r")
    plt.xlabel(timeseries)
    plt.ylabel("Total Units Produced")
    plt.title(f"Total Units Produced By Product ({timeseries})")
    plt.legend(bbox_to_anchor=(1.04, 0.5), loc="center left")
    plt.savefig(f"{mo_type}By{timeseries}.png", bbox_inches="tight")
    print(f"Saved: {mo_type}By{timeseries}.png")

def main():
    base_data = pd.read_csv('baseDate.csv')
    pd.options.display.max_columns = None
    base_data["Date"] = pd.to_datetime(base_data["Date"], format="%m/%d/%y")
    chart(mo_type="PACKET", data=base_data, timeseries="DAY")
    chart(mo_type="PACKET", data=base_data, timeseries="WEEK")
    chart(mo_type="POUCH", data=base_data, timeseries="DAY")
    chart(mo_type="POUCH", data=base_data, timeseries="WEEK")
    chart(mo_type="KIT", data=base_data, timeseries="DAY")
    chart(mo_type="KIT", data=base_data, timeseries="WEEK")
    chart(mo_type="MISC", data=base_data, timeseries="DAY")
    chart(mo_type="MISC", data=base_data, timeseries="WEEK")
    print('done')


if __name__ == "__main__":
    main()








