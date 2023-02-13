import pandas as pd
#Read in CSV and Choose Columns to Keep, Everything Else Will be Dropped
df = pd.read_csv(r'/Users/redacted/Downloads/orders_95_9-21-2021_124636.csv',encoding="ISO-8859-1"
                 , usecols= ['orderid','odate','ofirstname', 'olastname', 'oemail', 'itemid','itemname',
                             'numitems', 'unitprice', 'unitcost','itemamount'])
#Convert Date Column to DateTime so pandas can filter
df['odate'] = pd.to_datetime(df['odate'], format='%m/%d/%Y')
#Create View of Filtered Dates
filtered_df = df.loc[(df['odate']>= '1/1/2018') & (df['odate'] < '10/20/2020')]
#Save CSV remove Indexing
filtered_df.to_csv('ndorders2018-2020.csv', index=False)