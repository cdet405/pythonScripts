import requests
import pandas as pd
from config import XRAPI

key = XRAPI
url = f'https://v6.exchangerate-api.com/v6/{key}/pair/USD/CAD'

# Making our request
response = requests.get(url)
data = response.json()

# Insert Data into DataFrame
df = pd.json_normalize(data)
# Drop unused columns
df = df.drop(['documentation','terms_of_use','time_last_update_utc','time_next_update_utc'], axis=1)
# Save to CSV
df.to_csv('cadtousd.csv', index=False)
print('great success')
