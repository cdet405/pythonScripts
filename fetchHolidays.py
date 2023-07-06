import requests
import pandas as pd
fn = 'holidays.csv'
url = 'https://holidayapi.netlify.app/api/holidays'
r = requests.get(url)
try:
    if r.status_code == 200:
        d = r.json()
    else: 
        print(f'\n Request Failed \n Status Code: {r.status_code}')
        d = None
except Exception as e:
    print(f'\n Fatal Error Occurred:\n {e}')
hl = []
for i in d['holidays']:
    name = i['name'].replace('(', '').replace(')', '')
    date = i['prettyDate'].replace('(', '').replace(')', '')
    day = i['dayOfWeek'].replace('(', '').replace(')', '')
    observedOn = i['observedOn']
    h = {
        'name': name,
        'date': date,
        'day': day,
        'observedOn': observedOn
    }
    hl.append(h)
df = pd.DataFrame(hl)
df.to_csv(fn, index=False)
print(f'\n Saved {fn} \n Script Complete')
