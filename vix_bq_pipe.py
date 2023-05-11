# update time delta assuming script isnt connected to a cronjob
from google.cloud import bigquery
import pandas as pd
import pyarrow
from datetime import date, timedelta, datetime
import time
import os
from config import carkeys, dest

# gcp creds
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = carkeys
client = bigquery.Client()
# date params for df filter
dt = date.today()
sdt = date.today() - timedelta(10)
# scriptTS
epoch_time = int(time.time())
# vix data endpoint
url = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv'
# read in date
d = pd.read_csv(url)
# create dataframe
df = pd.DataFrame(d)
# add script ts
df['pyts'] = epoch_time
# rename columns
df.rename(columns={
    'DATE': 'vix_date',
    'OPEN': 'vix_open',
    'HIGH': 'vix_high',
    'LOW': 'vix_low',
    'CLOSE': 'vix_close'
}, inplace=True)
# convert dtype to date from obj
df['vix_date'] = pd.to_datetime(df['vix_date'], format='%m/%d/%Y')
# create new dataframe for data within params
filtered_df = df.loc[(df['vix_date'] >= sdt.strftime('%m/%d/%Y')) & (df['vix_date'] < dt.strftime('%m/%d/%Y'))]
path = '/Users/chaddetwiler/PycharmProjects/pythonProject3/VIXresults/'
fileName = 'vix.csv'
output_file = os.path.join(path, fileName)
# save filtered df as csv
filtered_df.to_csv(output_file, index=False)
print('------------------------------------------------------------------')
print('Starting Scripts (1/2)...')
print('Fetching Data...')
time.sleep(.1)
print('Building CSV...')
time.sleep(.1)
print('Saving CSV')
time.sleep(.1)
print('Loading data to Staged table')
time.sleep(.2)
print('Task Assigned to Robochad')
print('Target Table Found')
print('Truncating Table && Inserting New Data')
print('Loading...')


# staging table
table_id = dest
file_path = path+fileName
# configure job to write truncate
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)
# open and load file to bq
with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)
job.result()
print(f'Script Completed (1/2), {job.result()}')
print('------------------------------------------------------------------')
print('Second Scripted Started')
time.sleep(.3)
print('------------------------------------------------------------------')
time.sleep(.2)
print('Merging Stage to Prod...')
merge_sql = """
MERGE `project.dataset.vix` p
USING `project.dataset.vixStg` s
ON p.vix_date = s.vix_date
WHEN MATCHED THEN
  UPDATE SET
    p.vix_date = s.vix_date,
    p.vix_open = s.vix_open,
    p.vix_high = s.vix_high,
    p.vix_low = s.vix_low,
    p.vix_close = s.vix_close,
    p.pyts = s.pyts
WHEN NOT MATCHED THEN 
  INSERT(
    vix_date,
    vix_open,
    vix_high,
    vix_low,
    vix_close,
    pyts
  )
VALUES(
    s.vix_date,
    s.vix_open,
    s.vix_high,
    s.vix_low,
    s.vix_close,
    s.pyts
)
"""
query_job = client.query(merge_sql)
print('Merging...')
time.sleep(1)
print(f'Completed {query_job.result()}')
print('------------------------------------------------------------------')
