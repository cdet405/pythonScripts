from google.cloud import bigquery
import pandas as pd
import pyarrow
import time
import os
from config import robo
from config import ep_cci
from config import cci_table
from config import cci_folder
from config import cci_fn

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = robo
client = bigquery.Client()

epoch_time = int(time.time())
url = ep_cci

d = pd.read_csv(url)
df = pd.DataFrame(d)
df['ts'] = epoch_time
df.rename(columns={
    'LOCATION': 'location',
    'INDICATOR': 'indicator',
    'SUBJECT': 'subject',
    'MEASURE': 'measure',
    'FREQUENCY': 'frequency',
    'TIME': 'time',
    'Value': 'value',
    'Flag Codes': 'flagCodes'
}, inplace=True)

path = cci_folder
fileName = cci_fn
output_file = os.path.join(path, fileName)
df.to_csv(output_file, index=False)


table_id = cci_table
file_path = path+fileName

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)
with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)
job.result()



