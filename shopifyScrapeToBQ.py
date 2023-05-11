# cust facing shopify snapshot pipeline 20230511CD
import requests
import pandas as pd
import datetime
import os
import time
import re
import pyarrow
from google.cloud import bigquery
from config import carkeys, dest, parking

# open bq connection
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = carkeys
client = bigquery.Client()

# set file path
path = '~/Xresults/'
bqpath = parking

# Grab Current DateTime & convert to EST
d = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
cd = d.strftime("%Y_%m_%d-%I_%M_%S_%p")
sqld = d.strftime("%Y-%m-%d")

# Target URL
url_template = 'https://X.com/products.json?&limit=250&page={}'
# grab domain
pattern = r'https?://([a-z0-9-]+)\.'
match = re.search(pattern, url_template)
if match:
    subdomain = match.group(1)
# create empty list
product_list = []

# loop through pages
for page in range(1, 2):  # assuming you want to scrape n pages
    url = url_template.format(page)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
    r = requests.get(url, headers=headers)
    try:
        if r.status_code == 200:
            data = r.json()
        else:
            print(f"Page {page} Request failed with status code", r.status_code)
            data = None
    except Exception as e:
        print("An error occurred:", e)
        data = None

    for item in data['products']:
        id = item['id']
        title = item['title']
        handle = item['handle']
        body = item['body_html']
        created = item['created_at']
        vendor = item['vendor']
        product_type = item['product_type']
        tags = item['tags']
        for variant in item['variants']:
            vid = variant['id']
            vtitle = variant['title']
            price = variant['price']
            sku = variant['sku']
            requires_shipping = variant['requires_shipping']
            grams = variant['grams']
            compare_at_price = variant['compare_at_price']
            position = variant['position']
            available = variant['available']
            updated_at = variant['updated_at']
            product_id = variant['product_id']
            # create dictionary
            products = {
                'id': id,
                'title': title,
                'handle': handle,
                'body': body,
                'created': created,
                'vendor': vendor,
                'product_type': product_type,
                'vid': vid,
                'price': price,
                'compare_at_price': compare_at_price,
                'sku': sku,
                'vtitle': vtitle,
                'position': position,
                'product_id': product_id,
                'available': available,
                'requires_shipping': requires_shipping,
                'weight': grams,
                'updated_at': updated_at,
                'tags': tags
            }
            # add dictionary to list
            product_list.append(products)

    # sleep for a second to avoid 429 Too Many Requests Error
    print(f'Completed Scrape For Page {page}')
    time.sleep(1)
    print(f'Scraping Page {page + 1}')
    print('.')
    time.sleep(.1)
    print('..')
    time.sleep(.1)
    print('...')
    time.sleep(.1)
    print('....')
    time.sleep(.1)
    print('.....')

# Put list in dataframe

df = pd.DataFrame(product_list)
# Remove Most HTML & Clean Data
df['body'] = df['body'].str.replace(r'<[^<>]*>|✔︎|\s', ' ', regex=True)
df['tags'] = df['tags'].str.replace(r'\[|\]|\]|\'|#', ' ', regex=True)
df['body'] = df['body'].str.strip()
df['weight'] = df['weight']*0.00220462
df['site'] = subdomain
df['runDate'] = sqld
# Build Product URL
df['link'] = 'https://{subdomain}.com/products/'+df['handle'].astype(str)+'?variant='+df['vid'].astype(str)
# Convert dataframe to CSV file
fileName = f'test{subdomain}{cd}.csv'
bqFileName = f'{subdomain}.csv'
output_file = os.path.join(path, fileName)
bqOutputFile = os.path.join(bqpath, bqFileName)
# file to send to bq (will be overwritten daily)
df.to_csv(bqOutputFile, index=False)
# dated file to log snap
df.to_csv(output_file, index=False)
product_list.clear()
print(f'Scrape Completed - file: {fileName}')
print('Starting BigQuery Process...')
time.sleep(.5)
print('Building Job Config...')
time.sleep(.2)
print('Config Complete')

# select project dataset table and payload
table_id = dest
file_path = bqpath+bqFileName

# job configuration
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema=[
        bigquery.SchemaField("id", "INT64"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("handle", "STRING"),
        bigquery.SchemaField("body", "STRING"),
        bigquery.SchemaField("created", "TIMESTAMP"),
        bigquery.SchemaField("vendor", "STRING"),
        bigquery.SchemaField("product_type", "STRING"),
        bigquery.SchemaField("vid", "INT64"),
        bigquery.SchemaField("price", "FLOAT64"),
        bigquery.SchemaField("compare_at_price", "FLOAT64"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("vtitle", "STRING"),
        bigquery.SchemaField("position", "INT64"),
        bigquery.SchemaField("product_id", "INT64"),
        bigquery.SchemaField("available", "BOOL"),
        bigquery.SchemaField("requires_shipping", "BOOL"),
        bigquery.SchemaField("weight", "FLOAT64"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("tags", "STRING"),
        bigquery.SchemaField("site", "STRING"),
        bigquery.SchemaField("runDate", "DATE"),
        bigquery.SchemaField("link", "STRING"),
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)
with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file,table_id,job_config=job_config)
job.result()
print('Transfer Started')
time.sleep(1)
print(job.result())
print('script complete')
