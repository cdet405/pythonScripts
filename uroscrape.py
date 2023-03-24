import requests
import pandas as pd
import datetime
import os
import time

# set file path
path = '~/UROresults/'

# Grab Current DateTime & convert to EST
d = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
cd = d.strftime("%Y_%m_%d-%I_%M_%S_%p")

# Target URL
url_template = 'https://uro.com/products.json?&limit=250&page={}'

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

            # print(len.data['products'])
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
                'compare_at-price': compare_at_price,
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
# Remove Most HTML
df['body'] = df['body'].str.replace(r'<[^<>]*>', '', regex=True)
df['weight'] = df['weight']*0.00220462
# Build Product URL
df['link'] = 'https://uro.com/products/'+df['handle'].astype(str)+'?variant='+df['vid'].astype(str)
# Convert dataframe to CSV file
fileName = f'Pages1-{page + 1}_uro{cd}.csv'
output_file = os.path.join(path, fileName)
# save to csv
df.to_csv(output_file, index=False)
product_list.clear()
print(f'Scrape Completed - file: {fileName}')
