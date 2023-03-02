import requests
import pandas as pd
import datetime
import os

# set file path
path = '~/UROresults/'

# Grab Current DateTime & convert to EST
d = datetime.datetime.utcnow()-datetime.timedelta(hours=4)
cd = d.strftime("%Y_%m_%d-%I_%M_%S_%p")

# Target URL
url_template = 'https://uro.com/products.json?&limit=250&page={}'

#create empty list
product_list = []

# loop through pages
for page in range(1, 440):  # assuming you want to scrape 440 pages
    url = url_template.format(page)
    r = requests.get(url)
    try:
        r = requests.get(url)
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
        created = item['created_at']
        product_type = item['product_type']
        for variant in item['variants']:
            vid = variant['id']
            price = variant['price']
            sku = variant['sku']
            compare_at_price = variant['compare_at_price']
            position = variant['position']
            available = variant['available']
            updated_at = variant['updated_at']
            product_id = variant['product_id']
            #create dictionary
            products = {
                'id': id,
                'title': title,
                'handle': handle,
                'created': created,
                'product_type': product_type,
                'vid': vid,
                'price': price,
                'compare_at-price': compare_at_price,
                'sku': sku,
                'position': position,
                'product_id': product_id,
                'available': available,
                'updated_at': updated_at
            }
            # add dictionary to list
            product_list.append(products)

# Put list in dataframe
df = pd.DataFrame(product_list)

# Convert dataframe to CSV file 
fileName = f'{page}uro{cd}.csv'
output_file = os.path.join(path, fileName)
df.to_csv(output_file, index=False)

print('Scrape Completed')
