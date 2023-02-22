# TODO INSERT SHOPIFY DOMAIN LINE 12
import requests
import pandas as pd
import datetime
import os
# set file path
path = '~/results/'
# Grab Current DateTime & convert to EST
d = datetime.datetime.utcnow()-datetime.timedelta(hours=4)
cd = d.strftime("%Y_%m_%d-%I_%M_%S_%p")
# Target URL
url = 'https://<<SHOPIFY_DOMAIN_HERE>>.com/products.json?&limit=250&page=1'
r = requests.get(url)
data = r.json()
fileName = f'culturesforhealth{cd}.csv'
output_file = os.path.join(path, fileName)

#create empty list
product_list = []
#loop through json
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
#Put list in dataframe
df = pd.DataFrame(product_list)
#convert dataframe to CSV file 
df.to_csv(output_file, index=False)
print('Scrape Completed')





