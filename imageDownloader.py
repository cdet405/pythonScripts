# 2024-01-25 CD
# imageLoader -- Manual Version: have to feed it a csv
# TODO: Add Error Handling && Logging && Success/Fail Email?
import requests
import pandas as pd
import time

print('starting..')
path_temp = 'C:\\Users\\me\\project\\imageLoader\\{}\\{}.jpg'
file = 'C:\\Users\\me\\project\\imageLoader\\table.csv'

df = pd.read_csv(file, encoding='utf-8')

for _, row in df.iterrows():
    folder = row['vendor']
    image_name = row['productID']
    url = row['url']
    file_path = path_temp.format(folder, image_name)
    image = requests.get(url).content
    with open(file_path, 'wb') as f:
        f.write(image)
        print(f'Saved Primary Image For: {folder}>{image_name}')
        f.close()
    time.sleep(1)

print('done')
