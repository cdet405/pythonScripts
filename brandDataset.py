import requests
import pandas as pd
import datetime as dt

# Get API Token
def get_token(user,password):
    url = f'https://apps.semadata.org/sdapi/v2/token/get?userName={user}&password={password}'
    r = requests.get(url)
    try:
        if r.status_code == 200:
            data = r.json()
        else:
            print('Request for token failed with status code', r.status_code)
            data = None
    except Exception as e:
        print('Error:', e)
        data = None
    token = data['Token']
    return token

# Fetch Data
def sema_brand_datasets(token):
    url = f'https://apps.semadata.org/sdapi/v2/export/branddatasets?token={token}'
    r = requests.get(url)
    brand_list = []
    try:
        if r.status_code == 200:
            data = r.json()
        else:
            print('Request for Sema Brand Datasets failed with status code', r.status_code)
            data = None
    except Exception as e:
        print('Error:', e)
        data = None
    for i in data['BrandDatasets']:
        AAIABrandId = i['AAIABrandId']
        BrandName = i['BrandName']
        DatasetId = i['DatasetId']
        DatasetName = i['DatasetName']
        LastUpdated = i['LastUpdated']
        brands = {
            'aaiaBrandID': AAIABrandId,
            'aaiaBrandName': BrandName,
            'datasetID': DatasetId,
            'datasetName': DatasetName,
            'lastUpdated': LastUpdated
        }
        brand_list.append(brands)
    df = pd.DataFrame(brand_list)
    df['lastUpdated'] = pd.to_datetime(df['lastUpdated']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df.to_csv('semaBrandDatasets.csv', index=False)
    brand_list.clear()
    print('File Saved: semaBrandDatasets.csv')
    return df

def main():
    token = get_token(user='xxxx', password='xxxx')
    df = sema_brand_datasets(token=token)
    print(df.head)


if __name__ == '__main__':
    main()
    print('done')
