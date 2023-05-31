"""
############################################################
# Version 1 - *TEST* - 2023-05-30                          # 
# Pull all Ticket Data in Gorgias                          #
# https://developers.gorgias.com/reference/get_api-tickets #
# Retrieve All Tickets                                     #
############################################################
"""

import requests
import json
import time
from gorg import head, subdomain

base_url = f"https://{subdomain}.gorgias.com/api/tickets"
headers = head

all_tickets = []
print('starting script')
url = base_url
cycle = 0
while url:
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Check the rate-limiting headers
        retry_after = response.headers.get('Retry-after')
        api_call_limit = response.headers.get('X-Gorgias-Account-Api-Call-Limit')
        remaining_calls = response.headers.get('X-Gorgias-Account-Api-Call-Limit', '?')

        if retry_after:
            print(f"Waiting {retry_after} seconds before retrying the request")
            time.sleep(float(retry_after))

        if remaining_calls == '?':
            print("Rate limit exceeded")
        else:
            print(f"Remaining calls: {remaining_calls}/{api_call_limit}")

        data = response.json()

        # Append the data to the all_tickets list
        all_tickets.extend(data["data"])

        # Update the url with the next_cursor value
        next_cursor = data["meta"]["next_cursor"]
        print(next_cursor)
        query_params = f"?cursor={next_cursor}&limit=100&order_by=created_datetime%3Adesc"
        url = f"{base_url}{query_params}"
        cycle += 1

        print(url)
        print(f"cycle: {cycle} | Est Rows: {cycle * 100}")
        time.sleep(.575)
        print("-------------------------------------------------")
# //TODO: Last Clycle 2368 - Needs to break when next_cursor = None 
# How ever None creates an invalid HTTP error that breaks loop. 
    except requests.exceptions.HTTPError as error:
        print(f"An HTTP error occurred: {error}")
        break

# Convert the JSON list to ndjson format
ndjson_data = json.dumps(all_tickets)
print('** writing data **')

# Write the ndjson data to a file
with open('output.ndjson', 'w') as ndjson_file:
    for ticket in all_tickets:
        ndjson_file.write(json.dumps(ticket) + '\n')
        
# maybe forget the ndjson meme and go from json to parquet? 
        
print('done')
