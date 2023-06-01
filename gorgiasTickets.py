"""
############################################################
# Version 1.03 - *TEST* - 2023-06-01                       # 
# Pull all Ticket Data in Gorgias                          #
# https://developers.gorgias.com/reference/get_api-tickets #
# Retrieve All Tickets                                     #
############################################################
"""

import requests
import json
import time
from gorg import head, subdomain

ts = int(time.time())
end_point = 'tickets' # 'satisfaction-surveys', 'customers'
base_url = f"https://{subdomain}.gorgias.com/api/{end_point}"
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

        print(f"cycle: {cycle} | est rows: {cycle * 100}")
        time.sleep(.05)
        print(next_cursor)
        time.sleep(.05)
        print(url)
        time.sleep(.05)
        print("-------------------------------------------------------------------")
        # sleep for .65 seconds to avoid rate limit
        time.sleep(.5)
        
        # used during testing or updating only fetches first 500 records 
        # comment out for initial load
        if cycle >= 5:
            print('Cycle Limit Reached.')
            break
        
        # break when the end of the dataset is reached
        if next_cursor is not None:
            continue
        else:
            print("No New Cursor Found, Moving to Next Step")
            break

    except requests.exceptions.HTTPError as error:
        print(f"** HTTP ERROR OCCURRED ** : {error}")
        break


def remove_meta_from_dict(dct):
    if isinstance(dct, dict) and "meta" in dct:
        del dct["meta"]
    return dct

# Convert the JSON list to ndjson format
ndjson_data = json.dumps(all_tickets)
mod_data = json.loads(ndjson_data)

# Extract only desired columns
extracted_data = [
    {
        key: remove_meta_from_dict(value) if key in {"customer", "assignee_user"} else value
        for key, value in records.items()
        if key in {"id", "uri", "external_id", "language", "status", "priority", "channel", "via",
                   "from_agent", "to_agent", "customer", "assignee_user", "assignee_team", "subject",
                   "tags", "is_unread", "spam", "created_datetime", "opened_datetime",
                   "last_received_message_datetime", "last_message_datetime", "updated_datetime",
                   "closed_datetime", "snooze_datetime", "trash_datetime", "integrations",
                   "messages_count", "excerpt", "trashed_datetime"}
    }
    for record in mod_data
]

print('** Writing Data **')

# write data to an ez to read json file 
with open(f'formatted-{end_point}_{ts}.json', 'w', encoding='utf-8') as f:
    json.dump(extracted_data, f, ensure_ascii=False, indent=4)

# Write the ndjson data to a file
with open(f'output-{end_point}_{ts}.ndjson', 'w', encoding='utf-8') as ndjson_file:
    for ticket in extracted_data:
        ndjson_file.write(json.dumps(ticket, ensure_ascii=False) + '\n')
        
# maybe forget the ndjson meme and go from json to parquet? 
        
print(f'Script Complete {end_point} File(s) Saved.')
