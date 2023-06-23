# survey pipe to bigquery
from google.cloud import bigquery
import os
from x import h, z
import requests
import json
import time


ts = int(time.time())
prename = 'survey'

base_url = "https://nickisdiapers.gorgias.com/api/satisfaction-surveys"
headers = h

all_survey = []
print(f'** Starting {prename} script **')
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
        all_survey.extend(data["data"])

        # Update the url with the next_cursor value
        next_cursor = data["meta"]["next_cursor"]
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
        # sleep for .5 seconds to avoid rate limit
        time.sleep(3.5)

        # used during testing or updating only fetches first ~n*100 records
        if cycle >= 10:
            print('Cycle Limit Reached.')
            break

        if next_cursor is not None:
            continue
        else:
            print('**No New Cursor Found, Moving to Next Step**')
            break

    except requests.exceptions.HTTPError as error:
        print(f"An HTTP error occurred: {error}")
        break

# Convert the JSON list to ndjson format
ndjson_data = json.dumps(all_survey)
mod_data = json.loads(ndjson_data)
print('Writing Data')

# used for readable version of the file/log
with open(f'{prename}{ts}.json', 'w', encoding='utf-8') as f:
    json.dump(mod_data, f, ensure_ascii=False, indent=4)

# what actually gets loaded to bq    
with open(f'surveyUpdate.ndjson', 'w', encoding='utf-8') as ndjson_file:
    for survey in all_survey:
        ndjson_file.write(json.dumps(survey, ensure_ascii=False) + '\n')
print('File(s) Saved')
time.sleep(1)
print('###################################################################')
print('Starting BigQuery Transfer')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = z
client = bigquery.Client()

table_id = 'manifest.gorgSurveyStg'
file_path = 'surveyUpdate.ndjson'

# update to call out schema
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=[
        bigquery.SchemaField("ticket_id", "INT64"),
        bigquery.SchemaField("sent_datetime", "TIMESTAMP"),
        bigquery.SchemaField("scored_datetime", "TIMESTAMP"),
        bigquery.SchemaField("meta", "STRING"),
        bigquery.SchemaField("score", "INT64"),
        bigquery.SchemaField("customer_id", "INT64"),
        bigquery.SchemaField("created_datetime", "TIMESTAMP"),
        bigquery.SchemaField("uri", "STRING"),
        bigquery.SchemaField("body_text", "STRING"),
        bigquery.SchemaField("should_send_datetime", "TIMESTAMP"),
        bigquery.SchemaField("id", "INT64")
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)
with open(file_path, 'rb') as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)
print('Executing Insert...')
time.sleep(5)
print(f'Insert: {job.result()}')

merge_sql = """
merge manifest.gorgSurvey p
using (
	select 
	  * 
	from manifest.gorgSurveyStg 
	where load_date = current_date()
) s
on (
	p.id=s.id and 
	p.customer_id = s.customer_id
)
when matched and(
	(
		p.sent_datetime != s.sent_datetime
	) or(
		p.scored_datetime != s.scored_datetime
	) or(
		p.should_send_datetime != s.should_send_datetime
	)
) 
then
  update 
	  set
	    p.ticket_id = s.ticket_id,
	    p.sent_datetime = s.sent_datetime,
	    p.scored_datetime = s.scored_datetime,
	    p.meta = s.meta,
	    p.score = s.score,
	    p.customer_id = s.customer_id,
	    p.created_datetime = s.created_datetime,
	    p.uri = s.uri,
	    p.body_text = s.body_text,
	    p.should_send_datetime = s.should_send_datetime,
	    p.id = s.id,
	    p.mod_date = current_date()
when not matched then
  insert(
	  ticket_id,
	  sent_datetime,
	  scored_datetime,
	  meta,
	  score,
	  customer_id,
	  created_datetime,
	  uri,
	  body_text,
	  should_send_datetime,
	  id,
	  load_date
)
values(
  s.ticket_id,
  s.sent_datetime,
  s.scored_datetime,
  s.meta,
  s.score,
  s.customer_id,
  s.created_datetime,
  s.uri,
  s.body_text,
  s.should_send_datetime,
  s.id,
  s.load_date
);
"""

query_job = client.query(merge_sql)
print('Executing Merge...')
time.sleep(5)
print(f'Merge: {query_job.result()}')
time.sleep(1)
print('Executing Update: Updating body_text to NULL where Blank.')

update_sql = """
update manifest.gorgSurvey
set body_text = null,
mod_date = current_date()
where length(body_text)=0;
"""

update_job = client.query(update_sql)
print('Updating...')
time.sleep(3)
print(f'Update: {update_job.result()}')
time.sleep(.5)
print('Script Completed.')
