import os
import pickle
import google.auth
from google.auth.transport import requests
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.text import MIMEText
import base64
from google.cloud import bigquery
from datetime import datetime
import pandas as pd

client_secret_file = '/Users/me/supersecretfoldername/notakey.json'
scopes = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/gmail.compose',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.modify'
]

TOKEN_FILE = 'token.pickle'


def authenticate_user_account(client_secret_file, scopes):
    credentials = None

    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            credentials = pickle.load(token)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(google.auth.transport.requests.Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, scopes)
            credentials = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(credentials, token)
    return credentials


def send_email(credentials, to, subject, body=None, html_body=None, cc=None):
    try:
        gmail_service = build('gmail', 'v1', credentials=credentials)
        if html_body:
            message = MIMEText(html_body, 'html')
        else:
            message = MIMEText(body)
        message['to'] = to
        message['subject'] = subject
        message['cc'] = cc
        message['bcc'] = 'name@sample.com'
        create_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        send_message = (gmail_service.users().messages().send(userId="me", body=create_message).execute())
        print(f'sent message to {to} Message Id: {send_message["id"]}')
    except HttpError as error:
        print(f'An error occurred: {error}')
        send_message = None
    return send_message


def main():
    credentials = authenticate_user_account(client_secret_file, scopes)

    # Run a BigQuery query
    project_id = PROJECT
    # noinspection PyTypeChecker
    client = bigquery.Client(project=project_id, credentials=credentials)
    query = """
SELECT DISTINCT 
  CASE WHEN company_id = 2 THEN 'a'
       WHEN company_id = 3 THEN 'b'
       WHEN company_id = 4 THEN 'c'
       WHEN company_id = 5 THEN 'd'
       WHEN company_id = 6 THEN 'e'
       ELSE 
         CONCAT(
           'ERR:[action=break][type=OOB][note:company_id{',
           company_id,
           '}undefined]'
     )
  END AS company_name, 
  CASE WHEN (
    company_id = 3 
    AND channel_name LIKE '%Shopify%'
  ) THEN CONCAT(
    '<a href="', 'https://admin.shopify.com/store/x/orders/', 
    channel_identifier, '">Shopify URL</a>'
  ) WHEN (
    company_id = 3 
    AND channel_name LIKE '%Amazon%'
  ) THEN CONCAT(
    '<a href="', 'https://sellercentral.amazon.com/orders-v3/order/', 
    channel_identifier, '?ref=orddet', 
    '">Amazon URL</a>'
  ) WHEN (
    company_id = 5 
    AND channel_name LIKE '%Shopify%'
  ) THEN CONCAT(
    '<a href="', 'https://admin.shopify.com/store/x/orders/', 
    channel_identifier, '">Shopify URL</a>'
  ) WHEN (
    company_id = 5 
    AND channel_name LIKE '%Amazon%'
  ) THEN CONCAT(
    '<a href="', 'https://sellercentral.amazon.com/orders-v3/order/', 
    channel_identifier, '?ref=orddet', 
    '">Amazon URL</a>'
  ) WHEN (
    company_id = 2 
    AND channel_name LIKE '%Shopify%'
  ) THEN CONCAT(
    '<a href="', 'https://admin.shopify.com/store/x/orders/', 
    channel_identifier, '">Shopify URL</a>'
  ) WHEN (
    company_id = 2 
    AND channel_name LIKE '%Amazon%'
  ) THEN CONCAT(
    '<a href="', 'https://sellercentral.amazon.com/orders-v3/order/', 
    channel_identifier, '?ref=orddet', 
    '">Amazon URL</a>'
  ) WHEN (
    company_id = 4 
    AND channel_name LIKE '%Shopify%'
  ) THEN CONCAT(
    '<a href="', 'https://admin.shopify.com/store/x/orders/', 
    channel_identifier, '">Shopify URL</a>'
  ) WHEN (
    company_id = 4 
    AND channel_name LIKE '%Amazon%'
  ) THEN CONCAT(
    '<a href="', 'https://sellercentral.amazon.com/orders-v3/order/', 
    channel_identifier, '?ref=orddet', 
    '">Amazon URL</a>'
  ) WHEN (
    company_id = 6 
    AND channel_name LIKE '%Shopify%'
  ) THEN CONCAT(
    '<a href="', 'https://admin.shopify.com/store/x/orders/', 
    channel_identifier, '">Shopify URL</a>'
  ) WHEN (
    company_id = 6 
    AND channel_name LIKE '%Amazon%'
  ) THEN CONCAT(
    '<a href="', 'https://sellercentral.amazon.com/orders-v3/order/', 
    channel_identifier, '?ref=orddet', 
    '">Amazon URL</a>'
  ) ELSE NULL END AS marketplace_order_link, 
  order_id, 
  order_reference, 
  SUM(amount) OVER (
    PARTITION BY 
      order_id
    ) order_value, 
  order_date, 
  'Exception' as state, 
  DATE_DIFF(
    CURRENT_DATE(), 
    order_date, 
    DAY
  ) days_past, 
  CONCAT(
    '<a href="',
    'https://sub.domain.tld/client/#/model/sale.order/',
     order_id,
     '?views=%5B%5B1105,%22tree%22%5D,%5B1104,%22form%22%5D%5D',
     '">FulFil URL</a>'
   ) fulfil_order_link
FROM 
  `project.dataset.sales_orders`, 
  unnest(lines) l 
WHERE 
  (
    state = 'draft' 
    AND order_date IS NOT NULL 
    AND order_reference IS NOT NULL 
    AND order_number IS NULL
  ) 
ORDER BY 
  6 desc, 
  1 asc
;
"""
    query_job = client.query(query)
    result = query_job.result()
    df = result.to_dataframe()
    df['order_value'] = pd.to_numeric(df['order_value'], errors='coerce')
    df['order_value'] = df['order_value'].round(2)
    linecount = result.total_rows
    # Send an email using the authenticated Gmail account
    now = datetime.today().strftime('%Y-%m-%d')
    to = "name+exceptions@sample.com"
    #cc = "name@sample.com"
    subject = f"{linecount} Customer Orders Have Exceptions."
    msg = 'The following customer sales orders have exceptions preventing them from being fulfilled.'
    html_table = df.to_html(escape=False, index=False)\
        .replace('<td>', '<td align="center">')\
        .replace('<th>', '<th align="center">')\
        .replace('border="1"', 'border="4"')
    banner = """
    <img src=\
    "https://theimangesourcedomain.tld/theimage.png"\
     alt="banner" width="325" height=auto>
    """
    altmsg = '<html> <body> <small style="color:grey;">this email was automatically generated</small> </body> </html>'
    altsub = "LOG_INFO: " + subject
    html_body = f"""
    <html>
        <body>
            <p style="font-family: 'Trebuchet MS', sans-serif;">{msg}<br>Please Review in ERP:</p>
            {html_table}
            <br><br><br>
            {banner}
            <br><br>
            <small style="font-family: 'Trebuchet MS', sans-serif; color: lightgrey;">
                This Email was Automatically Generated on {now}.
            </small>
        </body>
    </html>
    """
    if not linecount:
        print(f"{now}: No Order Exceptions, Primary Email Aborted. Sent log notification to chad")
        send_email(credentials, to='name+exceptions@sample.com', subject=altsub, html_body=altmsg)
    else:
        send_email(credentials, to, subject, html_body=html_body)


if __name__ == '__main__':
    main()
