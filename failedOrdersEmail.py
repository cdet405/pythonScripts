# current working version 20230517CD
# to be executed every morning via cron
# emails support team when orders are in a failed state
 
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

client_secret_file = '/Users/robot/key.json'
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
    
"""
# If Running Headless use this for oauth
        
def authenticate_user_account(client_secret_file, scopes):
    credentials = None

    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            credentials = pickle.load(token)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(google.auth.transport.requests.Request())
        else:
            flow = Flow.from_client_secrets_file(client_secret_file, scopes)
            auth_url, _ = flow.authorization_url(prompt='consent')
            print('Please go to this URL: {}'.format(auth_url))
            code = input('Enter the authorization code: ')
            flow.fetch_token(code=code)
            credentials = flow.credentials
            with open(TOKEN_FILE, 'wb') as token:
                pickle.dump(credentials, token)
    return credentials
"""


def send_email(credentials, to, subject, body=None, html_body=None):
    try:
        gmail_service = build('gmail', 'v1', credentials=credentials)
        if html_body:
            message = MIMEText(html_body, 'html')
        else:
            message = MIMEText(body)
        message['to'] = to
        message['subject'] = subject
        create_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        send_message = (gmail_service.users().messages().send(userId="me", body=create_message).execute())
        print(f'sent message to {to} Message Id: {send_message["id"]}')
    except HttpError as error:
        print(f'An error occurred: {error}')
        send_message = None
    return send_message


def main():
    credentials = authenticate_user_account(client_secret_file, scopes)
    project_id = 'myProjectID'
    # noinspection PyTypeChecker
    client = bigquery.Client(project=project_id, credentials=credentials)
    query = """
    SELECT 
      CASE WHEN company_id = 2 THEN 'V'
           WHEN company_id = 3 THEN 'C'
           WHEN company_id = 4 THEN 'N'
           WHEN company_id = 5 THEN 'H'
           WHEN company_id = 6 THEN 'A'
           ELSE CONCAT(
            'ERR:[action=break][type=OOB][note:company_id{',
            company_id,
            '}undefined]'
          )
      END AS company_name,
      channel_name,
      order_id,
      order_reference,
      order_date,
      DATE_DIFF(CURRENT_DATE(), order_date, DAY) days_past,
      state,
      CONCAT(
        "https://subdomain.domain.tld/client/sale/",
        order_id,
        "?view/page=1"
      ) order_url
    FROM `project.dataset.salesOrders` 
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND state='failed'
    ORDER BY 1,2,5;
    """
    query_job = client.query(query)
    result = query_job.result()
    df = result.to_dataframe()
    linecount = result.total_rows

    # Send an email using the authenticated Gmail account
    now = datetime.today().strftime('%Y-%m-%d')
    to = "cs@example.com"
    subject = f"{linecount} Customer Orders In Failed Status. {now}"
    msg = 'The Following Orders Require Attention:'
    html_table = df.to_html()
    html_body = f"""\
    <html>
        <body>
            <p>{msg}</p>
            {html_table}
        </body>
    </html>
    """
    if not linecount:
        print("No Failed Orders, Email Aborted")
    else:
        send_email(credentials, to, subject, html_body=html_body)


if __name__ == '__main__':
    main()
    
