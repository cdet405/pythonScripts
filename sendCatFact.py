from config2 import myemail
from config2 import apppw
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
​
headers = {
    'accept': 'application/json',
    'X-CSRF-TOKEN': '6HkASJ9wlSju2kVhVSvudOLOYlKURMLR3gKgMaC6',
}
# endpoint url
url = 'https://catfact.ninja/fact'
# request random cat fact
r = requests.get(url, headers=headers)
d = r.json()
# parse response for fact
kittyFact = d["fact"]
​
# Set message to cat fact
mail_content =  kittyFact
​
#The mail addresses and password
sender_address = myemail
sender_pass = apppw
receiver_address = 'example@example.com'
#cc_address = ''
#bcc_address = ''
​
#Setup the MIME
message = MIMEMultipart()
message['From'] = sender_address
message['To'] = receiver_address
#message['Cc'] = cc_address
#message['Bcc'] = bcc_address
message['Subject'] = 'a little morning treat'   #The subject line
#The body and the attachments for the mail
message.attach(MIMEText(mail_content, 'plain'))
#Create SMTP session for sending the mail
session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
session.starttls() #enable security
session.login(sender_address, sender_pass) #login with mail_id and password
text = message.as_string()
session.sendmail(sender_address, receiver_address, text)
session.quit()
print('Mail Sent')  
