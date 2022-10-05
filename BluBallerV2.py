import re
import time
from datetime import datetime
import matplotlib.pyplot as plt
import os
import tweepy as tw
import sqlite3

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from base64 import urlsafe_b64decode, urlsafe_b64encode
from email.mime.text import MIMEText

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://mail.google.com/']

'''
Formats a given message correctly
'''

def build_message(dest, obj, body):
    message = MIMEText(body)
    message['to'] = dest
    message['from'] = 'william.kressner@gmail.com'
    message['subject'] = obj

    return {'raw': urlsafe_b64encode(message.as_bytes()).decode() }

'''
Uses the provided google service to send a message
'''
def send_message(service, dest, obj, body):
    return service.users().messages().send(userId="me", body=build_message(dest, obj, body)).execute()

'''
Authorize a user using the google oauth2 flow
'''
def authorize_google_services():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    service = build('gmail', 'v1', credentials=creds)
    return service

'''
Given a tweet from the account (format more or less guaranteed), split it on the newline and pull out the third price 
in the tweet (that's the avg). Return that price.
'''

def extract_avg_price(tweet):
    #ex_tweet = "#DolarBlue: 282.0 / 286.0 / 284.0\n#DolarOficial: 44.75 / 52.75 / 48.75\nhttps://t.co/06DoeIZ0eT"
    lines = re.split('\n', tweet)
    blu_line = ''
    for line in lines:
        if '#DolarBlue' in line:
            blu_line = line
    #print("Extracted out the blue line: ", blu_line)
    matches = re.findall("\d+\.\d+", blu_line)
    #for match in matches:
    #    print(match)
    price_avg = matches[-1]
    return price_avg

'''
Orders the tweets by descending date and returns the first one
'''

def get_latest_tweet():
    query = 'select * from blu order by date desc limit 1'
    cursor.execute(query)
    res = cursor.fetchall()
    return res

'''
Builds a query to bulk insert all the (price, date) data into the database.
'''

def write_new_tweets(prices, dates):
    query = 'insert into blu (price, date) values '
    data = []
    for i in range(len(prices)):
        data.append(f'({prices[i]}, \'{dates[i]}\')')
    query = query + ', '.join(data)
    cursor.execute(query)
    conn.commit()
    print(f'Added {len(prices)} row(s)')

'''
One-time set up stuff
Pull creds from environment
set up sql connection
'''

API_KEY = os.environ['API_KEY']
API_KEY_SECRET = os.environ['API_KEY_SECRET']
BEARER_TOKEN = os.environ['BEARER_TOKEN']

client = tw.Client(bearer_token=BEARER_TOKEN, consumer_key=API_KEY, consumer_secret=API_KEY_SECRET)

user = client.get_user(username='Dolar__Blue')

google_service = authorize_google_services()


'''
Main loop. Every 2 hours, will fetch tweets later than the most recent one found in the database, do some analyzing, 
and write the new tweets it found to the database.
'''


while(True):
    try:
        conn = sqlite3.connect('Blu.db')
        cursor = conn.cursor()
        prices = []
        dates = []
        latest_tweet = get_latest_tweet()[0]
        latest_date = latest_tweet[2]
    
        tweets = client.get_users_tweets(user.data['id'], tweet_fields=["created_at"], max_results=15, start_time=latest_date)[0]
        if len(tweets) > 1:
            for tweet in tweets[:-1]:
                avg_price = extract_avg_price(tweet["data"]["text"])
                creation_date = tweet.data["created_at"][:-5] + 'Z'
                prices.append(avg_price)
                dates.append(creation_date) 
            write_new_tweets(prices, dates)
            send_message(google_service, "william.kressner@gmail.com", "New Blu Dollar Price!", f'At {dates[0]}, the Blu Dollar was reported to be {prices[0]}')
        cursor.close()
    except sqlite3.Error as error:
        print("failed to connect to the database: ", error)
    finally:
        conn.close()
        print(f'Closed connection at {time.ctime()}. Will check again later.')
        time.sleep(600)


'''
#TODO: add the data analytics stuff
alert if the price is the best one found in the past 5, 7, x days
this command may help: 
    select * from blu where date > (get_latest_date.decode() - 7).encode()
idk something like that
'''
