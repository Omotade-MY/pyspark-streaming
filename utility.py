import logging
import socket
import tweepy
import ast
import time 
from datetime import datetime as dt
import json
import dotenv
dotenv.load_dotenv()

rule_1 = "COVID19 lang:en"
rule_2 = "#covid19 lang:en"

global start_time
start_time = time.time()

def create_socket(hostname='localhost'):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    host = hostname
    port = 5555

    s.bind((host, port))
    s.listen(10)
    csocket, address = s.accept()
    print("Connection established with client at {}".format(address))

    return csocket


def its20secs():
    global start_time
    c_time = time.time()
    e_time = start_time + 20
    if c_time >= e_time:
        start_time = e_time
        print(dt.now())
        return True  
    return False


def send_message(msg, csocket):
    print(msg.strip('EOL'))
    csocket.send(str(msg).encode('utf-8'))
    
    

def package_message(raw):
    
    tweet_data = ast.literal_eval(raw.decode('UTF-8'))
    tweet = tweet_data['data']['text']

    if "extended_tweet" in tweet_data:
        return "This wis an extended tweet>>>" + tweet_data['extended_tweet']['full_text'] 


    return tweet
    #json.dumps(tweet_dict, indent=2).encode('utf-8')

import cleantext
def clean_text(text):
    text = cleantext.remove_emoji(text)
    text = cleantext.replace_urls(text, '')
    text = text.replace('#', '').replace('RT', '').replace('\n', '').strip()
    text = text + 'EOL'

    if its20secs():
        text = text + '\n'

    return text

    


class MyStream(tweepy.StreamingClient):
    
    def on_connect(self):

        print("Connected to Stream!!!\n ===================")

        

    def on_data(self, raw_data):
        
        message = package_message(raw_data)

        message = clean_text(message)

        send_message(message, self.csocket)
       
        time.sleep(2)


    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False
    def on_errors(self, errors):
        
        error_msg = super().on_errors(errors)
        #notify(msg="ACTION REQUIRED: Error has occured while streaming from twitter.")

    def start_stream(self, csocket):
        self.csocket = csocket
        self.filter()

    def on_connection_error(self):
        print("Could not connect to the Stream")
        return super().on_connection_error()
    
    def on_disconnect(self):
        raise Exception

def reset_rules(stream):
    rules = stream.get_rules().data
    rules_ids =[]
    for rule in rules:
        rules_ids.append(rule.id)

    stream.delete_rules(rules_ids)
    stream.add_rules(tweepy.StreamRule(rule_1, tag="covid"))
    stream.add_rules(tweepy.StreamRule(rule_2, tag="covid"))
