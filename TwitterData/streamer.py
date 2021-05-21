### dependencies
import tweepy
import configparser as CFG
from os import path
import sys
import time
import urllib3
from datetime import date
import json
from queue import Queue
from threading import Thread 

### This scripts filter and downloads tweets in real-time based on the keywords inserted in the settings.cfg file in the same folder.
#   One file per day is created, containing a list of json documents, one for each tweet.


### StreamListener 
#   Remove quotes and does very fast preprocessing
#   Parallelization is used to handle multiple simultaneous tweets incoming
class StreamListener(tweepy.StreamListener):
    def __init__(self, q = Queue()):
        super().__init__()
        self.q = q
        num_core=4
        self.rate_error_count = 0
        self.status_downloaded=0
        for i in range(num_core):
            t = Thread(target=self.do_stuff)
            t.daemon = True
            t.start()
            
    def on_status(self, status):
        ### This function is called on each tweet. It simply moves it to the multiprocessing queue
        try:
            self.q.put(status)
        except:
            pass
        return True
    
    def do_stuff(self):
        ### This function is called on each tweet put in the multiprocessing queue.
        #    We have to extract the full text of each tweet from the extended_tweet object.
        #    We store only original tweets and retweets (no quotes)
        while True:
            today=date.today()
            filen=today.strftime("%Y_%m_%d")+'.jsonl'
              
            try:
                status=self.q.get()
                ret=[]
                self.status_downloaded+=1
                if hasattr(status, 'retweeted_status'):
                    tweet = status._json
                    if 'extended_tweet' in tweet['retweeted_status']:
                        ret=tweet['retweeted_status']['extended_tweet']

        # if "retweeted_status" attribute exists, flag this tweet as a retweet.
                is_retweet = hasattr(status, "retweeted_status")
        # check if this is a quote tweet.
                is_quote = hasattr(status, "quoted_status")
        
                if (not is_quote):
                                
            # check if text has been truncated
                    if hasattr(status, 'extended_tweet'):
                        js=status._json
                        js['text']=status.extended_tweet['full_text']
                    else:
                        js=status._json
                    
                    if ret:
                        js['extended_retweet']=ret
                    
                    # Make sure you created a "data/tweets/" folder in the directory before launching the script
                    with open('data/tweets/'+filen, 'a+') as f:
                        f.write(json.dumps(js) + '\n')
                        
                    # We print on the standard output each original tweet: comment this if the collection rate is high    
                    if not is_retweet:
                        print('VACC: ',status.id_str,status.created_at,js['text'])
                    if not self.status_downloaded%50:
                        print('{} tweets downloaded this session'.format(self.status_downloaded))
                    self.q.task_done()

            except:
                print('!!!EXCEPTION!!!')
                pass
                    
  
    def on_error(self, status_code):
        if status_code == 420:
            print('Waiting for a bit...')
            self.rate_error_count += 1
            # wait at least 15min
            time.sleep(self.rate_error_count*15*60)
        else:
            print("Encountered streaming error (", status_code, ")")
        return True

### read configuration
cfg = CFG.ConfigParser()
cfg.read("settings.cfg")

# Tags are written in multiline. Use indents for that
tags = [c for c in cfg['track']['tags'].split("\n") if c != ""]
# these are the credentials for the twitter api.
consumer_key = cfg["credentials"]["consumer_key"]
consumer_secret = cfg["credentials"]["consumer_secret"]
access_key = cfg["credentials"]["access_key"]
access_secret = cfg["credentials"]["access_secret"]

### Connect to twitter.
# set credentials
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
# initialize api
api = tweepy.API(auth,wait_on_rate_limit=True)


### initialize stream
streamListener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=streamListener,tweet_mode='extended')

### listen
while True:
    try:
        stream.filter(track=tags)
    except urllib3.exceptions.ProtocolError:
        print('ERROR: Incomplete read.\n')
        time.sleep(3)
        continue
    except KeyboardInterrupt:
        # hittting ctrl-c
        sys.exit()
    except:
        print('UNKNOWN EXCEPTION\n')

        # catch all
        # either sleep or exit
        time.sleep(10)
        


