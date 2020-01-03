from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import logging
from confluent_kafka import Producer
import sys

import os,datetime,exceptions,time

import tweetParser as t


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(lineno)d:%(message)s')
file_handler = logging.FileHandler('Producer.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)




access_token = "39104436-GrMK5WCq181eG7h1r6on633HgRrX0cTDAhgKB1oAY"
access_token_secret = "CIFLPb7rteCzAyRHIvrXOmJ3lpPp6dwy25qKIncLyvrmo"

consumer_key = "FNBJxfgjVS89Q4MTFU6rdCrWl"
consumer_secret = "NiZRm5Ah7nJTUxCVkXHl3P2NzGyAmnz4bQsNVnRfr3SLvWusLs"


conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'new'}

# Create Producer instance
p = Producer(**conf)



def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        print ('%% Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))


def producer_sync_send (key,message):

    print "Sending message value to kafka"
    logger.info("Sending message value to kafka")
    try:
        """created_at, tweet_id, tweet_text, source, user_name, user_screen_name, user_lang = t.tweetParse(message)
        message={}
        message['created_at']=created_at
        message['tweet_id']=tweet_id
        message['tweet_text']=tweet_text
        message['source']=source
        message['user_name']=user_name
        message['user_lang']=user_lang
        message['hashtags']=t.extract_hash_tags(tweet_text)
        hashtags = t.extract_hash_tags(tweet_text)


        print "....",hashtags

        msg = [str(tweet_id),created_at.encode('utf-8').strip(), tweet_text.encode('utf-8').strip(), source.encode('utf-8').strip(), user_name.encode('utf-8').strip(), user_lang.encode('utf-8').strip(),str(hashtags).replace (",",'-')]
        msg = ",".join(msg).decode('utf-8').strip()
        print msg
        # Produce line (without newline)"""
        print message
        p.produce("prabhat_topic", message.encode('utf-8'), callback=delivery_callback)
        print "Message sent successfully to kafka topic ."
        logger.info("Message sent successfully to kafka topic .")
    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(p))

    except exceptions as e:
        print e
        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()



class listener(StreamListener):

    def on_data(self, data):
        #print data
        producer_sync_send("twitter",data)
        return True

    def on_error(self, status):
        print status


print "connecting to twitter"
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

print "listening to twitter"
stream1 = Stream(auth, listener())

try :
    stream1.filter(track=['modi'])

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')