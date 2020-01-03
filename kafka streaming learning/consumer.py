
import logging
from confluent_kafka import Consumer,KafkaException
import sys


import os,datetime,exceptions,time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(lineno)d:%(message)s')
file_handler = logging.FileHandler('Consumer.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'new', 'session.timeout.ms': 6000,
            'auto.offset.reset': 'latest'}

consumer = Consumer(conf,logger=logger)

print "created consumer"

def print_assignment(consumer, partitions):
    print('Assignment:', partitions)


# Subscribe to topics
try :
    consumer.subscribe(["store_label"], on_assign=print_assignment)
    print "Topic subscribed"

except exceptions as e :
    print e

try:
    while True:
        print "polling topic"
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print "No messages in second topic"
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key())))

            print msg.value()

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Close down consumer to commit final offsets.
    consumer.close()