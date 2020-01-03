from confluent_kafka import Producer
import logging
import sys
import exceptions
import time
import json
from kafka import KafkaProducer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(lineno)d:%(message)s')
file_handler = logging.FileHandler('Producer.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'new'}

# Create Producer instance

p = KafkaProducer(value_serializer=lambda v: json.dumps(v))


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        print ('%% Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))


def producer_sync_send (key,message):

    print "Sending message value to kafka"
    logger.info("Sending message value to kafka")
    try:
        p.send("store-label3", message)
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



with open('/Users/p0d00mp/source.txt','r') as file_obj:
    lines=file_obj.readlines()


if __name__ == "__main__":
    try:
        count = 0
        for line in lines:
            key=time.time()
            message = json.loads(line)[0]
            producer_sync_send(key,message)
            time.sleep(0.5)
            count = count + 1
            print count


    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

