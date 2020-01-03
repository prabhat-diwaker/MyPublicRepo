from kafka import KafkaConsumer
import sqlite3,time


def consumer_processed_count (message_value):
    message_value =  eval(message_value)
    return  len(message_value[0])

def update_count_unit_test(message_value,message_key):
    conn = sqlite3.connect('test.db')
    query = "update kafka_log set consumer_records_processed_count = " + str(consumer_processed_count(message_value)) + " , consumer_timestamp = datetime('now', 'localtime') where key = '" + message_key + "'"
    print query
    conn.execute(query)
    conn.commit()


consumer = KafkaConsumer('first',
                     bootstrap_servers=['localhost:9092','localhost:9093'],
                     group_id="new_group",
					 key_deserializer = str.encode,
					 value_deserializer = str.encode,
                     auto_offset_reset='earliest')
consumer.subscribe(['first'])

print "Consuming messages from the given topic"
while True:
    for message in consumer:
        if message is not None:
            time.sleep(2)
            print message.value
            update_count_unit_test(message.value,message.key)
