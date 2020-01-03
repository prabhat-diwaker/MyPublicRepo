import os,datetime,exceptions,time
import sqlite3
from kafka import KafkaProducer
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(lineno)d:%(message)s')
file_handler = logging.FileHandler('Producer.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

##### read messages from json ##

root_dir = "/home/cloudera/kafka_practice/json_files/"

## get last timestamp of files read #

def get_last_timestamp():
    try :
        with open(root_dir + "timestamp.txt",'r') as file_obj:
            last_timestamp = file_obj.readline()
        print "Last timestamp stored was {}".format(last_timestamp)
        logger.info("Last timestamp for file read was : {}".format(last_timestamp))
        if last_timestamp == '':
            last_timestamp = '0000000000'
            print "No last timestamp for file read found. Setting to default '0000000000'"
            logger.info("No last timestamp for file read found. Setting to default '0000000000'")
        return  last_timestamp
    except Exception as e :
        print e
        exit()

#update timestamp
def update_last_timestamp(time):
    try :
        with open(root_dir + "timestamp.txt", 'w') as file_obj:
            file_obj.write(time)
        print "last timestamp updated to {}".format(time)
        logger.info("last timestamp updated to {}".format(time))
    except Exception as e :
        print e
        exit()

# get list of files to be read
def get_files_to_read () :
    try:
        all_files_list = os.listdir(root_dir)
        final_files_list = []
        last_timestamp=get_last_timestamp()
        print last_timestamp
        for files in all_files_list:
            if files.find("json") <> -1 and int(files[5:15]) > int(last_timestamp) :
                final_files_list.append(files)
        print "files to read are : {}".format(final_files_list)
        logger.info("files to read are : {}".format(final_files_list))
        return  final_files_list
    except Exception as e:
        print e
        exit()


def processed_count (file_name):
    with open(root_dir + file_name,'r') as  file_obj:
        content = eval(file_obj.read())
    content_array =  content["records"]
    transaction_nos = []
    for i in content_array:
        transaction_nos.append(i["transaction_no"])
    return  len(transaction_nos)

def update_count_unit_test(files):
    conn = sqlite3.connect('test.db')
    query = "INSERT INTO kafka_log (key,producer_records_processed_count,producer_timestamp) VALUES ( '" + files + "', " +str(processed_count(files)) + ",datetime('now', 'localtime'))"
    print query
    conn.execute(query)
    conn.commit()


def parse_json (file_name):
    with open(root_dir + file_name,'r') as  file_obj:
        content = eval(file_obj.read())
    content_array =  content["records"]
    transaction_nos = []
    csat_scores = []
    for i in content_array:
        transaction_nos.append(i["transaction_no"])
        csat_scores.append(i["csat_score"])
    return  str([transaction_nos,csat_scores])

###producer synchronous send
def producer_sync_send ():
    try :
        producer = KafkaProducer(
									bootstrap_servers=['localhost:9092', 'localhost:9093'],
									acks='all',retries =2,
									max_in_flight_requests_per_connection = 3,
									key_serializer=str.encode,
									value_serializer=str.encode
								)
        files_to_read = get_files_to_read()
        files_to_read.sort()
        if len(files_to_read) > 0 :
            for files in files_to_read:
                message =  parse_json(files)
                print "Sending content of file {} as message value to kafka".format(files)
                logger.info("Sending content of file {} as message value to kafka".format(files))
                try :
                    future_metadata = producer.send('first', key=files, value=message)
                    record_metadata = future_metadata.get()
                    print "Message sent successfully. Metadata from brokes are : topic - {}, partition - {}, offset - {}".format(record_metadata.topic, record_metadata.partition, record_metadata.offset)
                    logger.info("Message sent successfully. Metadata from brokes are : topic - {}, partition - {}, offset - {}".format(record_metadata.topic, record_metadata.partition, record_metadata.offset))
                    if record_metadata.partition >= 0 and record_metadata.offset >= 0:
                        print "updating last timestamp to {}".format(files[5:15])
                        logger.info("updating last timestamp to {}".format(files[5:15]))
                        update_last_timestamp(files[5:15])
                        update_count_unit_test(files)
                except Exception as e :
                    logger.info("Message Send Failed !!, {}, Filename : {}".format(e,files))
                    print "Message Send Failed !!, {}, Filename : {}".format(e,files)

        else :
            print "No new files to read !"
            logger.warning("No new files to read !")
    except Exception as e :
        print e
        exit()




def producer_Async_send ():
    try :
        producer = KafkaProducer(
									bootstrap_servers=['localhost:9092', 'localhost:9093'],
									acks='all',retries =2,
									max_in_flight_requests_per_connection = 3,
									key_serializer=str.encode,
									value_serializer=str.encode
								)
        files_to_read = get_files_to_read()
        files_to_read.sort()
        if len(files_to_read) > 0 :
            for files in files_to_read:
                file_string =  root_dir + files
                with open(file_string,'r') as file_obj:
                    message =  file_obj.read()
                print "Sending content of file {} as message value to kafka".format(files)
                logger.info("Sending content of file {} as message value to kafka".format(files))
                try :
                    producer.send('first', key=str.encode(files), value=str.encode(message))
                    print "updating last timestamp to {}".format(files[5:15])
                    logger.info("updating last timestamp to {}".format(files[5:15]))
                    update_last_timestamp(files[5:15])
                except Exception as e :
                    logger.info("Message Send Failed !!, {}, Filename : {}".format(e,files))
                    print "Message Send Failed !!, {}, Filename : {}".format(e,files)

        else :
            print "No new files to read !"
            logger.warning("No new files to read !")
    except Exception as e :
        print e
        exit()


if   __name__ == "__main__":
    while True :
        producer_sync_send()
        time.sleep(1)