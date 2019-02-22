import os,datetime,exceptions,time
import pymysql
from kafka import KafkaProducer
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(lineno)d:%(message)s')
file_handler = logging.FileHandler('Producer.log')
file_handler.setFormatter(formatter)

def sql_connection(**args):
	try :
		server,user_name,pwd,db_name = args['server_name'],args['user'],args['pwd'],args['db_name']
		connection = pymysql.connect(host=server, user=user_name, password = pwd,db=db_name)
		return connection
	
	except Exception as e:
		print "1",e


def get_lastLoadID () :
	try :
		conn = sql_connection(server_name="localhost",user="root",pwd="cloudera",db_name="kafka")
		cursor = conn.cursor()
		cursor.execute("SELECT id FROM source_delta_log")
		results = cursor.fetchall()
		if len(results) > 0:
			for i in results :
				last_id = i[0]
			print last_id
			return last_id
		else :
			return "No last id stored"
	except Exception as e:
		print "2", e
	
		
def get_recordsFromDB () :
	try :
		conn = sql_connection(server_name="localhost",user="root",pwd="cloudera",db_name="kafka")
		cursor = conn.cursor()
		last_loadedID = get_lastLoadID ()
		if str(last_loadedID) == "No last id stored" :
			print "No ID stored in log table"
			return []
		else :	
			query = "select * from source where id > " + str(last_loadedID) + ";"
			print query
			cursor.execute(query)
			records = cursor.fetchall()
			conn.close()
			cursor.close()
			return records
	except Exception as e :
		print "3",e
		
def update_lastLoadID(max_loadID):
	try :
		conn = sql_connection(server_name="localhost",user="root",pwd="cloudera",db_name="kafka")
		cursor = conn.cursor()
		cursor.execute("truncate source_delta_log")
		cursor.execute("insert into source_delta_log values ('" + str(max_loadID) + "');")
		conn.commit()
		cursor.close()
		conn.close()
	except Exception as e:
		print "4",e

		
def producer_send () :
	try :
		producer = KafkaProducer(
									bootstrap_servers=['localhost:9092', 'localhost:9093'],
									acks='all',retries =4,
									max_in_flight_requests_per_connection = 1,
									key_serializer=str.encode,
									value_serializer=str.encode
								)	
		records = get_recordsFromDB()
		keys = str.encode("source")
		message = []
		if len(records) > 0:
			for record in  records :
				message.append(record)
			print message
			max_loadID =  record[0]
			try :
				future_metadata = producer.send('shashi', key=keys, value=str.encode(str(message)))
				record_metadata = future_metadata.get()
				if record_metadata.partition >= 0 and record_metadata.offset >= 0:
					print record_metadata.partition,record_metadata.offset
					update_lastLoadID(max_loadID)
			except Exception as e :
				print "Message Send Failed !!, {}, Filename : {}".format(e,keys)
		else :
			print "No New records !!"
	except Exception as e :
		print "5",e
			
			
if __name__ == "__main__":
	producer_send()
