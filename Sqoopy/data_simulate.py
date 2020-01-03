import pymysql
import random
import datetime
import time





def sql_connection() :
	connection = pymysql.connect(host='localhost', user='root', password = 'cloudera',db='kafka')
	return connection
	
	

	
def simulate_source() :
	cnx	= sql_connection()
	cursor = cnx.cursor()
	i = 1
	date_value = datetime.datetime.today().strftime('%Y-%m-%d')
	while i <= 100:
		transaction = random.randint(1111,9999)
		query = "insert into source values (" + str(i) + "," + str(transaction) + ",'" + date_value + "')"
		print query
		cursor.execute(query)
		i = i + 1
		cnx.commit()
		time.sleep(3)
		
	cursor.close()
	cnx.close()
	
if   __name__ == "__main__":
	simulate_source()

