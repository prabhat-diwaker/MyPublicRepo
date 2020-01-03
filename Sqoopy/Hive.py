import mysql.connector
import exceptions
import mysql.connector.errorcode
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(lineno)d:%(message)s')
file_handler = logging.FileHandler('Mysql.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

class MysqlConnect :
    def __init__(self,**kwargs):
        self.host=kwargs['host']
        self.user=kwargs['user']
        self.password=kwargs['password']
        self.database=kwargs.get('database','default')

    def createDBConnection(self):
        print "Connecting to MySQL"
        if self.database == 'default':
            try:
            # Obtain a connection wihtout a DB name :
                cnx = mysql.connector.connect(host=self.host, user=self.user, password=self.password)
                logger.warning("connection to {0} server using username: {1} is successful !  No DB specified in connection !".format(self.host,self.user))
                return  cnx
            except Exception as err:
                logger.error("connection failed !! server name : {0}, username: {1}. No DB specified in connection!".format(self.host,self.user))
        else:
            try:
                # Obtain a connection wihtout a DB name :
                cnx = mysql.connector.connect(host=self.host, user=self.user, password=self.password,database=self.database)
                logger.info("connection to {0} server using username: {1} is successful ! Database Specified : {2} !".format(self.host,self.user,self.database))
                return  cnx
            except Exception as err:
                logger.error("connection failed !! server name : {0}, username: {1}.Database Specified : {2}".format(self.host,self.user,self.database))


def main():
    mysql_obj = MysqlConnect(host="localhost", user="root", password="cloudera", database="mysql")
    cnx = mysql_obj.createDBConnection()
    cursor = cnx.cursor()
    sql = "show databases"
    cursor.execute(sql)
    for i in cursor:
        print i

if   __name__ == "__main__":
        main()


