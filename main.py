import MySQL
import Hive
import logging
import commands


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(lineno)d:%(message)s')
file_handler = logging.FileHandler('Main.log')
file_handler.setFormatter(formatter)


def getConfigParameters(project_name,db_name,config_table_name):
    print "Geting parameter List from Mysql for project_name : " + project_name
    try :
        mysql_obj = MySQL.MysqlConnect(host="localhost", user="root", password="cloudera", database=db_name)
        cnx = mysql_obj.createDBConnection()
        cursor = cnx.cursor()
        sql = "select * from " + db_name + '.' + config_table_name + " where project_name = '" + project_name + "'"
        cursor.execute(sql)

        param_list= [list(i) for i in cursor]
        return param_list  ### List of lists if there are multiple tables in a project
    except Exception as e:
        logger.error("Error : {0}".format(e))
def parseConnectionDetailsSQLServer(connection_file):
    print "Parsing connection details from file for SQLSERVER : : " + connection_file
    try:
        conn_file_path = "/home/cloudera/sqoop/"+connection_file
        with open(conn_file_path,'r') as file :
            conn_lines=file.readlines()
            a,server,db=conn_lines[0].strip().split('=')
            return server.strip() + "=" + db.strip(),conn_lines[1].strip().split('=')[1].strip(),conn_lines[2].strip().split('=')[1].strip()
    except Exception as e:
        logger.error("Error : {0}".format(e))

def parseConnectionDetailsMySQL(connection_file):
    print "Parsing connection details from file for MYSQL: " + connection_file
    try :
        conn_file_path = "/home/cloudera/sqoop/"+connection_file
        with open(conn_file_path,'r') as file :
            conn_lines=file.readlines()
            conn_string = conn_lines[0].split('=')[1].strip()
            user_name = conn_lines[1].split('=')[1].strip()
            password = conn_lines[2].split('=')[1].strip()
            return conn_string,user_name,password
    except Exception as e :
        logger.error("Error : {0}".format(e))

def fetchSqoopOutputDetails(output):
    lines = output.splitlines()
    for line in lines :
        if line.find("Running job:") <> -1 :
            JobID = line.split(":")[4]
        elif line.find("The url to track the job:") <> -1 :
            URL = reduce(lambda x,y : x+y,(line.split(":")[4:]))
        elif line.find("stats:") <> -1 :
            Stats = line.split(":")[1]

    return JobID,URL,Stats


def createSqoopyCommand(conf_param_list):

    project_name,connection_file,source_table,target_table,target_db,column_mapping,query,mode,check_column,mappers,split_by = conf_param_list[0],conf_param_list[1],conf_param_list[2],conf_param_list[3],conf_param_list[4],conf_param_list[5],conf_param_list[6],conf_param_list[7],conf_param_list[8],conf_param_list[9],conf_param_list[10]
    conn_string, username, password = parseConnectionDetailsMySQL(connection_file)
    try :
        table_in_hive = Hive.searchHiveTable(target_db, target_table)
        if table_in_hive == 0:
            print "Table " + target_table + " not found in hive."
            print "Creating Sqoop command to create table and full load"
            if mappers == 1 :
                cmd = "sqoop import --connect " + '"' + conn_string + '"' + " --username " + username + " --password " + '"' + password + '"' + " --query " + '"' + query + '"' + " --target-dir /tmp/diwaker " + " --delete-target-dir " + " --hive-import " + " --create-hive-table " + " --hive-database " + target_db + " --hive-table " + target_table + " --hive-overwrite " + " --hive-drop-import-delims " + " --m " + mappers
            else :
                cmd = "sqoop import --connect " + '"' + conn_string + '"' + " --username " + username + " --password " + '"' + password + '"' + " --query " + '"' + query + '"' + " --target-dir /tmp/diwaker " + " --delete-target-dir " + " --hive-import " + " --create-hive-table " + " --hive-database " + target_db + " --hive-table " + target_table + " --hive-overwrite " + " --hive-drop-import-delims " + " --m " + mappers + " --split-by " + split_by
            return cmd

        elif table_in_hive == 1 and mode == 'F':
            print "Table " + target_table + " found in hive."
            print "Creating Sqoop command to full load table."
            cmd = "sqoop import --connect " + '"' + conn_string + '"' + " --username " + username + " --password " + '"' + password + '"' +" --query " + '"' + query + '"' + " --target-dir /tmp/diwaker " + " --delete-target-dir " + " --hive-import " + " --hive-overwrite " + " --hive-database " + target_db + " --hive-table " + target_table + " --hive-drop-import-delims " + " --m " + mappers

            if mappers > 1 :
                cmd = cmd + " --split-by " + split_by

            return cmd
        elif table_in_hive == 1 and mode == 'I':
            print "Table " + target_table + " found in hive."
            last_value = Hive.getlastloadedvalue(target_db,target_table,check_column)
            print "last loaded value for table : " + target_table + " is : " + last_value
            print "Creating Sqoop command for incremental load to table."

            cmd = "sqoop import --connect " + '"' + conn_string + '"' + " --username " + username + " --password " + '"' + password + '"' + " --query " + '"' + query + '"' + " --target-dir /tmp/diwaker " + " --incremental append " + " --check-column " + check_column + " --last-value '" + last_value + "' --hive-import " + " --hive-overwrite " + " --hive-database " + target_db + " --hive-table " + target_table + " --hive-drop-import-delims " + " --m " + mappers

            return cmd

    except Exception as e :
        logger.error("Error : {0}".format(e))


def main() :
    param_list = getConfigParameters('project1','ce_analytics','config')

    for conf_param_list in param_list:
        cmd = createSqoopyCommand (conf_param_list)
        print cmd
        print "running sqoop commnad"
        status,output = commands.getstatusoutput(cmd)
        JobID, URL, Stats =  fetchSqoopOutputDetails(output)
        print  "JobID : {0}".format(JobID)
        print  "URL : {0}".format(URL)
        print  "Load Stats : {0}".format(Stats)
        logger.info("Sqoop command ran : {0}".format(cmd))

if   __name__ == "__main__":
    main()

