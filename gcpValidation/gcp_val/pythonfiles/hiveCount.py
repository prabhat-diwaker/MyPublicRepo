import os
import errno
import paramiko
import sys,datetime,commands
import sys

engine =sys.argv[1]


#####login details
curr_date = str(datetime.datetime.now()).split(" ")[0]
abs_path = abs_path = "/u/users/p0d00mp/gcp_val/"

hive_query_file = abs_path + "Query/" + "hive_query_" + curr_date + ".txt"
rm_command = "rm hive_query_file"
status,output=commands.getstatusoutput(rm_command)

with open(abs_path+"config/login.txt", 'r') as file_obj:
    lines = file_obj.readlines()

for line in lines:
    line = line.strip()
    username, password = line.split(":")

#print username, password


####list of tables
with open(abs_path+"config/tables.txt", 'r') as file_obj:
    table_list = file_obj.readlines()

table_list = [table.strip() for table in table_list]



def createHiveQueryGeo(tables):
    op_cmpny_cd_flg = 0
    schema_table, partition = tables.split(":")
    #print partition
    partitions = partition.split(",")
    for i in partitions :
        if i == "geo_region_cd" :
            op_cmpny_cd_flg = 1  ## partition has geo_region_cd
            exit
        if i == '' :
            op_cmpny_cd_flg = -1
            exit


    if op_cmpny_cd_flg == 0 : #there is no op_cmpny_cd partition
        query = "select count(*) as count, '|', " + partition + " from " + schema_table +  " group by " + partition

    elif op_cmpny_cd_flg == 1 : ## only partition is op_cmpny_cd
        query = "select count(*) as count, '|', " + partition + " from " + schema_table + " where geo_region_cd = 'US' " +  " group by " + partition

    elif op_cmpny_cd_flg == -1 :
        query = "select count(*) as count, '|', " + "'not_partitioned'" + " from " + schema_table

    #print partitions,len(partitions) ,op_cmpny_cd_flg,query
    return  query


def createHiveQueryCmp(tables):
    op_cmpny_cd_flg = 0
    schema_table, partition = tables.split(":")
    #print partition
    partitions = partition.split(",")
    for i in partitions :
        if i == "op_cmpny_cd" :
            op_cmpny_cd_flg = 1  ## partition has op_cmpny_cd
            exit
        if i == '' :
            op_cmpny_cd_flg = -1
            exit

    if op_cmpny_cd_flg == 0 : #there is no op_cmpny_cd partition
        query = "select count(*) as count, '|', " + partition + " from " + schema_table +  " group by " + partition

    elif op_cmpny_cd_flg == 1 : ## only partition is op_cmpny_cd
        query = "select count(*) as count, '|', " + partition + " from " + schema_table + " where op_cmpny_cd = 'WMT-US' " + " group by " + partition

    elif op_cmpny_cd_flg == -1 :
        query = "select count(*) as count, '|', " + "'not_partitioned'" + " from " + schema_table

    #print partitions,len(partitions) ,op_cmpny_cd_flg,query
    return  query

# print table_list

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
paramiko.util.log_to_file("Hive.log")
Pros = []

def hadoopCounts(table, ssh):
    tez_engine = "set hive.execution.engine=" + engine +"; SET tez.queue.name=mdsedatload; SET hive.exec.compress.intermediate=true; SET hive.tez.auto.reducer.parallelism=true;SET hive.exec.reducers.bytes.per.reducer=268435456;SET tez.grouping.max-size=67108864;SET tez.queue.name=mdsedatload;SET hive.tez.container.size=8192;SET tez.am.resource.memory.mb=2148;SET hive.tez.java.opts=-Xmx6553m;SET tez.runtime.unordered.output.buffer.size-mb=819;SET tez.am.resource.memory.mb=8192;"
    table_folder = table.split(":")[0]
    partition = table.split(":")[1]

    ## Create query

    if partition.find("op_cmpny_cd") <> -1:
        query = createHiveQueryCmp(table)
    elif partition.find("geo_region_cd") <> -1:
        query = createHiveQueryGeo(table)
    else :
        query =createHiveQueryCmp(table)



    ## create hive command
    hive_command = "hive -e " + '"' + tez_engine + query + '"'
    hive_query_file = abs_path + "Query/" + "hive_query_" + curr_date + ".txt"


    ## write hive commamnd to file
    with open(hive_query_file,'a') as hive_query_file_obj:
        hive_query_file_obj.write(hive_command+'\n\n')

    print "\n\nExecuting Hive query for table: " + table
    print hive_command

    ## execute hive command
    
    stdin, stdout, stderr = ssh.exec_command(hive_command)

    filename = table.split(":")[0] + "_hive.txt"
    filename = abs_path + "Output/" + table_folder + "/" + filename
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    ## write hive partition and counts to file
    with open(filename, 'w') as file_obj:
        for count in stdout.readlines():
            file_obj.write(count)

    print "Hive Counts ready for " + table + " in " + filename

def mainHive():
    try:
        ssh.connect('oser502583.homeoffice.wal-mart.com', username=username, password=password)
    except paramiko.ssh_exception.AuthenticationException:
        print "invalid credentials"
    else:
        print "Login Successful"
        for table in table_list:
            hadoopCounts(table, ssh)

    """	# block until all the threads finish (i.e. block until all function_x calls finish)    
        for t in Pros:
            t.join()"""


if __name__ == "__main__":
    mainHive()
