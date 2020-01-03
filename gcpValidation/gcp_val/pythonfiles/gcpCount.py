import commands,os,errno,datetime
import sys

engine=sys.argv[1]

#####login details
curr_date = str(datetime.datetime.now()).split(" ")[0]
abs_path = abs_path = "/u/users/p0d00mp/gcp_val/"

gcp_query_file = abs_path + "Query/" + "gcp_query_" + curr_date + ".txt"
rm_command = "rm gcp_query_file"
status,output=commands.getstatusoutput(rm_command)

####list of tables
with open(abs_path+"config/tables.txt", 'r') as file_obj:
    table_list = file_obj.readlines()

table_list = [table.strip() for table in table_list]



def createGcpQuery(tables):
    op_cmpny_cd_flg = 0
    schema_table, partition = tables.split(":")
    schema,table = schema_table.split(".")
    schema = schema.replace("_tables","_secure")
    schema_table = schema + "." + table
    partitions = partition.split(",")

    for i in partitions :
        if i == '' :
            op_cmpny_cd_flg = -1
            exit
    #tez = "set hive.execution.engine=tez;SET hive.exec.compress.intermediate=true; SET hive.tez.auto.reducer.parallelism=true;SET hive.exec.reducers.bytes.per.reducer=268435456;SET hive.tez.container.size=8192;SET tez.am.resource.memory.mb=2148;SET hive.tez.java.opts=-Xmx6553m;SET tez.runtime.unordered.output.buffer.size-mb=819;SET tez.am.resource.memory.mb=8192;"
	tez = "set hive.execution.engine=" + engine + ";"
    if op_cmpny_cd_flg == 0 : #there is no op_cmpny_cd partition
        query = tez + "select count(*) as count, '|', " + partition + " from " + schema_table +  " group by " + partition
    elif op_cmpny_cd_flg == -1 : ## table is not partitioned
        query = tez + "select count(*) as count, '|', " + "'not_partitioned'" + " from " + schema_table


    return  query



def gcpCounts(table):
    table_folder = table.split(":")[0]
    partition = table.split(":")[1]
    gcp_query_file = abs_path + "Query/" + "gcp_query_" + curr_date + ".txt"


    ## Create query
    query = createGcpQuery(table)


    ## create GCP command
    beelineUrl = '"jdbc:hive2://gmertsp02-0.us-central1.us.walmart.net:2181,gmertsp02-5.us-central1.us.walmart.net:2181,gmertsp02-2.us-central1.us.walmart.net:2181,gmertsp02-3.us-central1.us.walmart.net:2181,gmertsp02-1.us-central1.us.walmart.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"'
    gcp_command = "beeline -u " + beelineUrl + " --showHeader=false  --silent=false --outputformat=tsv2 -e " + '"'  + query  + '"'


    ## write gcp commamnd to file
    with open(gcp_query_file,'a') as query_file_obj:
        query_file_obj.write(gcp_command+'\n\n')


    ## execute hive command
    print "\n\nExecuting GCP query for table: " + table
    print gcp_command
    status,output = commands.getstatusoutput(gcp_command)


    table_folder = table.split(":")[0]
    filename = table.split(":")[0] + "_gcp.txt"

    filename = abs_path + "Output/" + table_folder + "/" + filename
    print filename
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


    ## write GCP partition and counts to file
    with open(filename, 'w') as file_obj:
        for count in output:
            file_obj.write(count)

    print "GCP Counts ready for " + table + " in " + filename

def mainGcp():
    for table in table_list:
        gcpCounts(table)


if __name__ == "__main__":
    mainGcp()
