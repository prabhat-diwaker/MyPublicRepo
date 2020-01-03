import commands,os,errno,datetime,exceptions
import sys,config_parser as config


###   Variable setups
try:

    config_list = config.parseMain()
    curr_date = str(datetime.datetime.now()).split(" ")[0]

    abs_path =config_list['deploy_path']
    os.chdir(abs_path)

    beelineUrl = config_list['gcp_beeline']
    #print beelineUrl

except exceptions as e :
    print e


####list of GCP tables
def getTablesList(abs_path):

    try :
        print "Getting list of GCP tables to process.. \n"
        print "************************************************************************************************************"
        with open(abs_path + "config/tables.txt", 'r') as file_obj:
            table_list = file_obj.readlines()

        table_list = [table.strip() for table in table_list]
        # print table_list
        return table_list

    except exceptions as e:
        print e


###Get table partitions info
def getPartitionInfo(tableNameWithSchema):
    try :
        print "Getting partition info in GCP for table : " + tableNameWithSchema
        filename=abs_path + "stage/gcp_partition_info.txt"
        file_obj = open(filename, 'r')
        partition_lines = file_obj.readlines()
        tables = {}
        for line in partition_lines:
            tables[line.split(":")[0]]=line.split(":")[1]

        if tableNameWithSchema in tables:
            partition_columns=tables[tableNameWithSchema]
            print "Partition info in GCP retrieved. " + tableNameWithSchema + " is partitioned on columns: " + partition_columns
            return partition_columns


        else:
            query = "show create table " + tableNameWithSchema
            gcp_command = 'beeline -u "' + beelineUrl + '" --showHeader=true  --silent=false --outputformat=tsv2 -e ' + '"' + query + '"'
            #print gcp_command
            #print gcp_command
            status, output = commands.getstatusoutput(gcp_command)
            if status == 0 :
                start =  output.find("PARTITIONED BY")
                end = output.find(")",start)
                sub_string = output[start+18:end]
                col_list_temp =  sub_string.replace('`','').replace(',','').split('\n')
                partition_cols = []
                for i in col_list_temp:
                    partition_cols.append(i.strip().split(' ')[0])
                partition_columns = ','.join(partition_cols)

                file_obj = open(filename, 'a')
                line = tableNameWithSchema + ":" + partition_columns
                file_obj.write(line+'\n')
                print "Partition info in GCP retrieved. " + tableNameWithSchema + " is partitioned on columns: " + partition_columns
                return partition_columns
            else:
                print " Partition info could not retrieved in GCP for table : " + tableNameWithSchema
    except exceptions as e:
        print e



def createGcpQuery(tableNameWithSchema):
    try:
        partition_columns=getPartitionInfo(tableNameWithSchema)

        if partition_columns.find("geo_region_cd")<>-1:
            query = "select count(*) as count, '|'," + partition_columns + " from " + tableNameWithSchema + " where geo_region_cd = 'US' group by " + partition_columns

        elif partition_columns.find("op_cmpny_cd") <> -1:
            query = "select count(*) as count, '|'," + partition_columns + " from " + tableNameWithSchema + " where op_cmpny_cd = 'WMT-US' group by " + partition_columns

        else:
            query = "select count(*) as count, '|'," + partition_columns + " from " + tableNameWithSchema + " group by " + partition_columns
        return query

    except exceptions as e:
        print e


def gcpCounts(table):
    hive_table, gcp_table = table.split(",")
    table_folder = hive_table
    ## Create query
    query = createGcpQuery(gcp_table)

    ## create GCP command
    gcp_command = 'beeline -u "' + beelineUrl + '" --showHeader=false  --silent=false --outputformat=tsv2 -e ' + '"' + query + '"'

    ## write gcp commamnd to file
    query_filename = gcp_table + "_gcp_query.txt"
    filename = abs_path + "Output/" + table_folder + "/" + query_filename

    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(filename, 'w') as file_obj:
        file_obj.write(gcp_command + '\n\n')


    ## execute GCP command
    print "Executing GCP query for table: " + gcp_table
    print gcp_command
    status,output = commands.getstatusoutput(gcp_command)


    ## write gcp counts to file
    count_filename = gcp_table + "_gcp_counts.txt"
    filename = abs_path + "Output/" + table_folder + "/" + count_filename

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

    print "\nGCP Counts ready for " + gcp_table + " in " + filename + "\n"
    print "************************************************************************************************************"

def mainGcp():
    table_list=getTablesList(abs_path)
    for table in table_list:
        gcpCounts(table)

if __name__ == "__main__":
    mainGcp()
