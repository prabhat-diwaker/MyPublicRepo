import os,config_parser as config
import errno
import exceptions
import sys, datetime, commands
import sys

###   Variable setups
try:

    config_list = config.parseMain()
    curr_date = str(datetime.datetime.now()).split(" ")[0]

    abs_path =config_list['deploy_path']
    os.chdir(abs_path)

    hive_engine = config_list['hive_engine']

except exceptions as e :
    print e


####list of GCP tables
def getHiveTablesList(abs_path):

    try :
        print "Getting list of Hive tables to process.. \n"
        print "************************************************************************************************************"
        with open(abs_path + "config/tables.txt", 'r') as file_obj:
            table_list = file_obj.readlines()

        table_list = [table.strip().split(",")[0] for table in table_list]
        # print table_list
        return table_list

    except exceptions as e:
        print e


###Get table partitions info
def getPartitionInfo(tableNameWithSchema):
    try :
        print "Getting partition info for Hive table : " + tableNameWithSchema
        filename=abs_path + "stage/hive_partition_info.txt"
        file_obj = open(filename, 'r')
        partition_lines = file_obj.readlines()
        tables = {}
        for line in partition_lines:
            tables[line.split(":")[0]]=line.split(":")[1]

        if tableNameWithSchema in tables:
            partition_columns=tables[tableNameWithSchema]
            print "Partition info in Hive retrieved. " + tableNameWithSchema + " is partitioned on columns: " + partition_columns
            return partition_columns


        else:
            hive_command = 'hive -e "show create table '+ tableNameWithSchema + '"'

            #print hive_command

            ## execute hive command
            status, output = commands.getstatusoutput(hive_command)
            if status == 0:
                start = output.find("PARTITIONED BY")
                end = output.find(")", start)
                sub_string = output[start + 18:end]
                col_list_temp = sub_string.replace('`', '').replace(',', '').split('\n')
                partition_cols = []
                for i in col_list_temp:
                    partition_cols.append(i.strip().split(' ')[0])
                partition_columns = ','.join(partition_cols)

                file_obj = open(filename, 'a')
                line = tableNameWithSchema + ":" + partition_columns
                file_obj.write(line + '\n')
                print "Partition info in Hive retrieved. " + tableNameWithSchema + " is partitioned on columns: " + partition_columns
                return partition_columns
            else:
                print " Partition info could not retrieved in Hive for table : " + tableNameWithSchema
    except exceptions as e:
            print e





def createHiveQuery(tableNameWithSchema):
    try:
        partition_columns=getPartitionInfo(tableNameWithSchema)

        if partition_columns.find("geo_region_cd")<>-1:
            query = "select count(*) as count, '|'," + partition_columns + " from " + tableNameWithSchema + " where geo_region_cd = 'US'  group by " + partition_columns
        elif partition_columns.find("op_cmpny_cd") <> -1:
            query = "select count(*) as count, '|'," + partition_columns + " from " + tableNameWithSchema + " where op_cmpny_cd = 'WMT-US' group by " + partition_columns
        else :
            query = "select count(*) as count, '|'," + partition_columns + " from " + tableNameWithSchema + " group by " + partition_columns
        return query

    except exceptions as e:
        print e


def hiveCounts(tableNameWithSchema):

    table_folder = tableNameWithSchema

    ## Create query
    query = createHiveQuery(tableNameWithSchema)

    ## create GCP command
    tez_engine = "set hive.execution.engine=" + hive_engine + "; SET tez.queue.name=mdsedatload; SET hive.exec.compress.intermediate=true; SET hive.tez.auto.reducer.parallelism=true;SET hive.exec.reducers.bytes.per.reducer=268435456;SET tez.grouping.max-size=67108864;SET tez.queue.name=mdsedatload;SET hive.tez.container.size=4096;SET tez.am.resource.memory.mb=2148;SET hive.tez.java.opts=-Xmx6553m;SET tez.runtime.unordered.output.buffer.size-mb=819;SET tez.am.resource.memory.mb=4096;"


    ## create hive command
    hive_command = "hive -e " + '"' + tez_engine + query + '"'

    ## write hive commamnd to file
    query_filename = tableNameWithSchema + "_hive_query.txt"
    filename = abs_path + "Output/" + table_folder + "/" + query_filename

    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(filename, 'w') as file_obj:
        file_obj.write(hive_command + '\n\n')


    ## execute GCP command
    print "Executing Hive query for table: " + tableNameWithSchema
    print hive_command
    status,output = commands.getstatusoutput(hive_command)


    ## write hive counts to file
    count_filename = tableNameWithSchema + "_hive_counts.txt"
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

    print "\nHive Counts ready for " + tableNameWithSchema + " in " + filename + "\n"
    print "************************************************************************************************************"


def mainHive():
    table_list = getHiveTablesList(abs_path)
    for table in table_list:
        hiveCounts(table)


if __name__ == "__main__":
    mainHive()
