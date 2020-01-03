#################################################################################################
# Script Name :     td_table_load_polling.py                                                    #
# Author :          Prabhat Diwaker (p0d00mp)                                                   #
# Date :            2019-10-09                                                                  #
# Description :     This script polls a TD table on the record count  to determine if           #
#                   table load is completed in TD. It will take one initial count and           #
#                   two additional counts to determine that count has changed from initial      #
#                   count and last two counts are unchanged.                                    #
# Parameters :      Sleep time in seconds between count checks, last n counts,                  #
#                   table name in TD, target location where count will be written in HDFS       #
#################################################################################################


#to utilize pytz local installation from this directory
#import os
#os.chdir("/u/users/appaorta/udp/resources/scripts")

import commands, sys
import exceptions
import argparse
import logging
import time
from datetime import datetime
#import pytz



# creating the args parser object
parser = argparse.ArgumentParser()
parser.add_argument('--sleep', type=float, default="2")
parser.add_argument('--last_n_count', type=int, default="2")
parser.add_argument('--td_table', type=str)
parser.add_argument('--location', type=str)
parser.add_argument('--startHMSutc', type=str)
parser.add_argument('--endHMSutc', type=str)

job_yaml = '/u/users/appaorta/udp/resources/scripts/teradata_load_polling/td_table_load_polling.yaml'
connection_yaml = '/u/users/p0d00mp/pre_prod_nkp/p0d00mp/yamls/mdse_modlr_upc/history/connection.yaml'
log_path = "/u/users/appaorta/udp/resources/logs/teradata_load_polling/"



def get_count(sleep_time):
    time.sleep(sleep_time)
    cmd = 'sh /u/users/p0d00mp/data-pipeline-9.3.1/run-river.sh -streamMode false -master local -connectionFile ' + connection_yaml + ' -yaml ' + job_yaml + ' -kv location=' + location + ' -kv td_table=' + td_table
    #print cmd
    try :
        status,output =  commands.getstatusoutput(cmd)
        if status == 0:
            if (output.find("Running job: job_") != -1):
                application_id= output[output.find("Running job: job_")+13:output.find("Running job: job_")+35]

            success_string = "Job " + application_id + " completed successfully"

            if output.find(success_string) != -1 :
                status,output =  commands.getstatusoutput("hadoop fs -cat gs://prabhs_test/TD_POLLING/part-m-00000")
                if status==0:
                    count = output
                    dateTimeObj = datetime.now()
                    return int(count),str(dateTimeObj).split('.')[0]
                else:
                    print status
                    print output
                    logger.info(output)
                    sys.exit(-1)
        else :
            print output
            logger.info(output)
            sys.exit(-1)

    except Exception as e:
        print e
        logger.info(e)
        sys.exit(-1)



    except Exception as e:
        print e
        logger.info(e)
        sys.exit(-1)


if __name__ == '__main__':

    # parsing the user arguments to local variable
    args = parser.parse_args()
    sleep = args.sleep
    last_n_count = args.last_n_count
    td_table = args.td_table
    location = args.location
    startHMSutc = args.startHMSutc
    endHMSutc = args.endHMSutc

    start_hr,start_min,start_sec = startHMSutc.split(":")
    end_hr, end_min , end_sec = endHMSutc.split(":")



    dateTimeObj = datetime.now()
    start_time = dateTimeObj.replace(hour=int(start_hr), minute=int(start_min), second=int(start_sec), microsecond=0)
    end_time = dateTimeObj.replace(hour=int(end_hr), minute=int(end_min), second=int(end_sec), microsecond=0)

    # creating log file name

    log_file = "td_table_load_polling_" + td_table
    log_file = log_path + log_file + '_' + str(dateTimeObj).split(" ")[0] + '_' + str(dateTimeObj).split(" ")[1].split('.')[0] + '.log'

    # create logger on the log file name
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    print "\nGenerating logs at : {0}\n".format(log_file)


    #get initial count
    count_list=[]
    table_loaded_flag = 0

    print "Taking Initial count.\n"
    logger.info("Taking Initial count.\n")
    one_time_count,timestamp = get_count(0)
    print "Initial Count in table - {0} at {1} is : {2}\n".format(td_table,timestamp,one_time_count)
    logger.info("Initial Count in table - {0} at {1} is : {2}\n".format(td_table,timestamp,one_time_count))

    print "job start time and End time are : {0}  ,  {1} \n".format(start_time,end_time)
    logger.info("job start time and End time are : {0}  ,  {1} \n".format(start_time,end_time))


    #loop for 2 counts untill table is loaded or polling window is ocer
    while table_loaded_flag !=1:
        now = datetime.now()
        #print now
        if now >= start_time and now < end_time:
            #print "here"
            if len(count_list) < last_n_count :
                count,timestamp = get_count(sleep)
                print "Current Count in table at {0} is : {1}".format(timestamp,count)
                logger.info("Current Count in table at {0} is : {1}".format(timestamp,count))
                count_list.append(count)

            elif len(count_list) >=last_n_count :

                count1,count2 = count_list[-last_n_count:]

                print "Count1 : {0}, count2 : {1}, Initial count : {2}".format(count1,count2,one_time_count)
                logger.info("Count1 : {0}, count2 : {1}, Initial count : {2}".format(count1,count2,one_time_count))

                if count1==count2 & count1 != one_time_count:
                    print "Table load completed !"
                    print "Initial count and Last 2 counts in table are:{0},{1},{2}".format(one_time_count,count1,count2)
                    logger.info("Table load completed !")
                    logger.info("Initial count and Last 2 counts in table are:{0},{1},{2}".format(one_time_count,count1,count2))
                    table_loaded_flag = 1
                    sys.exit(0)

                elif count1 == count2 == one_time_count:
                    print "Table load has not started yet."
                    logger.info("Table load has not started yet.")

                    count,timestamp = get_count(sleep)
                    print "Current Count in table at {0} is : {1}\n".format(timestamp, count)
                    logger.info("Current Count in table at {0} is : {1}\n".format(timestamp, count))
                    count_list.append(count)

                elif count1 != count2 & count1 != one_time_count:
                    print "Table load is in progress."
                    logger.info("Table load is in progress.")
                    count,timestamp = get_count(sleep)
                    print "Current Count in table at {0} is : {1}\n".format(timestamp, count)
                    logger.info("Current Count in table at {0} is : {1}\n".format(timestamp, count))
                    count_list.append(count)

                else:
                    count,timestamp = get_count(sleep)
                    print "Current Count in table at {0} is : {1}".format(timestamp, count)
                    logger.info("Current Count in table at {0} is : {1}".format(timestamp, count))
                    count_list.append(count)


            else :
                sys.exit(-1)

        elif now >= end_time:
            table_loaded_flag = 1
            print "polling window is over. Table load did not start or complete in this window. Exiting.\n"
            logger.info("polling window is over. Table load did not start or complete in this window. Exiting.\n")
            table_loaded_flag = 1
            sys.exit(0)

        elif now < start_time :
            dateTimeObj = datetime.now()
            print "Script will start taking next counts at {0} AM/PM UTC. Sleeping for next 5 minutes.. Current Time : {1}\n".format(startHMSutc,str(dateTimeObj).split('.')[0])
            logger.info("Script will start taking next counts at {0} AM/PM UTC. Sleeping for next 5 minutes.. Current Time : {1}\n".format(startHMSutc,str(dateTimeObj).split('.')[0]))
            time.sleep(300)

        else :
            sys.exit(-1)