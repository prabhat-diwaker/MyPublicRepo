import os
import datetime
import commands
import errno
import xlwt,exceptions

import sys
from multiprocessing import Process
import hiveCounts as hive, gcpCounts as gcp
import sendMail as mail
import config_parser as config

runfromCompare = sys.argv[1]
sendMailFlag = sys.argv[2]

print runfromCompare
print sendMailFlag


###   Variable setups
try:

    config_list = config.parseMain()
    curr_date = str(datetime.datetime.now()).split(" ")[0]

    abs_path =config_list['deploy_path']
    os.chdir(abs_path)
    print abs_path
except exceptions as e :
    print e





curr_date = str(datetime.datetime.now()).split(" ")[0]

report_file = abs_path + "reports/" + "summary_report_" + curr_date + ".txt"
command = "rm " + report_file
status, output = commands.getstatusoutput(command)


def getTablesList(abs_path):

    try :
        #print "Getting list of Hive tables to process.. \n"
        print "************************************************************************************************************"
        with open(abs_path + "config/tables.txt", 'r') as file_obj:
            table_list = file_obj.readlines()

        table_list = [table.strip() for table in table_list]
        # print table_list
        return table_list

    except exceptions as e:
        print e


def xlGenerate(results):
    print "Generating excel report ... "
    report_file = abs_path + "reports/" + "summary_report_" + curr_date + ".xls"
    xl_book = xlwt.Workbook()
    sheet1 = xl_book.add_sheet("GCP_VALIDATION")
    cols = ["Schema Name", "Table Name", "Hive Partitions Count", "GCP Partitions Count",
            "Hive Count in Matching Partitions", "GCP Count in Matching Partition",
            "Counts mismatch in Matching Partitions", "No of Partitions Not in GCP", " No of Partitions Not in Hive",
            "Result"]
    results.insert(0, cols)
    ctype = 'string'
    xf = 0

    fail_fmt = xlwt.Style.easyxf("""
    font: name Arial;
    pattern: pattern solid, fore_colour orange;
    """, num_format_str='YYYY-MM-DD')

    pass_fmt = xlwt.Style.easyxf("""
    font: name Arial;
    pattern: pattern solid, fore_colour light_green;
    """, num_format_str='YYYY-MM-DD')

    for row in xrange(0, len(results)):
        for col in xrange(0, len(cols)):
            value = results[row][col]
            if col == 9 and value == 'FAIL':
                sheet1.write(row, col, value, fail_fmt)
            elif col == 9 and value == 'PASS':
                sheet1.write(row, col, value, pass_fmt)
            else:
                sheet1.write(row, col, value)

    xl_book.save(report_file)

    print "Excel report Generated ... "


def compareCounts(table):
    #schema_name, table_name = table.split(".")  # col1 and col2
    hive_table,gcp_table=table.split(",")

    counts_not_matching = {}
    hive_count = {}
    gcp_count = {}

    print "Comparing counts for tables : '" + hive_table + "' and '" + gcp_table + "'"

    ## Getting hive counts
    print "Getting hive counts"

    hive_filename = abs_path + "Output/" + hive_table + "/" + hive_table + "_hive_counts.txt"
    print hive_filename

    with open(hive_filename, 'r') as file:
        counts = file.readlines()

    for count in counts:
        if count.find("|")<> -1:
            count = count.strip().replace("\t", "")
            count, partition = count.split("|")
            hive_count[partition] = count
    #print hive_count

    ## Getting GCP counts

    gcp_filename = abs_path + "Output/" + hive_table + "/" + gcp_table + "_gcp_counts.txt"
    with open(gcp_filename, 'r') as file:
        counts = file.readlines()

    for count in counts:
        if count.find("|	") <> -1:
            count = count.strip().replace("\t", "")
            count, partition = count.split("|")
            gcp_count[partition] = count

    ## Generating Summary reports

    with open(report_file, 'a') as report_obj:
        total_hive_partitions = len(hive_count)  ## to write col3
        total_gcp_partitions = len(gcp_count)  ## to write col4

        print "total_gcp_partitions : ", total_gcp_partitions
        print "total_hive_partitions : ", total_hive_partitions
        report_obj.write("-------------------------------- " + table + "-------------------------------- " + "\n")
        report_obj.write("total_gcp_partitions : " + str(total_gcp_partitions) + " \n")
        report_obj.write("total_hive_partitions : " + str(total_hive_partitions) + " \n")

        hive_partitions = set(hive_count)
        gcp_partitions = set(gcp_count)

        for partition in hive_partitions.intersection(gcp_partitions):
            if hive_count[partition] <> gcp_count[partition]:
                counts_not_matching[partition] = "HIVE : " + hive_count[partition] + " , " + "GCP : " + gcp_count[
                    partition]  ## to write col 9

        hive_table_count = 0  ## to write col5
        gcp_table_count = 0  ## to write col6
        for partition in hive_partitions.intersection(gcp_partitions):
            hive_table_count = hive_table_count + int(hive_count[partition])
            gcp_table_count = gcp_table_count + int(gcp_count[partition])

        print "Total count in Hive for Matching partitions : ", hive_table_count
        print "Total count in GCP for Matching partitions : ", gcp_table_count
        report_obj.write("Total count in Hive for Matching partitions :" + str(hive_table_count) + " \n")
        report_obj.write("Total count in GCP for Matching partitions :" + str(gcp_table_count) + " \n")

        table_folder = hive_table
        filename = abs_path + "Output/" + table_folder + "/" + "count_report_" + hive_table + ".txt"
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        command = "rm " + filename
        status, output = commands.getstatusoutput(command)

        with open(filename, 'a') as file_obj:
            header = "Partitions        |       hive_count    |    gcp_count      |     run_date"
            file_obj.write(header + "\n")
            for partition in hive_partitions.union(gcp_partitions):
                record = partition + "    |    " + hive_count.get(partition, "NULL") + "    |    " + gcp_count.get(
                    partition, "NULL") + "    |    " + curr_date
                file_obj.write(record + "\n")

        only_in_hive = hive_partitions.difference(gcp_partitions)  ##col7
        only_in_gcp = gcp_partitions.difference(hive_partitions)  ##col8

        print "Partitions not present in GCP : ", only_in_hive
        print "Partitions not present in Hive : ", only_in_gcp

        report_obj.write("Partitions not present in GCP : " + str(only_in_hive) + " \n")
        report_obj.write("Partitions not present in Hive : " + str(only_in_gcp) + " \n")

        print "Counts not matching in these partitions :"
        report_obj.write("Counts not matching in these partitions :" + " \n")

        for i in counts_not_matching:
            print i, counts_not_matching[i]
            report_obj.write(i + "," + counts_not_matching[i] + " \n")

        result = []
        result.append(hive_table.split('.')[0])
        result.append(hive_table.split('.')[1])
        result.append(total_hive_partitions)
        result.append(total_gcp_partitions)
        result.append(hive_table_count)
        result.append(gcp_table_count)
        result.append(len(counts_not_matching))
        result.append(len(only_in_hive))
        result.append(len(only_in_gcp))

        if len(only_in_hive) == 0 and len(only_in_gcp) == 0 and len(
                counts_not_matching) == 0 and total_gcp_partitions <> 0 and total_hive_partitions <> 0:
            print "PASSED"
            report_obj.write("PASSED !! \n\n\n")
            result.append("PASS")
        else:
            print "FAILED"
            report_obj.write("FAILED !! \n\n\n")
            result.append("FAIL")

        return result


if __name__ == "__main__":

    table_list = getTablesList(abs_path)

    if runfromCompare == "Y":
        p1 = Process(target=hive.mainHive)
        p1.start()
        p2 = Process(target=gcp.mainGcp)
        p2.start()
        p1.join()
        p2.join()

        #hive.mainHive()
        #gcp.mainGcp()
        results = []

        for table in table_list:
            results.append(compareCounts(table))

        xlGenerate(results)

    else :
        results = []

        for table in table_list:
            results.append(compareCounts(table))

        xlGenerate(results)

    if sendMailFlag == "Y":
        mail.sendMail()
