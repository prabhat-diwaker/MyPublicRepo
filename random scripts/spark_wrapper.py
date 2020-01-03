#################################################################################################
# Script Name :     pyspark_wrapper.py                                                          #
# Author :          Prabhat Diwaker (p0d00mp)                                                   #
# Date :            2019-12-03                                                                  #
# Description :     A generic script to create, submit and log a pyspark job                    #
#################################################################################################

import commands, os
import exceptions
import argparse
from datetime import datetime
import pytz
import sys
import errno

if __name__ == '__main__':

    # setting timezone
    tz = pytz.timezone('US/Central')
    dateTimeObj = datetime.now(tz)

    # creating argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--configName', type=str, default="None")
    args = parser.parse_args()

    config_file = args.configName

    if config_file == "None":
        print "Please provide absolute path of config file with --configName option."
        exit(-1)

    else:

        conf_name = config_file.split("/")[-1].split(".")[0]
        # print conf_name
        path = "/".join(config_file.split("/")[:-2]) + "/"
        conf_path = "/".join(config_file.split("/")[:-1]) + "/"
        log_path = path + "log/"
        scrip_path = path + "pyfile/"

        if not os.path.exists(os.path.dirname(log_path)):
            try:
                os.makedirs(os.path.dirname(log_path))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        # importing config file
        sys.path.insert(1, conf_path)
        spark_config = __import__(conf_name)

        try:
            script_vars = spark_config.script_var
            # print script_vars
            schema = script_vars.get('schema_name', '')
            table = script_vars.get('table_name', '')

            if schema == '' or schema == ' ':
                raise ValueError('schema_name')
            elif table == '' or table == ' ':
                raise ValueError('table_name')

            spark_submit_var = spark_config.spark_submit_var
            # print spark_submit_var
            master = spark_submit_var.get('master', '')
            if master == '':
                print "\nmaster info not present in job config. Setting the master to yarn."

            deploy_mode = spark_submit_var.get('deploy-mode', '')
            if deploy_mode == '':
                print "\ndeploy-mode info not present in job config. Setting the deploy-mode to client."

            queue = spark_submit_var.get('queue', '')
            script_name = scrip_path + spark_submit_var.get('script_name', '')

            if script_name == '' or script_name == ' ':
                raise ValueError('script_name')
            elif queue == '' or queue == ' ':
                raise ValueError('queue_name')

            # Generating log file name
            log_file = schema + '.' + table
            log_file = log_path + log_file + '_' + str(dateTimeObj).split(" ")[0] + '_' + \
                       str(dateTimeObj).split(" ")[1].split('.')[0] + '.txt'

            spark_submit_cmd = "spark-submit " \
                               + "--master " + master \
                               + " --deploy-mode " + deploy_mode \
                               + " --queue " + queue \
                               + " --py-files " + config_file \
                               + " " + script_name

            print "Spark submit command:"
            print spark_submit_cmd

        except exceptions as e:
            print e

        except ValueError as err:
            print "schema_name, table_name, script_name and queue are mandatory variables in job config."
            print "Missing variable in job config:", err.args
            sys.exit(-1)

        try:
            os.putenv("SPARK_MAJOR_VERSION", "2")

            cmd = "echo $SPARK_MAJOR_VERSION"
            status, output = commands.getstatusoutput(cmd)
            print "SPARK_MAJOR_VERSION is set to : " + str(output)

            status, output = commands.getstatusoutput(spark_submit_cmd)
            print "\nGenerating logs at : {0}".format(log_file)
            with open(log_file, 'w') as log_file_obj:
                log_file_obj.write(output)

            if status == 0:
                print "\nJob run completed successfully. Logs generated at : {0}\n".format(log_file)
                sys.exit(0)
            else:
                print "\nJob run failed with exit code :" + str(status) + ". Please check logs at: {0}\n".format(
                    log_file)
                sys.exit(-1)

        except exceptions as e:
            print e
            sys.exit(-1)