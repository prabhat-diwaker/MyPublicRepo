import commands,os,errno,datetime,exceptions
import sys

def config_parse(file_name):
    try :
        with open(file_name, 'r') as file_obj:
            config_lines = file_obj.readlines()

        config_list = {}
        for line in config_lines:
            if line.count(":")==1:
                config_list[line.split(":")[0]]=line.split(":")[1].replace('\n','')
            if line.count(":")>1:
                config_list[line.split(":")[0]] = ":".join(line.split(":")[1:]).replace('\n', '')
        return  config_list

    except exceptions as e:
        print e

def parseMain():
    config_file = '/u/users/p0d00mp/new/gcp_val/config/config.txt'
    config_list=config_parse(config_file)
    return  config_list

if __name__ == "__main__":
    parseMain()
