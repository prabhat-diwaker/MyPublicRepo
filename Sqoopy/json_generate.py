import string,random,json,os,calendar,time,datetime

generator_string = "1234567890"+string.ascii_lowercase+string.ascii_uppercase



def generate_json_string():
    i = 0
    array = []
    final_dict = {}
    num = random.randint(1,999999)
    print num
    for i in (str(num)) :
        record = {}
        record["member_guid"] = "".join([random.choice(generator_string) for i in range (12)])
        record["transaction_no"] = random.randint(12345678,99999999)
        record["survey_id"] = "".join([random.choice(generator_string) for i in range (12)])
        record["csat_score"]=random.randint(0,10)
        array.append(record)
        i = i + 1
    final_dict["records"]= array
    return json.dumps(final_dict,indent=2 )




for i in ("1234567890") :
    time_string = str(int(time.mktime(datetime.datetime.now().timetuple())))
    filename_string = "/home/cloudera/kafka_practice/json_files/" + "file_" + time_string + ".json"
    with open(filename_string,'w') as file_obj:
        file_obj.write(generate_json_string())
    time.sleep(5)
