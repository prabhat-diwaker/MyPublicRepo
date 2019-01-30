import os
import pyodbc

filename = 'lighthouse-logs.log'

##### Function tp read log file and yield each line
def readLogFile(filename):
    with open(filename,'r') as file_obj:
        for line in file_obj:
            yield line

lines_generator = readLogFile(filename)


##### Open a connection to SQL SERVER DB
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=.\SQLExpress;"
                      "Database=prabhat;"
                      "Trusted_Connection=yes;")
cursor = cnxn.cursor()

##### Create target table in DB
cursor.execute("drop table if exists dbo.log_analysis")
cursor.execute("create table log_analysis (group_type varchar(10),experiment varchar(10),date varchar(15));")



##### Process each line from log to get  "Test" group, experiment and date info for each assignment

for log_line in lines_generator:
    if log_line.find("assigned to test") != -1 :
        group = "Test"
        experiment_index = log_line.find("MS-")
        date_index = log_line.find('"time":"')
        experiment = log_line[experiment_index:experiment_index+6]
        date = log_line[date_index+8:date_index+18]

        ##write records to DB
        query = "insert into dbo.log_analysis values ('"+ group + "'," + "'" + experiment + "'," + "'" + date + "')"
        cursor.execute(query)
        cursor.execute("commit")

##### Process each line from log to get  "Control" group, experiment and date info for each assignment

    elif log_line.find("assigned to control") != -1 :
        group = "Control"
        experiment_index = log_line.find("MS-")
        date_index = log_line.find('"time":"')
        experiment = log_line[experiment_index:experiment_index + 6]
        date = log_line[date_index + 8:date_index + 18]

        ##write records to DB
        query = "insert into dbo.log_analysis values ('" + group + "'," + "'" + experiment + "'," + "'" + date + "')"
        cursor.execute(query)
        cursor.execute("commit")

## Questtion 1 : What are the total number users assigned to the “Test” and “Control” groups in each experiment?

question1_answer_query = "select group_type,count(*) from dbo.log_analysis group by group_type"
cursor.execute(question1_answer_query)
for row in cursor:
    print('%r' % (row,))


## Questtion 2 : Which day had the highest number of user group assignments per experiment?

question2_answer_query = "select date,cnt from (select date,cnt,rank() over ( order by cnt desc) as rnk from (select date,count(*) as cnt from dbo.log_analysis group by date )tmp)a where rnk =1"

cursor.execute(question2_answer_query)
for row in cursor:
    print('%r' % (row,))

