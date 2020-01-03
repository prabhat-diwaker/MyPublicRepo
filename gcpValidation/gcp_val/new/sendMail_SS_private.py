import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import time
import os, datetime
import sys

child_email = sys.argv[1]
task_file_path = sys.argv[2]

def sendMail():
    print "Sending email with reports !! \n "
    recipient_list = [child_email]
    email_from = 'SecretSanta@MerchDatalake.com'

    curr_date = str(datetime.datetime.now()).split(" ")[0]
    abs_path = "/u/users/p0d00mp/new/gcp_val/"
    report_file = abs_path + "reports/" + "summary_report_" + curr_date + ".xls"

    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = ", ".join(recipient_list)
    msg['Subject'] = 'Secret Santa : Task '

    # email content
    message = """<html>
    <body>
    Hello Child !

    Attached is the task for you today !

    All the best and Merry christmas !!
	
    <br>Regards<br/>
    <br>yours Secret Santa<br/>
    <br><br>

    </body>
    </html>
    """

    msg.attach(MIMEText(message, 'html'))
    #task_file =  "/u/users/svcmdsedat/child_task.txt"

    files = [task_file_path]

    for a_file in files:
        attachment = open(a_file, 'rb')
        file_name = os.path.basename(a_file)
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        part.add_header('Content-Disposition',
                        'attachment',
                        filename=file_name)
        encoders.encode_base64(part)
        msg.attach(part)

    # sends email

    smtpserver = smtplib.SMTP('vsitemta.walmart.com', '25')
    smtpserver.sendmail(email_from, recipient_list, msg.as_string())
    smtpserver.quit()
    print "Email sent successfully!! \n "


if __name__ == "__main__":
    sendMail()
