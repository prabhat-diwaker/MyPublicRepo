import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import time
import os,datetime

def sendMail():

    print "Sending email with reports !! \n "
    recipient_list = ['prabhat.diwaker@walmartlabs.com','ramachandra.bhatm@walmart.com','tejaswi.chandra.chandra@walmart.com']
    email_from = 'GCPValidation@walmartlabs.com'

    curr_date = str(datetime.datetime.now()).split(" ")[0]
    abs_path = "/u/users/p0d00mp/gcp_val/"
    report_file = abs_path + "summary_report_" + curr_date + ".xls"

    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = ", ".join(recipient_list)
    msg['Subject'] = 'GCP Validation report is ready !'

#email content
    message = """<html>
    <body>
    Attached is the GCP validation report !.
    
    
    Automatic generated Mail, Please do not reply !
    <br><br>

    </body>
    </html>
    """

    msg.attach(MIMEText(message, 'html'))
    xl_report = abs_path + "summary_report_" + curr_date + ".xls"
    txt_report = abs_path + "summary_report_" + curr_date + ".txt"

    files = [xl_report,txt_report]

    for a_file in files:
        attachment = open(a_file, 'rb')
        file_name = os.path.basename(a_file)
        part = MIMEBase('application','octet-stream')
        part.set_payload(attachment.read())
        part.add_header('Content-Disposition',
                    'attachment',
                    filename=file_name)
        encoders.encode_base64(part)
        msg.attach(part)

    #sends email

    smtpserver = smtplib.SMTP('vsitemta.walmart.com', '25')
    smtpserver.sendmail(email_from, recipient_list, msg.as_string())
    smtpserver.quit()
    print "Email sent successfully!! \n "
if __name__ == "__main__":
    sendMail()
