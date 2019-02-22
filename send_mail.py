import os,sys
import smtplib
from datetime import datetime
from email import Encoders
from email.MIMEBase import MIMEBase
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formatdate
from email.mime.text import MIMEText

SMTP_USERNAME = 'ddome@adobe.com'
SMTP_PASSWORD = 'password'
SMTP_RECIPIENTS = ['diwaker@adobe.com', 'diwaker@adobe.com', 'diwaker@adobe.com']
SMTP_SERVER = 'mailsj-v1.corp.adobe.com'


def send_email():

	print ""
	print "Preparing to send email..."

	try:
		body = sys.argv[1]
		msg = MIMEMultipart()
		msg["From"] = SMTP_USERNAME
		msg['To'] = ', '.join(SMTP_RECIPIENTS) 
		msg["Subject"] = "<Stage> DDOMe Run !!"
		msg['Date'] = formatdate(localtime=True)
		msg.attach(MIMEText(body,'plain'))
	 
		smtpObj = smtplib.SMTP('mailsj-v1.corp.adobe.com')
		smtpObj.sendmail(SMTP_USERNAME, SMTP_RECIPIENTS, msg.as_string()) 
		
		print ""
		print "Email sent!"
		
	except Exception, e:
		errorMsg = "Unable to send email. Error: %s" % str(e)
		print errorMsg

send_email()