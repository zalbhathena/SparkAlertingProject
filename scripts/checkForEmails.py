import os
import time
import smtplib

directory = ".emails/"

if not os.path.exists(directory):
	os.makedirs(directory)

import smtplib

def send_email(recipient, subject, body):
	user = 'sparkstreamingalerts@gmail.com'
	gmail_user = user
	pwd = 'thepassword'
	gmail_pwd = pwd
	FROM = user
	TO = recipient if type(recipient) is list else [recipient]
	SUBJECT = subject
	TEXT = body

	print user
	print pwd
	print recipient
	print subject
	print body

	# Prepare actual message
	message = """From: %s\nTo: %s\nSubject: %s\n\n%s
	""" % (FROM, ", ".join(TO), SUBJECT, TEXT)
	try:
		server = smtplib.SMTP("smtp.gmail.com", 587)
		server.ehlo()
		server.starttls()
		server.login(gmail_user, gmail_pwd)
		server.sendmail(FROM, TO, message)
		server.close()
		print 'successfully sent the mail'
	except:
		print "failed to send mail"


while True:
	os.system("hdfs dfs -mv  emails/* .tempEmails/")
	os.system("hdfs dfs -copyToLocal .tempEmails/* "+directory)
	os.system("hdfs dfs -rm -r .tempEmails/email*")
	
	for root, subdirs, files in os.walk(directory):
		for file in files:
			if file.startswith("part-"):
				lines = [line.rstrip('\n') for line in open(os.path.join(root,file))]
				for line in lines:
					words = line.rstrip(')').lstrip('(').split(",")
					send_email(words[1], "Alert triggered!", words[2] + words[6] + words[8])
	
	os.system("rm -r .emails/*")
	time.sleep(2)
