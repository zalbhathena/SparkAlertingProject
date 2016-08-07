import sys
import os

from datetime import datetime
dt = datetime.now()
t = dt.microsecond


alertCountFile = "./.alertcount.txt"
tempdirectory = ".tempdirectory/"

if not os.path.exists(tempdirectory):
    os.makedirs(tempdirectory)

alertCount = 0
if os.path.isfile(alertCountFile):
	with open('myfile.txt', 'r') as f:
		first_line = f.readline()
		alertCount = int(alertCount)

if sys.argv[1] == "-a":
	alertArgs = argv[2:]
	lines = []
	for i in range(0, len(alertArgs)/6):
		lines.append(str(alertCount) + "," + ",".join(alertArgs[i:i+6])
		alertCount = alertCount + 1
	alertCountFile = open(alertCountFile, 'w+')
	alertCountFile.write(alertCount)
	alertCountFile.close()
		
	alertFile = open(tempdirectory+"tempfile.txt", 'w+')
	for line in lines:
		alertFile.write(line + "\n")
	alertFile.close()
        os.system("hdfs dfs -put "+tempdirectory+ "tempfile.txt tempstreaming/alert"+str(t))
        os.system("hdfs dfs -mv tempstreaming/alert"+str(t)+" streaminginput/alerts/alert"+str(t))	


elif sys.argv[1] == "-p":
	pass

os.system("rm -r "+tempdirectory)
