import sys
import os
import time
t = time.time()

alertCountFile = "./.alertcount.txt"
tempdirectory = ".tempdirectory/"

if not os.path.exists(tempdirectory):
    os.makedirs(tempdirectory)

alertCount = 0
if os.path.isfile(alertCountFile):
	with open(alertCountFile, 'r') as f:
		first_line = f.readline()
		alertCount = int(first_line.rstrip("\n"))

if sys.argv[1] == "-a":
	alertArgs = sys.argv[2:]
	lines = []
	for i in range(0, len(alertArgs)/6):
		lines.append(str(alertCount) + "," + ",".join(alertArgs[i*6:i*6+6]))
		alertCount = alertCount + 1
	alertCountFile = open(alertCountFile, 'w+')
	alertCountFile.write(str(alertCount))
	alertCountFile.close()
		
	alertFile = open(tempdirectory+"tempfile.txt", 'w+')
	for line in lines:
		alertFile.write(line + "\n")
	alertFile.close()
        os.system("hdfs dfs -put "+tempdirectory+ "tempfile.txt tempstreaming/alert"+str(t))
        os.system("hdfs dfs -mv tempstreaming/alert"+str(t)+" streaminginput/alerts/alert"+str(t))	


elif sys.argv[1] == "-p":
	portfolioArgs = sys.argv[3:]
	ticker = sys.argv[2]
	portfolioString = ticker
	for i in range(0, len(portfolioArgs)/2):
		portfolioString = portfolioString + "," + portfolioArgs[i*2]+":"+portfolioArgs[i*2+1]
	portfolioFile = open(tempdirectory+"tempfile.txt", 'w+')
	portfolioFile.write(portfolioString+"\n")
	portfolioFile.close()
        os.system("hdfs dfs -put "+tempdirectory+ "tempfile.txt tempstreaming/portfolio"+str(t))
        os.system("hdfs dfs -mv tempstreaming/portfolio"+str(t)+" streaminginput/portfolios/portfolio"+str(t))	

os.system("rm -r "+tempdirectory)

