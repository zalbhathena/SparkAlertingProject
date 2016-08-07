import os
import sched, time
import threading

s = sched.scheduler(time.time, time.sleep)

directory = ".tickersimulation/"

#os.system("rm -r "+directory)
if not os.path.exists(directory):
    os.makedirs(directory)

lines = [line for line in open('/home/znb205/allTicks.txt')]

numLines = len(lines)

x = 0

def writeFile(x):
	print x
	tempFile = open(directory+"tempfile"+str(x)+".txt", 'w+')
	for y in range (0,1000):
		tempFile.write(lines[x*1000 + y])
	tempFile.close()
	os.system("hdfs dfs -put "+directory+ "tempfile" +str(x)+".txt tempstreaming/tick"+str(x))
	os.system("hdfs dfs -mv tempstreaming/tick"+str(x)+" streaminginput/ticks/tick"+str(x))
	x = x + 1
	threading.Timer(2, writeFile, (x,)).start()

writeFile(x)
