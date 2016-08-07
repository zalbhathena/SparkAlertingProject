import os
import time

parent = "./.streamingtemp/"
alertsDirectory = parent+"alerts"
ticksDirectory = parent+"ticks"
portfoliosDirectory = parent+"portfolios"

if not os.path.exists(parent):
    os.makedirs(parent)

if not os.path.exists(alertsDirectory):
    os.makedirs(alertsDirectory)

if not os.path.exists(ticksDirectory):
    os.makedirs(ticksDirectory)

os.system("hdfs dfs -rm -r streaminginput/ticks/*")
os.system("hdfs dfs -rm -r streaminginput/alerts/*")
os.system("hdfs dfs -rm -r streaminginput/portfolios/*")

os.system("hdfs dfs -copyToLocal data/latestTicks/part-00000 " + ticksDirectory)

lines = [line.rstrip('\n') for line in open('initialAlerts.txt')]

alertArgs = ""
for line in lines:
	alertArgs = alertArgs + " " + " ".join(["'" + word + "'" for word in line.split(",")])

os.system("python createStreamingInputs.py -a" + alertArgs)

os.system("hdfs dfs -put "+ticksDirectory+"/part-00000 streaminginput/ticks/initialTicks")

lines = [line.rstrip('\n') for line in open('initialPortfolios.txt')]
for line in lines:
	portfolioArgs = " ".join(["'" + word + "'" for word in line.split(",")])
	os.system("python createStreamingInputs.py -p " + portfolioArgs)
	print portfolioArgs
os.system("rm -r .streamingtemp")

time.sleep(2)

os.system("python tickSimulation.py")
