import os

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

os.system("hdfs dfs -put "+ticksDirectory+"/part-00000 streaminginput/ticks/initialTicks")
os.system("hdfs dfs -put initialAlerts.txt streaminginput/alerts/initialAlerts")

os.system("rm -r .streamingtemp")
