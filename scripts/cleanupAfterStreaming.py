import os

os.system("hdfs dfs -rm -r tempstreaming/*")
os.system("hdfs dfs -rm -r streaminginput/ticks/*")
os.system("hdfs dfs -rm -r streaminginput/alerts/*")
os.system("hdfs dfs -rm -r streaminginput/portfolios/*")
