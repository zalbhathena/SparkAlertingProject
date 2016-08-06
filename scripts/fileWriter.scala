val tickers = sc.textFile("data/allTicks/part-00000").map(x=>x.split(","))
for(i <- 0 to 100) 
{
	Thread.sleep(1000)
	tickers.filter(x => x(0).toLong <= (i+1)*10 & x(0).toLong > i*10).map(x=>x.mkString(",")).saveAsTextFile("data/streamingExample/tickers_"+i.toString)
}
