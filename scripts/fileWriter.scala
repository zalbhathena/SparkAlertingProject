val tickers = sc.textFile("data/nasdaqlisted.txt").zipWithIndex()
for(i <- 0 to 100) 
{
	Thread.sleep(1000)
	tickers.filter(x => x._2 <= i).saveAsTextFile("data/streamingExample/tickers_"+i.toString)
}
