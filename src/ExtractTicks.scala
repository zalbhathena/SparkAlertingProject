package pl.japila.spark

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.MutableList
import scala.xml._


object ExtractTicks {
	def runRetreival(sc:SparkContext, startDateString:String, endDateString:String) = {
		val preTickers = sc.textFile("data/tickersToCompanyNames/*").map(x => List(x.split('|')(0)))
		val preTickers2 = preTickers.filter(x => x(0).forall(_.isLetter))

		val tickers = preTickers2.zipWithIndex().map(x => (x._2/8, x._1)).reduceByKey((x,y)=> x:::y).map(x => x._2)



		def pullHistoricalData(x:List[String]): List[String] = {
			var data = List[String]()
			/*
			query of format:
			select * from yahoo.finance.historicaldata where symbol="YHOO" and startDate = "2016-01-01" and endDate = "2016-01-05"
			*/
			var symbolsString = "symbol%20%3D%20%22" + x(0) + "%22"
			val part1 = "https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.historicaldata%20where%20("
			val part2 = ")%20and%20startDate%3D%22"+startDateString+"%22%20and%20endDate%3D%22"+endDateString+"%22&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys"

			var start = true
			for(ticker <- x)
			{
				if(start)
				{
					start = false
				}
				else
				{
					symbolsString = symbolsString + "%20or%20symbol%20%3D%20%22" + ticker + "%22"
				}
			}

			val url = part1 + symbolsString + part2
			var result = ""
			try {
				result = scala.io.Source.fromURL(url).mkString
			} catch {
				case _: Throwable => {
					result = ""
					var failureString = "INVALID"
					for(y <- x)
					{
						failureString = failureString + "," + y
					}
					data = List[String](failureString)
					return data
				}
			}
			data = data :+ result
			return data
		}


		val listRDD = tickers.flatMap(x => pullHistoricalData(x))
		val badValues = listRDD.filter(x => (x.length <= 100))
		val goodValues = listRDD.filter(x => x.length > 100)

		if(badValues.count() > 0)
		{
			badValues.saveAsTextFile("data/bad_values."+startDateString+"."+endDateString)
		}
		val historicalRaw = goodValues.flatMap(x => XML.loadString(x) \ "results" \ "quote")

		val historicalFormated = historicalRaw.map(x => List[String]((x \ "@Symbol").text, (x \ "Date").text, (x \ "Open").text, (x \ "High").text, (x \ "Low").text, (x \ "Close").text, (x \ "Volume").text, (x \ "Adj_Close").text) mkString "," )
		//second data set
		historicalFormated.saveAsTextFile("data/historical_data."+startDateString+"."+endDateString)
	}
        
	def main(args: Array[String]) {
                val conf = new SparkConf().setAppName("Extract Ticks From Yahoo Finance")
                val sc = new SparkContext(conf)
		runRetreival(sc, "2016-01-01", "2016-06-30")
                for(i <- 1 to 10){
			val dateString = (2015 - i).toString
			val startDate = dateString + "-01-01"
			val midDate = dateString + "-06-30"
			val endDate = dateString + "-12-31"
			runRetreival(sc, midDate, endDate)
			runRetreival(sc, startDate, midDate)
		}
        }
}
