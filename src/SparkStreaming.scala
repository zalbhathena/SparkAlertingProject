package pl.japila.spark
import scala.reflect.ClassTag
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import java.io._
import org.slf4j.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.joda.time.DateTime
import org.joda.time.Seconds
import org.joda.time.Period

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

import AlertingUtils.HDFSAcc._

/**
 * Counts words in new text files created in the given directory
 * Usage: HdfsWordCount <directory>
 *   <directory> is the directory that Spark Streaming will use to find and read new text files.
 *
 * To run this on your local machine on directory `localdir`, run this example
 *    $ bin/run-example \
 *       org.apache.spark.examples.streaming.HdfsWordCount localdir
 *
 * Then create a text file in `localdir` and the words in the file will get counted.
 */

//TODO: use classes instead of tuples and learn how to serialize those classes (time constraints prevented this)
object HdfsWordCount {
	val streamingInterval = 2
        val numExecutors = 20
        val numThreads = 5
        val partitions = numExecutors * numThreads
	type Alert = (Int, String, String, Boolean, Long, DateTime, String, String, Double)	
	type Tick = (Long, String, Double, Long)
	type SecurityPortfolioPair = (String, (String, Long))
	type PortfolioVolumePriceTuple = (String, Long, Double)
	type Portfolio = (String, Seq[(String, Long)])
	type AlertMap = scala.collection.mutable.Map[Int, Alert] 
	//Tickers of format (time, (time, ticker, price, volume))
	def updateStateForTickers(values: Seq[Tick], state: Option[Tick]): Option[Tick] = {
		if(values.length == 0)
			return state
		var earliest = values(0)
		for(item <- values)
		{
			if(item._1 < earliest._1)
				earliest = item
		}
		return Option(earliest)
	}
	//Alerts of format (ticker, (id, email, ticker, active, delay, last_triggered, operator, left_operand, right_operand))
	def updateStateForAlerts(values: Seq[Alert], state: Option[Seq[Alert]]): Option[Seq[Alert]] = {
		if(values.length == 0)
			return state
		return Option(state.getOrElse(Seq()) ++ values)
	}

	def isAlertTriggered(alert:Alert, tick:Tick): Boolean = {
		if(alert._8 == "P" & alert._7 == "<")
		{
			return tick._3 < alert._9
		}	
		else if(alert._8 == "P" & alert._7 == ">")
		{
			return tick._3 > alert._9
		}
		else if(alert._8 == "V" & alert._7 == "<")
		{
			return tick._4 < alert._9
		}
		else if(alert._8 == "V" & alert._7 == ">")
		{
			return tick._4 > alert._9
		}
		return false
	}

	def updateStateForJoinedAlerts(values:Seq[(Option[Seq[Alert]], Option[Tick])] , state: Option[AlertMap]): Option[AlertMap] = {
		if(values.length == 0)
			return state
		var alertMap:AlertMap = state.getOrElse(scala.collection.mutable.Map[Int, Alert]())
		/*	
		val writer = new PrintWriter(new File("/home/znb205/testo.txt" ))
		writer.write(ClassTag(values.getClass).toString+ "\n")
		writer.write(ClassTag(values(0).getClass).toString+ "\n")
		writer.write(ClassTag(castedValues.getClass).toString+"\n")
		writer.write(ClassTag(castedValues(0).getClass).toString+"\n")
		writer.write(castedValues(0).getClass.toString+"\n")
      		writer.write(castedValues.toString+"\n")
      		writer.write(castedValues(0).toString+"\n")
      		writer.write(castedValues(0)._1.toString+"\n")
      		writer.write(values.toString+"\n")
      		writer.write(values(0).toString+"\n")
      		writer.write(values(0)._2.toString+"\n")
		writer.close()
		*/
		val tick:Tick = values(0)._2.asInstanceOf[Option[Tick]].getOrElse((0,"",0.0,0))
		val alerts:Seq[Alert] = values(0)._1.asInstanceOf[Option[Seq[Alert]]].getOrElse(Seq[Alert]())
		for(alert <- alerts) {
			if(!alert._4 | DateTime.now.getMillis() - alert._6.getMillis() > alert._5) {
				val lastAlert = alertMap.get(alert._1).getOrElse(alert)
				val isTriggered = isAlertTriggered(lastAlert, tick)
				val triggerTime = if(isTriggered & lastAlert._4 == false) DateTime.now else lastAlert._6
				val newAlert = (lastAlert._1, lastAlert._2, lastAlert._3, isTriggered, lastAlert._5, triggerTime, lastAlert._7, lastAlert._8, lastAlert._9)
				alertMap += lastAlert._1 -> newAlert
			}	
		}
		return Option(alertMap)
	}

	def updateStateForPortfolios(values:Seq[(Option[(String,Long)], Option[Tick])], state:Option[ArrayBuffer[PortfolioVolumePriceTuple]]): Option[ArrayBuffer[PortfolioVolumePriceTuple]] = {
		if(values.length == 0)
			return state
		
		val pricedSecurities:ArrayBuffer[PortfolioVolumePriceTuple] = state.getOrElse(ArrayBuffer[PortfolioVolumePriceTuple]())
		for(value <- values) {
			val security:(String,Long) = value._1.getOrElse(("",0))
			val tick:Tick = value._2.getOrElse((0,"",0.0,0))
			val newTuple = (security._1, security._2, tick._3)
			pricedSecurities += newTuple
		}
		return Option(pricedSecurities)
	}


	def updateStateForPortfolioPrices(values:Seq[(Option[ArrayBuffer[PortfolioVolumePriceTuple]], Option[Tick])], state:Option[ArrayBuffer[PortfolioVolumePriceTuple]]): Option[ArrayBuffer[PortfolioVolumePriceTuple]] = {
		if(values.length == 0 || values(0)._1.isEmpty)
			return state
		val valuesPricedSecurities:ArrayBuffer[PortfolioVolumePriceTuple] = values(0)._1.getOrElse(ArrayBuffer[PortfolioVolumePriceTuple]())
		var pricedSecurities:ArrayBuffer[PortfolioVolumePriceTuple] = state.getOrElse(valuesPricedSecurities)
		val tick:Tick = values(0)._2.getOrElse((0,"",0.0,0))
		pricedSecurities = pricedSecurities.map(x => (x._1, x._2, tick._3))
		return Option(pricedSecurities)
	}

	def formatAlert(x:Seq[String]):(String, Alert) = {
		return (x(2).toString, (x(0).toInt, x(1).toString, x(2).toString, false, x(3).toLong, new DateTime(1,1,1,1,1), x(4).toString, x(5).toString, x(6).toDouble))
	} 

	def formatPortfolio(x:Array[String]): Seq[SecurityPortfolioPair] = {
		val ticker = x(0)
		val securities = x.drop(1).map(y => (y.split(":")(0),(ticker, y.split(":")(1).toLong)))
		return securities
	}

	def main(args: Array[String]) {
		if (args.length < 1) {
			System.err.println("Usage: HdfsWordCount <directory>")
			System.exit(1)
		}
		val sparkConf = new SparkConf().setAppName("HdfsWordCount")
		sparkConf.set("spark.executor.memory", "10g").set("spark.executor.cores", numThreads.toString).set("spark.executor.instances", numExecutors.toString)


		val ssc = new StreamingContext(sparkConf, org.apache.spark.streaming.Seconds(streamingInterval))
		ssc.checkpoint("checkpoints")	
		// Create the FileInputDStream on the directory and use the
    		// stream to count words in new files created
    		val rawTicks = ssc.textFileStream(args(0)).map(_.split(","))
    		val formattedTicks = rawTicks.map(x => (x(1), (x(0).toLong, x(1).toString, x(2).toDouble, x(3).toLong)) )
   		formattedTicks.print()
		val tickerState = formattedTicks.updateStateByKey(updateStateForTickers _)
		val latestTicks = formattedTicks.fullOuterJoin(tickerState).filter(x => x._2._1 != None & x._2._2 != None).map(x => (x._1, x._2._2.getOrElse[Tick]( (0,"",0.0,0) )))
		latestTicks.print()

		val rawAlerts = ssc.textFileStream(args(1)).map(_.split(","))
		val formattedAlerts = rawAlerts.map(x => formatAlert(x))
		formattedAlerts.print()
		val alertState = formattedAlerts.updateStateByKey(updateStateForAlerts _)
		//alertState.print()
	
		val rawPortfolios = ssc.textFileStream(args(2)).map(_.split(","))
		val formattedPortfolios = rawPortfolios.flatMap(x => formatPortfolio(x))
		formattedPortfolios.print()		
		val portfolioState =formattedPortfolios.fullOuterJoin(tickerState).filter(x => x._2._1 != None & x._2._2 !=None).updateStateByKey(updateStateForPortfolios _)

		portfolioState.print()

		val portfolioTickers = tickerState.fullOuterJoin(formattedPortfolios).filter(x => x._2._1 != None & x._2._2 !=None).map(x => (x._1, x._2._1.getOrElse[Tick]((0,"",0.0,0))))
		val portfolioPrices = portfolioState.fullOuterJoin(formattedTicks.union(portfolioTickers)).filter(x => x._2._1 != None & x._2._2 !=None).updateStateByKey(updateStateForPortfolioPrices _)
		val latestPortfolioTicks = portfolioPrices.flatMap(x => x._2.map(y => (y._1, (x._1, y._2, y._3)))).groupByKey().map(x => (x._1, (0L,x._1, x._2.map(y => y._2*y._3).sum, 1L)))

		val rawJoinedAlerts = alertState.fullOuterJoin(latestTicks.union(latestPortfolioTicks)).filter(x => x._2._1 != None & x._2._2 != None)
		val allAlertsState = rawJoinedAlerts.updateStateByKey(updateStateForJoinedAlerts _)
		allAlertsState.print()
		def getAlertInfo(x:(Any, AlertMap)): AlertMap = {
			val t = DateTime.now.getMillis()
			val alertMap = x._2.filter(y => y._2._4 & t - y._2._6.getMillis() < streamingInterval*1000)
			return alertMap
		}
		val alertsTriggered = allAlertsState.flatMap(getAlertInfo).map(x => x._2)
		alertsTriggered.repartition(1).saveAsTextFiles("emails/email")
		alertsTriggered.print()
		ssc.start()
    		ssc.awaitTermination()
	}
}
