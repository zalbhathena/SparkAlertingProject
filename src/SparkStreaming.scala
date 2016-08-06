package pl.japila.spark
import scala.reflect.ClassTag
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import java.io._
import org.slf4j.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.rdd.RDD

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

        val numExecutors = 20
        val numThreads = 5
        val partitions = numExecutors * numThreads

	type Alert = (Int, String, String, Boolean, Long, Long, String, String, Double)	
	type Tick = (Long, String, Double, Long)
	type AlertMap = scala.collection.mutable.Map[Int, Alert] 
	//Tickers of format (time, (time, ticker, price, volume))
	def updateStateForTickers(values: Seq[Tick], state: Option[Any]): Option[Any] = {
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
	def updateStateForAlerts(values: Seq[(Alert)], state: Option[Seq[Any]]): Option[Seq[Any]] = {
		if(values.length == 0)
			return state
		return Option(state.getOrElse(Seq()) ++ values)
	}


	def updateStateForJoinedAlerts(values: Seq[(Equals, Any)], state: Option[AlertMap]): Option[AlertMap] = {
		val castedValues = values.asInstanceOf[Seq[(String, (List[Alert], Tick))]]
		if(values.length == 0)
			return state
		var alertMap:AlertMap = state.getOrElse(scala.collection.mutable.Map[Int, Alert]())
		
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
		
		//val tickers = castedValues(0)._2._2
		val alerts = castedValues(0)._2._1
		for(alert <- alerts) {
			val lastAlert = alertMap.get(alert._1).getOrElse(alert)
			val newAlert = (lastAlert._1, lastAlert._2, lastAlert._3, true, lastAlert._5, lastAlert._6, lastAlert._7, lastAlert._8, lastAlert._9)
			alertMap += lastAlert._1 -> newAlert
		}
		return Option(alertMap)
	}

	def formatAlert(x:Seq[String]):(String, Alert) = {
		return (x(2).toString, (x(0).toInt, x(1).toString, x(2).toString, false, x(3).toLong, -1 * x(3).toLong, x(4).toString, x(5).toString, x(6).toDouble))
	} 

	def main(args: Array[String]) {
		if (args.length < 1) {
			System.err.println("Usage: HdfsWordCount <directory>")
			System.exit(1)
		}
		val sparkConf = new SparkConf().setAppName("HdfsWordCount")
		sparkConf.set("spark.executor.memory", "10g").set("spark.executor.cores", numThreads.toString).set("spark.executor.instances", numExecutors.toString)


		/*val log = LogManager.getRootLogger
    		log.setLevel(Level.WARN)
		log.warn("I am done")
		*/
		val ssc = new StreamingContext(sparkConf, Seconds(2))
		ssc.checkpoint("checkpoints")	
		// Create the FileInputDStream on the directory and use the
    		// stream to count words in new files created
    		val rawTicks = ssc.textFileStream(args(0)).map(_.split(","))
    		val formattedTicks = rawTicks.map(x => (x(1), (x(0).toLong, x(1).toString, x(2).toDouble, x(3).toLong)) )
   		formattedTicks.print()
		val tickerState = formattedTicks.updateStateByKey(updateStateForTickers _)
		tickerState.print()

		val rawAlerts = ssc.textFileStream(args(1)).map(_.split(","))
		val formattedAlerts = rawAlerts.map(x => formatAlert(x))
		formattedAlerts.print()
		val alertState = formattedAlerts.updateStateByKey(updateStateForAlerts _)
		alertState.print()
				
		val rawJoinedAlerts = alertState.fullOuterJoin(tickerState).filter(x => x._2._1 != None & x._2._2 != None)
		val formattedJoinedAlerts = rawJoinedAlerts.map(x => (x._1, (x._2._1.getOrElse(None), x._2._2.getOrElse(None) ) ) )
		rawJoinedAlerts.print()
		val alertsTriggered = formattedJoinedAlerts.updateStateByKey(updateStateForJoinedAlerts _)
		alertsTriggered.print()
		ssc.start()
    		ssc.awaitTermination()
	}
}
