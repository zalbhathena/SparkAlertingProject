package pl.japila.spark

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
object HdfsWordCount {
        val numExecutors = 20
        val numThreads = 5
        val partitions = numExecutors * numThreads	

	//Tickers of format (time, (time, ticker, price, volume))
	def updateStateForTickers(values: Seq[(Long,String,Double,Long)], state: Option[Any]): Option[Any] = {
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
	//Alerts of format (ticker, (email, ticker, active, delay, last_triggered, operator, left_operand, right_operand))
	def updateStateForAlerts(values: Seq[(String, String, Boolean, Long, Long, String, String, Double)], state: Option[Seq[Any]]): Option[Seq[Any]] = {
		if(values.length == 0)
			return state
		return Option(state.getOrElse(Seq()) ++ values)
	}

	def formatAlert(x:Seq[String]):(String, (String, String, Boolean, Long, Long, String, String, Double)) = {
		return (x(1).toString, (x(0).toString, x(1).toString, false, x(2).toLong, -1 * x(2).toLong, x(3).toString, x(4).toString, x(5).toDouble))
	} 

	def main(args: Array[String]) {
		if (args.length < 1) {
			System.err.println("Usage: HdfsWordCount <directory>")
			System.exit(1)
		}
		val sparkConf = new SparkConf().setAppName("HdfsWordCount")
		sparkConf.set("spark.executor.memory", "10g").set("spark.executor.cores", numThreads.toString).set("spark.executor.instances", numExecutors.toString)


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

		ssc.start()
    		ssc.awaitTermination()
	}
}
