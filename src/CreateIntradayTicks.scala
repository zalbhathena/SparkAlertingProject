package pl.japila.spark

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

import scala.util.Random

import AlertingUtils.HDFSAcc._

object GenerateTicks {
	val numExecutors = 20
	val numThreads = 5
	val partitions = numExecutors * numThreads
	
	def getAllTicks(latestTicks:RDD[Array[String]]): RDD[Array[Any]] = {
		val millisecondsInDay = 86400L * 10000L
		val numLatestTicks = latestTicks.count()
		val roundUp = if (millisecondsInDay % numLatestTicks == 0) 0 else 1
		val groupSize = ((millisecondsInDay / numLatestTicks) + roundUp)
		println("group size " + groupSize.toString)
		val EPSILON = .00001
		def tickToDailyTicks(x:Array[String], randomNormals:Random):Array[(Double,Array[Any])] = {
		        val dailyTicks = new Array[(Double,Array[Any])](groupSize.toInt)
		        var price = (x(3).toDouble + x(4).toDouble)/2.0
		        var averageVolumePerTick = x(6).toInt / groupSize
		        price = x(3).toDouble
			val priceX = 100
			val volX = 200
		        for( i <- 0 to groupSize.toInt-1)
		        {       
		                val vol = math.sqrt(price / 10.0) 
		                val priceDelta = vol * randomNormals.nextGaussian
		                if(price + priceDelta <= EPSILON)
		                {       
		                        price = math.abs(priceDelta)
		                }       
		                else    
		                {
					price = price + priceDelta
		                }       
		                val volumeDelta = (averageVolumePerTick/4.0).toDouble * randomNormals.nextGaussian
		                val volume = if(averageVolumePerTick + volumeDelta > 1) averageVolumePerTick + volumeDelta else 1
		        
		                val sortValue = randomNormals.nextDouble + i.toDouble

		                dailyTicks(i) = (sortValue, Array(x(0), price, math.ceil(volume).toInt))
		        }       
		        return dailyTicks
		}
		val seed = 91234 
		val allTicks = latestTicks.mapPartitionsWithIndex { (indx, iter) =>
		        val randomNormals = new Random(indx + seed) 
		        val rand = randomNormals.nextGaussian
		        iter.map(x => tickToDailyTicks(x, randomNormals))
		}


		val formattedTicks = allTicks.flatMap(x => x).sortByKey().zipWithIndex().map(x => x._2 +: x._1._2)
		return formattedTicks
	}
	
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Generate ticks from latest data").set("spark.executor.memory", "10g")
		conf.set("spark.executor.cores", numThreads.toString).set("spark.executor.instances", numExecutors.toString)
		val sc = new SparkContext(conf)
		val latestTicks = getLatestTicks(sc, partitions)
		//hard coded! we set num executors on the command line to be 20
            	val newTicks = getAllTicks(latestTicks)
		newTicks.map(x => x.mkString(",")).saveAsTextFile("data/allTicks")
    	}
}
