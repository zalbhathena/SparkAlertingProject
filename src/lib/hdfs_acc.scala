package AlertingUtils

import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.rdd.RDD


object HDFSAcc
{
	def getLatestTicks(sc:SparkContext, partitions:Int): RDD[Array[String]] = {
		def formatLines(x:String): (String, (Int, Array[String])) =
		{
                        val splitX = x.split(',')
                        val dateSplit = splitX(1).split('-')
                        val dateValue = dateSplit(0).toInt + dateSplit(1).toInt*100 + dateSplit(2).toInt
                        return (splitX(0), (dateValue, splitX.slice(1, splitX.length)))
                }
                def getLatestTick(a:(Int, Array[String]), b:(Int,Array[String])): (Int,Array[String]) =
                {
                        if(a._1 > b._1)
                                return a
                        return b
                }

                val ticks = sc.textFile("data/historical_data.2016-06-01.2016-07-01",partitions).map(x => formatLines(x))
                val latestTicks = ticks.reduceByKey((a:(Int, Array[String]),b:(Int, Array[String])) => getLatestTick(a,b)).map(x => Array(x._1) ++ x._2._2)
                return latestTicks
        }
}
