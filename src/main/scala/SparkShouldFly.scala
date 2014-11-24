


import java.lang.Exception
import java.util.Random
  import org.apache.spark.{Logging, SparkConf, SparkContext}
  import org.apache.spark.SparkContext._


  /**
   * Usage: GroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]
   */
  object SparkShouldFly extends  Logging{
    def main(args: Array[String]) {
      val sparkConf = new SparkConf().setAppName("GroupBy Test")
//      var numMappers = if (args.length > 0) args(0).toInt else 2
//      var numKVPairs = if (args.length > 1) args(1).toInt else 1000
//      var valSize = if (args.length > 2) args(2).toInt else 1000
//      var numReducers = if (args.length > 3) args(3).toInt else numMappers
//
//      val sc = new SparkContext(sparkConf)
//
//      val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
//        val ranGen = new Random
//        var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
//        for (i <- 0 until numKVPairs) {
//          val byteArr = new Array[Byte](valSize)
//          ranGen.nextBytes(byteArr)
//          arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
//        }
//        arr1
//      }.cache
//      // Enforce that everything has been calculated and in cache
//      pairs1.count
//
//      println(pairs1.groupByKey(numReducers).count)
//
//      sc.stop()

      val sc = new SparkContext(sparkConf)

//      val rdd1 = sc.textFile("/user/varunnidhi/spark/log.txt").map(s => {System.out.println(s);s.span(_ != ':')}).filter({ case (x,y) => System.out.println(y);y.startsWith(":Error")}).map(p => (p._1,1)).reduceByKey(_ + _).filter(_._2 > 5)
//
//      val rdd2 = sc.textFile("/user/varunnidhi/spark/soucres.txt").map(_.span(_ == ':'))
//
//      rdd1 join rdd2 saveAsTextFile "/user/varunnidhi/spark/join.txt"


      sc.textFile("/user/varunnidhi/spark/log.txt").foreach( s => logInfo("cdsdcdscs:cdscdscdscds"))

    }
  }

