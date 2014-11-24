


import org.apache.spark.SparkContext._
import org.apache.spark.{Logging, SparkConf, SparkContext}


object SparkShouldFly extends Logging {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Spark should fly")

    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.textFile("/user/varunnidhi/spark/log.txt").map(_.span(_ != ':')).filter(_._2.startsWith(":Error")).map(p => (p._1, 1)).reduceByKey(_ + _).filter(_._2 > 5)
    val rdd2 = sc.textFile("/user/varunnidhi/spark/soucres.txt").map(_.span(_ != ':'))

    rdd1 join rdd2 saveAsTextFile "/user/varunnidhi/spark/join"

  }
}

