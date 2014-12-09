


import org.apache.spark.SparkContext._
import org.apache.spark.{HashPartitioner, Logging, SparkConf, SparkContext}

object SparkShouldFly extends Logging {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Spark should fly")

    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.textFile("/user/varunnid/spark/log.txt").map(_.span(_ != ':')).filter(_._2.startsWith(":Error"))
      .map(p => (p._1, 1)).reduceByKey(new BPartitioner(),_ + _).filter(_._2 > 5)
    val rdd2 = sc.textFile("/user/varunnid/spark/soucres.txt").map(_.span(_ != ':'))

    rdd1.cache();

    rdd1 join (rdd2,new APartitioner()) saveAsTextFile "/user/varunnid/spark/join"

    val rdd3 = sc.textFile("/user/varunnid/spark/sources_new.txt").map(_.span(_ != ':'))

    rdd1 join  (rdd3,new BPartitioner()) saveAsTextFile "/user/varunnid/spark/join_new"

  }
}

class APartitioner extends HashPartitioner(5)

class BPartitioner extends HashPartitioner(3)

