import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{PairDStreamFunctions, DStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.reflect.ClassTag


object SparkStreamsAsWell {


  implicit def toPairDStreamFunctions[K, V](stream: DStream[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) = {
    new PairDStreamFunctions[K, V](stream)
  }

  def main(args: Array[String]){

    val conf = new SparkConf().setAppName("SparkStreamsAsWell")

    val ssc = new StreamingContext(conf,Seconds(60))

    val tweets = TwitterUtils.createStream(ssc,None)

    val status = tweets.map(_.getText)

    status.saveAsTextFiles("/user/varunnid/spark/twitter/status/tweets")

    val words = tweets.map(_.getText).flatMap(_.split(" "))

    val hashTags = words.filter(_.startsWith("#"))

    val counts = hashTags.map( s =>  (s,1)).reduceByKeyAndWindow(_+_, _+_ , Seconds(180), Seconds(180))

    counts.saveAsTextFiles("/user/varunnid/spark/twitter/counts")

    ssc.checkpoint("/user/varunnid/spark/twitter/checkpoint")

    ssc.start()

    ssc.awaitTermination(1000* 60 * 12)
  }

}
