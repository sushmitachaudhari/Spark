package pairRDD

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object TwitterFollowerCountRDDF {

  /**
    * A simple Spark-Scala program to implement follower count of Twitter users.
    * Users with zero followers are not considered.
    */

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\ntc.TwitterFollowerCount <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Follower Count")
    val sparkContext = new SparkContext(conf);

    val edgesFile = sparkContext.textFile(args(0))

    /*  split each line on "," and extract the userID at index 1
     *  map each userId
     *  reduce by summing the counts for each userID*/
    val followerCount = edgesFile.map(line => line.split(",")(1))
      .map(user => (user, 1))
      .foldByKey(0)((x, y)=>(x + y))

    //println(""+ followerCount.collect().toList)

    followerCount.saveAsTextFile(args(1))

  }
}
