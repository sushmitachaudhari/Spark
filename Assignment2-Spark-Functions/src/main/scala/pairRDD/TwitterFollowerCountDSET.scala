package pairRDD
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
/**
  * @author Sushmita Chaudhari
  */
object TwitterFollowerCountDSET {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\ntc.TwitterFollowerCount <input dir> <output dir>")
      System.exit(1)

    }

    val conf = new SparkConf().setAppName("Twitter Follower Count")
    val sc = new SparkContext(conf);

    val sparkSession = SparkSession.builder().master("local")
                        .appName("Twitter Follower Count").getOrCreate()

    val spark2 = sparkSession
    import spark2.implicits._

    val edgesFile = sc.textFile(args(0))

    val dataSet = sparkSession.createDataset(edgesFile)

    /*  split each line on "," and extract the userID at index 1
     *  map each userId
     *  reduce by summing the counts for each userID*/
    val followerCount = dataSet.map(line => line.split(",")(1))
                        .map(user => (user, 1))
                        .groupBy("col1").count().rdd

    followerCount.saveAsTextFile(args(1))

  }
}
