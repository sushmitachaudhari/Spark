package shortestpath

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.PriorityQueue

/**
  * @author Sushmita Chaudhari
  */
object ShortestPathRDD {
  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\npageRank.PageRank <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Page Rank")
      .getOrCreate()

    val spark2 = sparkSession
    import spark2.implicits._

    var edgeFile = sc.textFile(args(0))

    /* Creation of adjacency list */

    /* FromList from the edges file */
    var fromList = edgeFile.map(line => {
      new Tuple2(line.split(",")(0), ("F" + line))
    })

    fromList.foreach(println)

    val list = fromList.map {
      line => (line._1, line._2.charAt(3))
    }

    val list1 = list.groupByKey()
    list1.foreach(println)


    /* ToList from edges file */
    val toList = edgeFile.map(line => {
      new Tuple2(line.split(",")(1), ("to" + line))
    })

    var joinedlist = fromList ++ toList
    var groupedList = joinedlist.groupByKey()

    groupedList


    def getAdjListPairData(str: String) {

      var fromList = edgeFile.map(line => {
        new Tuple2(line.split(",")(0), ("F" + line))
      })

      fromList.foreach(println)

      val list = fromList.map {
        line => (line._1, line._2.charAt(3))
      }

      val list1 = list.groupByKey()
      list1.foreach(println)


      /* ToList from edges file */
      val toList = edgeFile.map(line => {
        new Tuple2(line.split(",")(1), ("to" + line))
      })

      var joinedlist = fromList ++ toList
      var groupedList = joinedlist.groupByKey()

      groupedList

    }


    val graph = edgeFile.map(line => getAdjListPairData(line))
    graph.foreach(println)

    // Tell Spark to try and keep this pair RDD around in memory for efficient re-use
    graph.persist()

    val infinity = Long.MaxValue

    //check if the
    def hasSourceFlag(value: AnyVal) {
      value match {
        case 0 => return true
        case 1 => return false
        }


    }

    // Create the initial distances. mapValues ensures that the same Partitioner is used as for the graph RDD.
    val distances = graph.collect().toList
    distances.foreach(println)


    //distances.mapValues( (id, adjList) => hasSourceFlag(adjList) match {
     // case true => 0
      //case _ => infinity })



  }

}
