package pageRank

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Sushmita Chaudhari
  */
object PageRankRDD {

  /* Given k, create RDDs Graph and Ranks for the k-by-k graph data as described above.
   You do not need to load the data from files but can directly generate it in your program.
   Make sure each RDD has at least two partitions. As an optional challenge, try to enforce that Graph and Ranks have the same Partitioner.*/

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\npageRank.PageRank <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Page Rank")
      .getOrCreate()

    val spark2 = sparkSession
    import spark2.implicits._

    /* constructing the graph RDD */
    var inputGraph: List[(Int, Int)] = List()
    val k = 100
    var counter = 1
    for (i <- 1 to k) {
      for (j <- counter to (counter + k - 1)) {
        if ((j % k) == 0) {
          inputGraph = inputGraph :+ ((j, 0))
        }
        else {
          inputGraph = inputGraph :+ ((j, j + 1))
        }
      }
      counter += k
    }
    //adding the dummy vertex for the dangling pages
    inputGraph = inputGraph :+ ((0, 0))
    val graphRDD = sc.parallelize(inputGraph, 5)



    /* constructing the rank  RDD*/
    var rankGraph: List[(Int, Double)] = List()
    for (m <- 1 to k * k) {
      rankGraph = rankGraph :+ (m, (1.toDouble / (k * k)))
    }
    // add dummy vertex and pr
    rankGraph = rankGraph :+ (0, 0.toDouble)
    var rankRDD = sc.parallelize(rankGraph, 5)

    //Algorithm for PageRank
    val iters = 10

    /* Step a. Join Graph with Ranks on v1 to create a new RDD of triples (v1, v2, pr). */
    for (i <- 1 to iters) {
      val joinedRDD = graphRDD.join(rankRDD).flatMap
      {
        /* step b: mapped (v1, v2, pr) to (v2, pr) */
        pair =>
          if(pair._1 % k == 1)
            List((pair._1, 0.0), pair._2)
          else
            List(pair._2)
      }

      /* assigned the step b output to temp */
      val temp = joinedRDD

      /* Step c: Group Temp by v2 and sump up the pr values in each group */
      val temp2 = temp.reduceByKey((x, y) => x + y)

      /* Step d : In Temp2, look up the pr value associated with v2=0 (the dummy vertex). Let us call this
      value delta. */

      //dummy vertices
      val dummyVertices = temp2.filter(k => k._1.equals(0))
      val delta = dummyVertices.collect().toList.head._2

      /* Step e. Add delta/k2 to the pr value of each record in Temp2, except for v2=0. Make sure that
              this final result RDD “overwrites” Ranks, so that the next iteration works with it, instead
              of the previous Ranks table. */

      val temp3 = temp2.map{
        x =>
          if(x._1 == 0)
            {
              (x._1, 0.0)
            }
          else
            {
              (x._1, x._2 + delta/(k*k))
            }
      }
      rankRDD = temp3

      //Step f : Sum up all pr values in Ranks and check if that sum is close to 1.0. (It may not be exactly
      //1.0 due to numerical issues.)
      val sum = rankRDD.values.fold(0){
        (a,b) => a+b
      }
      logger.info(sum+ " sum "+i )
      println(sum)

      joinedRDD.toDebugString

      logger.info(joinedRDD.toDebugString)

    }
    //3. Collect the top-100 (by pr value) tuples from the final Ranks RDD as a list at the driver.
    val top100= rankRDD.sortBy(x => x._2).take(100).toList
    top100.foreach(println)
  }

}
