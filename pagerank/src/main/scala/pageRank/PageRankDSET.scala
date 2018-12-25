package pageRank

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.functions.desc

/**
  * @author Sushmita Chaudhari
  */
object PageRankDSET {

  /* Given k, create RDDs Graph and Ranks for the k-by-k graph data as described above.
   You do not need to load the data from files but can directly generate it in your program.
   Make sure each RDD has at least two partitions. As an optional challenge, try to enforce that Graph and Ranks have the same Partitioner.*/

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\npageRank.PageRankDSET <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank DSET").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Page Rank DSET")
      .getOrCreate()

    val spark2 = sparkSession
    import spark2.implicits._

    conf.set("spark.sql.crossJoin.enabled", "true")

    /* constructing the graph DF */
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
    var graphRDD = sc.parallelize(inputGraph, 5)
    val graphDF = graphRDD.toDF("v1","v2")


    /* constructing the ranks DF */
    var rankGraph: List[(Int, Double)] = List()
    for (m <- 1 to k * k) {
      rankGraph = rankGraph :+ (m, (1.toDouble / (k * k)))
    }
    // add dummy vertex and pr
    rankGraph = rankGraph :+ (0, 0.toDouble)
    var rankRDD = sc.parallelize(rankGraph, 5)
    var rankDF = rankRDD.toDF("v1","pr")


    //Algorithm for PageRank
    val iters = 10

    /* Step a. Join Graph with Ranks on v1 to create a new RDD of triples (v1, v2, pr). */
    for (i <- 1 to iters) {
      val joinedDF = graphDF.join(rankDF, Seq("v1"))

      val ds1 = joinedDF.flatMap{
        case Row(x1:Int, x2:Int, x3:Double) =>
          if(x1 % k == 1)
            List((x1,0.0),(x2,x3))
          else
            List((x2,x3))
      }.toDF("v2", "pr")

      /* assigned the step b output to temp */
      val temp = ds1

      /* Step c: Group Temp by v2 and sump up the pr values in each group */
      val temp2 = temp.groupBy("v2").sum("pr")

      /* Step d : In Temp2, look up the pr value associated with v2=0 (the dummy vertex). Let us call this
      value delta. */
      val dummy = temp2.filter($"v2" === 0)
      val delta = dummy.head().get(1)

      /* Step e. Add delta/k2 to the pr value of each record in Temp2, except for v2=0. Make sure that
              this final result RDD “overwrites” Ranks, so that the next iteration works with it, instead
              of the previous Ranks table. */
      val temp3 = temp2.map{
        case Row (x1:Int, x2:Double) =>
          if(x1 == 0)
          {
            (x1, 0.0)
          }
          else
          {
            (x1, x2 + delta.toString.toDouble/(k*k))
          }
      }
      rankDF = temp3.toDF("v1", "pr")

      //Step f : Sum up all pr values in Ranks and check if that sum is close to 1.0. (It may not be exactly
      //1.0 due to numerical issues.)
      val sumline = temp3.toDF("v1", "pr").groupBy().sum()
      val sum = sumline.drop("sum(_1)")
      println(sum.show()+ " sum and i "+i )
      println(joinedDF.rdd.toDebugString)

    }

    //3. Collect the top-100 (by pr value) tuples from the final Ranks RDD as a list at the driver.
    var top100 = rankDF.toDF("v1", "pr").sort(desc("pr")).take(100)
    top100.foreach(println)
  }

}

