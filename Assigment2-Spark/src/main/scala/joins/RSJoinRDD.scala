package joins

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Sushmita Chaudhari
  */
object RSJoinRDD {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\njoins.RSJoinRDD <input dir> <output dir>")
      System.exit(1)

    }

    val MAX = 30000

    val conf = new SparkConf().setAppName("RS Join")
    val sc = new SparkContext(conf)

    var acc = sc.longAccumulator("a")

    val edgeFile = sc.textFile(args(0))

    val filteredEdge = edgeFile.filter( x =>
      {
        (Integer.parseInt(x.split(",")(0)) < MAX ) && (Integer.parseInt(x.split(",")(1)) < MAX)
      })


    /* making from list list from edges file */
    val fromList = filteredEdge.map(line =>
      new Tuple2(line.split(",")(0), ("from" + line)))


    /* ToList from edges file*/
    val toList =filteredEdge.map(line => {
      new Tuple2(line.split(",")(1), ("to"+line))
    })


    /* combine both from and to list and group by key */
    val combineList = fromList ++ toList
    val groupList = combineList.groupByKey()

    /*Based on from or to, make different lsits , same as MR*/
    val valuesList = groupList.map(x =>
      {
        val values = x._2.toList

        val fromList = values.filter(x => {
          x.contains("from")
        })

        val toList = values.filter(y => {
          y.contains("to")
        })

        /* Creating length-2 path from the from and to lists */
        var length2Path = List[String]()

        toList.foreach(to => {
          fromList.foreach(from => {

            val users1 = to.split(",")
            val users2 = from.split(",")

            length2Path = users1(0).substring(2) +  "," +users2(1):: length2Path

          })
        })

        length2Path

      })

      /* converting list of lists in to a single list - to get length 2 paths*/
      val listOfPaths = valuesList.collect().toList.flatten

      val map1 = (listOfPaths zip listOfPaths).toMap

      var counter = 0

    filteredEdge.foreach(edge =>
      {
        val reverseEdge = edge.reverse

        if(map1.contains(reverseEdge))
          {
            acc.add(1)
            println("counter: "+ acc)
          }
      })

      println("map: "+ map1)

      println("count: "+ acc)

      println("path2: "+ listOfPaths)

    logger.info("counter re join" + acc)

  }
}
