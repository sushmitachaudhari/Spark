package joinsDSET

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author Sushmita Chaudhari
  */
object RSJoin {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\njoinsDSET.RSJoin <input dir> <output dir>")
      System.exit(1)

    }

    val conf = new SparkConf().setAppName("Twitter RS Join")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder().master("local")
      .appName("Twitter RS Join").getOrCreate()

    val spark2 = sparkSession
    import spark2.implicits._

    val edgesFile = sc.textFile(args(0))
    val dataSet = sparkSession.createDataset(edgesFile)

    dataSet.createOrReplaceTempView("users")

    val results = spark2.sql("SELECT * from users")

    println("results" + results)

    /*val fromList = dataSet.map(line =>
      new Tuple2(line.split(",")(0), ("from" + line)))


    /* ToList from edges file*/
    val toList =dataSet.map(line => {
      new Tuple2(line.split(",")(1), ("to"+line))


      /* combine both from and to list and group by key */
      val combineList = fromList.union(toList)
      val groupList = combineList.groupBy().count().rdd
*/


    /*Based on from or to, make different lsits , same as MR*/
    /*val valuesList = groupList.map(x =>
      {*/
    /*val values = groupList.toList

        val fromList = values.filter(x => {
          x.contains("from")
        })

        val toList = values.filter(y => {
          y.contains("to")
        })*/

    /* Creating length-2 path from the from and to lists */
    //var length2Path = List[String]()

    /*toList.foreach(to => {
          fromList.foreach(from => {

            val users1 = to.split(",")
            val users2 = from.split(",")

            length2Path = users1(0).substring(2) +  "," +users2(1):: length2Path

          })
        })*/

    /*length2Path

      })

      /* converting list of lists in to a single list - to get length 2 paths*/
      val listOfPaths = valuesList.collect().toList.flatten

      val map1 = (listOfPaths zip listOfPaths).toMap

      var counter = 0*/

    /*edgeFile.foreach(edge =>
      {
        val reverseEdge = edge.reverse

        if(map1.contains(reverseEdge))
        {
          acc.add(1)
          println("counter: "+ acc)
        }
      })*/
    /*})
  }*/

  }
}
