package joins

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Spark program for Rep Join
  *
  * @author Sushmita Chaudhari
  */
object RepJoin {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\njoins.RepJoin <input dir> <output dir>")
      System.exit(1)

    }

    val MAX = 30000

    val conf = new SparkConf().setAppName("RS Join")
    val sc = new SparkContext(conf)

    /* Setting an accumulator for count */
    var acc = sc.longAccumulator("a")

    val edgeFile = sc.textFile(args(0))


    val filteredEdge = edgeFile.filter( x =>
    {
      //x.split(",")
      (Integer.parseInt(x.split(",")(0)) < MAX ) && (Integer.parseInt(x.split(",")(1)) < MAX)
    })

    //edgeFile.filter()

    /* Creating a Hashmap for user, and the users it follows */
    val map1 = new mutable.HashMap[String, mutable.ListBuffer[String]]()
    var followees = ListBuffer[String]()

    /* make a hashmap of string and list of string */
    filteredEdge.collect().toList.foreach(edge =>
    {
      val users = edge.split(",")
      if(map1.contains(users(0)))
        {
          val followees = map1(users(0))
          followees += users(1)
          map1.put(users(0), followees)
        }
      else
        {
          val followees2 = new ListBuffer[String]
          followees2 += users(1)
          map1.put(users(0), followees2)
        }
    })

    /* broadcast the map */
    val map2 = sc.broadcast(map1)

    /* for each edge, get users that each suer follows*/
    filteredEdge.collect().toList.foreach(edge =>
    {
      val users = edge.split(",")

      val getUsers = map2.value.get(users(1))

      /* for each user in the list, get the user/s it follows*/
      getUsers.get.foreach(user =>
      {

        val getUser1 = map2.value.get(user)

        /* if the list has user(0) from above, then we close the triangle */
        getUser1.get.foreach(u =>
        {
          if(u.equals(users(0)))
            {
              acc.add(1)
            }
        })
      })


    })

    logger.info("counter re join" + acc + "  " )

    println("edgeFile: "+ filteredEdge.collect().toList)
    println("acc: " + acc)

    println("map2: "+ map2)

    println("mapp"+map1)

  }

}
