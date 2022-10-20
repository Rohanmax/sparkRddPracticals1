import org.apache.spark.SparkContext
import org.apache.log4j.Level		
import org.apache.log4j.Logger


object logLevelGrouping extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val baseRdd = sc.textFile("D:/weekten/bigLog.txt")
/*
  val mappedRdd = baseRdd.map(x => {
      val fields = x.split(":")
      (fields(0),fields(1))
      })
  mappedRdd.groupByKey.collect().foreach(x => println(x._1, x._2.size))
*/
 
    val mappedRdd = baseRdd.map(x => {
      val fields = x.split(":")
      (fields(0),1)
      })
      
  mappedRdd.reduceByKey(_+_).collect.foreach(println)

  scala.io.StdIn.readLine()

}