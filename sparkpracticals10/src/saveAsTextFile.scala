import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.log4j.Logger


object saveAsTextFile extends App {

Logger.getLogger("org").setLevel(Level.ERROR)
  
val sc = new SparkContext("local[*]","wordcount")

val input = sc.textFile("D:/weeknine/customerorders.csv")

val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))

val totalByCustomer = mappedInput.reduceByKey((x,y) => x+y)

val premiumCustomers = totalByCustomer.filter(x => x._2 > 5000)

val doubledAmount = premiumCustomers.map(x => (x._1, x._2 * 2))

doubledAmount.collect.foreach(println)

println(doubledAmount.count)

//val sortedTotal = totalByCustomer.sortBy(x => x._2)

//sortedTotal.saveAsTextFile("D:/weekten/sparkoutput")

//val result = sortedTotal.collect

//result.foreach(println)

scala.io.StdIn.readLine()
}