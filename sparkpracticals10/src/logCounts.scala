
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object logCounts extends App {
Logger.getLogger("org").setLevel(Level.ERROR)
val sc = new SparkContext("local[*]","wordcount")


val myList =List("WARN:Tuesday 4 September 0405",
"ERROR:Tuesday 4 September 0408",
"ERROR:Tuesday 4 September 0408",
"ERROR:Tuesday 4 September 0408",
"ERROR:Tuesday 4 September 0408",
"ERROR:Tuesday 4 September 0408")

val originalLogsRdd = sc.parallelize(myList)

val newPairRdd = originalLogsRdd.map(x => {
val columns = x.split(":")
val logLevel = columns(0)
(logLevel,1)
})

val resultantRdd = newPairRdd.reduceByKey((x,y) =>x+y)

resultantRdd .collect().foreach(println)
}