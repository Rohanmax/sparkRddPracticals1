import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object eleminatingBoringwords extends App {
  
def loadBoringWords():Set[String]={
var boringWords:Set[String]=Set()
val lines = Source.fromFile("D:/weekten/boringwords.txt").getLines()
for (line <-lines) {
boringWords += line
}
boringWords
}

Logger.getLogger("org").setLevel(Level.ERROR)

val sc = new SparkContext("local[*]","wordcount")

var nameSet =sc.broadcast(loadBoringWords)

val initial_rdd = sc.textFile("D:/weekten/bigdatacampaigndata.csv")

val mappedInput =initial_rdd.map(x=> (x.split(",")(10).toFloat,x.split(",")(0)))

val words = mappedInput.flatMapValues(x => x.split(" "))

val finalMapped = words.map(x => (x._2.toLowerCase(),x._1))

val filteredRdd = finalMapped.filter(x => !nameSet.value(x._1))

val total = filteredRdd .reduceByKey((x,y) => x+y)

val sorted = total.sortBy(x =>x._2,false)

sorted.take(20).foreach(println)

}

