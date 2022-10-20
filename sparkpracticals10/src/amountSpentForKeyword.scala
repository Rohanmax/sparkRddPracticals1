import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object amountSpentForKeyword extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","wordcount")

  val initial_rdd = sc.textFile("D:/weekten/bigdatacampaigndata.csv")

  val mappedInput =initial_rdd.map(x=> (x.split(",")(10).toFloat,x.split(",")(0)))
  
  //mappedInput.take(20).foreach(println)
  
   /* 
   (24.06,big data contents)
   (59.94,spark training with lab access)
   (28.45,online hadoop training institutes in hyderabad)
  */

  val words = mappedInput.flatMapValues(x => x.split(" "))
  
  //words.take(20).foreach(println)
  /* 
  (24.06,big)
  (24.06,data)
  (24.06,contents)
  */

  val finalMapped = words.map(x => (x._2.toLowerCase(),x._1))
  
  //finalMapped.take(20).foreach(println)
  
  /* 
  (big,24.06)
  (data,24.06)
  (contents,24.06)
  */

  val total = finalMapped.reduceByKey((x,y) => x+y)

  val sorted = total.sortBy(x =>x._2,false)

  sorted.take(20).foreach(println)

}