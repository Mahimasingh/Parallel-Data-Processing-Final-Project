package ccs.neu.edu.pagerank
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
object danglingNodes {
  def main(args : Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
      if(args.length < 1){
        System.err.println("Usage: pageRank <file> <iterations>")
        System.exit(1)
    }
  
  val sparkConfig = new SparkConf().setAppName("Twitter Page Rank")
  val sparkContext = new SparkContext(sparkConfig)
  val graph = sparkContext.textFile(args(0),1)
  val rightEdges = graph.flatMap( s => 
             s.split("\\s+")(1)
              )
  val leftEdges = graph.flatMap( s => 
             s.split("\\s+")(0)
              )
 // val edgesVar = edges.map(record => (record,1))
 // val danglingEdges = edgesVar.reduceByKey(_+_).filter(_._2 == 1).distinct()
  val danglingEdges = rightEdges.subtract(leftEdges)
  println("Number of dangling boys are" + danglingEdges.count())
  
}
}