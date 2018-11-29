package ccs.neu.edu.hw3

/**
 * @author ${user.name}
 */
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
// import classes required for using GraphX
import org.apache.spark.graphx._
object pageRankRDD {
  
  class Edges (src:Int,dest:Int){
    var x: Int = src
    var y : Int = dest
  }
  
  class PageRank(node:Int,pr:Double){
    var v : Int = node
    var pageRank : Double = pr
  }
  
  def main(args : Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("Creating graph")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0)) 
    var eRDD = textFile.map(line=> line.split(","))
    
      
    var eRDD = sc.parallelize(edgesArray.map(edges => (edges.x, edges.y)), 2)
    var prRDD = sc.parallelize(pageRanks.map(pageRank => (pageRank.v,pageRank.pageRank)), 2)
    
    for(i <- 1 to 10) {
      val tempRDD = eRDD.join(prRDD).flatMap(joinPair => if(joinPair._1 % k == 1) List((joinPair._1,0.toDouble),joinPair._2) 
                                                         else List(joinPair._2)) 
      //println("----------------------" + i + "---------------------------------------------------")
      //println(tempRDD.toDebugString)
      //println("-----------------------" + i + "--------------------------------------------------")
      val temp2RDD = tempRDD.reduceByKey(_ + _)
      val delta = temp2RDD.lookup(0)(0)
      prRDD = temp2RDD.map(vertex => if(vertex._1 !=0) (vertex._1, (vertex._2 + delta / (k*k).toDouble)) else (vertex._1,vertex._2))
      val sum = prRDD.map(_._2).sum() - prRDD.lookup(0)(0)
      println("The sum of all Page Ranks at" +i+ "iteration"+ sum)
      //println(prRDD.toDebugString)
    } 
    prRDD.map(item => item.swap) // interchanges position of entries in each tuple
                 .sortByKey(ascending = false).top(101).foreach(println) 
  }
  

}
