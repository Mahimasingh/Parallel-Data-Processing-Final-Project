package ccs.neu.edu.pagerank

/**
 * @author Mahima Singh
 * @citation : https://github.com/eBay/Spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPageRank.scala
 */
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
// import classes required for using GraphX
import org.apache.spark.graphx._
object pageRank {
  
  def main(args : Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
      if(args.length < 1){
        System.err.println("Usage: pageRank <file> <iterations>")
        System.exit(1)
    }
  
  val sparkConfig = new SparkConf().setAppName("Twitter Page Rank")
  val iteration = args(1).toInt
  println("The number of iterations are" + iteration)
  val sparkContext = new SparkContext(sparkConfig)
  val graph = sparkContext.textFile(args(0),1)
  
  val rightEdges = graph.flatMap( s => 
             s.split("\\s+")(1)
              )
  val leftEdges = graph.flatMap( s => 
             s.split("\\s+")(0))
  val danglingEdges = rightEdges.subtract(leftEdges).map(x=> (x.toString,"dummy")).groupByKey()
  
  var edges = graph.map{ s => 
              val node = s.split("\\s+")
              (node(0),node(1))
              }.distinct().groupByKey()
  
  edges = edges.union(danglingEdges)
  var ranks = edges.mapValues(v => 1.0)
  val dummyRank=sparkContext.parallelize(Seq(("dummy",0.0)))
  edges = edges.union(danglingEdges)
  ranks = ranks.union(dummyRank)
  edges.cache()
  ranks.persist()
  
  for(i <- 1 to 3) {
    logger.info("Iterating : " + i)
    val intermediateRDD = edges.join(ranks).values.flatMap{ case(links,rank) =>
      val size = links.size
       links.map(links => (links, rank / size))
  }
    
   /*
    * Teleportating page rank : https://www.cs.princeton.edu/~chazelle/courses/BIB/pagerank.htm
    */
    
    ranks = intermediateRDD.reduceByKey(_+_)
    
    var delta = ranks.lookup("dummy")(0)
    println("The value of delta is" + delta)
    println("****************************************************************")
   
    var size = ranks.count() - 1
    
    ranks = ranks.map(vertex => if(vertex._1 !="dummy") (vertex._1, (vertex._2 +( delta / size.toDouble))) else (vertex._1,vertex._2))
    
  }
  
  ranks.saveAsTextFile(args(2))
 
  }
 }
