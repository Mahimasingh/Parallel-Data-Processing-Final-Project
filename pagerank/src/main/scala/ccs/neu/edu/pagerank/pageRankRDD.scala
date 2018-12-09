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
import org.apache.spark.sql.SparkSession
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
    val iters = if (args.length > 1) args(1).toInt else 10
    val sparkContext = new SparkContext(sparkConfig)
    val graph = sparkContext.textFile(args(0),1)
    
    val rightEdges = graph.map( s => 
             s.split("\\s+")(1)
              ).distinct()
    val leftEdges = graph.map( s => 
             s.split("\\s+")(0)).distinct()
   
    val danglingEdges = rightEdges.subtract(leftEdges).map(x=> (x.toString,"dummy")).groupByKey()
    var edges = graph.map{ s => 
              val node = s.split("\\s+")
              (node(0),node(1))
              }.distinct().groupByKey()
  
  edges = edges.union(danglingEdges)
  var ranks = edges.mapValues(v => 1.0)
  val dummyRank=sparkContext.parallelize(Seq(("dummy",0.0)))
  ranks = ranks.union(dummyRank)
  val globalRanks = ranks
  edges.cache()
  globalRanks.persist()
  var d = 0.15
    for (i <- 1 to iters) {
      val contribs = edges.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, (rank / size)))
      }
      
      ranks = contribs.reduceByKey(_ + _).mapValues((0.15 + 0.85 * _))
      var noIncoming = globalRanks.subtractByKey(ranks).map(x => (x._1,0.0))
      
      ranks = ranks.union(noIncoming)
      var delta = ranks.lookup("dummy")(0)
      var size = ranks.count() - 1
    
    ranks = ranks.map(vertex => if(vertex._1 !="dummy") (vertex._1, (vertex._2 +( delta / size.toDouble))) else (vertex._1,vertex._2))
    
    }

    
    ranks.saveAsTextFile(args(2))

  }
}