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
  val sparkContext = new SparkContext(sparkConfig)
  val graph = sparkContext.textFile(args(0),1)
  val edges = graph.map{ s => 
              val node = s.split("\\s+")
              (node(0),node(1))
              }.distinct().groupByKey().cache()
  var ranks = edges.mapValues(v => 1.0)
  for(i <- 1 to iteration) {
    logger.info("Iterating : " + i)
    val intermediateRDD = edges.join(ranks).values.flatMap{ case(links,rank) =>
      val size = links.size
      links.map(links => (links, rank / size))
  }
    /*
     * Teleportating page rank : https://www.cs.princeton.edu/~chazelle/courses/BIB/pagerank.htm
     */
    ranks = intermediateRDD.reduceByKey(_+_).mapValues(0.15 + 0.85 * _)
  }
  
  ranks.saveAsTextFile(args(2))
 
  }
 }
