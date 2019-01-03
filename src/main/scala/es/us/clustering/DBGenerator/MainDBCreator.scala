package es.us.clustering.DBGenerator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MainDBCreator {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("Generate DataSet")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    var features = 5   //Number of features (columns)
    var dummies = 0
    var tags = 5
    var K = 7       //Number of clusters
    var minimumPoints = 500    //Instances minimum per cluster
    var maximumPoints = 1000   //Instances maximum per cluster
    var desviation = 0.03f   //Standard deviation for the gaussian distribution
    var destination = ""

    if (args.length > 2){
      features = args.apply(0).toInt
      dummies = args.apply(1).toInt
      tags = args.apply(2).toInt
      K = args.apply(3).toInt
      minimumPoints = args.apply(4).toInt
      maximumPoints = args.apply(5).toInt
      desviation = args.apply(6).toInt
      destination = args.apply(7)
    }

    val DB = DBObject.createDataBase(features, tags, K, desviation, dummies, minimumPoints, maximumPoints, destination, 0)

    sc.stop()
  }
}
