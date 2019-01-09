package es.us.clustering.DBGenerator

import org.apache.log4j.{Level, Logger}

object MainDBCreator {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    //Configuration parameters
    var features = 3           //Number of relevant features of the dataset
    var dummies = 0            //Number of dummies features of the dataset
    var tags = 5               //Number of different value to construct Gaussian Distribution
    var K = 5                  //Number of clusters
    var minimumPoints = 500    //Instances minimum per cluster
    var maximumPoints = 1000   //Instances maximum per cluster
    var deviation = 0.03f      //Standard deviation for the gaussian distribution
    var destination = ""       //Path to saved destination dataset

    if (args.length > 2){
      features = args.apply(0).toInt
      dummies = args.apply(1).toInt
      tags = args.apply(2).toInt
      K = args.apply(3).toInt
      minimumPoints = args.apply(4).toInt
      maximumPoints = args.apply(5).toInt
      deviation = args.apply(6).toInt
      destination = args.apply(7)
    }

    //Create the dataset with the configurations parameters previously set
    DBObject.createDataBase(features, tags, K, deviation, dummies, minimumPoints, maximumPoints, destination)

  }
}
