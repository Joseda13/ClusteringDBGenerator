package es.us.clustering.DBGenerator

import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import spark.mllib.Utils

import scala.util.Random

object DBObject {

  def createDataBase(features: Int, tags: Int, number_cluster: Int, desviation: Float, dummies: Int, minimumPoints: Int, maximumPoints: Int, destination: String, index: Int): Unit = {

    println("*******************************")
    println("*******DATASET GENERATOR*******")
    println("*******************************")
    println("Configuration:")
    println("\tClusters: " + number_cluster)
    println("\tInstances per cluster between: " + minimumPoints + " - " + maximumPoints)
    println("\tClasses: " + tags)
    println("\tFeatures: " + features)
    println("\tSave directory: " + destination)
    println("Running...\n")

    val spark = SparkSession.builder()
      .appName(s"CreateDataBase")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //Create all permutations between the tags into the number of features
    val aux = Array.fill(features)(0 to tags-1).flatten.combinations(features).flatMap(_.permutations).toArray

    val shuffleArray = Random.shuffle(aux.toSeq)

    val resultTotal = Array.ofDim[Int](30,features)

    var result = Array.ofDim[Int](number_cluster,features)
    var indexResult = 1

    for (arrayAux <- shuffleArray){

      val arrayAuxCombinations = pairCombinationArrayInt(arrayAux)

      if (!resultTotal.contains(arrayAux)) {

        var contRepeat = 0

        for (indexRes <- resultTotal) {

          val arrayAuxResultCombinations = pairCombinationArrayInt(indexRes)

          val zipArray = arrayAuxResultCombinations.zip(arrayAuxCombinations)

          zipArray.map {
            x =>
              if (x._1 == x._2) {
                contRepeat += 1
              }
          }
        }

        if (contRepeat < 1) {
          resultTotal.update(indexResult, arrayAux)
          indexResult += 1
        }
      }

    }

    println(s"Different valid combinations number with $tags tags and $features features: " + (indexResult) )

    result = Random.shuffle(resultTotal.take(indexResult).toSeq).toArray.take(number_cluster)

    val testResult = spark.sparkContext.parallelize(result).toDF()
    println("Combinations choosen:")
    testResult.show(number_cluster, false)

    //Normalized the value of each tag between the range [0,1]
    val resultNormalized = result.map(v => v.map(value => ( value.toFloat / (tags-1)) ))

    //Add the cluster id of each features array normalized
    val resultClusterAndNormalized = for (cluster <- 0 to number_cluster-1) yield {
      (cluster, resultNormalized.apply(cluster))
    }

    //Create a RDD with the cluster id and the features array normalized
    val RDDDataBase = spark.sparkContext.parallelize(resultClusterAndNormalized)

    //Create the DataBase with the gaussian value in the features values
    val dataBase = RDDDataBase.flatMap { x =>
      val points = ThreadLocalRandom.current.nextInt(minimumPoints, maximumPoints)
      val clusterNumber = x._1
      println(s"Number of points to the cluster $clusterNumber: " + points)

      for {indexPoint <- 0 until points} yield {

        val arrayGaussian = for {indexGaussian <- 0 until features} yield {
          var valueGaussian = getGaussian(x._2(indexGaussian), desviation)

          valueGaussian
        }

        (x._1, arrayGaussian)
      }
    }.cache()

    val dataHomotecia = dataBase.map{
      row =>

        val arrayFeautues = for (index <- 0 until features) yield {
          val value = row._2.apply(index)
          value
        }

        (arrayFeautues.toString().replace("Vector(", "").replace(")", "").replace(" ", ""))
    }.toDF().withColumn("temp", split(col("value"), "\\,"))
      .select((0 until features).map(i => col("temp").getItem(i).cast("Float").as(s"col$i")): _*).cache()

    var resultData = Seq((0.0f, 0.0f)).toDF("min", "max")

    for (index <- 0 until features) {

      val auxMin_Max = dataHomotecia.agg(min(s"col$index"), max(s"col$index"))
      resultData = resultData.union(auxMin_Max)
    }

    val resultHomotecia = resultData.select("min", "max").withColumn("index", monotonically_increasing_id())
      .filter(value => value.getLong(2) > 0).collect()

    val resultDataBase = dataBase.map{
      row =>

        val arrayFeautues = for (index <- 0 until features) yield {
          val testFilter = filterDatasetByIndex(resultHomotecia, index)
          val col_min = testFilter.getFloat(0)
          val col_max = testFilter.getFloat(1)

          val valueAux = (row._2.apply(index) - col_min) / (col_max - col_min)

          valueAux
        }

        val arrayDummies = for {indexDummie <- 0 until dummies} yield {
          Math.random().toFloat
        }

        (row._1, arrayFeautues.union(arrayDummies))

    }

    println("Saving DataBase ...\n")

    //Save the DataBase
    resultDataBase.map(x => x._1 + "," + x._2.toString.replace("(", "").replace(")", "").replace("Vector", "").replace(" ",""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(s"K$number_cluster-N$features-D$dummies-I($minimumPoints-$maximumPoints)-${Utils.whatTimeIsIt()}")

    println("DataBase saved!")
  }

  def pairCombinationArrayInt (base: Array[Int]): Array[(Int,Int)] = {

    var result = new Array[(Int,Int)]((base.length * (base.length - 1)) / 2 )

    var cont = 0

    for (value <- base.indices){
      base.indices.map{
        case otherValue =>
          if (value != otherValue && value < otherValue){
            result.update(cont,(base.apply(value),base.apply(otherValue)))
            cont+=1
          }
      }
    }

    result
  }

  def filterDatasetByIndex (data: Array[Row], id: Long): Row = {
    val result = data.filter(row => row.getLong(2) == (8589934592l * (id + 1))).apply(0)

    result
  }

  /**
    * It generates a random number in a gaussian distribution with the given mean and standard deviation
    *
    * @param average The start point
    * @param desv The last point
    * @example getGaussian(0.5, 0.05)
    */
  def getGaussian(average: Float, desv: Float): Float = {
    val rnd = new Random()
    rnd.nextGaussian().toFloat * desv + average
  }
}

