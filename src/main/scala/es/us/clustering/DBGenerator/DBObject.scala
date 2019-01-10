package es.us.clustering.DBGenerator

import java.util.concurrent.ThreadLocalRandom
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import spark.mllib.Utils
import scala.util.Random

object DBObject {

  /**
    * Return a dataset with the configuration parameters
    *
    * @param features Number of relevant features of the dataset
    * @param tags Number of different value to construct Gaussian Distribution
    * @param numberClusters Number of clusters
    * @param deviation Standard deviation for the gaussian distribution
    * @param dummies Number of dummies features of the dataset
    * @param minimumPoints Instances minimum per cluster
    * @param maximumPoints Instances maximum per cluster
    * @param destination Path to saved destination dataset
    * @example createDataBase(5,5,7,0.3f,3,500,1000,"results/datasets")
    */
  def createDataBase(features: Int, tags: Int, numberClusters: Int, deviation: Float, dummies: Int, minimumPoints: Int, maximumPoints: Int, destination: String): Unit = {

    println("*******************************")
    println("*******DATASET GENERATOR*******")
    println("*******************************")
    println("Configuration parameters:")
    println("\tClusters: " + numberClusters)
    println("\tInstances per cluster between: " + minimumPoints + " - " + maximumPoints)
    println("\tClasses: " + tags)
    println("\tFeatures: " + features)
    println("\tDummies: " + dummies)
    println("\tSave directory: " + destination)
    println("Running...\n")

    //Initialize Spark Session
    val spark = SparkSession.builder()
      .appName(s"CreateDataBase")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //Calculate all permutations between the tags into the number of features
    val aux = Array.fill(features)(0 to tags-1).flatten.combinations(features).flatMap(_.permutations).toArray

    //Select a random element to the all previously permutations
    val shuffleArray = Random.shuffle(aux.toSeq)
    val elementRandom = shuffleArray.head

    //Create an array with two dimensions (tags*tags X features) and save an element random from previously permutations calculate
    val resultTotal = Array.ofDim[Int]((tags*tags),features)
    for (i <- 0 until resultTotal.length){
      resultTotal.update(i, elementRandom)
    }

    var indexResult = 1

    //For each permutation it is checked whether it complies with the restrictions to be included in the final result
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

    //Create auxiliary result array with two dimensions (number of clusters X features)
    var result = Array.ofDim[Int](numberClusters,features)
    if (indexResult < numberClusters){
      createDataBase(features, tags, numberClusters, deviation, dummies, minimumPoints, maximumPoints, destination)
    }else {
      result = Random.shuffle(resultTotal.take(indexResult).toSeq).toArray.take(numberClusters)
    }

    //Show the Gaussian distribution for each cluster choosen
    val dataResult = spark.sparkContext.parallelize(result).toDF().withColumnRenamed("value", "Gaussian Distribution")
    println("Combinations choosen:")
    dataResult.show(numberClusters, false)

    //Normalized the value of each tag between the range [0,1]
    val resultNormalized = result.map(v => v.map(value => ( value.toFloat / (tags-1)) ))

    //Add the cluster id of each features array normalized
    val resultClusterAndNormalized = for (cluster <- 0 to numberClusters-1) yield {
      (cluster, resultNormalized.apply(cluster))
    }

    //Create a RDD with the cluster id and the features array normalized
    val RDDDataBase = spark.sparkContext.parallelize(resultClusterAndNormalized)

    //Create the DataBase with the Gaussian value in the features values
    val dataBase = RDDDataBase.flatMap { x =>
      val points = ThreadLocalRandom.current.nextInt(minimumPoints, maximumPoints)
      val clusterNumber = x._1
      println(s"Number of points to the cluster $clusterNumber: " + points)

      for {indexPoint <- 0 until points} yield {

        val arrayGaussian = for {indexGaussian <- 0 until features} yield {
          val valueGaussian = getGaussian(x._2(indexGaussian), deviation)

          valueGaussian
        }

        (x._1, arrayGaussian)
      }
    }.cache()

    //Realize the Homotecy transform to the dataset
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

    //Create the result dataset add columns with dummies values
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

    println("Saving dataset ...\n")

    //Save the dataset
    resultDataBase.map(x => x._1 + "," + x._2.toString.replace("(", "").replace(")", "").replace("Vector", "").replace(" ",""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(s"K$numberClusters-N$features-D$dummies-I($minimumPoints-$maximumPoints)-${Utils.whatTimeIsIt()}")

    println("Dataset saved!")
  }

  /**
    * Return an array with all elements group by Par(Int,Int) from the first element to the last element without repetition or turning back
    *
    * @param base An array whose number of columns is equal to the number of features
    * @example pairCombinationArrayInt(data)
    */
  def pairCombinationArrayInt(base: Array[Int]): Array[(Int,Int)] = {

    val result = new Array[(Int,Int)]((base.length * (base.length - 1)) / 2 )

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

  /**
    * Return a specific row from a dataset filter for a id cluster
    *
    * @param data Dataset with all data
    * @param id The cluster id
    * @example filterDatasetByIndex(data, 1)
    */
  def filterDatasetByIndex(data: Array[Row], id: Long): Row = {
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

