package org.newDay.assignment

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{avg, max, min, row_number}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MovieRatings  {

  def readingMovieData(spark:SparkSession,moviesDataPath:String) = {
    /* Schema for Movies*/
    val movieSchema = StructType(Array(
      StructField("MovieID", IntegerType, true),
      StructField("Title", StringType, true),
      StructField("Genres", StringType, true))
    )
    /*Reading the movie data*/
    spark.read.option("delimiter", "::").schema(movieSchema).csv(moviesDataPath)
  }

  def readingRatingsData(spark:SparkSession,ratingsDataPath: String) = {
    /* Schema for Ratings */
    val ratingSchema = StructType(Array(
      StructField("UserID", StringType, true),
      StructField("MovieID", StringType, true),
      StructField("Rating", StringType, true),
      StructField("Timestamp", StringType, true))
    )
    /*Reading Rating Data*/
    spark.read.option("delimiter", "::").schema(ratingSchema).csv(ratingsDataPath)
  }

  /*Definition to Extract movie stats */
  def movieStats(movies:DataFrame,ratings: DataFrame): DataFrame = {

    val movieRatingStats = ratings.groupBy("MovieID")
      .agg(avg("Rating").as("avgRatings")
        , max("Rating").as("maxRatings")
        , min("Rating").as("minRatings"))
      .withColumn("avgRatings",col("avgRatings").cast("double"))
      .withColumn("maxRatings",col("maxRatings").cast("int"))
      .withColumn("minRatings",col("minRatings").cast("int"))
    movies.join(movieRatingStats, Seq("MovieID"), "left")
  }

  /* Definition to Extarct TOp 3 MOvies rated by a user */
  def topRatedMovies(ratings: DataFrame): DataFrame = {
    ratings.withColumn("rn_num", row_number().over(Window.partitionBy("UserID")
      .orderBy(col("Rating").desc, col("Timestamp").desc)))
      .filter(col("rn_num") <= 3)
      .drop("rn_num")
  }


def main(args: Array[String]) {
  /*Initialising  Spark Context*/

  val conf = new SparkConf().setAppName(MovieRatings.getClass.getName)
  val spark: SparkSession = SparkSession.builder.master("local").config(conf).getOrCreate()

  /*Reading Paths from arguments*/
  val moviesDataPath = args(0)
  val ratingsDataPath = args(1)
  val movieStatsPath = args(2)
  val topMoviesPath = args(3)


  val ratingsDF = readingRatingsData(spark, ratingsDataPath)
  val moviesDF = readingMovieData(spark, moviesDataPath)

  moviesDF.show(5, false)


  /*Writing movie Stats data to a parquet*/
  movieStats(moviesDF,ratingsDF ).write.parquet(movieStatsPath)

  /*Writing top rated movies to parquet*/
  topRatedMovies(ratingsDF).write.parquet(topMoviesPath)

  /* Terminating spark context */
  spark.stop()

}

}
