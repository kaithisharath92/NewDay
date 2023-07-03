import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.newDay.assignment.MovieRatings
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.functions._
import org.newDay.assignment.MovieRatings.{movieStats, readingMovieData, readingRatingsData, topRatedMovies}

class MovieRatingTest extends  FunSuite{

  private val master = "local"

  private val appName = "ReadFileTest"

  var spark: SparkSession = SparkSession.builder.appName(appName).master(master).getOrCreate()

  test("Reading Data ") {
  val moviesDataPath = "src/main/resources/movies.dat"
  val ratingsDataPath = "src/main/resources/ratings.dat"
  val moviesDF = readingMovieData(spark, moviesDataPath)
  val ratingsDF = readingRatingsData(spark, ratingsDataPath)
  moviesDF.show(2,false)

 }


test("Movie Stats ") {
    val moviesDataPath = "src/main/resources/movies.dat"
    val ratingsDataPath = "src/main/resources/ratings.dat"
    val moviesDF = readingMovieData(spark, moviesDataPath)
    val ratingsDF = readingRatingsData(spark, ratingsDataPath)
    movieStats(moviesDF,ratingsDF).show(2,false)
  }


  test("Top Rated Movies") {
    val ratingsDataPath = "src/main/resources/ratings.dat"
    val ratingsDF = readingRatingsData(spark, ratingsDataPath)

    val expectedMovies = Seq("48","2355","595")

    val topMoviesList = topRatedMovies(ratingsDF).filter(col("UserID") === 1).select("MovieID").collect().map(_.getString(0)).toList
    assert ((topMoviesList) === expectedMovies)
  }
}
