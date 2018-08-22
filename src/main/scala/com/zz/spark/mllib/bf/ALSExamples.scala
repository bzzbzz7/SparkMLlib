import com.zz.util.PathUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{SQLContext, SparkSession}


case class Movie(movieId: Int, title: String, genres: Seq[String])

case class User(userId: Int, gender: String, age: Int, occupation: Int, zip: String)

object ALSExamples {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local[2]")
      .appName("als-example")
      .getOrCreate()

    val sqlContext = sparkSession.sqlContext
    val sc = sparkSession.sparkContext

    Logger.getRootLogger.setLevel(Level.ERROR)

    import sqlContext.implicits._

    //Ratings analyst
    val ratingRDD = sc.textFile(PathUtil.root + "/user/spark/mllib/bf/MovieLens/ml-1m/ratings.dat").map(parseRating)
//    ratingRDD.first()
//    ratingRDD.cache()
//    println("Total number of ratings: " + ratingRDD.count())
//    println("Total number of movies rated: " + ratingRDD.map(_.product).distinct().count())
//    println("Total number of users who rated movies: " + ratingRDD.map(_.user).distinct().count())

    //Create DataFrames
    val ratingDF = ratingRDD.toDF()
    val movieDF = sc.textFile(PathUtil.root + "/user/spark/mllib/bf/MovieLens/ml-1m/movies.dat").map(parseMovie).toDF()
    val userDF = sc.textFile(PathUtil.root + "/user/spark/mllib/bf/MovieLens/ml-1m/users.dat").map(parseUser).toDF()
    ratingDF.printSchema()
    movieDF.printSchema()
    userDF.printSchema()
    ratingDF.registerTempTable("ratings")
    movieDF.registerTempTable("movies")
    userDF.registerTempTable("users")

    val mostCommentProduct = sqlContext.sql(
      """select title, rmax, rmin, ucnt from
          (select product, max(rating) as rmax, min(rating) as rmin, count(distinct user) as ucnt from ratings group by product)
          ratingsCNT join movies on product=movieId order by ucnt desc""")
    mostCommentProduct.show()

    val mostActiveUser = sqlContext.sql(
      """select user, count(*) as cnt
        from ratings group by user order by cnt desc limit 10""".stripMargin)
    mostActiveUser.show()

    val mostLoveMovie = sqlContext.sql(
      """select distinct title, rating
        from ratings join movies on movieId=product
        where user=4169 and rating>4""")
    mostLoveMovie.show()

    //ALS
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2), 0L)
    val trainingSet = splits(0).cache()
    val testSet = splits(1).cache()
    val model = (new ALS().setRank(20).setIterations(10).run(trainingSet))

    val recomForTopUser = model.recommendProducts(4169, 5)
    val movieTitle = movieDF.rdd.map(array => (array(0), array(1))).collectAsMap()
    val recomResult = recomForTopUser.map(rating => (movieTitle(rating.product), rating.rating)).foreach(println)

    val testUserProduct = testSet.map {
      case Rating(user, product, rating) => (user, product)
    }

    val testUserProductPredict = model.predict(testUserProduct)
    testUserProductPredict.take(10).mkString("\n")

    val testSetPair = testSet.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }
    val predictionsPair = testUserProductPredict.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }

    val joinTestPredict = testSetPair.join(predictionsPair)
    val mae = joinTestPredict.map {
      case ((user, product), (ratingT, ratingP)) =>
        val err = ratingT - ratingP
        Math.abs(err)
    }.mean()
    println("mae:" + mae)
    //FP,ratingT<=1, ratingP>=4  实际值，预测值
    val fp = joinTestPredict.filter {
      case ((user, product), (ratingT, ratingP)) =>
        (ratingT <= 1 & ratingP >= 4)
    }
    println("fp.count:" + fp.count())

    import org.apache.spark.mllib.evaluation._

    val ratingTP = joinTestPredict.map {
      case ((user, product), (ratingT, ratingP)) =>
        (ratingP, ratingT)
    }
    val evalutor = new RegressionMetrics(ratingTP)
    println("evalutor.meanAbsoluteError:" + evalutor.meanAbsoluteError)
    println("evalutor.rootMeanSquaredError:" + evalutor.rootMeanSquaredError)
  }

  //Define parse function
  def parseMovie(str: String): Movie = {
    val fields = str.split("::")
    assert(fields.size == 3)
    Movie(fields(0).toInt, fields(1).toString, Seq(fields(2)))
  }

  def parseUser(str: String): User = {
    val fields = str.split("::")
    assert(fields.size == 5)
    User(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt, fields(4).toString)
  }

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt)
  }

}



