
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuite
import util.SparkUtils

import main.scala.StatsUdf.rankValue
import main.scala.ImdbInfo.calculate_rank

import org.apache.spark.sql.{SparkSession,Row}
import org.apache.spark.sql.functions.lit

/* Creating a Trait and an empty Object will help us in avoiding multiple
times importing of Spark Implicits for every test case  */

trait SparkJob {
  val spark: SparkSession = SparkSession.builder.master("local").appName("Tests").getOrCreate()
}

object SparkJob extends SparkJob {}

import SparkJob.spark.implicits._


class MovieStatsTest extends FunSuite with BeforeAndAfterEach with  BeforeAndAfterAll
{
  var spark:SparkSession= _

  override def beforeAll(): Unit = {

    spark=SparkUtils.getSparkSession()
    spark.conf.set("spark.broadcast.compress", "false")
    spark.conf.set("spark.shuffle.compress", "false")
    spark.conf.set("spark.shuffle.spill.compress", "false")
    spark.conf.set("spark.io.compression.codec", "lzf")
    spark.conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  }

  override def afterAll(): Unit = {
    spark.sparkContext.stop()
  }

 test("RankValue Test"){
     //val spark2=spark
     //import spark2.implicits._

    val test_ratings_df = Seq(
      ("tt0000001", 5,30),
      ("tt0000002", 6,30),
      ("tt0000003", 6,30)
    ).toDF("tconst", "averageRating","numVotes")

   val result_df= test_ratings_df.withColumn("rankValue",
                               rankValue(30)((test_ratings_df("numVotes")),test_ratings_df("averageRating")))
    result_df.show()
    assert(result_df.select("rankValue")
                           .filter(test_ratings_df("tconst")===lit("tt0000001"))
                           .head()==(Row(5.0)))


  }


   test("Filtered Dataframe Count Test")
  {

    val test_ratings_df = Seq(
      ("tt0000001", 5,300),
      ("tt0000002", 6,30),
      ("tt0000003", 6,30),
      ("tt0000004", 6,490),
      ("tt0000005", 6,600)

    ).toDF("tconst", "averageRating","numVotes")

    val filtered_df=calculate_rank(test_ratings_df,50)
    filtered_df.show()
    print("££££££££££££££££££££££££££££££££££££££")
    print(filtered_df.count())
    assert(filtered_df.count()==3)

  }



}
