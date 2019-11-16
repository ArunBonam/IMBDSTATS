package main.scala

import com.typesafe.config.ConfigFactory
import main.scala.StatsUdf.rankValue
import org.apache.spark.sql.functions.{avg, broadcast, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import util.SparkUtils

object ImdbInfo {

  def main(args: Array[String]): Unit =
  {


    val spark=SparkUtils.getSparkSession()  /*gets the SparkSession from SparkUtils */
    val config =ConfigFactory.load("application.conf").getConfig("datasets") /* Reading file from src/main/resources */

    /*get all the file names and path*/
    val ratings_file=config.getString("ratings")
    val title_file= config.getString("titles")
    val principal_file = config.getString("principal")
    val names_file =config.getString("names")
    val file_path =config.getString("path")

    /*reads data from csv using a generic function*/
    val df_ratings = read_csv(spark,file_path+ratings_file)
    val df_title =read_csv(spark,file_path+title_file)
    val df_principal_const =read_csv(spark,file_path+principal_file)
    val df_names=read_csv(spark,file_path+names_file)

    /* Dataframe with top 20 movie titles*/
    val df_ratings_top20=calculate_rank(df_ratings,config.getInt("min_limit_votes"))

    val ratings_with_title=ratings_join_title(df_ratings_top20,df_title)
    val princpal_with_name =principal_join_names(df_principal_const,df_names)

    /*persist these dataframes to minimize the IO operations while joining,
    Serialization not required as we are not doing any aggregations which means no movement of data */
    ratings_with_title.persist(StorageLevel.MEMORY_AND_DISK)
    princpal_with_name.persist(StorageLevel.MEMORY_AND_DISK)

    /*Joining based on tconst and selecting only required columns*/
    val final_df=ratings_with_title.join( princpal_with_name
      ,ratings_with_title("titleId")===princpal_with_name("tconst")
      , "inner")
      .select(ratings_with_title("title"),princpal_with_name("primaryName")
        .alias("Person"))
    /*write the final dataframe */
     final_df.repartition(final_df("title")).write.csv("src/main/resources/final")


  }

  /* joins title.principals and names.basics */
  def principal_join_names(df_principal:DataFrame,df_names:DataFrame): DataFrame =
  {
    val df_names_princ_joined= df_principal.join(df_names,df_principal("nconst")===df_names("nconst"),"inner")
    val df_joined= df_names_princ_joined.select("primaryName","tconst")

    return df_joined

  }

  /* joins title.ratings and title.akas
  *  broadcasting of smaller dataframe with 20 rows  */

  def ratings_join_title(df_ratings: DataFrame,df_title:DataFrame): DataFrame =
  {
    val  df_title_ratings=df_title.join(broadcast(df_ratings),df_title("titleId")===df_ratings("tconst"),"inner")
      .select("titleId","title","tconst")
    return df_title_ratings

  }

  def read_csv(sparkSession: SparkSession,file:String):DataFrame=
  {
    val df =sparkSession.read.option("sep","\t").option("header","true").csv(file)
    return df
  }


  /* Filters movies having below 50 votes
     Calculates rank_value=(numVotes/averageNumberOfVotes) * averageRating and returns top 20 movies
     Utilises udf:rankValue
   */

  def calculate_rank(df:DataFrame,limit_votes:Int):DataFrame=
  {

    val df_filtered=df.filter(df("numVotes")>=lit(limit_votes))/*filtering the movies with less than 50 votes */

    /*persisting as we are using it twice */
    df_filtered.persist(StorageLevel.MEMORY_AND_DISK)

    val avg_voting=df_filtered.select(avg("numVotes")).head().getDouble(0) /*gets the avg voting into variable*/

    //val df_with_rank_value =df_filtered.withColumn("rank_value", addP(x)(df_filtered("numVotes"),df_filtered("averageRating")))
    val df_with_rank_value =df_filtered.withColumn("rank_value",rankValue(avg_voting)(df_filtered("numVotes"),df_filtered("averageRating")))

    /*
      Persisting with Serialization required as the data shuffling across workers
      Else engine throws Error:Object cant be serialized
     */

    df_with_rank_value.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val df_final_20=df_with_rank_value.orderBy(df_with_rank_value("rank_value").desc).limit(20)
    return df_final_20
  }

}
