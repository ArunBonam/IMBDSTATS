package main.scala

import org.apache.spark.sql.functions.udf

object StatsUdf {

  /*Registering the required UDF*/


 // def rankValue=udf((avg:Double,num_votes:Int,avg_rating:Double)=> (num_votes/avg)*avg_rating)

  val rankValue = (avg: Double) => udf( (num_votes: Int,avg_rating:Double) => (num_votes/avg)*avg_rating)
}
