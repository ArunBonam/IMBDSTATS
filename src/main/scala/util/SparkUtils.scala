
package util
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

object SparkUtils {

  def getSparkSession():SparkSession=
  {
    val spark_conf= ConfigFactory.load("conf/spark.conf").getConfig("spark")
    val spark=SparkSession.builder()
                          .appName(spark_conf.getString("appName"))
                          .master(spark_conf.getString("master"))
                          .getOrCreate()
    return spark
  }

}
