package discount

import org.apache.spark.sql.SparkSession

package object spark {

  trait SparkSessionTestWrapper {
    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark test example")
        .getOrCreate()
    }
  }
}
