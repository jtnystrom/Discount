package com.jnpersson.discount

import org.apache.spark.sql.SparkSession

package object spark {

  trait SparkSessionTestWrapper {
    /*
      SparkSession for unit tests.
      We use a very small max split size, to ensure that the relatively small test files
      end up generating multiple splits, which leads to more code paths being tested.
     */
    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .config("mapreduce.input.fileinputformat.split.maxsize", s"${64 * 1024}")
        .master("local")
        .appName("spark test example")
        .getOrCreate()
    }
  }
}
