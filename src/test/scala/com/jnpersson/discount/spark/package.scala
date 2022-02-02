/*
 * This file is part of Discount. Copyright (c) 2022 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

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
