/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
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

package com.jnpersson.discount.spark

import com.jnpersson.discount.hash.{MinimizerPriorities, SpacedSeed}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import java.util.Properties

object IndexParams {
  val maxVersion = 1

  /** Read index parameters from a given location */
  def read(location: String)(implicit spark: SparkSession): IndexParams = {
    val props = HDFSUtil.readProperties(s"$location.properties")
    println(s"Index parameters for $location: $props")
    try {
      val numBuckets = props.getProperty("buckets").toInt
      val version = props.getProperty("version").toInt
      if (version > maxVersion) {
        throw new Exception(s"A newer version of this software is needed to read $location. (Version $version, max supported version $maxVersion)")
      }
      val formatId = Option(props.getProperty("splitter")).getOrElse("standard")
      val format = Helpers.getFormat(formatId)
      val splitter = format.readSplitter(location, props)
      IndexParams(spark.sparkContext.broadcast(splitter), numBuckets, location)
    } catch {
      case nfe: NumberFormatException =>
        throw new Exception(s"Unable to read index parameters for $location", nfe)
    }
  }
}

/** Parameters for a k-mer index.
 * @param bcSplit The broadcast splitter (minimizer scheme/ordering)
 * @param buckets The number of buckets (Spark partitions) to partition the index into -
 *                NB, not the same as minimizer bins
 * @param location The location (directory/prefix name) where the index is stored
  */
final case class IndexParams(bcSplit: Broadcast[AnyMinSplitter], buckets: Int, location: String) {

  def format: SplitterFormat[MinimizerPriorities] = {
    splitter.priorities match {
      case SpacedSeed(s, inner) => Helpers.getFormat(inner.getClass)
      case _ =>     Helpers.getFormat(splitter.priorities.getClass)
    }
  }
  def splitter: AnyMinSplitter = bcSplit.value
  def k: Int = splitter.k
  def m: Int = splitter.priorities.width

  def properties: Properties = {
    val p = new Properties()
    p.setProperty("k", k.toString)
    p.setProperty("m", m.toString)
    p.setProperty("buckets", buckets.toString)
    //Allows for future format upgrades
    p.setProperty("version", "1")
    p.setProperty("splitter", format.id)
    p
  }

  /** Write index parameters to a given location */
  def write(newLocation: String, comment: String)(implicit spark: SparkSession): Unit = {
    val p = properties
    splitter.priorities match {
      case SpacedSeed(s, inner) =>
        p.setProperty("minimizerSpaces", s.toString)
        format.write(inner, p, newLocation)
      case _ =>
        format.write(splitter.priorities, p, newLocation)
    }
    HDFSUtil.writeProperties(s"$newLocation.properties", p, comment)
  }

  override def toString: String = properties.toString

  def compatibilityCheck(other: IndexParams, strict: Boolean): Unit = {
    if (this eq other) return //Trivially compatible

    if (k != other.k || m != other.m) {
      throw new Exception(s"Issue for $location and ${other.location}: Index parameters incompatible: $this and $other.")
    }
    if (splitter != other.splitter && strict) {
      throw new Exception(s"Issue for $location and ${other.location}: Two indexes use different minimizer schemes / splitters. Indexes are incompatible. ")
    }

    if (buckets != other.buckets) {
      println(s"Warning for $location and ${other.location}: number of index buckets is different ($buckets and ${other.buckets}). Operations may be slow.")
    }

  }
}


