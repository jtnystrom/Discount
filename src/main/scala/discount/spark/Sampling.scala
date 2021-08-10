package discount.spark

import discount.hash._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._

import java.io.PrintWriter

/**
 * Routines for creating and managing frequency sampled minimizer orderings.
 * @param spark
 */
class Sampling(implicit spark: SparkSession) {

  import spark.sqlContext.implicits._

  /**
   * Count motifs (m-length minimizers) in a set of reads using a simple in-memory counter.
   * For reducePartitions, ideally the total number of CPUs expected to be available
   * should be passed to improve performance.
   */
  def countFeatures(reads: Dataset[String], space: MotifSpace,
                    reducePartitions: Int): MotifCounter = {

    val r = reads.mapPartitions(reads => {
      val s = new ShiftScanner(space)
      val c = MotifCounter(s.space)
      s.countMotifs(c, reads)
      Iterator(c)
    })
    r.coalesce(reducePartitions).reduce(_ + _)
  }

  /**
   * Create a MotifSpace based on sampling reads.
   * @param input Input reads
   * @param template Template space
   * @param samplePartitions The number of CPUs expected to be available for sampling
   * @param persistLocation Location to optionally write the new space to for later reuse
   * @return
   */
  def createSampledSpace(input: Dataset[String], template: MotifSpace, samplePartitions: Int,
                         persistLocation: Option[String] = None): MotifSpace = {

    val counter = countFeatures(input, template, samplePartitions)
    counter.print(template, s"Discovered frequencies in sample")

    val r = counter.toSpaceByFrequency(template)
    persistLocation match {
      case Some(loc) =>
        /**
         * Writes two columns: minimizer, count.
         * We write the second column (counts) for informative purposes only.
         * It will not be read back into the application later when the minimizer ordering is reused.
         */
        val raw = counter.motifsWithCounts(template).sortBy(x => (x._2, x._1))
        val persistLoc = s"${loc}_minimizers_sample.txt"
        writeTextFile(persistLoc, raw.map(x => x._1 + "," + x._2).mkString("", "\n", "\n"))
        println(s"Saved ${r.byPriority.size} minimizers and sampled counts to $persistLoc")
      case _ =>
    }
    r
  }

  def persistMinimizers(space: MotifSpace, location: String): Unit = {
    val persistLoc = s"${location}_minimizers.txt"
    writeTextFile(persistLoc, space.byPriority.mkString("", "\n", "\n"))
    println(s"Saved ${space.byPriority.size} minimizers to $persistLoc")
  }

  def writeTextFile(location: String, data: String) = {
    val hadoopPath = new Path(location)
    val fs = hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val file = fs.create(hadoopPath, true)
    val writer = new PrintWriter(file)
    try {
      writer.write(data)
    } finally {
      writer.close()
    }
  }

  def readMotifList(location: String): Array[String] = {
    spark.read.csv(location).collect().map(_.getString(0))
  }
}

object Sampling {

  def createSampledSpace(sampledInput: Dataset[String], m: Int, samplePartitions: Int,
                         validMotifFile: Option[String])(implicit spark: SparkSession): MotifSpace = {
    val s = new Sampling
    val template = MotifSpace.ofLength(m)
    val template2 = validMotifFile match {
      case Some(mf) =>
        val uhs = s.readMotifList(mf)
        MotifSpace.fromTemplateWithValidSet(template, uhs)
      case _ => template
    }
    s.createSampledSpace(sampledInput, template2, samplePartitions, None)
  }
}
