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
   * should be passed to improve performance. However, if the number is too large,
   * the size of the data returned may be too large for the driver to accept, in which case an error will occur.
   */
  def countFeatures(reads: Dataset[String], space: MotifSpace, reducePartitions: Int): MotifCounter = {

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
    counter.print(template, "Discovered frequencies in sample")

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

  def readMotifList(location: String, k: Int, m: Int): Array[String] = {
    val hadoopDir = new Path(location)
    val fs = hadoopDir.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.getFileStatus(hadoopDir).isDirectory) {
      println(s"$location is a directory; searching for PASHA files")
      val selected = Sampling.findBestPashaFile(location, k, m)
      println(s"Using minimizers from $selected")
      readMotifList(selected)
    } else {
      readMotifList(location)
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

  /**
   * Given a directory with files such as pasha_all_28_10.txt,
   * pasha_all_55_9.txt... (pasha_all_${k}_${m}.txt), find the most
   * appropriate universal k-mer hitting set generated by PASHA by searching all possible filenames.
   * In general, it's possible to use a PASHA set generated for a smaller k (with some loss of performance),
   * but m must be the same.
   * See https://github.com/ekimb/pasha.
    * @param pashaDir
   */
  def findBestPashaFile(pashaDir: String, k: Int, m: Int)(implicit spark: SparkSession): String = {
    if (k <= m) {
      throw new Exception("k is less than or equal to m")
    }

    val filePaths = (k.to(m + 1, -1)).toList.map(x => new Path(s"$pashaDir/pasha_all_${x}_${m}.txt"))
    val hadoopDir = new Path(pashaDir)
    val fs = hadoopDir.getFileSystem(spark.sparkContext.hadoopConfiguration)
    filePaths.find(fs.exists).map(f => f.toUri.toString).
      getOrElse(throw new Exception(
        s"The file ${filePaths.head} (or a compatible file for a smaller k) was not found."))
  }
}

