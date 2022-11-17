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

package com.jnpersson.discount.spark

import com.jnpersson.discount
import org.apache.spark.sql.{Dataset, SparkSession}
import com.jnpersson.discount._
import com.jnpersson.discount.bucket.{Reducer, ReducibleBucket}
import com.jnpersson.discount.hash._
import org.apache.spark.broadcast.Broadcast
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem

import scala.util.Random

/** A Spark-based tool.
 * @param appName Name of the application */
abstract class SparkTool(appName: String) {

  /** The SparkSession */
  implicit lazy val spark: SparkSession = {
    val sp = SparkSession.builder().appName(appName).
      enableHiveSupport().
      getOrCreate()

    /* Reduce the verbose INFO logs that we get by default (to some degree, edit spark's conf/log4j.properties
   * for greater control)
   */
    sp.sparkContext.setLogLevel("WARN")
    //BareLocalFileSystem is needed to work around a dependency on winutils.exe on windows
    sp.sparkContext.hadoopConfiguration.
      setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[Nothing])
    sp
  }
}

/**
 * Configuration for a Spark-based tool, parsed using the Scallop library.
 * @param args command line arguments
 * @param spark the SparkSession
 */
abstract class SparkToolConf(args: Array[String])(implicit spark: SparkSession) extends Configuration(args) {
  lazy val discount = {
    validateMAndKOptions()
    new Discount(k(), parseMinimizerSource, minimizerWidth(), ordering(), sample(), maxSequenceLength(), normalize(),
      method(), pbuckets())
  }
}

/**
 * Configuration for Discount. Run the tool with --help to see the various arguments.
 * @param args commnad line arguments
 * @param spark the SparkSession
 */
class DiscountConf(args: Array[String])(implicit spark: SparkSession) extends SparkToolConf(args) {
  version(s"Discount ${getClass.getPackage.getImplementationVersion} beta (c) 2019-2022 Johan Nyström-Persson")
  banner("Usage:")
  shortSubcommandsHelp(true)

  def readIndex(location: String): Index =
    Index.read(location)

  val inFiles = trailArg[List[String]](descr = "Input sequence files", required = false)
  val indexLocation = opt[String](name = "index", descr = "Input index location")
  val min = opt[Abundance](descr = "Filter for minimum k-mer abundance", noshort = true)
  val max = opt[Abundance](descr = "Filter for maximum k-mer abundance", noshort = true)

  /** The index of input data, which may be either constructed on the fly from input sequence files,
   * or read from a pre-stored index created using the 'store' command. */
  def inputIndex(compatIndexLoc: Option[String] = None): Index = {
    requireOne(inFiles, indexLocation)
    if (indexLocation.isDefined) {
      readIndex(indexLocation())
    } else {
      val kmerReader =  compatIndexLoc match {
        case Some(ci) =>
          //Construct an index on the fly, but copy settings from a pre-existing index
          println(s"Copying index settings from $ci")
          val p = IndexParams.read(ci)
          Discount(p.k, Path(s"${ci}_minimizers.txt"), p.m, Given,
            sample(), maxSequenceLength(), normalize(), method(), indexBuckets = pbuckets())
        case _ => discount //Default settings
      }
      kmerReader.kmers(inFiles(): _*).index
    }
  }


  val count = new RunCmd("count") {
    banner("Count or export k-mers in input sequences or an index.")
    val output = opt[String](descr = "Location where the output is written", required = true)

    val tsv = opt[Boolean](default = Some(false),
      descr = "Use TSV output format instead of FASTA, which is the default")

    val sequence = toggle(default = Some(true),
      descrYes = "Output sequence for each k-mer in the counts table (default true)")
    val superkmers = opt[Boolean](default = Some(false),
      descr = "Instead of k-mers and counts, output human-readable superkmers and minimizers")
    val histogram = opt[Boolean](default = Some(false),
      descr = "Output a histogram instead of a counts table")
    val buckets = opt[Boolean](default = Some(false),
      descr = "Instead of k-mer counts, output per-bucket summaries (for minimizer testing)")

    validate(inFiles, superkmers) { (ifs, skm) =>
      if (skm && ifs.isEmpty) Left("Input sequence files required for superkmers.")
      else Right(Unit)
    }

    def run(): Unit = {
      lazy val index = inputIndex().filterCounts(min.toOption, max.toOption)
      def counts = index.counted(normalize())

      if (superkmers()) {
        discount.kmers(inFiles() : _*).segments.writeSupermerStrings(output())
      } else if (buckets()) {
        index.writeBucketStats(output())
      } else if (histogram()) {
        index.writeHistogram(output())
      } else if (tsv()) {
        counts.writeTSV(sequence(), output())
      } else {
        counts.writeFasta(output())
      }
    }
  }
  addSubcommand(count)

  val stats = new RunCmd("stats") {
    banner("Compute aggregate statistics for input sequences or an index.")
    val output = opt[String](descr = "Location where k-mer stats are written (optional)")

    requireOne(inFiles, indexLocation)

    def run(): Unit =
      Counting.showStats(inputIndex().stats(min.toOption, max.toOption), output.toOption)
  }
  addSubcommand(stats)

  val store = new RunCmd("store") {
    banner("Store k-mers in a new optimized index.")
    val compatible = opt[String](descr = "Location of index to copy settings from, for compatibility")
    val output = opt[String](descr = "Location where the new index is written", required = true)

    def run(): Unit = {
      inputIndex(compatible.toOption).write(output())
    }
  }
  addSubcommand(store)

  val intersect = new RunCmd("intersect") {
    banner("Intersect sequence files or an index with other indexes.")
    val inputs = trailArg[List[String]](descr = "Locations of additional indexes to intersect with", required = true)
    val output = opt[String](descr = "Location where the intersected index is written", required = true)
    val reducer = choice(Seq("max", "min"), default = Some("min"),
      descr = "Intersection rule for k-mer counts (default min)").map(Reducer.parseType)

    def run(): Unit = {
      val index1 = inputIndex(inputs().headOption)
      val intIdxs = inputs().map(readIndex)
      for {i <- intIdxs} index1.params.compatibilityCheck(i.params, true)
      index1.intersectAll(intIdxs, reducer()).write(output())
    }
  }
  addSubcommand(intersect)

  val union = new RunCmd("union") {
    banner("Union sequence files or an index with other indexes.")
    val inputs = trailArg[List[String]](descr = "Locations of additional indexes to union with", required = true)
    val output = opt[String](descr = "Location where the result is written", required = true)
    val reducer = choice(Seq("max", "min", "sum", "diff"), default = Some("sum"),
      descr = "Union rule for k-mer counts (default sum)").map(Reducer.parseType)

    def run(): Unit = {
      val index1 = inputIndex(inputs().headOption)
      val unionIdxs = inputs().map(readIndex)
      for {i <- unionIdxs} index1.params.compatibilityCheck(i.params, true)
      index1.unionAll(unionIdxs, reducer()).write(output())
    }
  }
  addSubcommand(union)

  val presample = new RunCmd("sample") {
    banner("Sample m-mers to generate a minimizer ordering.")
    val output = trailArg[String](required = true, descr = "Location to write the sampled ordering at")

    validate(ordering, inFiles) { (o, ifs) =>
      if (o != Frequency) Left("Sampling requires the frequency ordering (-o frequency)")
      else if (ifs.isEmpty) Left("Input files required.")
      else Right(Unit)
    }

    def run(): Unit =
      discount.kmers(inFiles() :_*).constructSampledMinimizerOrdering(output())
  }
  addSubcommand(presample)

  val reorder = new RunCmd("reorder") {
    banner(
      """|Change the minimizer ordering of an index (may reduce compression). A specific ordering can be supplied
         |with -o given and --minimizers, or an existing index can serve as the template.""".stripMargin)
    val compatible = opt[String](descr = "Location of index to copy settings from, for compatibility")
    val output = opt[String](descr = "Location where the result is written", required = true)

    override def run(): Unit = {
      val newSplitter: Broadcast[AnyMinSplitter] = compatible.toOption match {
        case Some(compatLoc) => IndexParams.read(compatLoc).bcSplit
        case _ => spark.sparkContext.broadcast(discount.getSplitter(None))
      }
      inputIndex().changeMinimizerOrdering(newSplitter).
        write(output())
    }
  }
  addSubcommand(reorder)

}

/**
 * Main API entry point for Discount.
 * Also see the command line examples in the documentation for more information on these options.
 *
 * @param k                 k-mer length
 * @param minimizers        source of minimizers. See [[MinimizerSource]]
 * @param m                 minimizer width
 * @param ordering          minimizer ordering. See [[MinimizerOrdering]]
 * @param sample            sample fraction for frequency orderings
 * @param maxSequenceLength max length of a single sequence (short reads)
 * @param normalize         whether to normalize k-mer orientation during counting. Causes every sequence to be scanned
 *                          in both forward and reverse, after which only forward orientation k-mers are kept.
 * @param method            counting method to use (or None for automatic selection)
 * @param indexBuckets      number of buckets for new indexes
 * @param spark             the SparkSession
 */
final case class Discount(k: Int, minimizers: MinimizerSource = Bundled, m: Int = 10,
                          ordering: MinimizerOrdering = Frequency, sample: Double = 0.01, maxSequenceLength: Int = 1000000,
                          normalize: Boolean = false, method: Option[CountMethod] = None,
                          indexBuckets: Int = 200)(implicit spark: SparkSession) {
    import spark.sqlContext.implicits._

  private def sampling = new Sampling
  private lazy val templateTable = MinTable.ofLength(m)

  //Validate configuration
  if (m >= k) {
    throw new Exception("m must be < k")
  }
  if (m > 32) {
    throw new Exception("m > 32 is not supported yet")
  }
  if (normalize && k % 2 == 0) {
    throw new Exception(s"normalizing mode is only supported for odd values of k (you supplied $k)")
  }

  /** Obtain an InputReader configured with settings from this object.
   * @param files Files to read. Can be a single file or multiple files.
   *              Wildcards can be used. A name of the format @list.txt
   *              will be parsed as a list of files.
   */
  def inputReader(files: String*) = new Inputs(files, k, maxSequenceLength)

  /** Load reads/sequences from files according to the settings in this object.
   * @param files  input files
   * @param addRCReads whether to add reverse complements
   */
  def getInputSequences(files: Seq[String], addRCReads: Boolean): Dataset[NTSeq] =
    getInputFragments(files, addRCReads).map(_.nucleotides)

  /** Single file version of the same method */
  def getInputSequences(file: String, addRCReads: Boolean = false): Dataset[NTSeq] =
    getInputSequences(List(file), addRCReads)

  /** Load input fragments (with sequence title and location) according to the settings in this object.
   * @param files input files
   * @param addRCReads whether to add reverse complements
   */
  def getInputFragments(files: Seq[String], addRCReads: Boolean): Dataset[InputFragment] =
    inputReader(files: _*).getInputFragments(addRCReads)

  /** Single file version of the same method */
  def getInputFragments(file: String, addRCReads: Boolean = false): Dataset[InputFragment] =
    getInputFragments(List(file), addRCReads)

  /** Load sequence titles only from the given input files */
  def sequenceTitles(input: String*): Dataset[SeqTitle] =
    inputReader(input :_*).getSequenceTitles

  /** Efficient frequency MinTable construction method.
   * The ordering of validMotifs will be preserved in the case of equally frequent motifs.
   * @param inFiles files to sample
   * @param validMotifs valid minimizers to keep (others will be ignored)
   * @param persistHashLocation location to persist the generated minimizer ordering, if any
   * @return A frequency-based MinTable
   */
  private def getFrequencyTable(inFiles: List[String], validMotifs: Seq[NTSeq],
                                persistHashLocation: Option[String] = None): MinTable = {
    val validSetTemplate = MinTable.using(validMotifs)

    //Keep ambiguous bases for efficiency - avoids a regex split
    val input = inputReader(inFiles: _*).getInputFragments(normalize, withAmbiguous = true).
      map(_.nucleotides)
    sampling.createSampledTable(input, validSetTemplate, sample, persistHashLocation)
  }

  /** Construct a read splitter for the given input files based on the settings in this object.
   * @param inFiles     Input files (for frequency orderings, which require sampling)
   * @param persistHash Location to persist the generated minimizer ordering (for frequency orderings), if any
   * @return a MinSplitter configured with a minimizer ordering and corresponding MinTable
   */
  def getSplitter(inFiles: Option[Seq[String]], persistHash: Option[String] = None):
    MinSplitter[_ <: MinimizerPriorities] = {

    (minimizers, ordering) match {
      case (All, discount.Random) =>
        val seed = Random.nextLong()
        println(s"Using RandomXOR with seed $seed")
        return MinSplitter(RandomXOR(m, Random.nextLong(), canonical = false), k)
      case _ =>
    }

    if (m > 15) {
      throw new Exception("The requested minimizer ordering can only be used with m <= 15.")
    }

    lazy val validMotifs = minimizers.load(k, m)

    val useTable = ordering match {
      case Given => MinTable.using(validMotifs)
      case Frequency =>
        getFrequencyTable(inFiles.getOrElse(List()).toList, validMotifs, persistHash)
      case Lexicographic =>
        //template is lexicographically ordered by construction
        MinTable.filteredOrdering(templateTable, validMotifs)
      case discount.Random =>
        Orderings.randomOrdering(
          MinTable.filteredOrdering(templateTable, validMotifs)
        )
      case Signature =>
        Orderings.minimizerSignatureTable(templateTable)
    }

    minimizers.finish(useTable, k)
  }

  /** Load k-mers from the given files. */
  def kmers(inFiles: String*): Kmers =
    new Kmers(this, inFiles, None)

  /** Load k-mers from the given files. */
  def kmers(knownSplitter: Broadcast[AnyMinSplitter], inFiles: String*): Kmers =
    new Kmers(this, inFiles, None, Some(knownSplitter))

  /** Sample a fraction of k-mers from the given files. */
  def sampledKmers(fraction: Double, inFiles: String*): Kmers =
    new Kmers(this, inFiles, Some(fraction))

  /** Construct an empty index, using the supplied sequence files to prepare the minimizer ordering.
   * This is useful when a frequency ordering is used and one wants to sample a large number of files in advance.
   * [[Index.newCompatible]] can then be used to construct indexes with actual data using the resulting ordering.
   * @param buckets Number of index buckets to use with Spark - for moderately sized indexes, 200 is usually fine
   * @param inFiles The input files to sample for frequency orderings
   * */
  def emptyIndex(buckets: Int, inFiles: String*): Index = {
    val splitter = new Kmers(this, inFiles, None).bcSplit
    new Index(IndexParams(splitter, buckets, ""), List[ReducibleBucket]().toDS)
  }
}

/**
 * Convenience methods for interacting with k-mers from a set of input files.
 *
 * TODO: fraction is currently unsupported. Keep or remove?
 * @param discount The Discount object
 * @param inFiles Input files
 * @param fraction Fraction of the k-mers to sample, or None for all data
 * @param spark
 */
class Kmers(val discount: Discount, val inFiles: Seq[String], fraction: Option[Double] = None,
  knownSplitter: Option[Broadcast[AnyMinSplitter]] = None)(implicit spark: SparkSession) {

  /** Broadcast of the read splitter associated with this set of inputs. */
  lazy val bcSplit: Broadcast[AnyMinSplitter] = knownSplitter.getOrElse(
    spark.sparkContext.broadcast(discount.getSplitter(Some(inFiles))))

  /** The overall method used for k-mer counting. If not specified, this will be guessed
   * from the input data according to a heuristic. */
  lazy val method: CountMethod = {
    discount.method match {
      case Some(m) => m
      case None =>
        //Auto-detect method
        //This is currently a very basic heuristic - to be refined over time
        val r = if (bcSplit.value.priorities.numLargeBuckets > 0) Pregrouped(discount.normalize) else Simple(discount.normalize)
        println(s"Counting method: $r (use --method to override)")
        r
    }
  }

  /** Input fragments associated with these inputs. */
  def inputFragments: Dataset[InputFragment] =
    discount.getInputFragments(inFiles, discount.normalize)

  def sequenceTitles: Dataset[SeqTitle] =
    discount.sequenceTitles(inFiles: _*)

  /** Sample the input data, counting minimizers and writing the generated frequency ordering to HDFS.
   * @param writeLocation Location to write the frequency ordering to
   * @return A splitter object corresponding to the generated ordering
   */
  def constructSampledMinimizerOrdering(writeLocation: String): MinSplitter[_] =
    discount.getSplitter(Some(inFiles), Some(writeLocation))

  private def inputSequences = discount.getInputSequences(inFiles, method.addRCToMainData)

  def segments: GroupedSegments =
    GroupedSegments.fromReads(inputSequences, method, bcSplit)

  /** A counting k-mer index containing all k-mers from the input sequences.
   */
  lazy val index: Index =
    GroupedSegments.fromReads(inputSequences, method, bcSplit).toIndex(discount.normalize, discount.indexBuckets)
}

object Discount extends SparkTool("Discount") {
  def main(args: Array[String]): Unit = {
    Commands.run(new DiscountConf(args)(spark))
  }
}