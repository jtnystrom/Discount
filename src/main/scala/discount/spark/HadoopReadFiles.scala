package discount.spark

import fastdoop._
import discount.{NTSeq, SequenceID}
import discount.util.DNAHelpers
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Routines for reading input data using fastdoop.
 * @param spark
 * @param k
 */
class HadoopReadFiles(spark: SparkSession, maxReadLength: Int, k: Int) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext
  import spark.sqlContext.implicits._

  val conf = new Configuration(sc.hadoopConfiguration)
  conf.set("k", k.toString)

  //Estimate for the largest string we need to read, plus some extra space.
  val bufsiz = maxReadLength * 2 + // sequence data and quality (fastq)
    1000 //ID string and separator characters
  conf.set("look_ahead_buffer_size", bufsiz.toString)

  /* Constrain the split sizes for input files (increasing the number of splits).
   * For longer sequences in "short read files", such as the NCBI bacterial sequences refseq library,
   * large splits will cause a lot of memory pressure. This helps control the effect.
   */
//  conf.set("mapred.max.split.size", (4 * 1024 * 1024).toString)

  /*
   * Size of splits for file inputs. The default is 128 MB.
   * Reducing this number will reduce the memory requirement of the input stage but also
   * create more partitions.
   */
  conf.set("mapred.max.split.size", (16 * 1024 * 1024).toString)

  /**
   * Read short read sequence data only from the input file.
   * @param file
   * @return
   */
  def getShortReads(file: String): RDD[NTSeq] = {
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord],
        conf)
      ss.map(_._2.getValue)
    } else {
      println(s"Assuming fasta format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record],
        conf)
      ss.map(_._2.getValue.replaceAll("\n", ""))
    }
  }

  /**
   * Read short sequence data together with sequence IDs.
   * @param file
   * @return
   */
  def getShortReadsWithID(file: String): RDD[(SequenceID, NTSeq)] = {
    if (file.toLowerCase.endsWith("fq") || file.toLowerCase.endsWith("fastq")) {
      println(s"Assuming fastq format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTQInputFileFormat], classOf[Text], classOf[QRecord],
        conf)
      ss.map(r => (r._2.getKey.split(" ")(0), r._2.getValue))
    } else {
      println(s"Assuming fasta format for $file")
      val ss = sc.newAPIHadoopFile(file, classOf[FASTAshortInputFileFormat], classOf[Text], classOf[Record],
        conf)
      ss.map(r => (r._2.getKey.split(" ")(0), r._2.getValue.replaceAll("\n", "")))
    }
  }

  /**
   * Read a single long sequence.
   * @param file
   * @return
   */
  def getLongSequence(file: String): RDD[NTSeq] = {
    println(s"Assuming fasta format (long sequences) for $file")
    val ss = sc.newAPIHadoopFile(file, classOf[FASTAlongInputFileFormat], classOf[Text], classOf[PartialSequence],
      conf)
    ss.map(_._2.getValue.replaceAll("\n", ""))
  }


  //See https://sg.idtdna.com/pages/support/faqs/what-are-the-base-degeneracy-codes-that-you-use-(eg.-r-w-k-v-s)-
//  val degenerate = "[RYMKSWHBVDN]+"
  val degenerateAndUnknown = "[^ACTGU]+"

  /**
   * Load sequences from files, optionally adding reverse complements and/or sampling.
   */
  def getReadsFromFiles(fileSpec: String, withRC: Boolean,
                        sample: Option[Double] = None,
                        longSequence: Boolean = false): Dataset[NTSeq] = {
    val raw = if(longSequence)
      getLongSequence(fileSpec).toDS
    else
      getShortReads(fileSpec).toDS

    val sampled = sample match {
      case Some(s) => raw.sample(s)
      case _ => raw
    }

    val degen = this.degenerateAndUnknown
    val valid = sampled.flatMap(r => r.split(degen))

    if (withRC) {
      valid.flatMap(r => {
        Seq(r, DNAHelpers.reverseComplement(r))
      })
    } else {
      valid
    }
  }

  /**
   * Load reads with their IDs from DNA files.
   * @param fileSpec
   * @param withRC
   * @param longSequence
   * @return
   */
  def getReadsFromFilesWithID(fileSpec: String, withRC: Boolean,
                              keepDegenerate: Boolean = false,
                              longSequence: Boolean = false): Dataset[(SequenceID, NTSeq)] = {
    val raw = if(longSequence)
      ???
    else
      getShortReadsWithID(fileSpec).toDS

    val degen = this.degenerateAndUnknown
    val valid = if (keepDegenerate) {
      raw
    } else {
      raw.flatMap(r => r._2.split(degen).map(s => (r._1, s)))
    }

    if (withRC) {
      if (keepDegenerate) {
        throw new Exception("Not supported")
      }
      valid.flatMap(r => {
        Seq(r, (r._1, DNAHelpers.reverseComplement(r._2)))
      })
    } else {
      valid
    }
  }

}
