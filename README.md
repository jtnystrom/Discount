## Discount

[![Maven Central](https://img.shields.io/maven-central/v/com.jnpersson/discount_2.12)](https://search.maven.org/artifact/com.jnpersson/discount_2.12)
[![Build and test](https://github.com/jtnystrom/discount/actions/workflows/ci.yml/badge.svg)](https://github.com/jtnystrom/Discount/actions/workflows/ci.yml)

Discount is a Spark-based tool for k-mer (genomic sequences of length k) counting and analysis. 
It is able to analyse large metagenomic-scale datasets while having a small memory footprint. 
It can be used as a standalone command line tool, but also as a general Spark library, including in interactive 
notebooks.

![](images/discountZeppelin.png "Discount running as a Zeppelin notebook")

Discount is a scalable distributed k-mer counter for Spark/HDFS.
It has been tested on the [Serratus](https://serratus.io/) dataset for a total of 5.59 trillion k-mers (5.59 x 10^12) 
with 1.57 trillion distinct k-mers.


This software includes [Fastdoop](https://github.com/umbfer/fastdoop) by U.F. Petrillo et al [1].
We have also included compact universal hitting sets generated by [PASHA](https://github.com/ekimb/pasha) [2].
 
For a detailed background and description, please see 
[our paper on evenly distributed k-mer binning](https://academic.oup.com/bioinformatics/advance-article/doi/10.1093/bioinformatics/btab156/6162158).

## Contents
1. [Basics](#basics)
    - [Running Discount](#running-discount)
    - [K-mer counting](#k-mer-counting)
    - [Index operations](#k-mer-indexes)
    - [Interactive notebooks and REPL](#interactive-notebooks-and-repl)
    - [Use as a library](#use-as-a-library)
    - [Tips](#tips)
    - [License and support](#license-and-support)
2. [Advanced topics](#advanced-topics)
    - [Minimizers](#minimizers)
    - [Generating a universal hitting set](#generating-a-universal-hitting-set)
    - [Evaluation of minimizer orderings](#evaluation-of-minimizer-orderings)
    - [Performance tuning for large datasets](#performance-tuning-for-large-datasets)
    - [Compiling Discount](#compiling-discount)
    - [Citation](#citation)
    - [Contributing](#contributing)
3. [References](#references)

## Basics

[Compiling](#compiling-discount) is optional. If you prefer not to compile Discount yourself, 
you can download a pre-built release from the [Releases](https://github.com/jtnystrom/Discount/releases) page.

### Running Discount

Discount can run locally on your laptop, on a cluster, or on cloud platforms that support Spark
(tested on AWS EMR and Google Cloud Dataproc).

To run locally, download the Spark distribution (3.0 or later) (http://spark.apache.org).

Scripts to run Discount are provided for macOS and Linux. To run locally, edit the file `discount.sh` and set the path 
to your unpacked Spark distribution). This will be the script used to run Discount. Other critical settings can also be 
changed in this file. It is very helpful to point `LOCAL_DIR` to a fast drive, such as an SSD.

To run on AWS EMR, you may use `discount-aws.sh`. In that case, change the example commands below to
use that script instead, and insert your EMR cluster name as an additional first parameter when invoking. To run on 
Google Cloud Dataproc, please use `discount-gcloud.sh` instead.

### K-mer counting

The following command produces a statistical summary of a dataset.
 
`
./discount.sh -k 55 /path/to/data.fastq stats
`

All example commands shown here accept multiple input files. The FASTQ and FASTA formats are supported,
and must be uncompressed.

To submit an equivalent job to AWS EMR, after creating a cluster with id j-ABCDEF1234 and uploading the necessary files
(the GCloud script `discount-gcloud.sh` works in the same way):

`
./discount-aws.sh j-ABCDEF1234 -k 55 s3://my-data/path/to/data.fastq stats
`

As of version 2.3, minimizer sets for k >=19, m=10,11 are bundled with Discount and do not need to be specified
explicitly. Advanced users may wish to override this ([see the section on minimizers](#minimizers))

To generate a full counts table with k-mer sequences (in many cases larger than the input data),
the `count` command may be used:

`
./discount.sh -k 55 /path/to/data.fastq count -o /path/to/output/dir --sequence
`

A new directory called `/path/to/output/dir_counts` (based on the location specified with `-o`) will be created for the 
output.

Usage of upper and lower bounds filtering, histogram generation, normalization of
 k-mer orientation, and other functions, may be seen in the online help:

`
./discount.sh --help
`

#### Chromosomes and very long sequences

If the input data contains sequences longer than 1,000,000 bp, you must use the `--maxlen` flag to specify the longest
expected single sequence length. However, if the sequences in a FASTA file are very long (for example full chromosomes),
it is essential to generate a FASTA index (.fai). Various tools can be used to do this, for example with 
[SeqKit](https://github.com/shenwei356/seqkit):

`
seqkit faidx myChromosomes.fasta
`

Discount will detect the presence of the `myChromosomes.fasta.fai` file and read the data efficiently. In this case, 
the parameter `--maxlen` is not necessary.

#### Repetitive or very large datasets

As of version 2.3, Discount contains two different counting methods, the "simple" method, which was the only method 
prior to this version, and the "pregrouped" method, which is essential for data that contains highly repetitive k-mers.
The pregrouped method counts each distinct super-mer separately prior to k-mer counting.
Discount will try to pick the best method automatically, but we would advise users to do their own experiments. 
If Spark crashes with an exception about buffers being too large, the pregrouped method may also help. It can be forced 
with a command such as:

`
./discount.sh --method pregrouped -k 55 /path/to/data.fastq stats
`

Or, to force the simple method to be used:

`
./discount.sh --method simple -k 55 /path/to/data.fastq stats
`

While highly scalable, the pregrouped method may sometimes cause a slowdown overall (by requiring one additional shuffle), 
so it should not be used for datasets that do not need it. See the section on [performance tuning](#performance-tuning-for-large-datasets).

### K-mer indexes

Discount can store a multiset of counted k-mers as an index (k-mer database). Indexes can be combined by various 
operations, inspired by the design of `kmc_tools` in [KMC3](https://github.com/refresh-bio/KMC).
They are stored in the Apache Parquet format, allowing for a high degree of compression and efficiency.

To create a new index, the `store` command may be used:

`
discount.sh -k 35 input.fasta store -o index_path
`

The directory `index_path` will be created and index files will be written to it (or overwritten if they already existed). 
Alongside it, the files `index_path_minimizers.txt` and `index_path.properties` will record the minimizer ordering and 
some other parameters of the index. These files should not be manually edited or moved.

By using the `-i` parameter, an index can be used instead of sequence files as a source of input data. 
For example, k-mers with minimum count 2 can be obtained from an index and written to a set of fasta files by using the 
`count` command from above in the following way:

`
discount.sh -i index_path --min 2 count -o index_min2
`

Only one index can be used as an input at once. When a new index is created (like `index_min2` above) it should always be 
written in a new location. Overwriting the input location from the same command may lead to data loss (the same location 
can not simultaneously be both an input and an output).

Indexes may be combined using binary operations such as `intersect`, `union`, and `subtract`. For example, to create the 
intersection of two indexes using the minimum count from either index:

`
discount.sh -i index1_path intersect -i index2_path -r min -o i1i2_min_path
`

The `min` rule is the default for intersection. Other intersection rules are `max`, `left`, `right` and `sum`.

Multiple indexes may be combined at once with the same rule. For example, to union three indexes at once with the 
maximum rule:

`
discount.sh -i index1_path union -r max -i index2_path index3_path -o union3_path
`

For additional guidance, consult the command line help for each command, e.g.:

`
discount.sh intersect --help
`

#### Partitions

For each index, a number of parquet files will be created in the corresponding directory. The number of partitions 
corresponds to the number of *shuffle partitions* that Spark uses. To set the number of partitions, the `-p` argument 
may be used. For example, for a very large index, creating 10,000 partitions may be helpful: 

`
discount.sh -k 35 -p 10000 input.fasta store -o index_path
`

This should be tuned so that the resulting files are neither too large nor too small.
The `reindex` command can be used to change the number of partitions of an existing index after 
construction.
Also see [the section on performance tuning](#performance-tuning-for-large-datasets) below.

#### Compatible indexes
The combination operations only work if the indexes being combined have the same values of `k` and `m`, and were created
using the same minimizer ordering (called a compatible index).  
To help create a compatible index, the `-c` flag is provided for the store operation. For example:

`
discount.sh input2.fasta store -c index1_path -o index2_path
`

Here, index settings will be copied from `index1_path` and reused, which creates a compatible index when indexing 
`input2.fasta` into index2.

When necessary, a pre-existing index can be converted to a different minimizer ordering using the reindex command, for 
example in the following way:

`
discount.sh -i index1_path reindex -c index2_path -o index3_path
`

This will create a new copy of index1 according to the parameters in index2, saving it as index3. After this step, 
index2 and index3 can be combined. However, this will usually reduce the level of data
compression, so it is recommended to avoid reindexing when possible.

### Interactive notebooks and REPL
Discount is well suited for data analysis in interactive notebooks. A demo notebook for [Apache Zeppelin](https://zeppelin.apache.org/) is included in the 
`notebooks/` directory. It has been tested with Zeppelin 0.10.1 and Spark 3.1.2.
(As of Zeppelin 0.10.1, beware that Spark versions above 3.1.2 are not supported out of the box, so we recommend using that version for notebooks.)

To try this out, after downloading the Spark distribution, also [download Zeppelin](https://zeppelin.apache.org/).  
(The smaller "Netinst" distribution is sufficient, but an external Spark is necessary.)
Then, load the notebook itself into Zeppelin through the browser to see example use cases and instructions.

The API examples from the notebook can also for the most part be used unchanged in the Spark shell (Scala REPL).
For example, to intersect k-mers from two sequence files, filtering the k-mer counts of one of them, after starting the shell using `discount-shell.sh`:

```scala
import com.jnpersson.discount.spark._
implicit val sp = spark
val discount = new Discount(k = 28)
val discountRoot = "/path/to/Discount"
val i1 = discount.index(s"$discountRoot/testData/SRR094926_10k.fasta")
val i2 = discount.index(i1, s"$discountRoot/testData/ERR599052_10k.fastq")
i1.intersect(i2.filterMin(2), Rule.Max).showStats()
```

Using the tool from the REPL in this way, instead of the command-line, can be an efficient alternative when working with temporary indexes, as they can be 
available for use in-memory without being stored on disk.

For both notebook and REPL use, it is recommended to consult the API docs
([available for the latest release here](https://jtnystrom.github.io/Discount/com/jnpersson/discount/spark/index.html)).

### Use as a library
You can add Discount as a dependency using the following syntax (SBT):

`
 libraryDependencies += "com.jnpersson" %% "discount" % "3.0.0"
`

Please note that Discount is still under heavy development and the API may change slightly even between minor versions.

### Tips
* Visiting http://localhost:4040 (if you run a standalone Spark cluster) in a browser will show progress details while
  Discount is running.
  
* If you are running a local standalone Spark (everything in one process) then it is helpful to increase driver memory 
as much as possible (this can be configured in discount.sh). Pointing LOCAL_DIR to a fast drive for temporary data 
  storage is also highly recommended.
  
* The number of files generated in the output tables will correspond to the number of partitions Spark uses, which you 
  can configure in the run scripts. However, we recommend configuring partitions for performance/memory usage 
  (the default value of 200 is usually fine) and manually joining the files later if you need to.

### License and support

Discount is available under a dual GPL/commercial license. For a commercial license, custom development, or commercial support 
please contact JNP Solutions at [info@jnpsolutions.io](mailto:info@jnpsolutions.io). We will also do our best to respond to non-commercial
inquiries on a best-effort basis.

## Advanced topics 

### Minimizers

Discount counts k-mers by constructing super k-mers (supermers) and shuffling these into bins. Each bin corresponds to 
a minimizer, which is the minimal m-mer for some m < k in each k-mer, for some ordering of a minimizer set.
The choice of minimizer set and ordering does not affect k-mer counting results, but can have a big effect on performance. 
By default, Discount will use internal minimizers, which are packaged into the jar from the [resources/PASHA](resources/PASHA) 
directory. These are universal hitting sets, available for k >= 19, m=9,10,11. 
By default, they will be ordered by sampled frequency in the dataset being analysed, prioritising uncommon minimizers 
over common ones.

We provide some additional minimizer sets at
https://jnpsolutions.io/minimizers. You can also generate your own set using PASHA (described below).

To manually select a minimizer set, it is possible to point Discount to a file containing a set, or to a directory 
containing minimizer sets. For example:

`
./discount.sh -m 10 --minimizers resources/PASHA/minimizers_55_10.txt -k 55 /path/to/data.fastq stats
`

In this case, minimizers have length 10 (m=10) and the supplied minimizer set will work for any k >= 55.

If you instead supply a directory, the best minimizer set in that directory will be chosen automatically,
by looking for files with the name minimizers_{k}_{m}.txt:

`
./discount.sh -m 10 --minimizers resources/PASHA -k 55 /path/to/data.fastq stats
`

It is also possible (but less efficient ) to operate without a minimizer set, in which case all m-mers will become 
minimizers.  This can be done using the `--allMinimizers` flag. Currently, this flag is must be used when k < 19 as we 
do not supply minimizer sets in that range:

`
./discount.sh -k 17 --allMinimizers /path/to/data.fastq stats
`

### Generating a universal hitting set

For optimal performance, compact universal hitting sets (of m-mers) should be used as minimizer sets.
They may be generated using the [PASHA](https://github.com/ekimb/pasha) tool.
Precomputed sets for many values of k and m may also be downloaded from the 
[PASHA website](http://pasha.csail.mit.edu/).
(Note that the PASHA authors use the symbols (k, L) instead of (m, k), which we use here. 
Their k corresponds to minimizer length, which we denote by m.)

A universal set generated for some pair of parameters (k, m) will also work for a larger k. However, the larger the gap, 
the greater the number of bins generated by Discount and the shorter the superkmers would be. This can negatively impact 
performance.

Computed sets must be combined with their corresponding decycling set (available at the link above), 
for example as follows:

`
cat PASHA11_30.txt decyc11.txt > minimizers_30_11.txt
`

This produces a set that is ready for use with Discount.

### Evaluation of minimizer orderings

Discount can be used to evaluate the bin distributions generated by various minimizer orderings.
See [docs/Minimizers.md](docs/Minimizers.md) for details.

### Performance tuning for large datasets

The philosophy of Discount is to achieve performance through a large number of small and evenly sized bins, 
which are grouped into a large number of modestly sized Spark partitions. This reduces memory pressure as well as CPU 
time for many of the algorithms that need to run. For most datasets, the default settings should be fine.
However, for huge datasets or constrained environments, the pointers below may be helpful.

1. Increase m. For very large datasets, with e.g. more than 10<sup>11</sup> total k-mers, m=11 (or more) may be helpful.
   This would generate a larger number of bins (which would be smaller) by using a larger universal hitting set.
   However, if m is too large relative to the dataset, then a slowdown may be expected. As of version 2.3, we have 
   tested up to m=13.   
2. Increase the number of partitions using the `-p` argument. However, if the number is 
   too large, shuffling will be slower, sometimes dramatically so.
3. Increase the number of input splits by reducing the maximum split size. This affects the number of tasks in the 
   hashing stage. This can be done in the run scripts, however the same caveat as above applies: the number of tasks 
   should not be too large for the data.

For very large datasets, it is helpful to understand where the difficulties come from. For a repetitive dataset, 
using `--method pregrouped` will have large benefits. On the other hand, for a highly complex dataset with many distinct k-mers, 
increasing m can help by spreading the k-mers into more bins. For some datasets, it may be
necessary to use both of these techniques.

In general, it is helpful to monitor CPU usage to make sure that the job is not I/O bound (if it is well configured, 
CPU utilisation should be close to 100% on average). 
To help with I/O pressure, fast SSDs and/or different partition sizes may help.

If memory pressure is too high (high GC time), then assigning more 
memory or increasing m may help.

### Compiling Discount

To compile the software, the SBT build tool (https://www.scala-sbt.org/) is needed.
Discount is by default compiled for Scala 2.12/Spark 3.1. An experimental Scala 2.13 branch is also available.

The command `sbt assembly` will compile the software and produce the necessary jar file in
target/scala-2.12/Discount-assembly-x.x.x.jar. This will be a "fat" jar that also contains some necessary dependencies.


API documentation may be generated using the command `sbt doc`.
### Citation

If you find Discount useful in your research, please cite our paper:

Johan Nyström-Persson, Gabriel Keeble-Gagnère, Niamat Zawad, Compact and evenly distributed k-mer binning for genomic 
sequences, Bioinformatics, 2021;, btab156, https://doi.org/10.1093/bioinformatics/btab156

### Contributing

Contributions are very welcome, for example in the form of bug reports,
pull requests, or general suggestions.

### References

1. Petrillo, U. F., Roscigno, G., Cattaneo, G., & Giancarlo, R. (2017). FASTdoop: A versatile and efficient library for 
   the input of FASTA and FASTQ files for MapReduce Hadoop bioinformatics applications. Bioinformatics, 33(10), 
   1575–1577.
2. Ekim, B.et al.(2020).  A Randomized Parallel Algorithm for Efficiently Finding Near-Optimal Universal Hitting Sets.  
   In R. Schwartz,  editor, Research  in  Computational  Molecular  Biology,  pages  37–53,  Cham.Springer International 
   Publishing 

