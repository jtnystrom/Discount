## Discount

![Maven Central](https://img.shields.io/maven-central/v/com.jnpersson/discount_2.12)
![Maven Central](https://img.shields.io/maven-central/v/com.jnpersson/discount_2.13)

Discount is a Spark-based tool for k-mer counting and analysis. 
It is able to analyse large metagenomic-scale datasets while having a small memory footprint. 
It can be used as a standalone command line tool, but also as a general Spark library, including in interactive notebooks.

![](images/discountZeppelin.png "Discount running as a Zeppelin notebook")

K-mer counting is the problem of identifying all unique sequences of length k in a genomic dataset, 
and counting how many times each one occurs. Discount is probably the most efficient k-mer counter for Spark/HDFS.
For some tasks, it is able to outperform traditional tools like [KMC3](https://github.com/refresh-bio/KMC).

This software includes [Fastdoop](https://github.com/umbfer/fastdoop) by U.F. Petrillo et al [1].
We have also included compact universal hitting sets generated by [PASHA](https://github.com/ekimb/pasha) [2].
 
For a detailed background and description, please see 
[our paper on evenly distributed k-mer binning](https://academic.oup.com/bioinformatics/advance-article/doi/10.1093/bioinformatics/btab156/6162158).

## Contents
1. [Overview](#overview)
2. [Basics](#basics)
    - [Running Discount](#running-discount)
    - [K-mer counting](#k-mer-counting)
    - [Interactive notebooks](#interactive-notebooks)
    - [Use as a library](#use-as-a-library)
    - [Tips](#tips)
3. [Advanced topics](#advanced-topics)
    - [Generating a universal hitting set](#generating-a-universal-hitting-set)
    - [Performance tuning for large datasets](#performance-tuning-for-large-datasets)
    - [Evaluation of minimizer orderings](#evaluation-of-minimizer-orderings)
    - [Compiling Discount](#compiling-discount)
    - [Citation](#citation)
    - [Contributing](#contributing)
8. [References](#references)

## Basics

[Compiling](#compiling-discount) is optional. If you prefer not to compile Discount yourself, 
you can download a pre-built release from the [Releases](https://github.com/jtnystrom/Discount/releases) page.

### Running Discount

Discount can run locally on your laptop, on a cluster, or in the cloud.
It has been tested standalone with Spark 3.1.0, and also on AWS EMR and on Google Cloud Dataproc.

To run locally, download the Spark distribution (3.0 or later) (http://spark.apache.org).

Scripts to run Discount are provided for macOS and Linux. To run locally, copy `spark-submit.sh.template` to a new file 
called `spark-submit.sh` and edit the necessary variables in the file (at a minimum, set the path to your unpacked Spark
distribution). This will be the script used to run Discount. It is also very helpful to point `LOCAL_DIR` to a fast 
drive, such as an SSD.

To run on Google Cloud Dataproc, you may use `submit-gcloud.sh.template`. In that case, change the example commands below to
use that script instead, and insert your GCloud cluster name as an additional first parameter when invoking. To run on 
AWS EMR, please use `submit-aws.sh.template` instead.

### K-mer counting

The following command produces a statistical summary of a dataset.
 
`
./spark-submit.sh --minimizers PASHA -k 55 -m 10 /path/to/data.fastq stats
`

To submit an equivalent job to Google Cloud Dataproc, after creating a cluster and uploading the necessary files
(the AWS script `submit-aws.sh` works in the same way):

`
./submit-gcloud.sh cluster-abcde --minimizers gs://my-data/PASHA -k 55 -m 10 gs://my-data/path/to/data.fastq stats
`

The parameters used here are:

* `-k` - length of k-mers
* `-m` - width of minimizers (optional parameter, the default value is 10)
* `--minimizers` - universal k-mer hitting set (see below), a directory of text files containing minimizer sets
* `data.fastq` - the input data file (multiple files can be supplied, separated by space or by comma). 

The parameters `--minimizers` and `-m` have no effect on the final result of counting, but may impact performance.
The minimizer set (universal hitting set) is used to split the input into bins.
Discount will automatically select the most appropriate set from the given directory.
A range of pre-generated sets for k >= 19 are bundled with Discount. You can also generate your own PASHA set 
([see below](#generating-a-universal-hitting-set)).

All example commands shown here accept multiple input files. The FASTQ and FASTA formats are supported, 
and must be uncompressed.

To generate a full counts table with k-mer sequences (in many cases larger than the input data),
the `count` command may be used:

`
./spark-submit.sh --minimizers PASHA -k 55 /path/to/data.fastq count -o /path/to/output/dir --sequence
`

A new directory called `/path/to/output/dir_counts` (based on the location specified with `-o`) will be created for the 
output.

Usage of upper and lower bounds filtering, histogram generation, normalization of
 k-mer orientation, and other functions, may be seen in the online help:

`
./spark-submit.sh --help
`

If the input data contains sequences longer than 1,000,000 bp, you must use the `--maxlen` flag to specify the longest
expected single sequence length. However, if the sequences in a FASTA file are very long (for example full chromosomes),
it is essential to generate a FASTA index (.fai). Various tools can be used to do this, for example with 
[SeqKit](https://github.com/shenwei356/seqkit):

`
seqkit faidx myChromosomes.fasta
`

Discount will detect the presence of the `myChromosomes.fasta.fai` file and read the data efficiently. In this case, the parameter `--maxlen` is 
not necessary. 

### Interactive notebooks
Discount is well suited for data analysis in interactive notebooks, and as of version 2.0 the API has been 
redesigned with this in mind. A demo notebook for [Apache Zeppelin](https://zeppelin.apache.org/) is included in the 
`notebooks/` directory. It has been tested with Zeppelin 0.10 and Spark 3.1.
To try this out, after downloading the Spark distribution, also [download Zeppelin](https://zeppelin.apache.org/).  
(The smaller "Netinst" distribution is sufficient, but as of Zeppelin 0.10 an external Spark is necessary.)
Then, load the notebook itself into Zeppelin through the browser to see example use cases and instructions.

### Use as a library
You can add Discount as a dependency using the following syntax (SBT):

`
 libraryDependencies += "com.jnpersson" %% "discount" % "2.2.1"
`

API docs for the current release are [available here](https://jtnystrom.github.io/Discount/com/jnpersson/discount/spark/index.html).

### Tips
* Visiting http://localhost:4040 (if you run a standalone Spark cluster) in a browser will show progress details while
  Discount is running.
  
* If you are running a local standalone Spark (everything in one process) then it is helpful to increase driver memory 
as much as possible (this can be configured in spark-submit.sh). Pointing LOCAL_DIR to a fast drive for temporary data 
  storage is also highly recommended.
  
* The number of files generated in the output tables will correspond to the number of partitions Spark uses, which you 
  can configure in the run scripts. However, we recommend configuring partitions for performance/memory usage 
  (the default value of 200 is usually fine) and manually joining the files later if you need to.

## Advanced topics 

### Generating a universal hitting set

Discount needs a compact universal hitting set (of m-mers) for optimal performance. Such sets determine what m-mers may 
become minimizers, which become bins. These sets are passed to Discount using the `--minimizers` argument.
They may be generated using the [PASHA](https://github.com/ekimb/pasha) tool.
Precomputed sets for many values of k and m may also be downloaded from the 
[PASHA website](http://pasha.csail.mit.edu/).
(Note that the PASHA authors use the symbols (k, L) instead of (m, k), which we use here. 
Their k corresponds to minimizer length, which we denote by m.)

A universal set generated for some pair of parameters (k, m) will also work for a larger k. However, the larger the gap, 
the greater the number of bins generated by Discount and the shorter the superkmers would be. This can negatively impact 
performance.

In either case, computed sets must be combined with their corresponding decycling set (available at the link above), 
for example as follows:

`
cat PASHA11_30.txt decyc11.txt > minimizers_30_11.txt
`

This produces a set that is ready for use with Discount.

### Performance tuning for large datasets

The philosophy of Discount is to achieve performance through a large number of small and evenly sized bins, 
which are grouped into a large number of modestly sized Spark partitions. This reduces memory pressure as well as CPU 
time for many of the algorithms that need to run. For most datasets, the default settings should be fine.
However, for huge datasets or constrained environments, the pointers below may be helpful.


1. Increase m. For very large datasets, with e.g. more than 10<sup>11</sup> total k-mers, m=11 (or more) may be helpful.
   This would generate a larger number of bins (which would be smaller) by using a larger universal hitting set.
   However, if m is too large relative to the dataset, then a slowdown may be expected.
2. Increase the number of partitions. This can be done in the run scripts. However, if the number is 
   too large, shuffling will be slower, sometimes dramatically so.
3. Increase the number of input splits by reducing the maximum split size. This affects the number of tasks in the 
   hashing stage. This can also be done in the run scripts. The same caveat as above applies. 

### Evaluation of minimizer orderings

Discount can be used to evaluate the bin distributions generated by various minimizer orderings.
See [docs/Minimizers.md](docs/Minimizers.md) for details.

### Compiling Discount

To compile the software, the SBT build tool (https://www.scala-sbt.org/) is needed.
Discount is by default compiled for Scala 2.12/Spark 3.1.
(It is possible to compile for Scala 2.11/Spark 2.4.x by editing build.sbt and the various run scripts according to the comments in those 
files.)

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

