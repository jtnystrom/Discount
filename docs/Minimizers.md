### Minimizer ordering evaluation

In addition to k-mer counting, Discount can also be used to evaluate the efficiency of various minimizer orderings
and minimizer sets, by outputting the k-mer bin distribution for a given dataset in detail.
For example:

`
./spark-submit.sh --minimizers PASHA/pasha_all_55_10.txt -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

The `--buckets` flag enables this mode. Other parameters are the same as above. A new directory called
`/path/to/output/dir_bucketStats` will be created for the output.
Each line in the output file will represent a single k-mer bin. The output files will contain six columns, which are:
Bin minimizer, number of superkmers, total number of k-mers, distinct k-mers, unique k-mers, maximum abundance for a
single k-mer. For example:

```
AGGGGTGCGA      1       9       9       9       1
CAAGGTCCAG      2       31      31      31      1
GAAGGTCCCA      1       6       6       6       1
CAAGGTGACC      1       19      19      19      1
```

Consult the documentation of the class `com.jnpersson.discount.bucket.BucketStats` for further details.

The above example uses the universal frequency ordering, which is the one we recommend for efficient k-mer counting.
The commands below can be used to enable other orderings. Please see our paper (linked above) for definitions of these
orderings.

Universal hitting set, lexicographic ordering, enabled by `-o lexicographic`:

`
./spark-submit.sh --minimizers PASHA/pasha_all_55_10.txt -o lexicographic -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

Universal hitting set, random ordering, enabled by `-o random`:

`
./spark-submit.sh --minimizers PASHA/pasha_all_55_10.txt -o random -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

Since version 2.3, every ordering that does not use a universal hitting set (bundled or user specified) needs the 
`--allMinimizers` flag to enable the use of all m-mers. For example, minimizer signature ordering, enabled by `-o signature`:

`
./spark-submit.sh --allMinimizers -o signature -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

Random ordering (all m-mers):

`
./spark-submit.sh --allMinimizers -o random -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

Frequency sampled (implicit when the first `-o` is not specified), all m-mers:

`
./spark-submit.sh --allMinimizers -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

A user-defined minimizer set and ordering (as given in the file, ordered from high to low priority), enabled by `-o given`:

`
./spark-submit.sh --minimizers my_minimizers.txt -o given -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`
