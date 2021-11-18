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
single k-mer. 
Consult the documentation for the class `com.jnpersson.discount.bucket.BucketStats` for details.

The above example uses the universal frequency ordering, which is the one we recommend for efficient k-mer counting.
The commands below can be used to enable other orderings. Please see our paper (linked above) for definitions of these
orderings.

Universal set ordering (lexicographic), enabled by `-o lexicographic`:

`
./spark-submit.sh --minimizers PASHA/pasha_all_55_10.txt -o lexicographic -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

Universal set ordering (random), enabled by `-o random`:

`
./spark-submit.sh --minimizers PASHA/pasha_all_55_10.txt -o random -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

Minimizer signature, enabled by `-o signature`, no `--minimizers` needed:

`
./spark-submit.sh -o signature -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

Random (all m-mers):

`
./spark-submit.sh -o random -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

User-defined ordering (as given in the file, ordered from high to low priority), enabled by `-o given`:

`
./spark-submit.sh --minimizers my_minimizers.txt -o given -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`

The frequency-sampled (all m-mers) ordering is the default if no other flags are supplied:

`
./spark-submit.sh -k 55 -m 10 /path/to/data.fastq count -o /path/to/output/dir --buckets
`
