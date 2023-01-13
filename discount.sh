#!/bin/bash
#Command-line run script for the Discount k-mer counter.
#This script (as well as the similar scripts in this directory) can be symlinked to somewhere in $PATH for convenient use.

#Run everything in one process (don't forget to adjust Spark's driver memory)
MASTER=local[*]

#Full cluster running independently
#MASTER=spark://localhost:7077

#Set this variable to the location of your Spark distribution.
SPARK=/path/to/spark-x.x.x-hadoop

#To reduce memory usage for big inputs, increase PARTITIONS. Default is 200 if unset.
#PARTITIONS="spark.sql.shuffle.partitions=4000"

DISCOUNT_HOME="$(dirname -- "$(readlink -f "${BASH_SOURCE}")")"

#For standalone mode (one process), it is helpful to provide as much memory as possible.
MEMORY=spark.driver.memory=16g

#Scratch space location. This has a big effect on performance; should ideally be a fast SSD or similar.
LOCAL_DIR="spark.local.dir=/tmp"

#Max size of input splits in bytes. A smaller number reduces memory usage but increases the number of 
#partitions for the first stage. If this variable is unset, Spark's default of 128 MB will be used.
#SPLIT="spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$((64 * 1024 * 1024))"

#On Windows: Change bin/spark-submit to bin/spark-submit.cmd.

#--conf $SPLIT
#--conf $PARTITIONS
exec $SPARK/bin/spark-submit \
  --driver-java-options -Dlog4j.configuration="file:$DISCOUNT_HOME/log4j.properties" \
  --conf spark.driver.maxResultSize=2g \
  --conf $MEMORY \
  --conf $LOCAL_DIR \
  --master $MASTER \
  --class com.jnpersson.hypercut.Hypercut "$DISCOUNT_HOME/target/scala-2.12/Discount-assembly-2.3.0.jar" $*