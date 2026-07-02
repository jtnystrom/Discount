#!/bin/bash
#Copy this file to spark-submit.sh and edit the config variables.

MASTER=local[*]
#Set this variable to the location of your Spark distribution.
SPARK=/set/spark/dir

DISCOUNT_HOME="$(dirname -- "$(readlink "${BASH_SOURCE}")")"

#For standalone mode (one process), it is helpful to provide as much memory as possible.
MEMORY=spark.driver.memory=16g

#Scratch space location. This has a big effect on performance; should ideally be a fast SSD or similar.
LOCAL_DIR="spark.local.dir=/tmp"

#Max size of input splits in bytes. A smaller number reduces memory usage but increases the number of 
#partitions for the first stage. If this variable is unset, Spark's default of 128 MB will be used.
#SPLIT="spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$((64 * 1024 * 1024))"

# --conf $SPLIT \  
exec $SPARK/bin/spark-submit \
  --driver-java-options -Dlog4j.configuration="file:$DISCOUNT_HOME/log4j.properties" \
  --conf spark.driver.maxResultSize=2g \
  --conf $MEMORY \
  --conf $LOCAL_DIR \
  --master $MASTER \
  --class com.jnpersson.discount.ReadSplitDemo "$DISCOUNT_HOME/target/scala-2.12/Discount-assembly-4.0.0.jar" $*
