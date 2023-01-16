#!/bin/bash
#Script to submit as an Amazon AWS EMR step. 

#For this script to work, it is necessary to install and configure the AWS CLI.

#The first argument is the cluster ID. The remaining arguments will be passed to the Discount driver process.
CLUSTER=$1
shift

#Bucket to store discount jars and data files
BUCKET=s3://my-bucket/discount

DISCOUNT_HOME="$(dirname -- "$(readlink "${BASH_SOURCE}")")"

aws s3 cp "$DISCOUNT_HOME/target/scala-2.13/Discount-assembly-3.0.0.jar" $BUCKET/

#aws s3 sync "$DISCOUNT_HOME/resources/PASHA" $BUCKET/PASHA/

#Max size of input splits in bytes. A smaller number reduces memory usage but increases the number of 
#partitions for the first stage. If this variable is unset, Spark's default of 128 MB will be used.
#SPLIT="spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$((64 * 1024 * 1024))"

#To set SPLIT or other variables, uncomment below.
COMMAND=( \
#  --conf $SPLIT \
  --class com.jnpersson.discount.spark.Discount $BUCKET/Discount-assembly-3.0.0.jar $*)

#Turn off paging for output
export AWS_PAGER=""

RUNNER_ARGS="spark-submit"
for PARAM in ${COMMAND[@]}
do
  RUNNER_ARGS="$RUNNER_ARGS,$PARAM"
done

aws emr add-steps --cluster $CLUSTER --steps Type=CUSTOM_JAR,Name=Discount,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=\[$RUNNER_ARGS\]
