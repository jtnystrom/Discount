#!/bin/bash
#Script to submit to a GCloud dataproc cluster. 
#For this script to work, it is necessary to install and configure the GCloud CLI.

#Region where the cluster is running (change as needed)
REGION=asia-northeast1

#The first argument is the cluster ID. The remaining arguments will be passed to the Discount driver process.
CLUSTER=$1
shift

MAXRES=spark.driver.maxResultSize=3g

#Max size of input splits in bytes. A smaller number reduces memory usage but increases the number of
#partitions for the first stage. If this variable is unset, a reasonable default will be used.
#SPLIT=spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$((64 * 1024 * 1024))

#YARN memory is allocated using on GCP using executor memory and memoryOverhead.
#The number of executors that will be spawned by YARN is (total memory)/(executor memory + memoryOverhead).
#Furthermore, on GCP every executor runs two task threads by default.
#Below are some suggested settings for EXECMEM and OVERHEAD. It is also safe to leave them blank,
#in which case the GCP environment will supply some default settings.

#The two settings below are suitable for k-mer counting on highcpu 16-core nodes.
#They also work well for standard 4-core nodes.
#OVERHEAD=spark.executor.memoryOverhead=768
#EXECMEM=spark.executor.memory=4352m

#Include custom settings here to apply them
#PROPERTIES=$MAXRES,$SPLIT,$OVERHEAD,$EXECMEM
PROPERTIES=$MAXRES

DISCOUNT_HOME="$(dirname -- "$(readlink "${BASH_SOURCE}")")"

exec gcloud --verbosity=info  dataproc jobs submit spark --region $REGION --cluster $CLUSTER \
  --class com.jnpersson.discount.spark.Discount --jars "$DISCOUNT_HOME/target/scala-2.13/Discount-assembly-3.0.1.jar" \
  --properties $PROPERTIES -- "$@"

