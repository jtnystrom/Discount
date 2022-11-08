#!/bin/bash
CLUSTER=$1
shift

aws emr list-steps --cluster-id $CLUSTER --step-states PENDING | jq '.["Steps"] | .[] | .["Id"]' |while read id
do
aws emr cancel-steps --step-ids $(echo $id | sed s/\"//g)  --cluster-id $CLUSTER
done


