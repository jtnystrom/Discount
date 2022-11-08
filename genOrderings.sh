#!/bin/bash

FRACTIONS=(0.1 1)
MS=(9 10 11)

FILE=$1
shift
K=28
OUTPREF=$1
shift
SUF=minimizers_sample.txt

for M in ${MS[@]}
do
  for F in ${FRACTIONS[@]}
  do
    ARGS="--maxlen 100000000 --sample $F -m $M -k $K"
    NAME=${OUTPREF}_${K}_${M}_$F
    [ ! -f ${NAME}_$SUF ] && ./spark-submit.sh $ARGS $FILE sample $NAME
    NAME=${OUTPREF}_all_${M}_$F
    [ ! -f ${NAME}_$SUF ] && ./spark-submit.sh --allMinimizers $ARGS $FILE sample $NAME
  done
done

