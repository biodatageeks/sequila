#!/bin/bash -x

for i in "$@"
do
case $i in
    -m=*|--master=*)
    MASTER="${i#*=}"
    ;;
    -ne=*|--num-executors=*)
    NUM_EXECUTORS="${i#*=}"
    ;;
    -em=*|--executor-memory=*)
    EXECUTOR_MEMORY="${i#*=}"
    ;;
    -ec=*|--executor-cores=*)
    EXECUTOR_CORES="${i#*=}"
    ;;
   -dm=*|--driver-memory=*)
    DRIVER_MEMORY="${i#*=}"
    ;;
     -s=*|--script=*)
    SCRIPT="${i#*=}"
   ;;
    -sh=*|--spark-home=*)
    SPARK_HOME="${i#*=}"
    ;;
   -it=*|--iterations=*)
    ITER="${i#*=}"
    ;;

    *)
# unknown option
    ;;
esac
done

#defaults
if [[ -z $MASTER ]]; then
    MASTER="local[1]"
fi

if [[ -z $NUM_EXECUTORS ]]; then
    NUM_EXECUTORS=1
fi

if [[ -z $EXECUTOR_MEMORY ]]; then
    EXECUTOR_MEMORY=1g
fi

if [[ -z $EXECUTOR_CORES ]]; then
    EXECUTOR_CORES=1
fi

if [[ -z $DRIVER_MEMORY ]]; then
    DRIVER_MEMORY=1g
fi

if [[ -z $ITER ]]; then
    ITER=1
fi

export SPARK_HOME=${SPARK_HOME}

export PATH=$SPARK_HOME/bin:$PATH

#cleanup
rm -rf ~/.ivy2/cache/org.biodatageeks/bdg-spark-granges_2.11/*
rm -rf ~/.ivy2/jars/org.biodatageeks_bdg-spark-granges_2.11*

seq 0 $ITER | xargs -I {} spark-shell --verbose --packages org.biodatageeks:bdg-spark-granges_2.11:0.1-SNAPSHOT \
--repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots --master ${MASTER} \
--driver-memory ${DRIVER_MEMORY} --num-executors ${NUM_EXECUTORS} --executor-memory ${EXECUTOR_MEMORY} \
--executor-cores ${EXECUTOR_CORES} -i ${SCRIPT}