#!/usr/bin/env bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
#$SPARK_HOME/sbin/start-thriftserver.sh --conf spark.sql.hive.thriftServer.singleSession=true \
#spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-8-oracle --packages org.biodatageeks:bdg-spark-granges_2.11:${BDG_VERSION} \
#--repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ $@



set -o posix

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# NOTE: This exact class name is matched downstream by SparkSubmit.
# Any changes need to be reflected there.
CLASS="org.apache.spark.sql.hive.thriftserver.SequilaThriftServer"

function usage {
  echo "Usage: ./sbin/start-thriftserver [options] [thrift server options]"
  pattern="usage"
  pattern+="\|Spark assembly has been built with Hive"
  pattern+="\|NOTE: SPARK_PREPEND_CLASSES is set"
  pattern+="\|Spark Command: "
  pattern+="\|======="
  pattern+="\|--help"

  "${SPARK_HOME}"/bin/spark-submit --help 2>&1 | grep -v Usage 1>&2
  echo
  echo "Thrift server options:"
  "${SPARK_HOME}"/bin/spark-class $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

export SUBMIT_USAGE_FUNCTION=usage

###Download fresh assembly from maven
echo "Downloading SeQuiLa jar please wait..."
if [[ $BDG_VERSION =~ SNAPSHOT ]]; then
    wget https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/org/biodatageeks/bdg-sequila_2.11/${BDG_VERSION}/bdg-sequila_2.11-${BDG_VERSION}-assembly.jar -O /tmp/bdg-sequila-assembly-${BDG_VERSION}.jar
else wget https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/org/biodatageeks/bdg-sequila_2.11/${BDG_VERSION}/bdg-sequila_2.11-${BDG_VERSION}-assembly.jar -O /tmp/bdg-sequila-assembly-${BDG_VERSION}.jar
fi

#--conf spark.sql.catalogImplementation=hive


#"-Dderby.system.home=${SEQ_METASTORE_LOCATION}/derby"
export SEQ_METASTORE_LOCATION=/data/output
#exec "${SPARK_HOME}"/bin/spark-shell -i /tmp/bdg-toolset/sequilathriftinit.scala  --conf spark.sql.warehouse.dir=/data/input/bams --jars /tmp/bdg-sequila-assembly-${BDG_VERSION}.jar

exec "${SPARK_HOME}"/bin/spark-submit --conf spark.driver.extraJavaOptions="-Dseq.metastore.dir=${SEQ_METASTORE_LOCATION}/metastore" --class $CLASS --name "Thrift JDBC/ODBC Server"  \
--conf spark.sql.hive.thriftServer.singleSession=true  "$@" /tmp/bdg-sequila-assembly-${BDG_VERSION}.jar

PG_PID=$(ps -o pid,cmd -C java | grep "org.apache.spark.sql.hive.thriftserver.SequilaThriftServer" | sed -e 's/^[ \t]*//' | cut -d' ' -f 1)
while [ -e /proc/${PG_PID} ]; do sleep 100; done