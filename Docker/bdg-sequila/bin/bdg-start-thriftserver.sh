#!/usr/bin/env bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
$SPARK_HOME/sbin/start-thriftserver.sh --conf spark.sql.hive.thriftServer.singleSession=true \
spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-8-oracle --packages org.biodatageeks:bdg-spark-granges_2.11:${BGD_VERSION} \
--repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ $@