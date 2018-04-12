#!/usr/bin/env bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
$SPARK_HOME/sbin/stop-thriftserver.sh