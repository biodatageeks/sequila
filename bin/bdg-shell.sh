#!/usr/bin/env bash

BDG_GRANGES_VERSION=0.1-SNAPSHOT
#cleanup
rm -rf ~/.ivy2/cache/org.biodatageeks/bdg-spark-granges_2.11/*
rm -rf ~/.ivy2/jars/org.biodatageeks_bdg-spark-granges_2.11*

#initialize params
spark-shell -i bdginit.scala --packages org.biodatageeks:bdg-spark-granges_2.11:${BDG_GRANGES_VERSION} --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots $@ -v