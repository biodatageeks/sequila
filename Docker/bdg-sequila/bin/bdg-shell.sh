#!/usr/bin/env bash

#cleanup
rm -rf ~/.ivy2/cache/org.biodatageeks/bdg-sequila_2.11/*
rm -rf ~/.ivy2/jars/org.biodatageeks_bdg-sequila_2.11*

#initialize params
echo '
   _____      ____        _ __
  / ___/___  / __ \__  __(_) /   ____ _
  \__ \/ _ \/ / / / / / / / /   / __ `/
 ___/ /  __/ /_/ / /_/ / / /___/ /_/ /
/____/\___/\___\_\__,_/_/_____/\__,_/
                                                          '
echo $BDG_VERSION
echo -e "\n"
if [[ $3 != 'build' ]]; then
#spark-shell -i /tmp/bdg-toolset/bdginit.scala --packages org.biodatageeks:bdg-sequila_2.11:${BDG_VERSION} \
#  --conf spark.sql.warehouse.dir=/home/bdgeek/spark-warehouse \
#  --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/  $@
#--conf spark.jars.ivySettings=/tmp/ivy.xml
spark-shell -i /tmp/bdg-toolset/bdginit.scala --jars /tmp/bdg-toolset/bdg-sequila-assembly-${BDG_VERSION}.jar \
  --conf spark.sql.warehouse.dir=/home/bdgeek/spark-warehouse  $@
else
  spark-shell -i /tmp/bdg-toolset/bdginit.scala --jars /tmp/bdg-toolset/bdg-sequila-assembly-${BDG_VERSION}.jar  $@
fi
