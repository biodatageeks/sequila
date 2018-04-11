#!/usr/bin/env bash

#cleanup
rm -rf ~/.ivy2/cache/org.biodatageeks/bdg-sequila_2.11/*
rm -rf ~/.ivy2/jars/org.biodatageeks_bdg-sequila_2.11*

#initialize params
echo '
   _____      ____        _ __          ____
  / ___/___  / __ \__  __(_) /   ____ _/ __ \
  \__ \/ _ \/ / / / / / / / /   / __ `/ /_/ /
 ___/ /  __/ /_/ / /_/ / / /___/ /_/ / _, _/
/____/\___/\___\_\__,_/_/_____/\__,_/_/ |_|
                                             '
echo $BGD_VERSION
echo -e "\n"

rm -rf ~/metastore_db
sparkR --packages org.biodatageeks:bdg-sequila_2.11:${BGD_VERSION} \
  --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/  $@

