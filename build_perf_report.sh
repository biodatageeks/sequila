#!/usr/bin/env bash

# extract version
version=`grep 'version :=' build.sbt | sed  's|version := \"||g' | sed 's|\"||g'`
echo Version is $version


cd performance/bdg_perf && Rscript -e 'rmarkdown::render("bdg-perf-sequila.Rmd")'


docker build --no-cache -t zsi-bio/bdg-sequila-snap-perf .
if [ $(docker ps | grep bdg-sequila-snap-perf | wc -l) -gt 0 ]; then docker stop bdg-sequila-snap-perf && docker rm bdg-sequila-snap-perf; fi
docker run -p 82:80 -d --name bdg-sequila-snap-perf zsi-bio/bdg-sequila-snap-perf


