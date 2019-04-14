#!/usr/bin/env bash

# extract version
version=`grep 'version :=' build.sbt | sed  's|version := \"||g' | sed 's|\"||g'`
echo Version is $version

#update deps

cd docs && ./docs.sh html

if [[ $version =~ SNAPSHOT ]]; then
    docker build --no-cache -t zsi-bio/bdg-sequila-snap-doc .
    if [ $(docker ps | grep bdg-sequila-snap-doc | wc -l) -gt 0 ]; then docker stop bdg-sequila-snap-doc && docker rm bdg-sequila-snap-doc; fi
    docker run -p 81:80 -d --name bdg-sequila-snap-doc zsi-bio/bdg-sequila-snap-doc
else
    docker build --no-cache -t zsi-bio/bdg-sequila-doc .
    if [ $(docker ps | grep bdg-sequila-doc | wc -l) -gt 0 ]; then docker stop bdg-sequila-doc && docker rm bdg-sequila-doc; fi
    docker run -d -p 80:80 --name bdg-sequila-doc zsi-bio/bdg-sequila-doc
fi


