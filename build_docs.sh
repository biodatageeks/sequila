#!/bin/sh


cd docs
echo $VERSION
if  echo $VERSION | grep -Eq "SNAPSHOT$"; then
    docker build --no-cache -t biodatageeks/bdg-sequila-snap-doc .
    if [ $(docker ps | grep bdg-sequila-snap-doc | wc -l) -gt 0 ]; then docker stop bdg-sequila-snap-doc && docker rm bdg-sequila-snap-doc; fi
    docker run -p 81:80 -d --name bdg-sequila-snap-doc biodatageeks/bdg-sequila-snap-doc
else
    docker build --no-cache -t biodatageeks/bdg-sequila-doc .
    if [ $(docker ps | grep bdg-sequila-doc | wc -l) -gt 0 ]; then docker stop bdg-sequila-doc && docker rm bdg-sequila-doc; fi
    docker run -d -p 80:80 --name bdg-sequila-doc biodatageeks/bdg-sequila-doc
fi


