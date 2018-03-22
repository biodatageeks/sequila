#!/usr/bin/env bash

docker run --rm  -d -v /data/samples/NA12878:/usr/share/nginx/html -p 8082:80 --name bdg-sequila-testdata nginx