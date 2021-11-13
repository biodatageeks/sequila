#!/usr/bin/env bash
cd sbt-jdk
export JAVA_VERSION=11.0.11.hs-adpt
export SBT_VERSION=1.3.10
docker build -t biodatageeks/jdk-sbt:$JAVA_VERSION-$SBT_VERSION \
  --build-arg JAVA_VERSION=$JAVA_VERSION \
  --build-arg SBT_VERSION=$SBT_VERSION \
  .

docker push biodatageeks/jdk-sbt:$JAVA_VERSION-$SBT_VERSION
