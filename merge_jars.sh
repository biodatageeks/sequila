#!/bin/bash -x

WORKDIR=/tmp/jars
mkdir -p $WORKDIR
cd $WORKDIR; unzip -uo $1
cd $WORKDIR; unzip -uo $2
jar -cvf  /builds/$CI_PROJECT_NAMESPACE/$CI_PROJECT_NAME/target/scala-$SCALA_MAJOR_VERSION.$SCALA_MINOR_VERSION/sequila_$SCALA_MAJOR_VERSION.$SCALA_MINOR_VERSION-$VERSION.jar -C $WORKDIR .

rm -rf $WORKDIR