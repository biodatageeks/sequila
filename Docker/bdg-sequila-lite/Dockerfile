FROM biodatageeks/bdg-spark
MAINTAINER biodatageeks <team@biodatageeks.ii.pw.edu.pl>

RUN apt-get update && apt-get install --yes git sudo curl

RUN useradd -m tempuser

# Spark related variables.
##COMPONENT_VERSION variable will be replaced during the build process

ARG BDG_VERSION={{COMPONENT_VERSION}}

####set variable used in all shell scritps for downloading jars
ENV BDG_VERSION={{COMPONENT_VERSION}}



RUN mkdir /tmp/bdg-toolset

###once the repo is public we can use git instead
#RUN git clone --branch ${BDG_VERSION} https://github.com/ZSI-Bio/bdg-spark-granges /tmp/bdg-toolset

COPY bin/bdg-shell.sh /tmp/bdg-toolset/bdg-shell
COPY bin/bdginit.scala /tmp/bdg-toolset/bdginit.scala
COPY warmcache.scala /tmp/bdg-toolset/warmcache.scala

COPY bin/bdg-start-thriftserver.sh /tmp/bdg-toolset/bdg-start-thriftserver
COPY bin/bdg-stop-thriftserver.sh /tmp/bdg-toolset/bdg-stop-thriftserver



#copy test data
COPY NA12878.slice.bam /tmp/NA12878.slice.bam
COPY unittest.scala /tmp/unittest.scala
COPY sequilathriftinit.scala /tmp/bdg-toolset/sequilathriftinit.scala
RUN ln -s /tmp/unittest.scala /tmp/smoketest.scala

#copy ivy ivySettings
COPY ivy.xml /tmp/ivy.xml

RUN chmod +x /tmp/bdg-toolset/*

RUN mkdir /usr/local/spark/logs && chmod -R 777 /usr/local/spark/logs

RUN mkdir -p /data/input && mkdir -p /data/output && mkdir -p /data/input/bams  && chmod -R 777 /data
ENV PATH="/tmp/bdg-toolset:/usr/local/spark/bin/:${PATH}"

RUN gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4
RUN curl -o /usr/local/bin/gosu -SL "https://github.com/tianon/gosu/releases/download/1.4/gosu-$(dpkg --print-architecture)" \
    && curl -o /usr/local/bin/gosu.asc -SL "https://github.com/tianon/gosu/releases/download/1.4/gosu-$(dpkg --print-architecture).asc" \
    && gpg --verify /usr/local/bin/gosu.asc \
    && rm /usr/local/bin/gosu.asc \
    && chmod +x /usr/local/bin/gosu

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh


#featureCounts scripts
COPY bin/featureCounts.sh /tmp/bdg-toolset/featureCounts



USER tempuser

WORKDIR /home/tempuser
##just to download all depencies and speedup start
RUN bdg-shell -i /tmp/bdg-toolset/warmcache.scala build

RUN rm -rf /home/tempuser/spark-warehouse

WORKDIR /tmp

#COPY bdg-sequila-assembly-{{COMPONENT_VERSION}}.jar /tmp/bdg-sequila-assembly-${BDG_VERSION}.jar

USER root
EXPOSE 4040 10000
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
