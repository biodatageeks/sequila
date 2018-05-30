FROM rocker/shiny

RUN apt-get update && apt-get install --yes git sudo curl libssl-dev libxml2-dev

#install devtools
RUN Rscript -e "install.packages('devtools')"

#install sequila
RUN Rscript -e "devtools::install_github('ZSI-Bio/bdg-sparklyr-sequila')"

#install spark (installed by .onLoad when package loaded)
RUN Rscript -e "library(sequila)"


#install jdk8
RUN apt-get install --yes gnupg2
##A quick & dirty fix for failing Oracle JDK installer
RUN if [ ! -d /usr/share/man/man1 ]; then  mkdir -p /usr/share/man/man1; fi
RUN \
   echo "===> add webupd8 repository..."  && \
   echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee /etc/apt/sources.list.d/webupd8team-java.list  && \
   echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list  && \
   apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886  && \
   apt-get update  && \
   \
   \
   echo "===> install Java"  && \
   echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections  && \
   echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections  && \
   cd /var/lib/dpkg/info && \
   DEBIAN_FRONTEND=noninteractive  apt-get install -y --force-yes oracle-java8-installer oracle-java8-set-default  && \
   \
   \
   echo "===> clean up..."  && \
   rm -rf /var/cache/oracle-jdk8-installer  && \
   apt-get clean  && \
   rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

#copy test data
COPY NA12878.slice.bam /tmp/NA12878.slice.bam
COPY warmcache.scala /tmp/warmcache.scala

#sequila versions

ARG BDG_VERSION 0.4-SNAPSHOT
ENV BGD_VERSION 0.4-SNAPSHOT
RUN /root/spark/spark-2.2.1-bin-hadoop2.7/bin/spark-shell --packages org.biodatageeks:bdg-sequila_2.11:${BGD_VERSION} \
 -i /tmp/warmcache.scala --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/
