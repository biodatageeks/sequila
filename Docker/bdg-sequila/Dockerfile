FROM biodatageeks/bdg-spark:2.4.1
MAINTAINER biodatageeks <team@biodatageeks.ii.pw.edu.pl>

RUN apt-get update && apt-get install --yes git sudo curl

#fix bug/issue-129
RUN useradd -u 99999 -m tempuser

###SeQuiLaThrift fixes
#RUN  mkdir -p /user/hive && chmod 777 -R  /user/hive


# Spark related variables.
##COMPONENT_VERSION variable will be replaced during the build process

ARG BDG_VERSION

####set variable used in all shell scritps for downloading jars
ENV BDG_VERSION=$BDG_VERSION


RUN apt-get update && apt-get install --yes git sudo curl libssl-dev libxml2-dev






RUN mkdir /tmp/bdg-toolset

###once the repo is public we can use git instead
#RUN git clone --branch ${BDG_VERSION} https://github.com/ZSI-Bio/bdg-spark-granges /tmp/bdg-toolset

COPY bin/bdg-shell.sh /tmp/bdg-toolset/bdg-shell
COPY bin/bdginit.scala /tmp/bdg-toolset/bdginit.scala
COPY warmcache.scala /tmp/bdg-toolset/warmcache.scala

COPY bin/bdg-start-thriftserver.sh /tmp/bdg-toolset/bdg-start-thriftserver
COPY bin/bdg-stop-thriftserver.sh /tmp/bdg-toolset/bdg-stop-thriftserver

COPY bin/bdg-sequilaR.sh /tmp/bdg-toolset/bdg-sequilaR

#featureCounts scripts
COPY bin/featureCounts.sh /tmp/bdg-toolset/featureCounts
RUN echo $BDG_VERSION
RUN bash -c " if [[ $BDG_VERSION =~ SNAPSHOT ]]; then \
    wget https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/org/biodatageeks/bdg-sequila_2.11/${BDG_VERSION}/bdg-sequila_2.11-${BDG_VERSION}-assembly.jar -O /tmp/bdg-toolset/bdg-sequila-assembly-${BDG_VERSION}.jar ; \
    else wget https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/org/biodatageeks/bdg-sequila_2.11/${BDG_VERSION}/bdg-sequila_2.11-${BDG_VERSION}-assembly.jar -O /tmp/bdg-toolset/bdg-sequila-assembly-${BDG_VERSION}.jar ; \
    fi"


COPY bin/depthOfCoverage.sh /tmp/bdg-toolset/depthOfCoverage

#copy test data
COPY NA12878.slice.bam /tmp/NA12878.slice.bam
COPY unittest.scala /tmp/unittest.scala
RUN ln -s /tmp/unittest.scala /tmp/smoketest.scala

#copy ivy ivySettings
COPY ivy.xml /tmp/ivy.xml

RUN chmod +x /tmp/bdg-toolset/*

RUN mkdir /usr/local/spark/logs && chmod -R 777 /usr/local/spark/logs

RUN mkdir -p /data/input && mkdir -p /data/output  && chmod -R 777 /data
ENV PATH="/tmp/bdg-toolset:/usr/local/spark/bin/:${PATH}"

RUN gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4
RUN curl -o /usr/local/bin/gosu -SL "https://github.com/tianon/gosu/releases/download/1.4/gosu-$(dpkg --print-architecture)" \
    && curl -o /usr/local/bin/gosu.asc -SL "https://github.com/tianon/gosu/releases/download/1.4/gosu-$(dpkg --print-architecture).asc" \
    && gpg --verify /usr/local/bin/gosu.asc \
    && rm /usr/local/bin/gosu.asc \
    && chmod +x /usr/local/bin/gosu

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

#install R-base
RUN echo "deb http://cloud.r-project.org/bin/linux/ubuntu xenial/" > /etc/apt/sources.list.d/cran.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9

ENV R_BASE_VERSION 3.4.4

## Now install R and littler, and create a link for littler in /usr/local/bin
## Also set a default CRAN repo, and make sure littler knows about it too
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		ed \
		less \
		locales \
		vim-tiny \
		wget \
		ca-certificates \
		apt-transport-https \
		gsfonts \
		libxml2-dev \
		libcurl4-openssl-dev \

	&& rm -rf /var/lib/apt/lists/*

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		littler\
                r-cran-littler \
		r-base=${R_BASE_VERSION}* \
		r-base-dev=${R_BASE_VERSION}* \
		r-recommended=${R_BASE_VERSION}* \
        && echo 'options(repos = c(CRAN = "https://cloud.r-project.org/"), download.file.method = "libcurl")' >> /etc/R/Rprofile.site \
        && echo 'source("/etc/R/Rprofile.site")' >> /etc/littler.r \
	&& ln -s /usr/share/doc/littler/examples/install.r /usr/local/bin/install.r \
	&& ln -s /usr/share/doc/littler/examples/install2.r /usr/local/bin/install2.r \
	&& ln -s /usr/share/doc/littler/examples/installGithub.r /usr/local/bin/installGithub.r \
	&& ln -s /usr/share/doc/littler/examples/testInstalled.r /usr/local/bin/testInstalled.r \
	&& install.r docopt \
	&& rm -rf /tmp/downloaded_packages/ /tmp/*.rds \
	&& rm -rf /var/lib/apt/lists/*

#rna-seq stuff
#RUN Rscript -e 'source("http://bioconductor.org/biocLite.R")' -e 'biocLite("edgeR")'
#RUN Rscript -e 'source("http://bioconductor.org/biocLite.R")' -e 'biocLite("DESeq2")'

#install devtools
RUN Rscript -e "install.packages('devtools')"
#install sequila
RUN Rscript -e "devtools::install_github('ZSI-Bio/bdg-sparklyr-sequila')"

USER tempuser

WORKDIR /home/tempuser
##just to download all depencies and speedup start
RUN bdg-shell -i /tmp/bdg-toolset/warmcache.scala build



#install spark (installed by .onLoad when package loaded)
RUN Rscript -e "library(sequila)"

USER root
EXPOSE 4040 10000
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
