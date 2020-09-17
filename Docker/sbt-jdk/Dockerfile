FROM ubuntu:bionic
RUN apt-get update
RUN apt-get -qq -y install \
    curl \
    unzip \
    zip
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN apt-get -qq -y install curl
RUN curl -s https://get.sdkman.io | bash
RUN chmod a+x "$HOME/.sdkman/bin/sdkman-init.sh"

ARG JAVA_VERSION=8.0.265.hs-adpt
ARG SCALA_VERSION=2.11.12
ARG SBT_VERSION=1.3.10

RUN source "$HOME/.sdkman/bin/sdkman-init.sh" && sdk install java ${JAVA_VERSION}
RUN source "$HOME/.sdkman/bin/sdkman-init.sh" && sdk install scala ${SCALA_VERSION}
RUN source "$HOME/.sdkman/bin/sdkman-init.sh" && sdk install sbt ${SBT_VERSION}
RUN source "$HOME/.sdkman/bin/sdkman-init.sh" && sdk use java ${JAVA_VERSION}

ADD resources/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

RUN apt update  &&  apt install gnupg -y
RUN mkdir -p /root/gpg
RUN mkdir -p /root/.gnupg
RUN echo -e "\nyes\n" > /root/.gnupg/gpg.conf
RUN echo use-agent >> /root/.gnupg/gpg.conf
RUN echo pinentry-mode loopback >> /root/.gnupg/gpg.conf
RUN echo allow-loopback-pinentry >> /root/.gnupg/gpg-agent.conf

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]