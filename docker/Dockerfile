FROM ubuntu:14.04
MAINTAINER Denis Gabaydulin gabaden@gmail.com

VOLUME ["/data"]

RUN echo "deb http://archive.ubuntu.com/ubuntu trusty main universe" > /etc/apt/sources.list
RUN echo "deb http://archive.ubuntu.com/ubuntu trusty-updates main universe" >> /etc/apt/sources.list

RUN apt-get update && apt-get upgrade -y && apt-get install -y software-properties-common debconf-utils rsync openssh-server net-tools vim-tiny sudo iputils-ping python2.7 less curl

RUN /bin/echo -e oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections

RUN add-apt-repository ppa:webupd8team/java

RUN apt-get update && apt-get upgrade -y && apt-get install -y oracle-java8-installer

RUN apt-get install -y jsvc

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
ENV PATH $PATH:$JAVA_HOME/bin

RUN ssh-keygen -q -N "!Aa123456!" -t rsa -f /root/.ssh/id_rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

ADD ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config
RUN chown root:root /root/.ssh/config

RUN sed  -i "/^[^#]*UsePAM/ s/.*/#&/"  /etc/ssh/sshd_config
RUN echo "UsePAM no" >> /etc/ssh/sshd_config
RUN echo "Port 2222" >> /etc/ssh/sshd_config

RUN service ssh start
