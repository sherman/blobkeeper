#!/bin/bash

BASE_PATH=$1
BIND_ADDRESS=127.0.0.1
USER=blobkeeper

JAVA=/usr/lib/jvm/java-8-oracle

export JAVA_HOME=$JAVA

PIDFILE=$BASE_PATH/bin/blobkeeper.pid

JARS_CP=$(JARS=("$BASE_PATH/bin"/*.jar); IFS=:; echo "${JARS[*]}")

CP=$BASE_PATH/config/:$JARS_CP:

# Seconds to wait between attempts
WAIT=20

# Class implementing Daemon interface
CLASS=io.blobkeeper.server.BlobKeeperServerApp

/usr/bin/jsvc \
        -user $USER \
        -debug \
        -home $JAVA \
        -pidfile $PIDFILE \
        -wait $WAIT \
        -cp $CP \
        -errfile $BASE_PATH/log/error.log \
        -Djgroups.bind_addr=$BIND_ADDRESS \
        -Djava.net.preferIPv4Stack=true \
        -DconfigFile=config/node1.properties
        -Dcom.sun.management.jmxremote \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Dcom.sun.management.jmxremote.port=6789 \
        -Djava.rmi.server.hostname=localhost \
        -XX:+PrintGC \
        $CLASS
