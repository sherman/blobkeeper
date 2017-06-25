# Installation

Example installation of two nodes cluster.

## Prerequisites

* Install cassandra >= 2.2 (required as index storage)
* Install jsvc (optional)

## Make directory structure

```
mkdir /home/user/node1/
mkdir /home/user/node1/log
mkdir /home/user/node1/bin
mkdir /home/user/node1/config
mkdir /home/user/node1/data
mkdir /home/user/node1/data/0
mkdir /home/user/node1/data/1
```

```
mkdir /home/user/node2/
mkdir /home/user/node2/log
mkdir /home/user/node2/bin
mkdir /home/user/node2/config
mkdir /home/user/node2/data
mkdir /home/user/node2/data/0
mkdir /home/user/node2/data/1
```
Directory /home/user/node2/data/ is a data root. It's recommended to make symlinks to disks. E.g.

/home/user/node2/data/0 -> /storage0

/home/user/node2/data/1 -> /storage1

## Create index schema

Run schema.cql script in cassandra cqlsh.

## Build

```
mvn package -DskipTests=true
```

## Set up node1

```
unzip distribution/target/distribution-0.3.0-SNAPSHOT-bin.zip -d /home/user/node1/bin
```

### Set up configuration

Copy the following configuration to /home/user/node1/config/node1.properties.

```
blobkeeper.server.name=node1 # just a unique node name
blobkeeper.server.host=localhost
blobkeeper.server.port=6622 # http port
blobkeeper.server.allowed.headers=X-Metadata-Content-Type
blobkeeper.server.api.token=ff415efe71ac2ecf46a8c30fdaa7010c60559cd1
blobkeeper.server.secret.token=fFbTPwfka]Aefj2313f

blobkeeper.index.cache.enabled=true
blobkeeper.index.gc.grace.seconds=864000 # 10 days

blobkeeper.cassandra.nodes=127.0.0.1
blobkeeper.cassandra.keyspace=blobkeeper_test
blobkeeper.cassandra.consistency.level=TWO

blobkeeper.base.path=/home/user/node1/data/
blobkeeper.file.max.size=33554432
blobkeeper.disk.max.errors=2

blobkeeper.cluster.config=config/node1.xml
blobkeeper.cluster.min.servers=1
blobkeeper.cluster.replication.max.files=64
blobkeeper.cluster.replication.delay=500
blobkeeper.cluster.master=true # that's server has a master role

blobkeeper.compaction.worker.delay.seconds=30
blobkeeper.compaction.finalizer.delay.seconds=30
blobkeeper.compaction.min.percent=10

```

Copy the following jgroups cluster configuration to /home/user/node1/config/node1.xml.
This configuration based on TCP/IP stack only and isn't required a multicast support.

```
<config xmlns="urn:org:jgroups"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="urn:org:jgroups http://www.jgroups.org/schema/JGroups-4.0.3.xsd">
    <TCP_NIO2
            bind_addr="127.0.0.1"
            bind_port="7400"

            thread_pool.enabled="true"
            thread_pool.min_threads="4"
            thread_pool.max_threads="64"
            thread_pool.keep_alive_time="20000"

            port_range="0"
            />
    <TCPPING initial_hosts="${jgroups.tcpping.initial_hosts:127.0.0.1[7400],127.0.0.1[7401]}" port_range="0"/>
    <MERGE3/>
    <FD_SOCK/>
    <FD_ALL2 timeout="90000"/>
    <VERIFY_SUSPECT/>
    <pbcast.NAKACK2 use_mcast_xmit="false"/>
    <UNICAST3/>
    <pbcast.STABLE/>
    <pbcast.GMS/>
    <MFC/>
    <FRAG2/>
    <pbcast.STATE_TRANSFER/>
    <CENTRAL_LOCK/>
    <FORK/>
</config>
```

Do the same for the second node (node2).



