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
Directory /home/user/node2/data/ is the root for a data. Recommends make symlinks to disks. E.g.
/home/user/node2/data/0 -> /storage0
/home/user/node2/data/1 -> /storage1

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

```
<config xmlns="urn:org:jgroups"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="urn:org:jgroups http://www.jgroups.org/schema/JGroups-3.6.7.xsd">
    <TCP_NIO2
            bind_addr="127.0.0.1"
            bind_port="7400"
            timer_type="new3"
            timer.min_threads="4"
            timer.max_threads="10"
            timer.keep_alive_time="3000"
            timer.queue_max_size="500"

            thread_pool.enabled="true"
            thread_pool.min_threads="4"
            thread_pool.max_threads="64"
            thread_pool.keep_alive_time="20000"
            thread_pool.queue_enabled="false"
            thread_pool.queue_max_size="100"
            thread_pool.rejection_policy="discard"

            oob_thread_pool.enabled="true"
            oob_thread_pool.min_threads="2"
            oob_thread_pool.max_threads="8"
            oob_thread_pool.keep_alive_time="20000"
            oob_thread_pool.queue_enabled="false"
            oob_thread_pool.queue_max_size="100"
            oob_thread_pool.rejection_policy="discard"
            />
    <TCPPING initial_hosts="${jgroups.tcpping.initial_hosts:127.0.0.1[7400],127.0.0.1[7401]}" port_range="0"/>
    <MERGE3/>
    <FD_SOCK/>
    <FD_ALL timeout="90000"/>
    <VERIFY_SUSPECT/>
    <pbcast.NAKACK2 use_mcast_xmit="false"/>
    <UNICAST3/>
    <pbcast.STABLE/>
    <pbcast.GMS/>
    <MFC/>
    <FRAG2/>
    <pbcast.STATE_TRANSFER/>
    <CENTRAL_LOCK/>
</config>
```

Do the same for the second node (node2).

-Djava.net.preferIPv4Stack=true -DconfigFile=cluster/example/node2.properties



