# Blobkeeper

Blobkeeper is high-performance, reliable distributed file storage. It was built on top of netty and jgroups.

## Architecture

### Index

User files are stored in the blobs which are pre-allocated in continuous space on the disk.
A blob file has index stored in cassandra. The index structure is pretty simple:

```
CREATE TABLE BlobIndex (
  id bigint,
  type int,
  created bigint,
  data blob,
  deleted boolean,
  disk int,
  part int,
  crc bigint,
  offset bigint,
  length bigint,
  PRIMARY KEY (id, type)
);
```

**id** - unique generated file id

**type** - additional field to groups multiple files with single id (can be used to store thumbs)

**data** - metadata

**disk** and **part** - address a blob on a disk

**offset** and **length** - address a file in a blob

### Write/Read request path

HTTP server handles requests. The upload request just upload file and put it to the writing queue. It will be written on a disk later.
The queue is polled by a few workers. Each disk has single writer worker.
The read request gets the file index, reads the offset and length and streams the bytes from a file channel to a network socket w/o copy.

### Replication

Server supports a replication. If the cluster has at least one slave, file will be copied after it has been written on the master.
The replication is implemented on top of jgroups framework.

Additionally, the server has repair command. It used to sync blobs between replicas in case of a new slave has been added or a disk has been replaced.

To reduce traffic of repair process the server builds the [merkle tree](https://en.wikipedia.org/wiki/Merkle_tree) structure on top of the index. Then compares blobs and sends only missed parts. Find out more information (in russian) in my [personal blog](https://medium.com/@denisgabaydulin/merkle-tree-a0f251594d78).

### Sharding

It's possible to have multiple clusters to scale out writes. File identifier format is inspired by [Twitter's snowflake](https://github.com/twitter/snowflake). The identifier supports up to 1024 shards.

## Load balancer configuration example (nginx)

```
upstream readers {
    server 172.16.41.1:7890;
    server 172.16.41.2:7890;
    server 172.16.41.3:7890;
}

upstream writers {
    server 172.16.41.1:7890;
}

listen *:80;
    server_name filez-storage.tld;

    location / {
        proxy_next_upstream http_502;

        if ($request_method ~* GET) {
            proxy_pass http://readers;
        }

        if ($request_method !~* GET) {
            proxy_pass http://writers;
        }
    }
}
```

## Other features

A bunch of miscellaneous features:
 * Authentication of individual file through auth tokens
 * Delete/restore (restore will be a bit later)
 * Admins API: repair, get master, set master, get nodes list (TBD)
 * Compaction (cleanup deleted files from disk) - **still in progress**
 * Index cache

## Plans
 * Improve documentations and tests
 * Dockerization
 * Smart balancing of writes to disks (currently, only round-robin is supported)

