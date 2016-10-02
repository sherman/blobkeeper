CREATE KEYSPACE blobkeeper WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '3'
};

USE blobkeeper;

CREATE TABLE BlobIndex (
  id bigint,
  type int,
  created bigint,
  updated bigint,
  data blob,
  deleted boolean,
  disk int,
  part int,
  crc bigint,
  offset bigint,
  length bigint,
  PRIMARY KEY (id, type)
);

CREATE TABLE BlobPartition (
  disk int,
  part int,
  crc bigint,
  tree blob,
  state int,
  PRIMARY KEY (disk, part)
) WITH CLUSTERING ORDER BY (part DESC);

CREATE TABLE BlobIndexByPart (
  disk int,
  part int,
  id bigint,
  type int,
  PRIMARY KEY ((disk, part), id, type)
);

CREATE TABLE BlobPartitionMoveInfo (
  disk_from int,
  part_from int,
  disk_to int,
  part_to int,
  PRIMARY KEY ((disk_from, part_from))
);

CREATE TABLE BlobIndexTemp (
  id bigint,
  type int,
  created bigint,
  data blob,
  file text,
  PRIMARY KEY (id, type)
);
