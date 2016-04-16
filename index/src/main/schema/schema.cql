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
