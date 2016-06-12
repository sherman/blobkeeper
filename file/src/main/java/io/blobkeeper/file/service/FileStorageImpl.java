package io.blobkeeper.file.service;

/*
 * Copyright (C) 2015-2016 by Denis M. Gabaydulin
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.io.ByteSource;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.*;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.index.domain.DiskIndexElt;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.IndexTempElt;
import io.blobkeeper.index.service.IndexService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;
import static java.nio.channels.Channels.newChannel;
import static org.slf4j.LoggerFactory.getLogger;

@Singleton
public class FileStorageImpl implements FileStorage {
    private static final Logger log = getLogger(FileStorageImpl.class);

    @Inject
    private DiskService diskService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private IndexService indexService;

    private volatile boolean running;

    @Override
    public void start() {
        checkArgument(!running, "Can't start the service twice");

        log.info("File storage is started, was running {}", running);

        diskService.openOnStart();

        running = true;
    }

    @Override
    public void stop() {
        checkArgument(running, "Can't stop the service twice");

        log.info("File storage is stopped");

        diskService.closeOnStop();

        running = false;
    }

    @Override
    public void refresh() {
    }

    @Override
    public ReplicationFile addFile(int diskId, @NotNull StorageFile storageFile) {
        ByteBuffer dataBuffer;
        try {
            checkArgument(running, "Storage is not running!");

            WritablePartition writablePartition = diskService.getWritablePartition(diskId, storageFile.getLength());
            Disk disk = writablePartition.getDisk();

            checkNotNull(disk.getActivePartition(), "Active partition is required!");

            FileChannel writerChannel = disk.getWriter().getFileChannel();

            dataBuffer = storageFile.getData();

            byte[] dataBufferBytes = new byte[dataBuffer.remaining()];
            dataBuffer.get(dataBufferBytes);

            long fileCrc = FileUtils.getCrc(dataBufferBytes);
            dataBuffer.flip();

            IndexElt indexElt = new IndexElt.IndexEltBuilder()
                    .id(storageFile.getId())
                    .type(storageFile.getType())
                    .partition(disk.getActivePartition())
                    .offset(writablePartition.getNextOffset() - storageFile.getLength())
                    .length(storageFile.getLength())
                    .crc(fileCrc)
                    .metadata(storageFile.getMetadata())
                    .build();

            log.debug("Index elt for new file {}", indexElt);

            long writeStarted = currentTimeMillis();

            // write data
            long transferred = writerChannel.write(dataBuffer, indexElt.getOffset());
            if (transferred < indexElt.getLength()) {
                throw new IllegalStateException("Data writing error, transferred " + transferred);
            }

            log.trace("Bytes transferred {}", transferred);

            log.trace("Write time is {}", currentTimeMillis() - writeStarted);

            long updateIndexStarted = currentTimeMillis();

            // update index
            indexService.add(indexElt);

            log.trace("Update index time is {}", currentTimeMillis() - updateIndexStarted);

            long replicationTime = currentTimeMillis();
            // create replication ready file
            ReplicationFile replicationFile = new ReplicationFile(indexElt.getDiskIndexElt(), dataBufferBytes);

            log.trace("Replication copy time is {}", currentTimeMillis() - replicationTime);

            diskService.resetErrors(diskId);

            return replicationFile;
        } catch (IOException e) {
            log.error("Can't add file to the storage", e);

            diskService.updateErrors(diskId);

            throw new IllegalArgumentException("Can't add file to the storage");
        } catch (Exception e) {
            log.error("Can't add file to the storage", e);
            throw new IllegalArgumentException("Can't add file to the storage");
        } finally {
            long maintainTime = currentTimeMillis();

            cleanFile(storageFile);

            log.trace("Maintain time is {}", currentTimeMillis() - maintainTime);
        }
    }

    @Override
    public void addFile(@NotNull ReplicationFile replicationFile) {
        log.info("Replicate file {}", replicationFile);

        checkArgument(running, "Storage is not running!");

        DiskIndexElt indexElt = replicationFile.getIndex();
        File file = diskService.getFile(indexElt.getPartition());

        checkNotNull(file, "Blob file is required!");

        InputStream is;
        try {
            is = ByteSource.wrap(replicationFile.getData()).openStream();
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't wrap the buffer", e);
        }

        ReadableByteChannel dataChannel = newChannel(is);

        try {
            long transferred = file.getFileChannel().transferFrom(dataChannel, indexElt.getOffset(), indexElt.getLength());

            if (transferred < indexElt.getLength()) {
                throw new IllegalStateException("Data writing error, transferred " + transferred);
            }
        } catch (IOException e) {
            log.error("Can't add file to the storage", e);

            diskService.updateErrors(indexElt.getPartition().getDisk());

            throw new IllegalArgumentException("Can't add file to the storage");
        } finally {
            if (null != dataChannel) {
                try {
                    dataChannel.close();
                } catch (IOException e) {/*_*/}
            }
        }
    }

    @Override
    public void copyFile(@NotNull TransferFile transferFile) {
        log.info("Transfer file {}", transferFile);

        checkArgument(running, "Storage is not running!");

        File from = diskService.getFile(transferFile.getFrom().getPartition());
        File to = diskService.getFile(transferFile.getTo().getPartition());

        DiskIndexElt fromElt = transferFile.getFrom();
        DiskIndexElt toElt = transferFile.getTo();

        try {
            ByteBuffer data = FileUtils.readFile(from, fromElt.getOffset(), fromElt.getLength());
            long transferred = to.getFileChannel().write(data, toElt.getOffset());
            if (transferred < transferFile.getFrom().getLength()) {
                throw new IllegalStateException("Data writing error, transferred " + transferred);
            }
        } catch (IOException e) {
            log.error("Can't transfer file to the storage", e);

            diskService.updateErrors(transferFile.getTo().getPartition().getDisk());

            throw new IllegalArgumentException("Can't transfer file to the storage");
        }
    }

    @Override
    public void copyFile(int diskId, @NotNull StorageFile from) {
        log.info("Transfer file {}", from);

        try {
            checkArgument(running, "Storage is not running!");

            IndexElt indexElt = indexService.getById(from.getId(), from.getType());

            // FIXME: file could be delete, but not expired
            checkArgument(indexElt != null && !indexElt.isDeleted(), "Index elt must be exists and live!");

            WritablePartition writablePartition = diskService.getWritablePartition(diskId, indexElt.getLength());
            Disk disk = writablePartition.getDisk();

            checkNotNull(disk.getActivePartition(), "Active partition is required!");

            TransferFile transferFile = new TransferFile(
                    indexElt.getDiskIndexElt(),
                    new DiskIndexElt(disk.getActivePartition(), writablePartition.getNextOffset() - indexElt.getLength(), indexElt.getLength())
            );

            long writeStarted = currentTimeMillis();

            // write data
            copyFile(transferFile);

            log.trace("Write time is {}", currentTimeMillis() - writeStarted);

            long updateIndexStarted = currentTimeMillis();

            // update index
            indexService.move(indexElt, transferFile.getTo());

            log.trace("Update index time is {}", currentTimeMillis() - updateIndexStarted);

            long replicationTime = currentTimeMillis();
            // create replication ready file
            // TODO: return replication

            log.trace("Replication copy time is {}", currentTimeMillis() - replicationTime);

            diskService.resetErrors(diskId);
        } catch (Exception e) {
            log.error("Can't copy file to the storage", e);

            // TODO: extract i/o errors and count disk.onError() ?

            throw new IllegalArgumentException("Can't copy file to the storage");
        } finally {
            long maintainTime = currentTimeMillis();

            log.trace("Maintain time is {}", currentTimeMillis() - maintainTime);
        }
    }

    @Override
    public File getFile(@NotNull IndexElt indexElt) {
        return diskService.getFile(indexElt.getPartition());
    }

    private void cleanFile(StorageFile storageFile) {
        IndexTempElt elt = new IndexTempElt.IndexTempEltBuilder()
                .id(storageFile.getId())
                .type(storageFile.getType())
                .build();

        indexService.delete(elt);

        if (null != storageFile.getFile()) {
            if (!storageFile.getFile().delete()) {
                log.error("Can't delete file {}", storageFile.getName());
            }
        }
    }
}
