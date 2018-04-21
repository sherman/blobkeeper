package io.blobkeeper.file.util;

/*
 * Copyright (C) 2015 by Denis M. Gabaydulin
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
 *
 *
 * Gets the list of the blob files
 */

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import io.blobkeeper.common.util.*;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkArgument;
import static io.blobkeeper.common.util.MerkleTree.MAX_LEVEL;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.String.valueOf;
import static java.util.Collections.sort;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.io.FilenameUtils.concat;

public class FileUtils {
    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    private static final int CHUNK_SIZE = 8192;

    private FileUtils() {
    }

    public static boolean isFileEmpty(@NotNull File file, @NotNull IndexElt elt) {
        ByteBuffer fourBytes = ByteBuffer.allocate(4);
        try {
            file.getFileChannel().read(fourBytes, elt.getOffset());
        } catch (IOException e) {
            log.error("Can't read blob file", e);
            throw new IllegalArgumentException(e);
        }

        fourBytes.flip();
        for (byte b : fourBytes.array()) {
            if (b != 0) {
                return false;
            }
        }

        return true;
    }

    public static long getCrc(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    public static long getCrc(@NotNull File file) {
        CRC32 crc = new CRC32();

        while (true) {
            ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);
            while (buffer.hasRemaining()) {
                int bytes = 0;
                try {
                    bytes = file.getFileChannel().read(buffer);
                } catch (IOException e) {
                    log.error("Can't read blob file " + file, e);
                    throw new IllegalArgumentException(e);
                }
                if (bytes < 0) {
                    break;
                }
            }
            buffer.flip();
            if (buffer.remaining() == 0) {
                break;
            } else {
                crc.update(buffer.array());
            }
        }

        return crc.getValue();
    }

    public static java.io.File getDiskPathByDisk(@NotNull FileConfiguration configuration, int disk) {
        return new java.io.File(concat(configuration.getBasePath(), valueOf(disk)));
    }

    public static java.io.File getFilePathByPartition(@NotNull FileConfiguration configuration, int disk, int partition) {
        return new java.io.File(concat(concat(configuration.getBasePath(), valueOf(disk)), valueOf(partition) + ".data"));
    }

    public static java.io.File getFilePathByPartition(@NotNull FileConfiguration configuration, @NotNull Partition partition) {
        return new java.io.File(concat(concat(configuration.getBasePath(), valueOf(partition.getDisk())), valueOf(partition.getId()) + ".data"));
    }

    public static File getOrCreateFile(@NotNull FileConfiguration configuration, @NotNull Partition partition) {
        java.io.File newFile = getFilePathByPartition(configuration, partition);

        boolean preallocateRequired = !newFile.exists();

        if (preallocateRequired) {
            log.info("Creating file name {}", newFile);

            try {
                if (!newFile.createNewFile()) {
                    log.error("Can't create file {}", newFile);
                }
            } catch (IOException e) {
                log.error("Can't create file {}", newFile, e);
                throw new IllegalArgumentException(e);
            }
        }

        checkArgument(newFile.isFile(), "It must be file");
        checkArgument(newFile.exists(), "It must be exists");
        checkArgument(newFile.canRead(), "It must be readable");
        checkArgument(newFile.canWrite(), "It must be writable");

        File file = new File(newFile);
        if (preallocateRequired) {
            file.preallocate(configuration.getMaxFileSize());
        }

        return file;
    }

    public static ByteBuffer readFile(@NotNull File file, long offset, long length) {
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) length);
        int bytesRead = 0;
        try {
            // TODO: for exclusively read of file channel by single thread we can use read w/o offset (avoid additional seeks?)
            while ((bytesRead = file.getFileChannel().read(byteBuffer, offset)) != -1) {
                if (!byteBuffer.hasRemaining()) {
                    byteBuffer.flip();
                    return byteBuffer;
                }
                offset += bytesRead;
            }
        } catch (Exception e) {
            log.error("Can't read file", e);
        }

        if (bytesRead < byteBuffer.capacity()) {
            String error = String.format("File read error for file %s", file);
            log.error(error);
            throw new IllegalArgumentException(error);
        }

        byteBuffer.flip();
        return byteBuffer;
    }

    public static int writeFile(@NotNull File file, byte[] bytes, long offset) {
        try {
            return file.getFileChannel().write(ByteBuffer.wrap(bytes), offset);
        } catch (IOException e) {
            log.error("Can't write file", e);
            throw new RuntimeException(e);
        }
    }

    @NotNull
    public static ByteBuffer readFile(@NotNull java.io.File javaFile) {
        File file = new File(javaFile);
        try {
            return readFile(file, 0, javaFile.length());
        } finally {
            file.close();
        }
    }

    @NotNull
    public static String readFileToString(@NotNull java.io.File javaFile) {
        return new String(readFile(javaFile).array(), Charsets.UTF_8);
    }

    @NotNull
    public static SortedMap<Long, Block> readBlob(@NotNull IndexService indexService, @NotNull File blob, @NotNull Partition partition) {
        List<IndexElt> elts = new ArrayList<>(indexService.getListByPartition(partition));

        // sort it by offset, to read file consequentially
        sort(elts, new IndexEltOffsetComparator());

        List<BlockElt> blockElts = new ArrayList<>();

        for (IndexElt elt : elts) {
            try {
                ByteBuffer dataBuffer = readFile(blob, elt.getOffset(), elt.getLength());
                byte[] dataBufferBytes = new byte[dataBuffer.remaining()];
                dataBuffer.get(dataBufferBytes);

                long fileCrc = FileUtils.getCrc(dataBufferBytes);
                if (fileCrc == elt.getCrc()) {
                    blockElts.add(new BlockElt(elt.getId(), elt.getType(), elt.getOffset(), elt.getLength(), fileCrc));
                }
            } catch (Exception e) {
                log.error("Can't read file {} from blob", elt, e);
            }
        }

        return blockElts.stream()
                .collect(groupingBy(BlockElt::getId))
                .values().stream()
                .map(groupedElts -> new Block(groupedElts.get(0).getId(), groupedElts.stream()
                        .sorted(new BlockEltComparator())
                        .collect(ImmutableList.toImmutableList()))
                ).collect(
                        // TODO: replace with ImmutableSortedMap
                        toMap(
                                Block::getId,
                                Function.identity(),
                                Utils.throwingMerger(),
                                TreeMap::new
                        )
                );
    }

    @NotNull
    public static MerkleTree buildMerkleTree(
            @NotNull IndexService indexService,
            @NotNull File blob,
            @NotNull Partition partition
    ) {
        SortedMap<Long, Block> blocks = readBlob(indexService, blob, partition);

        MerkleTree tree = new MerkleTree(indexService.getMinMaxRange(partition), MAX_LEVEL);
        MerkleTree.fillTree(tree, blocks);
        tree.calculate();

        return tree;
    }

    public static int getPercentOfDeleted(
            @NotNull FileConfiguration configuration,
            @NotNull IndexService indexService,
            @NotNull Partition partition
    ) {
        long deleted = min(indexService.getSizeOfDeleted(partition), configuration.getMaxFileSize());
        return (int) round((double) deleted / configuration.getMaxFileSize() * 100);
    }
}
