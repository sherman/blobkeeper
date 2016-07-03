package io.blobkeeper.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import io.blobkeeper.client.service.BlobKeeperClient;
import io.blobkeeper.client.service.BlobKeeperClientImpl;
import io.blobkeeper.client.util.BlobKeeperClientUtils;
import io.blobkeeper.common.configuration.MetricModule;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.domain.Result;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.Disk;
import io.blobkeeper.file.service.*;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexCacheService;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.server.configuration.ServerConfiguration;
import io.blobkeeper.server.configuration.ServerModule;
import io.blobkeeper.server.util.JsonUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.asynchttpclient.Response;
import org.mockito.Mockito;
import org.testng.annotations.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.io.Files.write;
import static io.blobkeeper.file.util.FileUtils.getDiskPathByDisk;
import static io.blobkeeper.server.TestUtils.assertResponseOk;
import static java.io.File.createTempFile;
import static java.nio.charset.Charset.forName;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * @author Denis Gabaydulin
 * @since 03.07.16
 */
@Guice(modules = {RootModule.class, ServerModule.class, MetricModule.class, DiskErrorsTest.Module.class})
public class DiskErrorsTest {
    @Inject
    private BlobKeeperServer server;

    @Inject
    private ServerConfiguration serverConfiguration;

    @Inject
    private JsonUtils jsonUtils;

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private FileListService fileListService;

    @Inject
    private IndexService indexService;

    @Inject
    private BlobKeeperClientUtils clientUtils;

    @Inject
    private DiskService diskService;

    private BlobKeeperClient client;

    @Test
    public void disableDiskOnError() throws Exception {
        io.blobkeeper.file.domain.File mockFile = mock(io.blobkeeper.file.domain.File.class);
        WritablePartition partition = mock(WritablePartition.class);
        Disk disk = new Disk.Builder(0)
                .setWriter(mockFile)
                .setWritable(true)
                .setActivePartition(new Partition(0, 0))
                .build();
        when(partition.getDisk()).thenReturn(disk);
        FileChannel fileChannel = mock(FileChannel.class);
        doThrow(IOException.class).when(fileChannel).write(any(ByteBuffer.class), anyLong());
        when(mockFile.getFileChannel()).thenReturn(fileChannel);
        when(diskService.getWritablePartition(anyInt(), anyLong())).thenReturn(partition);
        Map<Integer, Disk> disks = new HashMap<>();
        disks.put(0, disk);
        when(diskService.getActiveDisks()).thenReturn(disks);
        when(fileListService.getDisks()).thenReturn(ImmutableList.of(0));

        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        for (int i = 0; i < fileConfiguration.getMaxDiskWriteErrors(); i++) {
            client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        }

        assertEquals(disk.getErrors().get(), fileConfiguration.getMaxDiskWriteErrors());
        when(diskService.getActiveDisks()).thenReturn(ImmutableMap.of());
    }

    @BeforeClass
    private void start() throws Exception {
        deleteIndexFiles();
        clearIndex();

        server.startAsync();
        server.awaitRunning();

        client = new BlobKeeperClientImpl(objectMapper, serverConfiguration.getBaseUrl());
        client.startAsync();
        client.awaitRunning();

        clientUtils.waitForMaster(client);
    }

    @AfterClass
    private void stop() throws InterruptedException {
        server.stopAsync();
        server.awaitTerminated();

        client.stopAsync();
    }

    private void clearIndex() {
        indexService.clear();
    }

    private void deleteIndexFiles() {
        java.io.File basePath = new java.io.File(fileConfiguration.getBasePath());
        if (!basePath.exists()) {
            basePath.mkdir();
        }

        java.io.File disk1 = getDiskPathByDisk(fileConfiguration, 0);
        if (!disk1.exists()) {
            disk1.mkdir();
        }

        for (int disk : fileListService.getDisks()) {
            java.io.File diskPath = getDiskPathByDisk(fileConfiguration, disk);
            for (java.io.File indexFile : diskPath.listFiles((FileFilter) new SuffixFileFilter(".data"))) {
                indexFile.delete();
            }
        }

        java.io.File uploadPath = new java.io.File(fileConfiguration.getUploadPath());
        if (!uploadPath.exists()) {
            uploadPath.mkdir();
        }
    }

    @BeforeMethod
    private void reset() {
        Mockito.reset(diskService);
        Mockito.reset(fileListService);
    }


    public static class Module extends AbstractModule {
        @Override
        protected void configure() {
        }

        @Provides
        @Singleton
        public DiskService diskService(Injector injector) {
            DiskService diskService = spy(new DiskServiceImpl());
            injector.injectMembers(diskService);
            return diskService;
        }

        @Provides
        @Singleton
        public FileListService fileListService(Injector injector) {
            FileListService fileListService = spy(new FileListServiceImpl());
            injector.injectMembers(fileListService);
            return fileListService;
        }
    }
}
