package io.blobkeeper.cluster.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import io.blobkeeper.common.configuration.MetricModule;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.service.IdGeneratorService;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.configuration.FileModule;
import io.blobkeeper.file.domain.StorageFile;
import io.blobkeeper.file.service.BaseFileTest;
import io.blobkeeper.file.service.FileStorage;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.FIVE_HUNDRED_MILLISECONDS;
import static org.testng.Assert.assertTrue;

/**
 * @author Denis Gabaydulin
 * @since 10/01/2017
 */
@Guice(modules = {RootModule.class, MetricModule.class, FileModule.class})
public class BalancingServiceIntegrationTest extends BaseFileTest {
    private static final Logger log = LoggerFactory.getLogger(CompactionServiceTest.class);

    @Inject
    private FileStorage fileStorage;

    @Inject
    private IndexService indexService;

    @Inject
    private IdGeneratorService generatorService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private CompactionService compactionService;

    @Inject
    private ClusterMembershipService clusterMembershipService;

    @Inject
    private BalancingService balancingService;

    @Inject
    private PartitionService partitionService;

    @Test
    public void balanceSingleDisk() {
        // add some files to disk 0
        for (int i = 0; i < 32; i++) {
            Long fileId = generatorService.generate(1);

            StorageFile file = new StorageFile.StorageFileBuilder()
                    .id(fileId)
                    .type(0)
                    .name("test")
                    .data(Strings.repeat("1234", 10).getBytes())
                    .headers(ImmutableMultimap.of())
                    .build();

            fileStorage.addFile(0, file);
        }

        for (int i = 0; i < 8; i++) {
            balancingService.balance(0);
        }

        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for resource cleanup");

                    return !FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 0)).exists()
                            && !FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1)).exists()
                            && !FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 2)).exists()
                            && !FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 3)).exists();
                });
    }

    @Test
    public void completeRebalancing() {
        // add some files to disk 0
        for (int i = 0; i < 10; i++) {
            Long fileId = generatorService.generate(1);

            StorageFile file = new StorageFile.StorageFileBuilder()
                    .id(fileId)
                    .type(0)
                    .name("test")
                    .data(Strings.repeat("1234", 10).getBytes())
                    .headers(ImmutableMultimap.of())
                    .build();

            fileStorage.addFile(0, file);
        }

        // emulate starting of rebalancing
        Partition dst = partitionService.getNextActivePartition(1);
        partitionService.move(new Partition(0, 0), dst);

        balancingService.balance(0);

        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for resource cleanup");

                    return !FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 0)).exists();
                });
    }

    @Test
    public void completeRebalancingWithStatusChangedToRebalancing() {
        // add some files to disk 0
        for (int i = 0; i < 10; i++) {
            Long fileId = generatorService.generate(1);

            StorageFile file = new StorageFile.StorageFileBuilder()
                    .id(fileId)
                    .type(0)
                    .name("test")
                    .data(Strings.repeat("1234", 10).getBytes())
                    .headers(ImmutableMultimap.of())
                    .build();

            fileStorage.addFile(0, file);
        }

        // emulate starting of rebalancing
        Partition src = partitionService.getById(0, 0);
        Partition dst = partitionService.getNextActivePartition(1);
        partitionService.move(src, dst);
        partitionService.tryStartRebalancing(src);

        balancingService.balance(0);

        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for resource cleanup");

                    return !FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 0)).exists();
                });
    }

    @Test
    public void completeRebalancingWithStatusChangedToDataMoved() {
        // add some files to disk 0
        for (int i = 0; i < 10; i++) {
            Long fileId = generatorService.generate(1);

            StorageFile file = new StorageFile.StorageFileBuilder()
                    .id(fileId)
                    .type(0)
                    .name("test")
                    .data(Strings.repeat("1234", 10).getBytes())
                    .headers(ImmutableMultimap.of())
                    .build();

            fileStorage.addFile(0, file);
        }

        // emulate starting of rebalancing
        Partition src = partitionService.getById(0, 0);
        Partition dst = partitionService.getNextActivePartition(1);
        partitionService.move(src, dst);
        partitionService.tryStartRebalancing(src);
        partitionService.tryFinishRebalancing(src);

        balancingService.balance(0);

        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for resource cleanup");

                    return !FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 0)).exists();
                });
    }

    @BeforeMethod(dependsOnMethods = {"deleteFiles"})
    private void start() throws InterruptedException {
        clusterMembershipService.start("node1");

        indexService.clear();
        fileStorage.start();
        compactionService.start();
    }

    @AfterMethod
    private void stop() throws InterruptedException {
        compactionService.stop();
        fileStorage.stop();

        clusterMembershipService.stop();
    }
}
