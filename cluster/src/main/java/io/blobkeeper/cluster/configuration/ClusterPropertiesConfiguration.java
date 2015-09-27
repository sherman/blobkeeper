package io.blobkeeper.cluster.configuration;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class ClusterPropertiesConfiguration {

    @Inject
    @Named("blobkeeper.cluster.config")
    private String clusterConfig;

    @Inject
    @Named("blobkeeper.cluster.replication.max.files")
    private int replicationMaxFiles;

    @Inject
    @Named("blobkeeper.cluster.replication.delay")
    private int replicationDelay;

    @Inject
    @Named("blobkeeper.cluster.min.servers")
    private int minServers;

    @Inject
    @Named("blobkeeper.cluster.master")
    private boolean master;

    public int getReplicationMaxFiles() {
        return replicationMaxFiles;
    }

    public int getReplicationDelay() {
        return replicationDelay;
    }

    public int getMinServers() {
        return minServers;
    }

    public String getClusterConfig() {
        return clusterConfig;
    }

    public boolean isMaster() {
        return master;
    }
}
