package com.instaclustr.cassandra.backup.impl.backup;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.Time;
import picocli.CommandLine.Option;

@ValidBackupOperationRequest
public class BackupOperationRequest extends BaseBackupOperationRequest {

    @Option(names = {"-s", "--st", "--snapshot-tag"},
        description = "Snapshot tag name. Default is equiv. to 'autosnap-`date +s`'")
    public String snapshotTag = format("autosnap-%d", MILLISECONDS.toSeconds(currentTimeMillis()));

    @Option(names = "--entities",
        description = "entities to backup, if not specified, all keyspaces will be backed up, form 'ks1,ks2,ks2' or 'ks1.cf1,ks2.cf2'")
    public String entities;

    public BackupOperationRequest() {
        // for picocli
    }

    @JsonCreator
    public BackupOperationRequest(@JsonProperty("storageLocation") final StorageLocation storageLocation,
                                  @JsonProperty("duration") final Time duration,
                                  @JsonProperty("bandwidth") final DataRate bandwidth,
                                  @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                                  @JsonProperty("waitForLock") final boolean waitForLock,
                                  @JsonProperty("lockFile") final Path lockFile,
                                  @JsonProperty("sharedContainerPath") final Path sharedContainerPath,
                                  @JsonProperty("cassandraDirectory") final Path cassandraDirectory,
                                  @JsonProperty("entities") final String entities,
                                  @JsonProperty("snapshotTag") final String snapshotTag,
                                  @JsonProperty("offlineSnapshot") final boolean offlineSnapshot,
                                  @JsonProperty("k8sNamespace") final String k8sNamespace,
                                  @JsonProperty("k8sSecretName") final String k8sSecretName) {
        super(storageLocation, duration, bandwidth, concurrentConnections, waitForLock, sharedContainerPath, cassandraDirectory, lockFile, k8sNamespace, k8sSecretName);
        this.entities = entities;
        this.snapshotTag = snapshotTag == null ? format("autosnap-%d", MILLISECONDS.toSeconds(currentTimeMillis())) : snapshotTag;
        this.offlineBackup = offlineSnapshot;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("storageLocation", storageLocation)
            .add("duration", duration)
            .add("bandwidth", bandwidth)
            .add("concurrentConnections", concurrentConnections)
            .add("waitForLock", waitForLock)
            .add("lockFile", lockFile)
            .add("sharedContainerPath", sharedContainerPath)
            .add("cassandraDirectory", cassandraDirectory)
            .add("entities", entities)
            .add("snapshotTag", snapshotTag)
            .add("offlineSnapshot", offlineBackup)
            .add("k8sNamespace", k8sNamespace)
            .add("k8sSecretName", k8sBackupSecretName)
            .toString();
    }
}
