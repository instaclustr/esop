package com.instaclustr.cassandra.backup.impl.backup;

import java.nio.file.Path;

import com.amazonaws.services.s3.model.MetadataDirective;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.instaclustr.cassandra.backup.impl.ProxySettings;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.jackson.PathSerializer;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.Time;
import com.instaclustr.picocli.typeconverter.PathTypeConverter;
import picocli.CommandLine.Option;

@ValidBackupCommitLogsOperationRequest
public class BackupCommitLogsOperationRequest extends BaseBackupOperationRequest {

    @Option(names = {"--cl-archive"},
        description = "Override path to the commitlog archive directory, relative to the container root.",
        converter = PathTypeConverter.class)
    @JsonProperty("commitLogArchiveOverride")
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    public Path commitLogArchiveOverride;

    @Option(names = {"--commit-log"},
        description = "single commit log file to backup, it will ignore setting --cl-archive.",
        converter = PathTypeConverter.class)
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    public Path commitLog;

    @Option(names = {"--online"},
        description = "If set, storage location will be updated with resolved cluster name, dc name and node id of a node to backup a commit logs for "
            + ", having that node online. By doing so, a user does not need to figure out this information on his own because a backup of a commit log might be done "
            + "in an offline fashion and this information does not need to be known in advance")
    public boolean online;

    public BackupCommitLogsOperationRequest() {
        // for picocli
    }

    @JsonCreator
    public BackupCommitLogsOperationRequest(@JsonProperty("storageLocation") final StorageLocation storageLocation,
                                            @JsonProperty("duration") final Time duration,
                                            @JsonProperty("bandwidth") final DataRate bandwidth,
                                            @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                                            @JsonProperty("metadataDirective") final MetadataDirective metadataDirective,
                                            @JsonProperty("cassandraDirectory") final Path cassandraDirectory,
                                            @JsonProperty("commitLogArchiveOverride") final Path commitLogArchiveOverride,
                                            @JsonProperty("k8sNamespace") final String k8sNamespace,
                                            @JsonProperty("k8sSecretName") final String k8sSecretName,
                                            @JsonProperty("insecure") final boolean insecure,
                                            @JsonProperty("createMissingBucket") final boolean createMissingBucket,
                                            @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                                            @JsonProperty("commitLog") final Path commitLog,
                                            @JsonProperty("online") boolean online,
                                            @JsonProperty("proxySettings") final ProxySettings proxySettings) {
        super(storageLocation,
              duration,
              bandwidth,
              concurrentConnections,
              cassandraDirectory,
              metadataDirective,
              k8sNamespace,
              k8sSecretName,
              insecure,
              createMissingBucket,
              skipBucketVerification,
              proxySettings);
        this.type = "commitlog-backup";
        this.commitLogArchiveOverride = commitLogArchiveOverride;
        this.commitLog = commitLog;
        this.online = online;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("storageLocation", storageLocation)
            .add("duration", duration)
            .add("bandwidth", bandwidth)
            .add("concurrentConnections", concurrentConnections)
            .add("metadataDirective", metadataDirective)
            .add("cassandraDirectory", cassandraDirectory)
            .add("commitLogArchiveOverride", commitLogArchiveOverride)
            .add("commitLog", commitLog)
            .add("online", online)
            .add("k8sNamespace", k8sNamespace)
            .add("k8sSecretName", k8sSecretName)
            .add("createMissingBucket", createMissingBucket)
            .add("skipBucketVerification", skipBucketVerification)
            .add("insecure", insecure)
            .add("proxySettings", proxySettings)
            .toString();
    }
}
