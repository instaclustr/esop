package com.instaclustr.cassandra.backup.impl.backup;

import java.nio.file.Path;

import com.amazonaws.services.s3.model.MetadataDirective;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
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
    @JsonProperty("commitLogRestoreDirectory")
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    public Path commitLogArchiveOverride;

    public BackupCommitLogsOperationRequest() {
        // for picocli
    }

    @JsonCreator
    public BackupCommitLogsOperationRequest(@JsonProperty("storageLocation") final StorageLocation storageLocation,
                                            @JsonProperty("duration") final Time duration,
                                            @JsonProperty("bandwidth") final DataRate bandwidth,
                                            @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                                            @JsonProperty("lockFile") final Path lockFile,
                                            @JsonProperty("metadataDirective") final MetadataDirective metadataDirective,
                                            @JsonProperty("cassandraDirectory") final Path cassandraDirectory,
                                            @JsonProperty("commitLogRestoreDirectory") final Path commitLogArchiveOverride,
                                            @JsonProperty("k8sNamespace") final String k8sNamespace,
                                            @JsonProperty("k8sSecretName") final String k8sSecretName) {
        super(storageLocation, duration, bandwidth, concurrentConnections, cassandraDirectory, lockFile, metadataDirective, k8sNamespace, k8sSecretName);
        this.commitLogArchiveOverride = commitLogArchiveOverride;
        this.type = "commitlog-backup";
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("storageLocation", storageLocation)
            .add("duration", duration)
            .add("bandwidth", bandwidth)
            .add("concurrentConnections", concurrentConnections)
            .add("lockFile", lockFile)
            .add("metadataDirective", metadataDirective)
            .add("cassandraDirectory", cassandraDirectory)
            .add("commitLogRestoreDirectory", commitLogArchiveOverride)
            .add("k8sNamespace", k8sNamespace)
            .add("k8sSecretName", k8sSecretName)
            .toString();
    }
}
