package com.instaclustr.esop.impl.backup;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import com.google.common.base.MoreObjects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.jackson.PathSerializer;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.Time;
import com.instaclustr.picocli.typeconverter.PathTypeConverter;
import picocli.CommandLine.Option;
import software.amazon.awssdk.services.s3.model.MetadataDirective;

public class BackupCommitLogsOperationRequest extends BaseBackupOperationRequest {

    @Option(names = {"--commit-log-dir"},
            description = "Base directory that contains cassandra commit logs in node's runtime",
            converter = PathTypeConverter.class,
            defaultValue = "/var/lib/cassandra/data/commitlog")
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    public Path cassandraCommitLogDirectory;

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
                                            @JsonProperty("cassandraCommitLogDirectory") final Path cassandraCommitLogDirectory,
                                            @JsonProperty("commitLogArchiveOverride") final Path commitLogArchiveOverride,
                                            @JsonProperty("insecure") final boolean insecure,
                                            @JsonProperty("createMissingBucket") final boolean createMissingBucket,
                                            @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                                            @JsonProperty("commitLog") final Path commitLog,
                                            @JsonProperty("online") boolean online,
                                            @JsonProperty("proxySettings") final ProxySettings proxySettings,
                                            @JsonProperty("retry") final RetrySpec retry,
                                            @JsonProperty("skipRefreshing") final boolean skipRefreshing,
                                            @JsonProperty("kmsKeyId") final String kmsKeyId,
                                            @JsonProperty("gcpUniformBucketLevelAccess") final boolean gcpUniformBucketLevelAccess) {
        super(storageLocation,
              duration,
              bandwidth,
              concurrentConnections,
              metadataDirective,
              insecure,
              createMissingBucket,
              skipBucketVerification,
              proxySettings,
              retry,
              skipRefreshing,
              null,
              kmsKeyId,
              gcpUniformBucketLevelAccess);
        this.type = "commitlog-backup";
        this.commitLogArchiveOverride = commitLogArchiveOverride;
        this.commitLog = commitLog;
        this.online = online;
        this.cassandraCommitLogDirectory = cassandraCommitLogDirectory;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("storageLocation", storageLocation)
            .add("duration", duration)
            .add("bandwidth", bandwidth)
            .add("concurrentConnections", concurrentConnections)
            .add("metadataDirective", metadataDirective)
            .add("cassandraCommitLogDirectory", cassandraCommitLogDirectory)
            .add("commitLogArchiveOverride", commitLogArchiveOverride)
            .add("commitLog", commitLog)
            .add("online", online)
            .add("createMissingBucket", createMissingBucket)
            .add("skipBucketVerification", skipBucketVerification)
            .add("insecure", insecure)
            .add("proxySettings", proxySettings)
            .add("retry", retry)
            .add("skipRefreshing", skipRefreshing)
            .toString();
    }

    @JsonIgnore
    public void validate(final Set<String> storageProviders) {
        super.validate(storageProviders);
        if (this.cassandraCommitLogDirectory == null || this.cassandraCommitLogDirectory.toFile().getAbsolutePath().equals("/")) {
            this.cassandraCommitLogDirectory = Paths.get("/var/lib/cassandra/data/commitlog");
        }

        if (!Files.exists(this.cassandraCommitLogDirectory)) {
            throw new IllegalStateException(String.format("cassandraCommitLogDirectory %s does not exist", cassandraCommitLogDirectory));
        }
    }
}
