package com.instaclustr.esop.impl.restore;

import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.jackson.PathSerializer;
import com.instaclustr.picocli.typeconverter.KeyspaceTablePairsConverter;
import com.instaclustr.picocli.typeconverter.PathTypeConverter;
import picocli.CommandLine.Option;

public class RestoreCommitLogsOperationRequest extends BaseRestoreOperationRequest {

    @Option(names = {"--commit-log-dir"},
            description = "Base directory that contains cassandra commit logs in node's runtime",
            converter = PathTypeConverter.class,
            defaultValue = "/var/lib/cassandra/data/commitlog")
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    public Path cassandraCommitLogDirectory;

    @Option(names = {"-p", "--shared-path"},
        description = "Shared Container path for pod",
        converter = PathTypeConverter.class,
        defaultValue = "/")
    @JsonDeserialize(using = PathDeserializer.class)
    @JsonSerialize(using = PathSerializer.class)
    public Path sharedContainerPath;

    @Option(names = {"--cd", "--config-directory"},
        description = "Directory where configuration of Cassandra is stored.",
        converter = PathTypeConverter.class,
        defaultValue = "/etc/cassandra/")
    @JsonDeserialize(using = PathDeserializer.class)
    @JsonSerialize(using = PathSerializer.class)
    public Path cassandraConfigDirectory;

    @Option(names = {"--ts", "--timestamp-start"},
        description = "When the base snapshot was taken. Only relevant if archived commitlogs are available.",
        required = true)
    @NotNull
    public long timestampStart;

    @Option(names = {"--te", "--timestamp-end"},
        description = "Point-in-time to restore up to. Only relevant if archived commitlogs are available.",
        required = true)
    @NotNull
    public long timestampEnd;

    @Option(names = {"--kt", "--keyspace-tables"},
        description = "Comma separated list of tables to restore. Must include keyspace name in the format <keyspace.table>",
        converter = KeyspaceTablePairsConverter.class)
    public Multimap<String, String> keyspaceTables = ImmutableMultimap.of();

    @Option(names = {"--commitlog-download-dir"},
        description = "Path to directory where commitlogs will be downloaded for restoration.",
        required = true)
    public Path commitlogDownloadDir;

    public RestoreCommitLogsOperationRequest() {
        // for picocli
    }

    @JsonCreator
    public RestoreCommitLogsOperationRequest(@JsonProperty("storageLocation") final StorageLocation storageLocation,
                                             @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                                             @JsonProperty("cassandraCommitLogDirectory") final Path cassandraCommitLogDirectory,
                                             @JsonProperty("sharedContainerPath") final Path sharedContainerPath,
                                             @JsonProperty("cassandraConfigDirectory") final Path cassandraConfigDirectory,
                                             @JsonProperty("commitlogDownloadDir") final Path commitlogDownloadDir,
                                             @JsonProperty("timestampStart") final long timestampStart,
                                             @JsonProperty("timestampEnd") final long timestampEnd,
                                             @JsonProperty("keyspaceTables") final Multimap<String, String> keyspaceTables,
                                             @JsonProperty("k8sNamespace") final String k8sNamespace,
                                             @JsonProperty("k8sSecretName") final String k8sSecretName,
                                             @JsonProperty("insecure") final boolean insecure,
                                             @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                                             @JsonProperty("proxySettings") final ProxySettings proxySettings,
                                             @JsonProperty("retry") final RetrySpec retry) {
        super(storageLocation, concurrentConnections, k8sNamespace, k8sSecretName, insecure, skipBucketVerification, proxySettings, retry);
        this.cassandraCommitLogDirectory = cassandraCommitLogDirectory == null ? Paths.get("/var/lib/cassandra/data/commitlog") : cassandraCommitLogDirectory;
        this.sharedContainerPath = sharedContainerPath == null ? Paths.get("/") : sharedContainerPath;
        this.cassandraConfigDirectory = cassandraConfigDirectory == null ? Paths.get("/etc/cassandra") : cassandraConfigDirectory;
        this.timestampStart = timestampStart;
        this.timestampEnd = timestampEnd;
        this.keyspaceTables = keyspaceTables;
        this.commitlogDownloadDir = commitlogDownloadDir;
        this.type = "commitlog-restore";
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("storageLocation", storageLocation)
            .add("concurrentConnections", concurrentConnections)
            .add("cassandraCommitLogDirectory", cassandraCommitLogDirectory)
            .add("sharedContainerPath", sharedContainerPath)
            .add("cassandraConfigDirectory", cassandraConfigDirectory)
            .add("timestampStart", timestampStart)
            .add("timestampEnd", timestampEnd)
            .add("keyspaceTables", keyspaceTables)
            .add("commitlogDownloadDir", commitlogDownloadDir)
            .add("k8sNamespace", k8sNamespace)
            .add("k8sSecretName", k8sSecretName)
            .add("insecure", insecure)
            .add("skipBucketVerification", skipBucketVerification)
            .add("proxySettings", proxySettings)
            .toString();
    }
}
