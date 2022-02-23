package com.instaclustr.esop.impl.remove;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.restore.BaseRestoreOperationRequest;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.jackson.PathSerializer;
import com.instaclustr.measure.Time;
import com.instaclustr.picocli.typeconverter.TimeMeasureTypeConverter;
import picocli.CommandLine.Option;

public class RemoveBackupRequest extends BaseRestoreOperationRequest {

    @Option(names = {"-n", "--name", "--backup-name"},
            description = "Name of manifest file to delete a backup for")
    public String backupName;

    @Option(names = {"-d", "--dry"}, description = "If set, deletion is not performed but all processing leading up to it is.")
    public boolean dry;

    @Option(names = {"--skip-node-resolution"}, description = "If set, we expect storage location to contain path to node, e.g. file:///my/path/cluster/dc/node1, "
                                                              + "If this is not set, there will be automatic attempt to resolve cluster, dc and node names by connecting to "
                                                              + "a running node Esop / Icarus is connected to. This expects that node to be up as it uses JMX to resolve it. If this is not set, "
                                                              + "it is expected that storageLocation represents the correct path.")
    public boolean skipNodeCoordinatesResolution = false;

    @Option(names = {"-o", "--oldest"}, description = "Removes oldest backup there is, backup names does not need to be specified then")
    public boolean removeOldest;

    @Option(names = {"--older-than"},
            description = "All backups older than this time period will be removed, computed from point when this request was submitted",
            converter = TimeMeasureTypeConverter.class)
    public Time olderThan = Time.zeroTime();

    @Option(names = {"--dcs"}, description = "Only in effect when --global-request is set, if not specified, it will "
                                             + "remove backup(s) for all datacenters")
    @JsonIgnore
    public List<String> dcs = new ArrayList<>();

    @Option(names = {"--global-request"}, description = "If true, it will remove backups for all nodes in storage location, in datacenters based on --dcs option")
    public boolean globalRequest;

    @Option(names = {"--cache-dir"}, description = "Directory where Esop caches downloaded manifests, defaults to a directory called '.esop' in user's home dir.")
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    public Path cacheDir = Paths.get(System.getProperty("user.home"), ".esop");

    public RemoveBackupRequest() {
        // for picocli
    }

    @JsonCreator
    public RemoveBackupRequest(@JsonProperty("type") final String type,
                               @JsonProperty("storageLocation") final StorageLocation storageLocation,
                               @JsonProperty("k8sNamespace") final String k8sNamespace,
                               @JsonProperty("k8sSecretName") final String k8sSecretName,
                               @JsonProperty("insecure") final boolean insecure,
                               @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                               @JsonProperty("proxySettings") final ProxySettings proxySettings,
                               @JsonProperty("retry") final RetrySpec retry,
                               @JsonProperty("backupName") final String backupName,
                               @JsonProperty("dry") final boolean dry,
                               @JsonProperty("skipNodeCoordinatesResolution") final boolean skipNodeCoordinatesResolution,
                               @JsonProperty("olderThan") final Time olderThan,
                               @JsonProperty("cacheDir") final Path cacheDir,
                               @JsonProperty("removeOldest") final boolean removeOldest,
                               @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                               @JsonProperty("globalRequest") final boolean globalRequest) {
        super(storageLocation, 1, k8sNamespace, k8sSecretName, insecure, skipBucketVerification, proxySettings, retry);
        this.type = type;
        this.backupName = backupName;
        this.dry = dry;
        this.skipNodeCoordinatesResolution = skipNodeCoordinatesResolution;
        this.olderThan = olderThan == null ? Time.zeroTime() : olderThan;
        this.cacheDir = (cacheDir == null) ? Paths.get(System.getProperty("user.home"), ".esop") : cacheDir;
        this.removeOldest = removeOldest;
        this.concurrentConnections = concurrentConnections;
        this.globalRequest = globalRequest;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("backupName", backupName)
                          .add("dry", dry)
                          .add("skipNodeCoordinatesResolution", skipNodeCoordinatesResolution)
                          .add("olderThan", olderThan)
                          .add("cacheDir", cacheDir)
                          .add("globalRemoval", globalRequest)
                          .add("dcs", dcs)
                          .toString();
    }

    @Override
    public void validate(final Set<String> storageProviders) {
        if (olderThan == null) {
            olderThan = Time.zeroTime();
        }

        if (removeOldest) {
            if (backupName != null) {
                throw new IllegalStateException("You have specified you want to remove the oldest backup but you specified backupName too!");
            }
            if (olderThan.value != 0) {
                throw new IllegalStateException("You have specified you want to remove the oldest backup but you specified olderThan too!");
            }
        } else {
            if (backupName != null) {
                if (olderThan.value != 0) {
                    throw new IllegalStateException(String.format("You have specified you want to remove backup %s but you specified olderThan too!", backupName));
                }
            } else {
                if (olderThan.value == 0) {
                    throw new IllegalStateException("You have not specified you want to remove any specific backup but you have not specified olderThan either!");
                }
            }
        }
    }
}
