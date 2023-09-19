package com.instaclustr.esop.impl.list;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.base.MoreObjects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.restore.BaseRestoreOperationRequest;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.jackson.PathSerializer;
import picocli.CommandLine.Option;

public class ListOperationRequest extends BaseRestoreOperationRequest {

    @Option(names = {"--json"}, description = "Prints listing as a JSON, not as a table")
    public boolean json = false;

    @Option(names = {"--resolve-nodes"}, description = "If set, we expect storage location to contain path to node, e.g. file:///my/path/cluster/dc/node1, "
                                                              + "If this is not set, there will be automatic attempt to resolve cluster, dc and node names by connecting to "
                                                              + "a running node Esop / Icarus is connected to. This expects that node to be up as it uses JMX to resolve it. If this is not set, "
                                                              + "it is expected that storageLocation represents the correct path.")
    public boolean resolveNodes = false;

    @Option(names = {"--human-units"}, description = "Displays statistics for listing backups in a more human friendly output.")
    public boolean humanUnits = false;

    @Option(names = {"--to-file"}, description = "Dumps the result into file, not to standard output")
    public String toFile;

    public boolean toRequest;

    public Manifest.AllManifestsReport response;

    @Option(names = {"--simple-format"}, description = "If set, the output will consists of name of backups only and nothing else")
    public boolean simpleFormat = false;

    @Option(names = {"--from-timestamp"}, description = "unix timestamp to say from where we should print the list, including")
    public long fromTimestamp = Long.MAX_VALUE;

    @Option(names = {"--last-n"}, description = "Number of last reports to print")
    public int lastN = 0;

    @Option(names = {"--skip-download"}, description = "Skip downloading backup information from the cloud into the local cache everytime listing is done." +
                                                       "Only set if listing has been done previously and your backup collection has not changed.")
    public boolean skipDownload = false;

    @Option(names = {"--cache-dir"}, description = "Directory where Esop caches downloaded manifests, defaults to a directory called '.esop' in user's home dir.")
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    public Path cacheDir = Paths.get(System.getProperty("user.home"), ".esop");

    public ListOperationRequest() {
        // for picocli
    }

    @JsonCreator
    public ListOperationRequest(@JsonProperty("type") final String type,
                                @JsonProperty("storageLocation") final StorageLocation storageLocation,
                                @JsonProperty("insecure") final boolean insecure,
                                @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                                @JsonProperty("proxySettings") final ProxySettings proxySettings,
                                @JsonProperty("retry") final RetrySpec retry,
                                @JsonProperty("json") final boolean json,
                                @JsonProperty("resolveNodes") final boolean resolveNodes,
                                @JsonProperty("humanUnits") final boolean humanUnits,
                                @JsonProperty("toFile") final String toFile,
                                @JsonProperty("simpleFormat") final boolean simpleFormat,
                                @JsonProperty("fromTimestamp") final Long fromTimestamp,
                                @JsonProperty("lastN") final Integer lastN,
                                @JsonProperty("skipDownload") final boolean skipDownload,
                                @JsonProperty("cacheDir") final Path cacheDir,
                                @JsonProperty("toRequest") final boolean toRequest,
                                @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                                @JsonProperty("response") final Manifest.AllManifestsReport response) {
        super(storageLocation, 1, insecure, skipBucketVerification, proxySettings, retry, null);
        this.json = json;
        this.resolveNodes = resolveNodes;
        this.humanUnits = humanUnits;
        this.toFile = toFile;
        this.simpleFormat = simpleFormat;
        this.fromTimestamp = fromTimestamp == null ? Long.MAX_VALUE : fromTimestamp;
        this.lastN = lastN == null ? 0 : lastN;
        this.skipDownload = skipDownload;
        this.cacheDir = (cacheDir == null) ? Paths.get(System.getProperty("user.home"), ".esop") : cacheDir;
        this.response = response;
        this.toRequest = toRequest;
        this.type = type;
    }

    public static ListOperationRequest getForLocalListing(final BaseRestoreOperationRequest request,
                                                          final Path cacheDir,
                                                          final StorageLocation original) {
        final Path localPathToNode = cacheDir
                .resolve(original.bucket)
                .resolve(original.clusterId)
                .resolve(original.datacenterId)
                .resolve(original.nodeId);

        final StorageLocation cacheLocation = new StorageLocation("file://" + localPathToNode);

        return new ListOperationRequest(
                "list",
                cacheLocation,
                request.insecure,
                request.skipBucketVerification,
                request.proxySettings,
                request.retry,
                false,
                false,
                false,
                null,
                false,
                null,
                null,
                true,
                cacheDir,
                false,
                null,
                null);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("json", json)
                          .add("resolveNodes", resolveNodes)
                          .add("humanUnits", humanUnits)
                          .add("toFile", toFile)
                          .add("simpleFormat", simpleFormat)
                          .add("fromTimestamp", fromTimestamp)
                          .add("lastN", lastN)
                          .add("skipDownload", skipDownload)
                          .add("cacheDir", cacheDir)
                          .toString();
    }
}
