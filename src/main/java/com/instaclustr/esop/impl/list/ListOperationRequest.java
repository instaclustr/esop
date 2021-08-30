package com.instaclustr.esop.impl.list;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.restore.BaseRestoreOperationRequest;
import com.instaclustr.esop.impl.retry.RetrySpec;
import picocli.CommandLine.Option;

public class ListOperationRequest extends BaseRestoreOperationRequest {

    @Option(names = {"--json"}, description = "Prints listing as a JSON, not as a table")
    public boolean json = false;

    @Option(names = {"--skip-node-resolution"}, description = "If set, we expect storage location to contain path to node, e.g. file:///my/path/cluster/dc/node1, "
        + "If this is not set, there will be automatic attempt to resolve cluster, dc and node names by connecting to "
        + "a running node Esop / Icarus is connected to. This expects that node to be up as it uses JMX to resolve it. If this is not set, "
        + "it is expected that storageLocation represents the correct path.")
    public boolean skipNodeCoordinatesResolution = false;

    @Option(names = {"--human-units"}, description = "Displays statistics for listing backups in a more human friendly output.")
    public boolean humanUnits = false;

    @Option(names = {"--to-file"}, description = "Dumps the result into file, not to standard output")
    public String toFile;

    @Option(names = {"--simple-format"}, description = "If set, the output will consists of name of backups only and nothing else")
    public boolean simpleFormat = false;

    @Option(names = {"--from-timestamp"}, description = "unix timestamp to say from where we should print the list, including")
    public long fromTimestamp = Long.MAX_VALUE;

    @Option(names = {"--last-n"}, description = "Number of last reports to print")
    public int lastN = 0;

    @Option(names = {"--skip-download"}, description = "Skip downloading manifests from the cloud into the local cache everytime listing is called." +
            "Only set if manifests have already been downloaded previously and your backup collection has not changed.")
    public boolean skipDownload = false;

    public ListOperationRequest() {
        // for picocli
    }

    @JsonCreator
    public ListOperationRequest(@JsonProperty("storageLocation") final StorageLocation storageLocation,
                                @JsonProperty("k8sNamespace") final String k8sNamespace,
                                @JsonProperty("k8sSecretName") final String k8sSecretName,
                                @JsonProperty("insecure") final boolean insecure,
                                @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                                @JsonProperty("proxySettings") final ProxySettings proxySettings,
                                @JsonProperty("retry") final RetrySpec retry,
                                @JsonProperty("json") final boolean json,
                                @JsonProperty("skipNodeCoordinatesResolution") final boolean skipNodeCoordinatesResolution,
                                @JsonProperty("humanUnits") final boolean humanUnits,
                                @JsonProperty("toFile") final String toFile,
                                @JsonProperty("simpleFormat") final boolean simpleFormat,
                                @JsonProperty("fromTimestamp") final Long fromTimestamp,
                                @JsonProperty("lastN") final Integer lastN,
                                @JsonProperty("skipDownload") final boolean skipDownload) {
        super(storageLocation, 1, k8sNamespace, k8sSecretName, insecure, skipBucketVerification, proxySettings, retry);
        this.json = json;
        this.skipNodeCoordinatesResolution = skipNodeCoordinatesResolution;
        this.humanUnits = humanUnits;
        this.toFile = toFile;
        this.simpleFormat = simpleFormat;
        this.fromTimestamp = fromTimestamp == null ? Long.MAX_VALUE : fromTimestamp;
        this.lastN = lastN == null ? 0 : lastN;
        this.skipDownload = skipDownload;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("json", json)
            .add("skipNodeCoordinatesResolution", skipNodeCoordinatesResolution)
            .add("humanUnits", humanUnits)
            .add("toFile", toFile)
            .add("simpleFormat", simpleFormat)
            .add("fromTimestamp", fromTimestamp)
            .add("lastN", lastN)
            .add ("skipDownload", skipDownload)
            .toString();
    }
}
