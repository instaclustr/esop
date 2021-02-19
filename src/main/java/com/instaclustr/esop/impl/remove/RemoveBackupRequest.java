package com.instaclustr.esop.impl.remove;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.restore.BaseRestoreOperationRequest;
import com.instaclustr.esop.impl.retry.RetrySpec;
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

    @Option(names = {"-o", "--oldest"}, description = "removes oldest backup there is, backup names does not need to be specified then")
    public boolean removeOldest;

    public ManifestReport report;

    public RemoveBackupRequest() {
        // for picocli
    }

    @JsonCreator
    public RemoveBackupRequest(@JsonProperty("storageLocation") final StorageLocation storageLocation,
                               @JsonProperty("k8sNamespace") final String k8sNamespace,
                               @JsonProperty("k8sSecretName") final String k8sSecretName,
                               @JsonProperty("insecure") final boolean insecure,
                               @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                               @JsonProperty("proxySettings") final ProxySettings proxySettings,
                               @JsonProperty("retry") final RetrySpec retry,
                               @JsonProperty("backupName") final String backupName,
                               @JsonProperty("dry") final boolean dry,
                               @JsonProperty("report") final ManifestReport report,
                               @JsonProperty("skipNodeCoordinatesResolution") final boolean skipNodeCoordinatesResolution) {
        super(storageLocation, 1, k8sNamespace, k8sSecretName, insecure, skipBucketVerification, proxySettings, retry);
        this.backupName = backupName;
        this.dry = dry;
        this.report = report;
        this.skipNodeCoordinatesResolution = skipNodeCoordinatesResolution;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("backupName", backupName)
            .add("dry", dry)
            .add("report", report)
            .add("skipNodeCoordinatesResolution", skipNodeCoordinatesResolution)
            .toString();
    }

    @Override
    public void validate(final Set<String> storageProviders) {
        super.validate(storageProviders);
        if (backupName == null && !removeOldest) {
            throw new IllegalStateException("You have not set backup name but you have not set --oldest flag!");
        }

        if (backupName != null && removeOldest) {
            throw new IllegalStateException(String.format("You have set backup name %s but you have also set --oldest flag! Choose just one.", backupName));
        }
    }
}
