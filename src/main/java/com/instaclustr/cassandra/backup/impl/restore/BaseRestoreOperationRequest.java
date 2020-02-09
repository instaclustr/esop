package com.instaclustr.cassandra.backup.impl.restore;

import javax.validation.constraints.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.cassandra.backup.impl.KubernetesAwareRequest;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationDeserializer;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationSerializer;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationTypeConverter;
import com.instaclustr.cassandra.backup.impl.StorageLocation.ValidStorageLocation;
import com.instaclustr.operations.OperationRequest;
import picocli.CommandLine.Option;

public class BaseRestoreOperationRequest extends OperationRequest implements KubernetesAwareRequest {

    @Option(names = {"--sl", "--storage-location"},
            converter = StorageLocationTypeConverter.class,
            description = "Location from which files will be fetched for restore, in form " +
                    "cloudProvider://bucketName/clusterId/datacenterId/nodeId or file:///some/path/bucketName/clusterId/datacenterId/nodeId. " +
                    "'cloudProvider' is one of 'aws', 'azure' or 'gcp'.",
            required = true
    )
    @NotNull
    @ValidStorageLocation
    @JsonSerialize(using = StorageLocationSerializer.class)
    @JsonDeserialize(using = StorageLocationDeserializer.class)
    public StorageLocation storageLocation;

    @Option(names = {"--cc", "--concurrent-connections"},
            description = "Number of files (or file parts) to download concurrently. Higher values will increase throughput. Default is 10.",
            defaultValue = "10"
    )
    public Integer concurrentConnections = 10;

    @Option(names = {"-w", "--waitForLock"},
            description = "Wait to acquire the global transfer lock (which prevents more than one backup or restore from running)."
    )
    public boolean waitForLock = true;

    @Option(names = {"--lock-file"},
        description = "Directory which will be used for locking purposes for backups")
    public Path lockFile;

    @Option(names = {"--k8s-namespace"},
        description = "Name of Kubernetes namespace backup tool runs in, if any.",
        defaultValue = "default")
    public String k8sNamespace = "default";

    @Option(names = {"--k8s-backup-secret-name"},
        description = "Name of Kubernetes secret used for credential retrieval for backup / restores when talking to cloud storages.")
    public String k8sBackupSecretName;

    public BaseRestoreOperationRequest() {
        // for picocli
    }

    public BaseRestoreOperationRequest(final StorageLocation storageLocation,
                                       final Integer concurrentConnections,
                                       final boolean waitForLock,
                                       final Path lockFile,
                                       final String k8sNamespace,
                                       final String k8sSecretName) {
        this.storageLocation = storageLocation;
        this.concurrentConnections = concurrentConnections;
        this.waitForLock = waitForLock;
        this.lockFile = lockFile;
        this.k8sNamespace = k8sNamespace;
        this.k8sBackupSecretName = k8sSecretName;
    }

    @Override
    public String getNamespace() {
        return k8sNamespace;
    }

    @Override
    public String getSecretName() {
        return k8sBackupSecretName;
    }
}
