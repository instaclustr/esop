package com.instaclustr.cassandra.backup.impl;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationDeserializer;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationSerializer;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationTypeConverter;
import com.instaclustr.cassandra.backup.impl.StorageLocation.ValidStorageLocation;
import com.instaclustr.kubernetes.KubernetesSecretsReader;
import com.instaclustr.operations.OperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

public abstract class AbstractOperationRequest extends OperationRequest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractOperationRequest.class);

    @Option(names = {"--sl", "--storage-location"},
        converter = StorageLocationTypeConverter.class,
        description = "Location to which files will be backed up or restored from, in form " +
            "cloudProvider://bucketName/clusterId/datacenterId/nodeId or file:///some/path/bucketName/clusterId/datacenterId/nodeId. " +
            "'cloudProvider' is one of 's3', 'azure' or 'gcp'.",
        required = true)
    @NotNull
    @ValidStorageLocation
    @JsonSerialize(using = StorageLocationSerializer.class)
    @JsonDeserialize(using = StorageLocationDeserializer.class)
    public StorageLocation storageLocation;

    @Option(names = {"--k8s-namespace"},
        description = "Name of Kubernetes namespace backup tool runs in, if any.",
        defaultValue = "default")
    public String k8sNamespace = "default";

    @Option(names = {"--k8s-backup-secret-name"},
        description = "Name of Kubernetes secret used for credential retrieval for backup / restores when talking to cloud storages.")
    public String k8sBackupSecretName;

    public AbstractOperationRequest() {
        // for picocli
    }

    public AbstractOperationRequest(@NotNull final StorageLocation storageLocation,
                                    final String k8sNamespace,
                                    final String k8sBackupSecretName) {
        this.storageLocation = storageLocation;
        this.k8sNamespace = k8sNamespace;
        this.k8sBackupSecretName = k8sBackupSecretName;
    }

    @JsonIgnore
    public String resolveSecretName() {
        String resolvedSecretName;

        if (k8sBackupSecretName == null) {
            resolvedSecretName = String.format("cassandra-backup-restore-secret-cluster-%s", storageLocation.clusterId);
        } else {
            resolvedSecretName = k8sBackupSecretName;
        }

        logger.info("Resolved secret name {}", resolvedSecretName);

        return resolvedSecretName;
    }

    @JsonIgnore
    public String resolveKubernetesNamespace() {
        String resolvedNamespace;

        if (k8sNamespace != null) {
            resolvedNamespace = k8sNamespace;
        } else {
            resolvedNamespace = KubernetesSecretsReader.readNamespace();
        }

        logger.info("Resolved k8s namespace {}", resolvedNamespace);

        return resolvedNamespace;
    }
}
