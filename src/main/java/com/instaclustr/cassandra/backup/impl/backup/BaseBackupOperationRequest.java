package com.instaclustr.cassandra.backup.impl.backup;

import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.cassandra.backup.impl.KubernetesAwareRequest;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationDeserializer;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationSerializer;
import com.instaclustr.cassandra.backup.impl.StorageLocation.StorageLocationTypeConverter;
import com.instaclustr.cassandra.backup.impl.StorageLocation.ValidStorageLocation;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.jackson.PathSerializer;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.Time;
import com.instaclustr.operations.OperationRequest;
import com.instaclustr.picocli.typeconverter.DataRateMeasureTypeConverter;
import com.instaclustr.picocli.typeconverter.PathTypeConverter;
import com.instaclustr.picocli.typeconverter.TimeMeasureTypeConverter;
import picocli.CommandLine.Option;

public class BaseBackupOperationRequest extends OperationRequest implements KubernetesAwareRequest {

    @Option(names = {"--sl", "--storage-location"},
            converter = StorageLocationTypeConverter.class,
            description = "Location to which files will be backed up, in form " +
                    "cloudProvider://bucketName/clusterId/datacenterId/nodeId or file:///some/path/bucketName/clusterId/datacenterId/nodeId. " +
                    "'cloudProvider' is one of 's3', 'azure' or 'gcp'.",
            required = true)
    @NotNull
    @ValidStorageLocation
    @JsonSerialize(using = StorageLocationSerializer.class)
    @JsonDeserialize(using = StorageLocationDeserializer.class)
    public StorageLocation storageLocation;

    @Option(names = {"--dd", "--data-directory"},
            description = "Base directory that contains the Cassandra data, cache and commitlog directories",
            converter = PathTypeConverter.class,
            defaultValue = "/var/lib/cassandra")
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    @NotNull
    public Path cassandraDirectory;

    @Option(names = {"-p", "--shared-path"},
            description = "Shared Container path for pod",
            converter = PathTypeConverter.class,
            defaultValue = "/")
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    @NotNull
    public Path sharedContainerPath = Paths.get("/");

    @Option(names = {"-d", "--duration"},
            description = "Calculate upload throughput based on total file size ÷ duration.",
            converter = TimeMeasureTypeConverter.class)
    public Time duration;

    @Option(names = {"-b", "--bandwidth"},
            description = "Maximum upload throughput.",
            converter = DataRateMeasureTypeConverter.class)
    public DataRate bandwidth;

    @Option(names = {"--cc", "--concurrent-connections"},
            description = "Number of files (or file parts) to upload concurrently. Higher values will increase throughput. Default is 10.",
            defaultValue = "10")
    public Integer concurrentConnections;

    @Option(names = {"--lock-file"},
            description = "Directory which will be used for locking purposes for backups")
    public Path lockFile;

    @Option(names = {"-w", "--waitForLock"},
            description = "Wait to acquire the global transfer lock (which prevents more than one backup or restore from running).")
    public Boolean waitForLock = true;

    @Option(names = {"--k8s-namespace"},
            description = "Name of Kubernetes namespace backup tool runs in, if any.",
            defaultValue = "default")
    public String k8sNamespace = "default";

    @Option(names = {"--k8s-backup-secret-name"},
        description = "Name of Kubernetes secret used for credential retrieval for backup / restores when talking to cloud storages.")
    public String k8sBackupSecretName;

    @Option(names = "--offline",
        description = "Cassandra is not running (won't use JMX to snapshot, no token lists uploaded)")
    public boolean offlineBackup;

    public BaseBackupOperationRequest() {
        // for picocli
    }

    public BaseBackupOperationRequest(final StorageLocation storageLocation,
                                      final Time duration,
                                      final DataRate bandwidth,
                                      final Integer concurrentConnections,
                                      final boolean waitForLock,
                                      final Path cassandraDirectory,
                                      final Path sharedContainerPath,
                                      final Path lockFile,
                                      final String k8sNamespace,
                                      final String k8sBackupSecretName) {
        this.storageLocation = storageLocation;
        this.duration = duration;
        this.bandwidth = bandwidth;
        this.sharedContainerPath = sharedContainerPath == null ? Paths.get("/") : sharedContainerPath;
        this.cassandraDirectory = cassandraDirectory == null ? Paths.get("/var/lib/cassandra") : cassandraDirectory;
        this.concurrentConnections = concurrentConnections == null ? 10 : concurrentConnections;
        this.waitForLock = waitForLock;
        this.lockFile = lockFile;
        this.k8sNamespace = k8sNamespace;
        this.k8sBackupSecretName = k8sBackupSecretName;
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
