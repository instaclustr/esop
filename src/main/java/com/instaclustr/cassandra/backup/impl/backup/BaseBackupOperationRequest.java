package com.instaclustr.cassandra.backup.impl.backup;

import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.services.s3.model.MetadataDirective;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.cassandra.backup.impl.AbstractOperationRequest;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.jackson.PathSerializer;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.Time;
import com.instaclustr.picocli.typeconverter.DataRateMeasureTypeConverter;
import com.instaclustr.picocli.typeconverter.PathTypeConverter;
import com.instaclustr.picocli.typeconverter.TimeMeasureTypeConverter;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class BaseBackupOperationRequest extends AbstractOperationRequest {

    @Option(names = {"--dd", "--data-directory"},
        description = "Base directory that contains the Cassandra data, cache and commitlog directories",
        converter = PathTypeConverter.class,
        defaultValue = "/var/lib/cassandra")
    @JsonSerialize(using = PathSerializer.class)
    @JsonDeserialize(using = PathDeserializer.class)
    @NotNull
    public Path cassandraDirectory;

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

    public static class MetadataDirectiveTypeConverter implements CommandLine.ITypeConverter<MetadataDirective> {
        @Override
        public MetadataDirective convert(final String value) {
            return MetadataDirective.fromValue(value.toUpperCase());
        }
    }

    @Option(names = {"--md", "--metadata-directive"},
            description = "COPY or REPLACE the metadata from the source object when copying S3 objects.",
            converter = MetadataDirectiveTypeConverter.class)
    public MetadataDirective metadataDirective = MetadataDirective.COPY;

    public BaseBackupOperationRequest() {
        // for picocli
    }

    public BaseBackupOperationRequest(final StorageLocation storageLocation,
                                      final Time duration,
                                      final DataRate bandwidth,
                                      final Integer concurrentConnections,
                                      final Path cassandraDirectory,
                                      final Path lockFile,
                                      final MetadataDirective metadataDirective,
                                      final String k8sNamespace,
                                      final String k8sBackupSecretName) {
        super(storageLocation, k8sNamespace, k8sBackupSecretName);
        this.storageLocation = storageLocation;
        this.duration = duration;
        this.bandwidth = bandwidth;
        this.cassandraDirectory = (cassandraDirectory == null || cassandraDirectory.toFile().getAbsolutePath().equals("/")) ? Paths.get("/var/lib/cassandra") : cassandraDirectory;
        this.concurrentConnections = concurrentConnections == null ? 10 : concurrentConnections;
        this.lockFile = lockFile;
        this.metadataDirective = metadataDirective;
        this.k8sNamespace = k8sNamespace;
        this.k8sSecretName = k8sBackupSecretName;
    }
}
