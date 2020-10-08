package com.instaclustr.esop.impl.backup;

import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.services.s3.model.MetadataDirective;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.AbstractOperationRequest;
import com.instaclustr.esop.impl.ProxySettings;
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

    @Option(names = {"--create-missing-bucket"},
        description = "Automatically creates a bucket if it does not exist. If a bucket does not exist, backup operation will fail.")
    public boolean createMissingBucket;

    public static class MetadataDirectiveTypeConverter implements CommandLine.ITypeConverter<MetadataDirective> {

        @Override
        public MetadataDirective convert(final String value) {
            return MetadataDirective.fromValue(value.toUpperCase());
        }
    }

    @Option(names = {"--md", "--metadata-directive"},
        description = "COPY or REPLACE the metadata from the source object when copying S3 objects.",
        converter = MetadataDirectiveTypeConverter.class)
    public MetadataDirective metadataDirective;

    public BaseBackupOperationRequest() {
        // for picocli
        if (metadataDirective == null) {
            metadataDirective = MetadataDirective.COPY;
        }
    }

    public BaseBackupOperationRequest(final StorageLocation storageLocation,
                                      final Time duration,
                                      final DataRate bandwidth,
                                      final Integer concurrentConnections,
                                      final Path cassandraDirectory,
                                      final MetadataDirective metadataDirective,
                                      final String k8sNamespace,
                                      final String k8sBackupSecretName,
                                      final boolean insecure,
                                      final boolean createMissingBucket,
                                      final boolean skipBucketVerification,
                                      final ProxySettings proxySettings) {
        super(storageLocation, k8sNamespace, k8sBackupSecretName, insecure, skipBucketVerification, proxySettings);
        this.storageLocation = storageLocation;
        this.duration = duration;
        this.bandwidth = bandwidth;
        this.cassandraDirectory = (cassandraDirectory == null || cassandraDirectory.toFile().getAbsolutePath().equals("/")) ? Paths.get("/var/lib/cassandra") : cassandraDirectory;
        this.concurrentConnections = concurrentConnections == null ? 10 : concurrentConnections;
        this.metadataDirective = metadataDirective == null ? MetadataDirective.COPY : metadataDirective;
        this.k8sNamespace = k8sNamespace;
        this.k8sSecretName = k8sBackupSecretName;
        this.createMissingBucket = createMissingBucket;
    }
}
