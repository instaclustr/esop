package com.instaclustr.esop.impl.backup;

import java.nio.file.Path;
import java.util.List;

import com.amazonaws.services.s3.model.MetadataDirective;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.esop.impl.AbstractOperationRequest;
import com.instaclustr.esop.impl.ListPathSerializer;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.jackson.PathDeserializer;
import com.instaclustr.measure.DataRate;
import com.instaclustr.measure.Time;
import com.instaclustr.picocli.typeconverter.DataRateMeasureTypeConverter;
import com.instaclustr.picocli.typeconverter.PathTypeConverter;
import com.instaclustr.picocli.typeconverter.TimeMeasureTypeConverter;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class BaseBackupOperationRequest extends AbstractOperationRequest {

    @Option(names = {"--data-dir"},
            converter = PathTypeConverter.class,
            defaultValue = "/var/lib/cassandra/data/data")
    @JsonProperty
    @JsonSerialize(using = ListPathSerializer.class)
    @JsonDeserialize(contentUsing = PathDeserializer.class)
    public List<Path> dataDirs;

    @Option(names = {"-d", "--duration"},
        description = "Calculate upload throughput based on total file size ÷ duration.",
        converter = TimeMeasureTypeConverter.class)
    public Time duration;

    @Option(names = {"-b", "--bandwidth"},
        description = "Maximum upload throughput.",
        converter = DataRateMeasureTypeConverter.class)
    public DataRate bandwidth;

    @Option(names = {"--create-missing-bucket"},
        description = "Automatically creates a bucket if it does not exist. If a bucket does not exist, backup operation will fail.")
    public boolean createMissingBucket;

    @Option(names = {"--kmsKeyId"},
        description = "Amazon AWS KMS Key ID to use during backup")
    public String kmsKeyId;

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

    @Option(names = {"--skip-refreshing"},
        description = "Skip refreshing files on their last modification date in remote storage upon backup. When turned on, "
            + "there will be no attempt to change the last modification time, there will be just a check done on their presence "
            + "based on which a respective local file will be upload or not, defaults to false.")
    public boolean skipRefreshing;

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
                                      final MetadataDirective metadataDirective,
                                      final String k8sNamespace,
                                      final String k8sBackupSecretName,
                                      final boolean insecure,
                                      final boolean createMissingBucket,
                                      final boolean skipBucketVerification,
                                      final ProxySettings proxySettings,
                                      final RetrySpec retrySpec,
                                      final boolean skipRefreshing,
                                      final List<Path> dataDirs,
                                      final String kmsKeyId) {
        super(storageLocation, k8sNamespace, k8sBackupSecretName, insecure, skipBucketVerification, proxySettings, retrySpec, concurrentConnections);
        this.duration = duration;
        this.bandwidth = bandwidth;
        this.metadataDirective = metadataDirective == null ? MetadataDirective.COPY : metadataDirective;
        this.createMissingBucket = createMissingBucket;
        this.skipRefreshing = skipRefreshing;
        this.dataDirs = dataDirs;
        this.kmsKeyId = kmsKeyId;
    }
}
