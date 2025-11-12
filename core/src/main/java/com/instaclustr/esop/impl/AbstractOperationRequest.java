package com.instaclustr.esop.impl;

import java.util.Arrays;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.instaclustr.esop.impl.StorageLocation.StorageLocationDeserializer;
import com.instaclustr.esop.impl.StorageLocation.StorageLocationSerializer;
import com.instaclustr.esop.impl.StorageLocation.StorageLocationTypeConverter;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.operations.OperationRequest;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import static java.lang.String.format;

public abstract class AbstractOperationRequest extends OperationRequest {

    @Option(names = {"--sl", "--storage-location"},
        converter = StorageLocationTypeConverter.class,
        description = "Location to which files will be backed up or restored from, in form " +
            "cloudProvider://bucketName/clusterId/datacenterId/nodeId or file:///some/path/bucketName/clusterId/datacenterId/nodeId. " +
            "'cloudProvider' is one of 's3', 'azure' or 'gcp'.",
        required = true)
    @JsonSerialize(using = StorageLocationSerializer.class)
    @JsonDeserialize(using = StorageLocationDeserializer.class)
    public StorageLocation storageLocation;

    @Option(names = {"--insecure-http"},
        description = "If specified, the connection to remote bucket will be insecure, instead HTTPS, HTTP will be used, currently relevant only for S3 and Azure.")
    @JsonProperty("insecure")
    public boolean insecure;

    @Option(names = {"--skip-bucket-verification"},
        description = "Do not check the existence of a bucket. Some storage providers (e.g. S3) requires a special permissions to "
            + "be able to list buckets or query their existence which might not be allowed. This flag will skip that check. Keep in mind "
            + "that if that bucket does not exist, the whole backup operation will fail.")
    public boolean skipBucketVerification;

    @Mixin
    @JsonProperty("proxySettings")
    public ProxySettings proxySettings;

    @Mixin
    @JsonProperty("retry")
    public RetrySpec retry = new RetrySpec();

    @Option(names = {"--cc", "--concurrent-connections", "--parallelism"},
            description = "Number of files (or file parts) to download / upload / hash concurrently. Higher values will increase throughput. Default is 50% of available CPUs."
    )
    @JsonProperty("concurrentConnections")
    public Integer concurrentConnections;

    @Option(names = {"--kmsKeyId"}, description = "Amazon AWS KMS Key ID to use during backup / restore")
    @JsonProperty("kmsKeyId")
    public String kmsKeyId;

    public AbstractOperationRequest() {
        // for picocli
        if (concurrentConnections == null)
            concurrentConnections = getDefaultConcurrentConnections();
    }

    public AbstractOperationRequest(final StorageLocation storageLocation,
                                    final boolean insecure,
                                    final boolean skipBucketVerification,
                                    final ProxySettings proxySettings,
                                    final RetrySpec retry,
                                    final Integer concurrentConnections,
                                    final String kmsKeyId) {
        this.storageLocation = storageLocation;
        this.insecure = insecure;
        this.skipBucketVerification = skipBucketVerification;
        this.proxySettings = proxySettings;
        this.retry = retry == null ? new RetrySpec() : retry;
        this.concurrentConnections = concurrentConnections == null ? getDefaultConcurrentConnections() : concurrentConnections;
        this.kmsKeyId = kmsKeyId;
    }

    public void validate(final Set<String> storageProviders) {
        if (storageLocation == null) {
            throw new IllegalStateException("storageLocation has to be specified!");
        }

        if (retry != null) {
            retry.validate();
        }

        try {
            storageLocation.validate();
        } catch (Exception ex) {
            throw new IllegalStateException(format("Invalid storage location: %s", ex.getLocalizedMessage()));
        }

        if (storageProviders != null && !storageProviders.contains(storageLocation.storageProvider)) {
            throw new IllegalStateException(format("Available storage providers: %s", Arrays.toString(storageProviders.toArray())));
        }

        if (concurrentConnections <= 0) {
            throw new IllegalStateException("--parallelism must be greater than 0");
        }

        if (concurrentConnections > Runtime.getRuntime().availableProcessors()) {
            throw new IllegalStateException("--parallelism value cannot be greater than number of available processors: "
                    + Runtime.getRuntime().availableProcessors());
        }
    }

    /**
     * Get default number of concurrent connections based on 50% of available processors.
     */
    private static int getDefaultConcurrentConnections() {
        return Runtime.getRuntime().availableProcessors() / 2;
    }
}
