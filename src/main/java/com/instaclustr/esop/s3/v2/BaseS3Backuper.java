package com.instaclustr.esop.s3.v2;

import java.io.InputStream;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.v1.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.StorageClass;

import static java.nio.charset.StandardCharsets.UTF_8;
import static software.amazon.awssdk.core.retry.RetryMode.STANDARD;
import static software.amazon.awssdk.core.retry.backoff.BackoffStrategy.defaultStrategy;
import static software.amazon.awssdk.core.sync.RequestBody.fromBytes;
import static software.amazon.awssdk.core.sync.RequestBody.fromInputStream;

public class BaseS3Backuper extends Backuper {
    private static final Logger logger = LoggerFactory.getLogger(BaseS3Backuper.class);
    private final S3Clients s3Clients;

    public BaseS3Backuper(final S3ClientsFactory s3ClientsFactory,
                          final S3ConfigurationResolver configurationResolver,
                          final BackupOperationRequest request) {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    public BaseS3Backuper(final S3ClientsFactory s3ClientsFactory,
                          final S3ConfigurationResolver configurationResolver,
                          final BackupCommitLogsOperationRequest request) {
        super(request);
        s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) {
        return new S3RemoteObjectReference(objectKey, objectKey.toString());
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) {
        return new S3RemoteObjectReference(objectKey, resolveNodeAwareRemotePath(objectKey));
    }


    @Override
    protected void cleanup() throws Exception {
        s3Clients.close();
    }

    @Override
    public FreshenResult freshenRemoteObject(RemoteObjectReference object) throws Exception {

        try {
            s3Clients.getClient()
                     .headObject(HeadObjectRequest.builder()
                                                  .bucket(request.storageLocation.bucket)
                                                  .key(object.canonicalPath)
                                                  .build());
        } catch (NoSuchKeyException e) {
            return FreshenResult.UPLOAD_REQUIRED;
        }

        if (!request.skipRefreshing) {
            CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                                                                   .sourceBucket(request.storageLocation.bucket)
                                                                   .destinationBucket(request.storageLocation.bucket)
                                                                   .sourceKey(object.canonicalPath)
                                                                   .destinationKey(object.canonicalPath)
                                                                   // we need to translate this because request is still working with api v1
                                                                   .metadataDirective(MetadataDirective.fromValue(request.metadataDirective.toString()))
                                                                   .storageClass(StorageClass.STANDARD)
                                                                   .build();
            s3Clients.getClient().copyObject(copyObjectRequest);
            waitForCompletion(object);
        }

        return FreshenResult.FRESHENED;
    }

    @Override
    public void uploadFile(long size, InputStream localFileStream, RemoteObjectReference objectReference) throws Exception {
        s3Clients.getNonEncryptingClient()
                 .putObject(getPutObjectRequest(objectReference),
                            fromInputStream(localFileStream, size));

        waitForCompletion(objectReference);
    }

    @Override
    public void uploadEncryptedFile(long size, InputStream localFileStream, RemoteObjectReference objectReference) throws Exception
    {
        if (!s3Clients.getEncryptingClient().isPresent()) {
            uploadFile(size, localFileStream, objectReference);
            return;
        }

        s3Clients.getEncryptingClient().get().putObject(getPutObjectRequest(objectReference),
                                                        fromInputStream(localFileStream, size));

        waitForCompletion(objectReference);
    }

    @Override
    public void uploadText(String text, RemoteObjectReference objectReference) throws Exception {
        s3Clients.getNonEncryptingClient()
                 .putObject(getPutObjectRequest(objectReference),
                            fromBytes(text.getBytes(UTF_8)));

        waitForCompletion(objectReference);
    }

    @Override
    public void uploadEncryptedText(String plainText, RemoteObjectReference objectReference) throws Exception
    {
        if (!s3Clients.getEncryptingClient().isPresent()) {
            uploadText(plainText, objectReference);
            return;
        }

        s3Clients.getEncryptingClient().get()
                 .putObject(getPutObjectRequest(objectReference),
                            fromBytes(plainText.getBytes(UTF_8)));

        waitForCompletion(objectReference);
    }

    private void waitForCompletion(RemoteObjectReference objectReference) throws Exception
    {
        WaiterResponse<HeadObjectResponse> response = s3Clients.getClient()
                                                               .waiter()
                                                               .waitUntilObjectExists(HeadObjectRequest.builder()
                                                                                                       .bucket(request.storageLocation.bucket)
                                                                                                       .key(objectReference.canonicalPath)
                                                                                                       .build(),
                                                                                      WaiterOverrideConfiguration.builder()
                                                                                                                 .backoffStrategy(defaultStrategy(STANDARD))
                                                                                                                 .build());

        if (response.matched().exception().isPresent()) {
            logger.debug("Failed to upload {}.", objectReference.canonicalPath);
            throw new RuntimeException(response.matched().exception().get());
        }

        logger.debug("Successfully uploaded {}.", objectReference.canonicalPath);
    }

    private PutObjectRequest getPutObjectRequest(RemoteObjectReference s3RemoteObjectReference) {
        return PutObjectRequest.builder()
                               .bucket(request.storageLocation.bucket)
                               .key(s3RemoteObjectReference.canonicalPath)
                               .build();
    }
}
