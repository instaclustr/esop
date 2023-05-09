package com.instaclustr.esop.s3.v2;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.s3.v1.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static software.amazon.awssdk.core.retry.RetryMode.STANDARD;
import static software.amazon.awssdk.core.retry.backoff.BackoffStrategy.defaultStrategy;

public class BaseS3Backuper extends Backuper
{
    private static final Logger logger = LoggerFactory.getLogger(BaseS3Backuper.class);
    private final S3Clients s3Clients;

    private S3TransferManager nonEncryptingTransferManager;
    private Optional<S3TransferManager> encryptingTransferManager;

    public BaseS3Backuper(final S3Clients s3Clients,
                          final BackupOperationRequest request)
    {
        super(request);
        this.s3Clients = s3Clients;
        prepareTransferManager();
    }

    public BaseS3Backuper(final S3Clients s3Clients,
                          final BackupCommitLogsOperationRequest request)
    {
        super(request);
        this.s3Clients = s3Clients;
        prepareTransferManager();
    }

    private void prepareTransferManager() {
        nonEncryptingTransferManager = S3TransferManager.builder()
                                                        .s3Client(s3Clients.getNonEncryptingClient())
                                                        .build();

        encryptingTransferManager = s3Clients.getEncryptingClient()
                                             .map(c -> S3TransferManager.builder().s3Client(c).build());
    }

    private static class UploadTransferListener implements TransferListener
    {

        private final String key;
        public UploadTransferListener(String key) {
            this.key = key;
        }

        @Override
        public void transferInitiated(Context.TransferInitiated context) {
            logger.info("Uploading " + key);
        }

        @Override
        public void transferComplete(Context.TransferComplete context) {
            logger.info("Finished uploading " + key);
        }

        @Override
        public void transferFailed(Context.TransferFailed context) {
            logger.error("Failed to upload " + key, context.exception().getMessage());
        }
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey)
    {
        return new S3RemoteObjectReference(objectKey, objectKey.toString());
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey)
    {
        return new S3RemoteObjectReference(objectKey, resolveNodeAwareRemotePath(objectKey));
    }


    @Override
    protected void cleanup() throws Exception
    {
        s3Clients.close();
        nonEncryptingTransferManager.close();
        encryptingTransferManager.ifPresent(SdkAutoCloseable::close);
    }

    @Override
    public FreshenResult freshenRemoteObject(RemoteObjectReference object) throws Exception
    {

        try
        {
            s3Clients.getClient()
                     .headObject(HeadObjectRequest.builder()
                                                  .bucket(request.storageLocation.bucket)
                                                  .key(object.canonicalPath)
                                                  .build());
        }
        catch (NoSuchKeyException e)
        {
            return FreshenResult.UPLOAD_REQUIRED;
        }

        if (!request.skipRefreshing)
        {
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
    public void uploadFile(long size, InputStream localFileStream, RemoteObjectReference objectReference) throws Exception
    {
        UploadFileRequest request = UploadFileRequest.builder()
                                                     .putObjectRequest(getPutObjectRequest(objectReference, size))
                                                     .source(Paths.get(objectReference.canonicalPath))
                                                     .addTransferListener(new UploadTransferListener(objectReference.canonicalPath))
                                                     .build();

        nonEncryptingTransferManager.uploadFile(request).completionFuture().get();
        waitForCompletion(objectReference);
    }

    @Override
    public void uploadEncryptedFile(long size, InputStream localFileStream, RemoteObjectReference objectReference) throws Exception
    {
        if (!encryptingTransferManager.isPresent())
        {
            uploadFile(size, localFileStream, objectReference);
            return;
        }

        UploadFileRequest request = UploadFileRequest.builder()
                                                     .putObjectRequest(getPutObjectRequest(objectReference, size))
                                                     .source(Paths.get(objectReference.canonicalPath))
                                                     .addTransferListener(new UploadTransferListener(objectReference.canonicalPath))
                                                     .build();
        encryptingTransferManager.get().uploadFile(request).completionFuture().get();
        waitForCompletion(objectReference);
    }

    @Override
    public void uploadText(String text, RemoteObjectReference objectReference) throws Exception
    {
        byte[] bytes = text.getBytes(UTF_8);
        s3Clients.getNonEncryptingClient()
                 .putObject(getPutObjectRequest(objectReference, bytes.length),
                            AsyncRequestBody.fromBytes(bytes)).get();

        waitForCompletion(objectReference);
    }

    @Override
    public void uploadEncryptedText(String plainText, RemoteObjectReference objectReference) throws Exception
    {
        if (!s3Clients.getEncryptingClient().isPresent()) {
            uploadText(plainText, objectReference);
            return;
        }

        byte[] bytes = plainText.getBytes(UTF_8);

        s3Clients.getEncryptingClient().get()
                 .putObject(getPutObjectRequest(objectReference, bytes.length),
                            AsyncRequestBody.fromBytes(bytes)).get();

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
                                                                                                                 .build())
                                                               .get();

        if (response.matched().exception().isPresent())
        {
            logger.debug("Failed to upload {}.", objectReference.canonicalPath);
            throw new RuntimeException(response.matched().exception().get());
        }

        logger.info("Successfully uploaded {}.", objectReference.canonicalPath);
    }

    private PutObjectRequest getPutObjectRequest(RemoteObjectReference s3RemoteObjectReference, long size)
    {
        return PutObjectRequest.builder()
                               .bucket(request.storageLocation.bucket)
                               .key(s3RemoteObjectReference.canonicalPath)
                               .storageClass(StorageClass.STANDARD_IA)
                               .build();
    }
}
