package com.instaclustr.esop.s3.v2;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.s3.v1.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import com.instaclustr.threading.Executors;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import static java.nio.charset.StandardCharsets.UTF_8;
import static software.amazon.awssdk.core.retry.RetryMode.STANDARD;
import static software.amazon.awssdk.core.retry.backoff.BackoffStrategy.defaultStrategy;

public class BaseS3Backuper extends Backuper
{
    private static final Logger logger = LoggerFactory.getLogger(BaseS3Backuper.class);
    private ExecutorService executorService;

    public final S3Clients s3Clients;
    public final BucketService s3BucketService;

    public BaseS3Backuper(final S3Clients s3Clients,
                          final BackupOperationRequest request)
    {
        super(request);
        this.s3Clients = s3Clients;
        this.executorService = new Executors.FixedTasksExecutorSupplier().get(100);
        this.s3BucketService = new BaseS3BucketService(s3Clients);
    }

    public BaseS3Backuper(final S3Clients s3Clients,
                          final BackupCommitLogsOperationRequest request)
    {
        super(request);
        this.s3Clients = s3Clients;
        this.executorService = new Executors.FixedTasksExecutorSupplier().get(100);
        this.s3BucketService = new BaseS3BucketService(s3Clients);
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
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Override
    public FreshenResult freshenRemoteObject(RemoteObjectReference object) throws Exception
    {
        List<Tag> tags;
        try
        {
//            s3Clients.getNonEncryptingClient()
//                     .headObject(HeadObjectRequest.builder()
//                                                  .bucket(request.storageLocation.bucket)
//                                                  .key(object.canonicalPath)
//                                                  .build())
//                     .get();

            tags = s3Clients.getNonEncryptingClient()
                            .getObjectTagging(GetObjectTaggingRequest.builder()
                                                                     .bucket(request.storageLocation.bucket)
                                                                     .key(object.canonicalPath)
                                                                     .build()).tagSet();
        }
        catch (NoSuchKeyException ex)
        {
            return FreshenResult.UPLOAD_REQUIRED;
        }

        Tagging.Builder taggingBuilder = Tagging.builder();

        if (s3Clients.getEncryptingClient().isPresent()) {
            taggingBuilder.tagSet(Tag.builder().key("kmsKey").value(s3Clients.getKMSKeyOfEncryptedClient().get()).build());
        }

        if (!request.skipRefreshing)
        {
            CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                                                                   .sourceBucket(request.storageLocation.bucket)
                                                                   .destinationBucket(request.storageLocation.bucket)
                                                                   .sourceKey(object.canonicalPath)
                                                                   .destinationKey(object.canonicalPath)
                                                                   .tagging(taggingBuilder.build())
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
        logger.info("Uploading {}", objectReference.canonicalPath);
        s3Clients.getNonEncryptingClient()
                 .putObject(getPutObjectRequest(objectReference, size),
                            RequestBody.fromInputStream(localFileStream, size));

        waitForCompletion(objectReference);
    }

    @Override
    public void uploadEncryptedFile(long size, InputStream localFileStream, RemoteObjectReference objectReference) throws Exception
    {
        if (!s3Clients.getEncryptingClient().isPresent())
        {
            uploadFile(size, localFileStream, objectReference);
            return;
        }

        logger.info("Uploading encrypted file {}", objectReference.canonicalPath);

        assert s3Clients.getEncryptingClient().isPresent() : "kms key is not present!";

        s3Clients.getEncryptingClient()
                 .get()
                 .putObject(getPutObjectRequest(objectReference, size,
                                                Tag.builder()
                                                   .key("kmsKey")
                                                   .value(s3Clients.getKMSKeyOfEncryptedClient().get())
                                                   .build()),
                            RequestBody.fromInputStream(localFileStream, size));

        waitForCompletion(objectReference);
    }

    @Override
    public void uploadText(String text, RemoteObjectReference objectReference) throws Exception
    {
        logger.info("Uploading {}", objectReference.canonicalPath);
        byte[] bytes = text.getBytes(UTF_8);

        s3Clients.getNonEncryptingClient()
                 .putObject(getPutObjectRequest(objectReference, bytes.length),
                            RequestBody.fromBytes(bytes));

        waitForCompletion(objectReference);
    }

    @Override
    public void uploadEncryptedText(String plainText, RemoteObjectReference objectReference) throws Exception
    {
        if (!s3Clients.getEncryptingClient().isPresent()) {
            uploadText(plainText, objectReference);
            return;
        }

        logger.info("Uploading {}", objectReference.canonicalPath);
        byte[] bytes = plainText.getBytes(UTF_8);

        s3Clients.getEncryptingClient().get()
                 .putObject(getPutObjectRequest(objectReference, bytes.length),
                            RequestBody.fromBytes(bytes));

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

        if (response.matched().exception().isPresent())
        {
            logger.debug("Failed to upload {}.", objectReference.canonicalPath);
            throw new RuntimeException(response.matched().exception().get());
        }

        logger.info("Finished uploading {}.", objectReference.canonicalPath);
    }

    private PutObjectRequest getPutObjectRequest(RemoteObjectReference s3RemoteObjectReference,
                                                 long size,
                                                 Tag... tags)
    {
        return PutObjectRequest.builder()
                               .bucket(request.storageLocation.bucket)
                               .key(s3RemoteObjectReference.canonicalPath)
                               .storageClass(StorageClass.STANDARD_IA)
                               .tagging(Tagging.builder().tagSet(tags).build())
                               .build();
    }
}
