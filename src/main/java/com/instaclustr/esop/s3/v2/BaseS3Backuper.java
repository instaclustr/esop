package com.instaclustr.esop.s3.v2;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.s3.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import com.instaclustr.threading.Executors;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import static java.nio.charset.StandardCharsets.UTF_8;

public class BaseS3Backuper extends Backuper {
    private static final Logger logger = LoggerFactory.getLogger(BaseS3Backuper.class);
    private ExecutorService executorService;

    public final S3Clients s3Clients;
    public final BucketService s3BucketService;

    public BaseS3Backuper(final S3Clients s3Clients,
                          final BackupOperationRequest request) {
        super(request);
        this.s3Clients = s3Clients;
        this.executorService = new Executors.FixedTasksExecutorSupplier().get(100);
        this.s3BucketService = new BaseS3BucketService(s3Clients);
    }

    public BaseS3Backuper(final S3Clients s3Clients,
                          final BackupCommitLogsOperationRequest request) {
        super(request);
        this.s3Clients = s3Clients;
        this.executorService = new Executors.FixedTasksExecutorSupplier().get(100);
        this.s3BucketService = new BaseS3BucketService(s3Clients);
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
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Override
    public FreshenResult freshenRemoteObject(ManifestEntry manifestEntry, RemoteObjectReference object) {
        List<Tag> tags;
        try {
            tags = s3Clients.getNonEncryptingClient()
                            .getObjectTagging(GetObjectTaggingRequest.builder()
                                                                     .bucket(request.storageLocation.bucket)
                                                                     .key(object.canonicalPath)
                                                                     .build()).tagSet();
        }
        catch (NoSuchKeyException ex) {
            return FreshenResult.UPLOAD_REQUIRED;
        }

        Tagging.Builder taggingBuilder = Tagging.builder();

        // If kms key was specified, it means we want to encrypt
        // however if remote tag is not equal to the local one,
        // we for sure need to re-upload because we are going to
        // basically re-encrypt it with a new key.
        if (s3Clients.getKMSKeyOfEncryptedClient().isPresent()) {
            String kmsKey = s3Clients.getKMSKeyOfEncryptedClient().get();
            Tag kmsKeyTag = Tag.builder().key("kmsKey").value(kmsKey).build();
            if (!tags.contains(kmsKeyTag)) {
                return FreshenResult.UPLOAD_REQUIRED;
            }
        // However, if we have not set kmsKey as we do not want to encrypt
        // but remote tag contains kmsKey, then we need to basically re-upload
        // a file, but it will not be encrypted.
        } else if (!tags.isEmpty()) {
            if (tags.stream().anyMatch(t -> t.key().equals("kmsKey"))) {
                return FreshenResult.UPLOAD_REQUIRED;
            }
        }

        // if we reached here, it means that
        // either local kms key equals to remote one
        // or there is no local kms key nor remote one

        // in this case, we just want to refresh a file by copying it into itself
        // which changes last modification date

        // we want to preserve whatever tags it had
        Tagging tagging = taggingBuilder.tagSet(tags).build();

        if (!request.skipRefreshing) {
            CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                                                                   .sourceBucket(request.storageLocation.bucket)
                                                                   .destinationBucket(request.storageLocation.bucket)
                                                                   .sourceKey(object.canonicalPath)
                                                                   .destinationKey(object.canonicalPath)
                                                                   .tagging(tagging)
                                                                   // we need to translate this because request is still working with api v1
                                                                   .metadataDirective(MetadataDirective.fromValue(request.metadataDirective.toString()))
                                                                   .storageClass(StorageClass.STANDARD)
                                                                   .build();
            s3Clients.getClient().copyObject(copyObjectRequest);

            GetObjectAttributesResponse objectAttributes = s3Clients.getNonEncryptingClient()
                                                                    .getObjectAttributes(GetObjectAttributesRequest
                                                                                         .builder()
                                                                                         .bucket(request.storageLocation.bucket)
                                                                                         .key(object.canonicalPath)
                                                                                         .objectAttributes(ObjectAttributes.OBJECT_SIZE)
                                                                                         .build());
            manifestEntry.size = objectAttributes.objectSize();
        }

        return FreshenResult.FRESHENED;
    }

    @Override
    public void uploadFile(ManifestEntry manifestEntry, InputStream localFileStream, RemoteObjectReference objectReference) {
        logger.info("Uploading {}", objectReference.canonicalPath);
        s3Clients.getNonEncryptingClient()
                 .putObject(getPutObjectRequest(objectReference, manifestEntry.size),
                            RequestBody.fromInputStream(localFileStream, manifestEntry.size));
    }

    @Override
    public void uploadEncryptedFile(ManifestEntry manifestEntry, InputStream localFileStream, RemoteObjectReference objectReference) {
        if (!s3Clients.getEncryptingClient().isPresent()) {
            uploadFile(manifestEntry, localFileStream, objectReference);
            return;
        }

        logger.info("Uploading encrypted file {}", objectReference.canonicalPath);

        assert s3Clients.getEncryptingClient().isPresent() : "encrypting client is not present!";
        assert s3Clients.getKMSKeyOfEncryptedClient().isPresent() : "kms key is not present!";

        s3Clients.getEncryptingClient()
                 .get()
                 .putObject(getPutObjectRequest(objectReference, manifestEntry.size,
                                                Tag.builder()
                                                   .key("kmsKey")
                                                   .value(s3Clients.getKMSKeyOfEncryptedClient().get())
                                                   .build()),
                            RequestBody.fromInputStream(localFileStream, manifestEntry.size));

        GetObjectAttributesResponse objectAttributes = s3Clients.getEncryptingClient()
                                                                .get()
                                                                .getObjectAttributes(GetObjectAttributesRequest
                                                                                     .builder()
                                                                                     .bucket(request.storageLocation.bucket)
                                                                                     .key(objectReference.canonicalPath)
                                                                                     .objectAttributes(ObjectAttributes.OBJECT_SIZE)
                                                                                     .build());
        manifestEntry.kmsKeyId = s3Clients.getKMSKeyOfEncryptedClient().get();
        manifestEntry.size = objectAttributes.objectSize();
    }

    @Override
    public void uploadText(String text, RemoteObjectReference objectReference) throws Exception {
        logger.info("Uploading {}", objectReference.canonicalPath);
        byte[] bytes = text.getBytes(UTF_8);

        s3Clients.getNonEncryptingClient()
                 .putObject(getPutObjectRequest(objectReference, bytes.length),
                            RequestBody.fromBytes(bytes));
    }

    @Override
    public void uploadEncryptedText(String plainText, RemoteObjectReference objectReference) throws Exception {
        if (!s3Clients.getEncryptingClient().isPresent()) {
            uploadText(plainText, objectReference);
            return;
        }

        logger.info("Uploading {}", objectReference.canonicalPath);
        byte[] bytes = plainText.getBytes(UTF_8);

        s3Clients.getEncryptingClient().get()
                 .putObject(getPutObjectRequest(objectReference, bytes.length),
                            RequestBody.fromBytes(bytes));
    }

    private PutObjectRequest getPutObjectRequest(RemoteObjectReference s3RemoteObjectReference,
                                                 long unencryptedSize,
                                                 Tag... tags) {
        return PutObjectRequest.builder()
                               .bucket(request.storageLocation.bucket)
                               .key(s3RemoteObjectReference.canonicalPath)
                               .storageClass(StorageClass.STANDARD_IA)
                               .tagging(Tagging.builder().tagSet(tags).build())
                               .build();
    }
}
