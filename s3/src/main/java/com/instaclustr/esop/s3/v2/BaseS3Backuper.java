
package com.instaclustr.esop.s3.v2;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.impl.backup.BaseBackupOperationRequest;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.s3.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.SdkPartType;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.encryption.s3.S3EncryptionClient;

import static java.nio.charset.StandardCharsets.UTF_8;

public class BaseS3Backuper extends Backuper {
    private static final Logger logger = LoggerFactory.getLogger(BaseS3Backuper.class);

    public final S3Clients s3Clients;
    public final BucketService s3BucketService;
    public final MultipartAbortionService multipartAbortionService;

    public BaseS3Backuper(final S3Clients s3Clients,
                          final BackupOperationRequest request) {
        super(request);
        this.s3Clients = s3Clients;
        this.s3BucketService = new BaseS3BucketService(s3Clients);
        this.multipartAbortionService = new MultipartAbortionService(s3Clients.getClient(), this);
    }

    public BaseS3Backuper(final S3Clients s3Clients,
                          final BackupCommitLogsOperationRequest request) {
        super(request);
        this.s3Clients = s3Clients;
        this.s3BucketService = new BaseS3BucketService(s3Clients);
        this.multipartAbortionService = new MultipartAbortionService(s3Clients.getClient(), this);
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
    public void init(List<ManifestEntry> manifestEntries) {
        multipartAbortionService.abortOrphanedMultiparts(manifestEntries, request);
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
        catch (S3Exception ex) {
            return FreshenResult.UPLOAD_REQUIRED;
        }

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

        return FreshenResult.FRESHENED;
    }

    @Override
    public void uploadFile(ManifestEntry manifestEntry, InputStream localFileStream, RemoteObjectReference objectReference) {
        logger.info("Uploading {}", objectReference.canonicalPath);
        uploadFile(s3Clients.getNonEncryptingClient(),
                   manifestEntry,
                   localFileStream,
                   objectReference,
                   Tagging.builder().build());
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

        uploadFile(s3Clients.getEncryptingClient().get(),
                   manifestEntry,
                   localFileStream,
                   objectReference,
                   Tagging.builder()
                          .tagSet(Tag.builder()
                                     .key("kmsKey")
                                     .value(s3Clients.getKMSKeyOfEncryptedClient().get())
                                     .build())
                          .build());

        manifestEntry.kmsKeyId = s3Clients.getKMSKeyOfEncryptedClient().get();

    }

    @Override
    public void uploadText(String text, RemoteObjectReference objectReference) throws Exception {
        logger.info("Uploading {}", objectReference.canonicalPath);
        byte[] bytes = text.getBytes(UTF_8);

        s3Clients.getNonEncryptingClient()
                 .putObject(getPutObjectRequest(objectReference,
                                                bytes.length,
                                                getDigest(prepareMessageDigest().digest(bytes))),
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
                 .putObject(getPutObjectRequest(objectReference,
                                                bytes.length,
                                                getDigest(prepareMessageDigest().digest(bytes))),
                            RequestBody.fromBytes(bytes));
    }

    private PutObjectRequest getPutObjectRequest(RemoteObjectReference s3RemoteObjectReference,
                                                 long unencryptedSize,
                                                 String checksumSHA256,
                                                 Tag... tags) {
        return PutObjectRequest.builder()
                               .bucket(request.storageLocation.bucket)
                               .key(s3RemoteObjectReference.canonicalPath)
                               .storageClass(StorageClass.STANDARD_IA)
                               .tagging(Tagging.builder().tagSet(tags).build())
                               .checksumSHA256(checksumSHA256)
                               .build();
    }

    private void uploadFile(S3Client s3Client,
                            ManifestEntry manifestEntry,
                            InputStream localFileStream,
                            RemoteObjectReference objectReference,
                            Tagging tagging) {

        CreateMultipartUploadRequest multipartUploadRequest = CreateMultipartUploadRequest.builder()
                                                                                          .bucket(request.storageLocation.bucket)
                                                                                          .key(objectReference.canonicalPath)
                                                                                          .tagging(tagging)
                                                                                          .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                                                                                          .build();

        CreateMultipartUploadResponse multipartUploadResponse = s3Client.createMultipartUpload(multipartUploadRequest);

        String uploadId = multipartUploadResponse.uploadId();

        MessageDigest sha256 = prepareMessageDigest();

        try
        {
            long partSize = Long.parseLong(System.getProperty("upload.max.part.size", Long.toString(100 * 1024 * 1024)));

            if (s3Client instanceof S3EncryptionClient)
                partSize = (partSize / 16) * 16;

            int numberOfParts = (int) Math.ceil((double) manifestEntry.size / partSize);
            List<CompletedPart> completedParts = new ArrayList<>();

            byte[] buffer = new byte[(int) partSize];

            for (int partNumber = 1; partNumber <= numberOfParts; partNumber++)
            {
                int bytesRead = localFileStream.read(buffer);
                ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);
                UploadPartRequest partRequest = UploadPartRequest.builder()
                                                                 .bucket(request.storageLocation.bucket)
                                                                 .key(objectReference.canonicalPath)
                                                                 .uploadId(uploadId)
                                                                 .partNumber(partNumber)
                                                                 .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                                                                 .sdkPartType(partNumber == numberOfParts ? SdkPartType.LAST : SdkPartType.DEFAULT)
                                                                 .build();

                logger.info("Uploading part #{} of {}", partNumber, objectReference.canonicalPath);
                UploadPartResponse partResponse = s3Client.uploadPart(partRequest, RequestBody.fromByteBuffer(byteBuffer));

                completedParts.add(CompletedPart.builder()
                                                .partNumber(partNumber)
                                                .eTag(partResponse.eTag())
                                                .checksumSHA256(partResponse.checksumSHA256())
                                                .build());

                sha256.update(byteBuffer);
            }

            // Complete the multipart upload
            CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                                                                                           .bucket(request.storageLocation.bucket)
                                                                                           .key(objectReference.canonicalPath)
                                                                                           .uploadId(uploadId)
                                                                                           .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                                                                                           .build();

            CompleteMultipartUploadResponse completeResponse = s3Client.completeMultipartUpload(completeRequest);

            if (!completeResponse.sdkHttpResponse().isSuccessful()) {
                throw new RuntimeException(String.format("Unsuccessful multipart upload of %s, upload id %s", objectReference.canonicalPath, uploadId));
            } else {
                logger.debug("Completed multipart upload of {}, upload id {}, etag {}", objectReference.canonicalPath, uploadId, completeResponse.eTag());
            }

            logger.debug("Waiting for " + objectReference.canonicalPath + " to exist");

            s3Clients.getNonEncryptingClient().waiter().waitUntilObjectExists(HeadObjectRequest.builder()
                                                                                               .bucket(request.storageLocation.bucket)
                                                                                               .key(objectReference.canonicalPath)
                                                                                               .build(),
                                                                              WaiterOverrideConfiguration.builder()
                                                                                                         .waitTimeout(Duration.of(1, ChronoUnit.MINUTES))
                                                                                                         .build());

            logger.debug("Object under key " + objectReference.canonicalPath + " exists");

            byte[] fullObjectChecksum = sha256.digest();
            String hexedChecksum = HashSpec.HashAlgorithm.SHA_256.getHasher().getHash(fullObjectChecksum);

            // TODO do we actually need this tag? I don't see it being used anywhere
            Tag checksumTag = Tag.builder()
                    .key("fullObjectChecksum")
                    .value(hexedChecksum)
                    .build();

            // To preserve existing tags, we need to get them first and then add checksumTag to the list
            List<Tag> tags = new ArrayList<>(tagging.tagSet());
            tags.add(checksumTag);

            PutObjectTaggingResponse putObjectTaggingResponse = s3Client.putObjectTagging(PutObjectTaggingRequest.builder()
                                                                                                  .bucket(request.storageLocation.bucket)
                                                                                                  .key(objectReference.canonicalPath)
                                                                                                  .tagging(Tagging.builder().tagSet(tags).build()).build());

            if (!putObjectTaggingResponse.sdkHttpResponse().isSuccessful()) {
                throw new RuntimeException(String.format("Unsuccessful tagging of %s with checksum, upload id %s", objectReference.canonicalPath, uploadId));
            } else {
                logger.debug("Tagged {} with {}", objectReference.canonicalPath, checksumTag.toString());
            }

            if (s3Clients.hasEncryptingClient()) {
                try {
                    GetObjectAttributesResponse objectAttributes = s3Clients.getNonEncryptingClient()
                            .getObjectAttributes(GetObjectAttributesRequest
                                                         .builder()
                                                         .bucket(request.storageLocation.bucket)
                                                         .key(objectReference.canonicalPath)
                                                         .objectAttributes(ObjectAttributes.OBJECT_SIZE)
                                                         .build());

                    manifestEntry.size = objectAttributes.objectSize();
                    manifestEntry.hash = hexedChecksum;
                }
                catch (Throwable t) {
                    logger.warn("Unable to get attribute {} for key {} by GetObjectAttributes request. Please check your permissions.",
                                ObjectAttributes.OBJECT_SIZE, objectReference.canonicalPath);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            multipartAbortionService.abortMultipartUpload(uploadId, request, objectReference);
            throw new RuntimeException(t);
        }
    }

    public static class MultipartAbortionService {

        private final S3Client s3Client;
        private final BaseS3Backuper backuper;

        public MultipartAbortionService(S3Client s3Client,
                                        BaseS3Backuper backuper) {
            this.s3Client = s3Client;
            this.backuper = backuper;
        }

        public void abortOrphanedMultiparts(List<ManifestEntry> manifestEntries, BaseBackupOperationRequest request) {
            ListMultipartUploadsRequest listRequest = ListMultipartUploadsRequest.builder()
                                                                                 .bucket(request.storageLocation.bucket)
                                                                                 .build();

            ListMultipartUploadsResponse listResponse = s3Client.listMultipartUploads(listRequest);

            List<String> entriesKeys = manifestEntries.stream()
                                                      .map(me -> backuper.objectKeyToNodeAwareRemoteReference(me.objectKey).canonicalPath)
                                                      .collect(Collectors.toList());

            for (MultipartUpload upload : listResponse.uploads()
                                                      .stream()
                                                      .filter(upload -> entriesKeys.contains(upload.key()))
                                                      .collect(Collectors.toList())) {
                AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                                                                                      .bucket(request.storageLocation.bucket)
                                                                                      .key(upload.key())
                                                                                      .uploadId(upload.uploadId())
                                                                                      .build();

                logger.info("Aborting orphaned multipart upload of id {} for key {} in bucket {}",
                            abortRequest.uploadId(),
                            abortRequest.key(),
                            abortRequest.bucket());

                s3Client.abortMultipartUpload(abortRequest);
            }
        }

        public void abortMultipartUpload(String uploadId,
                                         BaseBackupOperationRequest request,
                                         RemoteObjectReference objectReference) {
            // Abort the multipart upload if an exception occurs or if it's not completed
            if (uploadId != null) {
                AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                                                                                      .bucket(request.storageLocation.bucket)
                                                                                      .key(objectReference.canonicalPath)
                                                                                      .uploadId(uploadId)
                                                                                      .build();
                try {
                    s3Client.abortMultipartUpload(abortRequest);
                    logger.info("Aborted multipart upload of {}, uploadId: {}", objectReference.canonicalPath, uploadId);
                } catch (NoSuchUploadException ex) {
                    logger.info("There is no such multipart upload of {}, uploadId: {} to delete", objectReference.canonicalPath, uploadId);
                }
            }
        }
    }

    private static MessageDigest prepareMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Throwable t) {
            throw new IllegalStateException("Unable to get instance of SHA-256 message digest");
        }
    }

    private String getDigest(byte[] digest) {
        return Base64.getEncoder().encodeToString(digest);
    }
}