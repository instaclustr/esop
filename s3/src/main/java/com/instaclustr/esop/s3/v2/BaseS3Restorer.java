package com.instaclustr.esop.s3.v2;

import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.io.CharStreams;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.esop.s3.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import com.instaclustr.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class BaseS3Restorer extends Restorer
{
    private static final Logger logger = LoggerFactory.getLogger(BaseS3Restorer.class);

    private Map<String, S3Client> kmsSpecificS3Clients = new ConcurrentHashMap<>();

    public final S3Clients s3Clients;
    public BucketService s3BucketService;

    public BaseS3Restorer(S3Clients s3Clients, RestoreOperationRequest request)
    {
        super(request);
        this.s3Clients = s3Clients;
        prepareS3Clients();
        this.s3BucketService = new BaseS3BucketService(s3Clients);
    }

    public BaseS3Restorer(S3Clients s3Clients, RestoreCommitLogsOperationRequest request)
    {
        super(request);
        this.s3Clients = s3Clients;
        this.s3BucketService = new BaseS3BucketService(s3Clients);
        prepareS3Clients();
    }

    public BaseS3Restorer(S3Clients s3Clients, ListOperationRequest request)
    {
        super(request);
        this.s3Clients = s3Clients;
        prepareS3Clients();
        this.s3BucketService = new BaseS3BucketService(s3Clients);
        prepareS3Clients();
    }

    public BaseS3Restorer(S3Clients s3Clients, RemoveBackupRequest request)
    {
        super(request);
        this.s3Clients = s3Clients;
        this.s3BucketService = new BaseS3BucketService(s3Clients);
        prepareS3Clients();
    }

    private void prepareS3Clients() {
        if (s3Clients.getEncryptingClient().isPresent() && s3Clients.getKMSKeyOfEncryptedClient().isPresent()) {
            kmsSpecificS3Clients.put(s3Clients.getKMSKeyOfEncryptedClient().get(), s3Clients.getEncryptingClient().get());
        }
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) {
        return new S3RemoteObjectReference(objectKey, objectKey.toFile().toString());
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
    public String downloadFileToString(RemoteObjectReference objectReference, boolean isEncrypted) throws Exception {

        S3Client s3Client;

        if (isEncrypted) {
            s3Client = s3Clients.getEncryptingClient().orElse(s3Clients.getNonEncryptingClient());
        } else {
            s3Client = s3Clients.getNonEncryptingClient();
        }

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                                            .bucket(request.storageLocation.bucket)
                                                            .key(objectReference.canonicalPath)
                                                            .build();

        try (final InputStreamReader isr = new InputStreamReader(s3Client.getObject(getObjectRequest))) {
            return CharStreams.toString(isr);
        }
    }

    @Override
    public String downloadFileToString(RemoteObjectReference objectReference) throws Exception {
        return downloadFileToString(objectReference, false);
    }

    @Override
    public void downloadFile(Path localPath, ManifestEntry manifestEntry, RemoteObjectReference objectReference) throws Exception {

        try {
            GetObjectTaggingResponse taggingResponse = s3Clients.getNonEncryptingClient()
                                                                .getObjectTagging(GetObjectTaggingRequest.builder()
                                                                                                         .bucket(request.storageLocation.bucket)
                                                                                                         .key(objectReference.canonicalPath)
                                                                                                         .build());

            String kmsKey = taggingResponse.tagSet()
                                           .stream()
                                           .filter(t -> t.key().equals("kmsKey"))
                                           .findFirst()
                                           .map(Tag::value)
                                           .orElse(null);

            // We need to resolve S3 manager which uses kms key which remote file is encrypted with,
            // so we have the right one for decryption.
            // It is expected that every file in a logical backup will be encrypted with same KMS key
            // however we need to resolve it per manifest entry anyway
            S3Client s3Client = resolveS3Client(kmsKey);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                                                .bucket(request.storageLocation.bucket)
                                                                .key(objectReference.canonicalPath)
                                                                .build();

            FileUtils.createDirectory(localPath.getParent());

            Files.copy(s3Client.getObject(getObjectRequest), localPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Override
    public String downloadTopology(Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        S3Object s3Object = getBlobItemPath(remotePrefix.toString(), keyFilter);
        return downloadFileToString(objectKeyToRemoteReference(Paths.get(s3Object.key())), false);
    }

    @Override
    public String downloadManifest(Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        final S3Object manifestObject = getManifest(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = manifestObject.key().split("/")[manifestObject.key().split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)), false);
    }

    @Override
    public String downloadNodeFile(Path remotePrefix, Predicate<String> keyFilter) throws Exception {
        final S3Object s3Object = getBlobItemPath(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = s3Object.key().split("/")[s3Object.key().split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public void consumeFiles(RemoteObjectReference prefix, Consumer<RemoteObjectReference> consumer) throws Exception {
        final Path bucketPath = Paths.get(request.storageLocation.clusterId).resolve(request.storageLocation.datacenterId).resolve(request.storageLocation.nodeId);
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                                                                        .bucket(request.storageLocation.bucket)
                                                                        .prefix(prefix.canonicalPath)
                                                                        .build();

        ListObjectsV2Response listObjectsV2Response;

        do {
            listObjectsV2Response = s3Clients.getClient().listObjectsV2(listObjectsV2Request);
            listObjectsV2Response.contents().stream()
                                 .filter(o -> !o.key().endsWith("/"))
                                 .forEach(o -> consumer.accept(objectKeyToNodeAwareRemoteReference(bucketPath.relativize(Paths.get(o.key())))));
        } while (listObjectsV2Response.isTruncated());
    }

    @Override
    public void delete(final Path objectKey, boolean nodeAware) throws Exception {
        RemoteObjectReference remoteObjectReference;
        if (nodeAware) {
            remoteObjectReference = objectKeyToNodeAwareRemoteReference(objectKey);
        } else {
            remoteObjectReference = objectKeyToRemoteReference(objectKey);
        }
        final Path fileToDelete = Paths.get(request.storageLocation.bucket,
                                            remoteObjectReference.canonicalPath);
        logger.info("Deleting file {} ", fileToDelete);
        s3Clients.getNonEncryptingClient()
                 .deleteObject(DeleteObjectRequest.builder()
                                                  .bucket(request.storageLocation.bucket)
                                                  .key(remoteObjectReference.canonicalPath)
                                                  .build());

    }

    @Override
    public void delete(final Manifest.ManifestReporter.ManifestReport backupToDelete, final RemoveBackupRequest request) throws Exception {
        logger.info("Deleting backup {}", backupToDelete.name);
        if (backupToDelete.reclaimableSpace > 0 && !backupToDelete.getRemovableEntries().isEmpty()) {
            //convert removable entries into S3 object keys
            List<String> toRemove = backupToDelete.getRemovableEntries()
                                                  .stream()
                                                  .map(entry -> objectKeyToNodeAwareRemoteReference(Paths.get(entry)).canonicalPath)
                                                  .collect(toList());

            for (String remove : toRemove) {
                if (request.dry) {
                    logger.info("Deletion of {} was executed in dry mode.", remove);
                } else {
                    logger.info("Deleting file {}", remove);
                }
            }

            // deletion by 100 items at most
            List<List<String>> removalSets = splitList(toRemove, 100);
            List<DeleteObjectsRequest> deletes = removalSets.stream()
                                              .map(set -> set.stream()
                                                             .map(entry -> ObjectIdentifier.builder().key(entry).build())
                                                             .collect(toList()))
                                              .map(list -> Delete.builder().objects(list).build())
                                              .map(delete -> DeleteObjectsRequest.builder()
                                                                                 .bucket(request.storageLocation.bucket)
                                                                                 .delete(delete)
                                                                                 .build())
                                              .collect(toList());


            if (!request.dry){
                for (DeleteObjectsRequest delete : deletes) {
                    DeleteObjectsResponse deleteObjectsResponse = s3Clients.getNonEncryptingClient().deleteObjects(delete);
                    if (deleteObjectsResponse.hasErrors() && !deleteObjectsResponse.errors().isEmpty()) {
                        throw new RuntimeException(deleteObjectsResponse.errors()
                                                                        .stream()
                                                                        .map(S3Error::toString)
                                                                        .collect(joining(",")));
                    }
                }
                logger.info("Deletion of files complete");
            }
        }

        Path key = backupToDelete.manifest.objectKey;

        // manifest and topology as the last
        if (!request.dry) {
            logger.info("Deleting file {} from S3" , key);
            //delete in S3
            deleteNodeAwareKey(key);
            //delete in local cache
            logger.info("Deleting file {} from local cache" , key);
            localFileRestorer.deleteNodeAwareKey(key);
        } else {
            logger.info("Deletion of {} was executed in dry mode.", key);
        }

    }

    @Override
    public List<Manifest> listManifests() throws Exception {
        //If skipDownload flag is not set, download manifests
        if (this.request instanceof ListOperationRequest) {
            if (!((ListOperationRequest) this.request).skipDownload) {
                StorageLocation location = this.localFileRestorer.getStorageLocation();
                Path downloadDirectory = location.fileBackupDirectory.resolve(this.localFileRestorer.getStorageLocation().bucket);
                downloadManifestsToDirectory(downloadDirectory);
            }
        }
        return localFileRestorer.listManifests();
    }

    @Override
    public List<StorageLocation> listNodes() throws Exception {
        return localFileRestorer.listNodes();
    }

    @Override
    public List<StorageLocation> listNodes(final String dc) throws Exception {
        return localFileRestorer.listNodes(dc);
    }

    @Override
    public List<StorageLocation> listNodes(final List<String> dcs) throws Exception {
        return localFileRestorer.listNodes(dcs);
    }

    @Override
    public List<String> listDcs() throws Exception {
        return localFileRestorer.listDcs();
    }

    private List<S3Object> listBucket(final String remotePrefix, final Predicate<String> keyFilter) throws Exception {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                                                                        .bucket(request.storageLocation.bucket)
                                                                        .prefix(remotePrefix)
                                                                        .build();

        ListObjectsV2Response listObjectsV2Response;

        final List<S3Object> summaryList = new ArrayList<>();

        do {
            listObjectsV2Response = s3Clients.getClient().listObjectsV2(listObjectsV2Request);
            listObjectsV2Response.contents().stream()
                                 .filter(o -> !o.key().endsWith("/"))
                                 .filter(o -> keyFilter.test(o.key()))
                                 .collect(toCollection(() -> summaryList));
        } while (listObjectsV2Response.isTruncated());

        return summaryList;
    }

    private S3Object getBlobItemPath(final String remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final List<S3Object> summaryList = listBucket(remotePrefix, keyFilter);

        if (summaryList.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter: %s", summaryList));
        }

        return summaryList.get(0);
    }

    private S3Object getManifest(final String remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final List<S3Object> summaryList = listBucket(remotePrefix, keyFilter);

        if (summaryList.isEmpty()) {
            throw new IllegalStateException("There is no manifest requested found.");
        }

        final String manifestFullKey = Manifest.parseLatestManifest(summaryList.stream().map(S3Object::key).collect(toList()));

        return summaryList.stream()
                          .filter(o -> o.key().endsWith(manifestFullKey))
                          .findFirst()
                          .orElseThrow(() -> new IllegalStateException("Unable to get the latest manifest from remote prefix " + remotePrefix));
    }

    private S3Client resolveS3Client(String remoteKmsKey) {
        if (remoteKmsKey == null) {
            return s3Clients.getNonEncryptingClient();
        }

        S3Client s3Client = kmsSpecificS3Clients.get(remoteKmsKey);

        if (s3Client != null) {
            return s3Client;
        }

        S3Client encryptingClient = new S3ClientsFactory().getEncryptingClient(s3Clients.getNonEncryptingClient(), remoteKmsKey);
        kmsSpecificS3Clients.put(remoteKmsKey, encryptingClient);

        return kmsSpecificS3Clients.get(remoteKmsKey);
    }

    public static List<List<String>> splitList(List<String> list, int maxLength) {
        List<List<String>> result = new ArrayList<>();
        int index = 0;
        while (index < list.size()) {
            int endIndex = Math.min(index + maxLength, list.size());
            List<String> subList = new ArrayList<>(list.subList(index, endIndex));
            result.add(subList);
            index += maxLength;
        }
        return result;
    }

    public void downloadManifestsToDirectory(Path downloadDir) throws Exception {
        FileUtils.createDirectory(downloadDir);
        FileUtils.cleanDirectory(downloadDir.toFile());

        final List<S3Object> manifestSumms = listBucket(resolveNodeAwareRemotePath(Paths.get("manifests")), filter -> true);
        for (S3Object o : manifestSumms) {
            Path manifestPath = Paths.get(o.key());
            Path destination = downloadDir.resolve(manifestPath);
            downloadFile(destination, objectKeyToRemoteReference(manifestPath));
        }
    }
}
