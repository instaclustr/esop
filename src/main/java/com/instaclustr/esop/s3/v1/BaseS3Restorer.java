package com.instaclustr.esop.s3.v1;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.esop.impl.retry.Retrier.RetriableException;
import com.instaclustr.esop.impl.retry.RetrierFactory;
import com.instaclustr.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toCollection;


public class BaseS3Restorer extends Restorer {

    private static final Logger logger = LoggerFactory.getLogger(BaseS3Restorer.class);

    protected final AmazonS3 amazonS3;
    protected final TransferManager transferManager;

    public BaseS3Restorer(final TransferManagerFactory transferManagerFactory,
                          final RestoreOperationRequest request) {
        super(request);
        this.transferManager = transferManagerFactory.build(request);
        this.amazonS3 = this.transferManager.getAmazonS3Client();
    }

    public BaseS3Restorer(final TransferManagerFactory transferManagerFactory,
                          final RestoreCommitLogsOperationRequest request) {
        super(request);
        this.transferManager = transferManagerFactory.build(request);
        this.amazonS3 = this.transferManager.getAmazonS3Client();
    }

    public BaseS3Restorer(final TransferManagerFactory transferManagerFactory,
                          final ObjectMapper objectMapper,
                          final ListOperationRequest request) {
        super(request);
        this.transferManager = transferManagerFactory.build(request);
        this.amazonS3 = this.transferManager.getAmazonS3Client();
    }

    public BaseS3Restorer(final TransferManagerFactory transferManagerFactory,
                          final ObjectMapper objectMapper,
                          final RemoveBackupRequest request) {
        super(request);
        this.transferManager = transferManagerFactory.build(request);
        this.amazonS3 = this.transferManager.getAmazonS3Client();
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
    public String downloadFileToString(final RemoteObjectReference objectReference) throws Exception {
        final GetObjectRequest getObjectRequest = new GetObjectRequest(request.storageLocation.bucket, objectReference.canonicalPath);
        try (final InputStream is = transferManager.getAmazonS3Client().getObject(getObjectRequest).getObjectContent();
             final InputStreamReader isr = new InputStreamReader(is)) {
            return CharStreams.toString(isr);
        }
    }

    @Override
    public void downloadFile(final Path localPath, final RemoteObjectReference objectReference) throws Exception {
        RetrierFactory.getRetrier(request.retry).submit(() -> {
            try {
                Files.createDirectories(localPath.getParent());

                final GetObjectRequest getObjectRequest = new GetObjectRequest(request.storageLocation.bucket, objectReference.canonicalPath);

                try {
                    transferManager.download(getObjectRequest,
                                             localPath.toFile(),
                                             new DownloadProgressListener(objectReference)).waitForCompletion();
                } catch (final Exception ex) {
                    Files.deleteIfExists(localPath);
                    throw ex;
                }
            } catch (final AmazonServiceException ex) {
                if (ex.getStatusCode() > 500) {
                    throw new RetriableException(ex.getMessage(), ex);
                }
                if (ex.getStatusCode() == 404) {
                    logger.error("Remote object reference {} does not exist.", objectReference);
                }
                throw ex;
            } catch (final AmazonClientException ex) {
                throw new RetriableException(format("Error in S3 client while downloading %s", objectReference.objectKey), ex);
            } catch (final IOException | InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public String downloadFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final S3Object s3Object = getBlobItemPath(remotePrefix.toString(), keyFilter);
        return downloadFileToString(objectKeyToRemoteReference(Paths.get(s3Object.getKey())));
    }

    @Override
    public String downloadManifestToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final S3Object manifestObject = getManifest(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = manifestObject.getKey().split("/")[manifestObject.getKey().split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    public void downloadManifestsToDirectory(Path downloadDir) throws Exception {
        FileUtils.createDirectory(downloadDir);
        FileUtils.cleanDirectory(downloadDir.toFile());
        final List<S3ObjectSummary> manifestSumms = listBucket("", s -> s.contains("manifests"));
        for (S3ObjectSummary o : manifestSumms) {
            Path manifestPath = Paths.get(o.getKey());
            Path destination = downloadDir.resolve(manifestPath);

            downloadFile(destination, objectKeyToRemoteReference(manifestPath));
        }
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
        logger.info("Non dry: " + fileToDelete);
        amazonS3.deleteObject(new DeleteObjectRequest(request.storageLocation.bucket, remoteObjectReference.canonicalPath));
    }

    @Override
    public void delete(final Manifest.ManifestReporter.ManifestReport backupToDelete, final RemoveBackupRequest request) throws Exception {
        logger.info("Deleting backup {}", backupToDelete.name);
        if (backupToDelete.reclaimableSpace > 0 && !backupToDelete.getRemovableEntries().isEmpty()) {
            //convert removable entries into S3 object keys
            String[] toRemove = backupToDelete.getRemovableEntries()
                    .stream()
                    .map(entry -> objectKeyToNodeAwareRemoteReference(Paths.get(entry)).canonicalPath)
                    .toArray(String[]::new);

            for (String remove : toRemove) {
                if (request.dry) {
                    logger.info("Deletion of {} was executed in dry mode.", remove);
                } else {
                    logger.info("Deleting file {}", remove);
                }
            }

            if (!request.dry){
                amazonS3.deleteObjects(new DeleteObjectsRequest(request.storageLocation.bucket).withKeys(toRemove));
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

    @Override
    public String downloadNodeFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final S3Object s3Object = getBlobItemPath(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = s3Object.getKey().split("/")[s3Object.getKey().split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public Path downloadNodeFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final S3Object s3Object = getBlobItemPath(resolveNodeAwareRemotePath(remotePrefix), keyFilter);
        final String fileName = s3Object.getKey().split("/")[s3Object.getKey().split("/").length - 1];
        final Path destination = destinationDir.resolve(fileName);

        downloadFile(destination, objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));

        return destination;
    }

    private S3Object getManifest(final String remotePrefix, final Predicate<String> keyFilter) {
        final List<S3ObjectSummary> summaryList = listBucket(remotePrefix, keyFilter);

        if (summaryList.isEmpty()) {
            throw new IllegalStateException("There is no manifest requested found.");
        }

        final String manifestFullKey = Manifest.parseLatestManifest(summaryList.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList()));
        return amazonS3.getObject(request.storageLocation.bucket, manifestFullKey);
    }

    private List<S3ObjectSummary> listBucket(final String remotePrefix, final Predicate<String> keyFilter) {
        ObjectListing objectListing = amazonS3.listObjects(request.storageLocation.bucket, remotePrefix);

        boolean hasMoreContent = true;

        final List<S3ObjectSummary> summaryList = new ArrayList<>();

        while (hasMoreContent) {
            objectListing.getObjectSummaries().stream()
                         .filter(objectSummary -> !objectSummary.getKey().endsWith("/")) // no dirs
                         .filter(file -> keyFilter.test(file.getKey()))
                         .collect(toCollection(() -> summaryList));

            if (objectListing.isTruncated()) {
                objectListing = amazonS3.listNextBatchOfObjects(objectListing);
            } else {
                hasMoreContent = false;
            }
        }

        return summaryList;
    }

    private S3Object getBlobItemPath(final String remotePrefix, final Predicate<String> keyFilter) {
        final List<S3ObjectSummary> summaryList = listBucket(remotePrefix, keyFilter);

        if (summaryList.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter: %s", summaryList));
        }

        return amazonS3.getObject(request.storageLocation.bucket, summaryList.get(0).getKey());
    }

    private static class DownloadProgressListener implements S3ProgressListener {

        private final RemoteObjectReference objectReference;

        public DownloadProgressListener(final RemoteObjectReference objectReference) {
            this.objectReference = objectReference;
        }

        @Override
        public void onPersistableTransfer(final PersistableTransfer persistableTransfer) {
            // We don't resume downloads
        }

        @Override
        public void progressChanged(final ProgressEvent progressEvent) {
            if (progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
                logger.info("Successfully downloaded {}.", objectReference.canonicalPath);
            }
        }
    }


    @Override
    public void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) {

        final Path bucketPath = Paths.get(request.storageLocation.clusterId).resolve(request.storageLocation.datacenterId).resolve(request.storageLocation.nodeId);

        ObjectListing objectListing = amazonS3.listObjects(request.storageLocation.bucket, prefix.canonicalPath);

        boolean hasMoreContent = true;

        while (hasMoreContent) {
            objectListing.getObjectSummaries().stream()
                         .filter(objectSummary -> !objectSummary.getKey().endsWith("/"))
                         .forEach(objectSummary -> consumer.accept(objectKeyToNodeAwareRemoteReference(bucketPath.relativize(Paths.get(objectSummary.getKey())))));

            if (objectListing.isTruncated()) {
                objectListing = amazonS3.listNextBatchOfObjects(objectListing);
            } else {
                hasMoreContent = false;
            }
        }
    }

    @Override
    public void cleanup() {
        transferManager.shutdownNow();
    }
}
