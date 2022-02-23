package com.instaclustr.esop.azure;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.io.FileUtils;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureRestorer extends Restorer {

    private static final Logger logger = LoggerFactory.getLogger(AzureRestorer.class);

    private final CloudBlobContainer blobContainer;
    private final CloudBlobClient cloudBlobClient;
    private final CloudStorageAccount cloudStorageAccount;

    @AssistedInject
    public AzureRestorer(final CloudStorageAccountFactory cloudStorageAccountFactory,
                         @Assisted final RestoreOperationRequest request) throws Exception {
        super(request);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureRestorer(final CloudStorageAccountFactory cloudStorageAccountFactory,
                         @Assisted final RestoreCommitLogsOperationRequest request) throws Exception {
        super(request);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureRestorer(final CloudStorageAccountFactory cloudStorageAccountFactory,
                         @Assisted final ListOperationRequest request,
                         final ObjectMapper objectMapper) throws Exception {
        super(request);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureRestorer(final CloudStorageAccountFactory cloudStorageAccountFactory,
                         @Assisted final RemoveBackupRequest request,
                         final ObjectMapper objectMapper) throws Exception {
        super(request);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        String canonicalPath = objectKey.toFile().toString();

        if (canonicalPath.startsWith("/" + this.blobContainer.getName() + "/")) {
            canonicalPath = canonicalPath.replaceFirst("/" + this.blobContainer.getName() + "/", "");
        }

        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainer.getBlockBlobReference(canonicalPath));
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws StorageException, URISyntaxException {
        final String canonicalPath = resolveNodeAwareRemotePath(objectKey);
        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainer.getBlockBlobReference(canonicalPath));
    }

    @Override
    public String downloadFileToString(final RemoteObjectReference objectReference) throws Exception {
        return ((AzureRemoteObjectReference) objectReference).blob.downloadText();
    }

    @Override
    public void downloadFile(final Path localPath, final RemoteObjectReference objectReference) throws Exception {
        final CloudBlockBlob blob = ((AzureRemoteObjectReference) objectReference).blob;
        Files.createDirectories(localPath.getParent());
        blob.downloadToFile(localPath.toAbsolutePath().toString());
    }

    @Override
    public String downloadFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getBlobItemPath(globalList(remotePrefix), keyFilter);
        return downloadFileToString(objectKeyToRemoteReference(Paths.get(blobItemPath)));
    }

    @Override
    public String downloadManifestToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getManifest(nodeList(remotePrefix), keyFilter);
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public String downloadNodeFileToString(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getBlobItemPath(nodeList(remotePrefix), keyFilter);
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public Path downloadNodeFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getBlobItemPath(nodeList(remotePrefix), keyFilter);
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];

        final Path destination = destinationDir.resolve(fileName);

        downloadFile(destination, objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));

        return destination;
    }

    private String getManifest(final Iterable<ListBlobItem> blobItemsIterable, final Predicate<String> keyFilter) {

        final List<ListBlobItem> manifests = new ArrayList<>();

        for (final ListBlobItem listBlobItem : blobItemsIterable) {
            if (keyFilter.test(listBlobItem.getUri().getPath())) {
                manifests.add(listBlobItem);
            }
        }

        if (manifests.isEmpty()) {
            throw new IllegalStateException("There is no manifest requested found.");
        }

        return Manifest.parseLatestManifest(manifests.stream().map(m -> m.getUri().getPath()).collect(toList()));
    }

    private String getBlobItemPath(final Iterable<ListBlobItem> blobItemsIterable, final Predicate<String> keyFilter) {
        final List<ListBlobItem> blobItems = new ArrayList<>();

        for (final ListBlobItem listBlobItem : blobItemsIterable) {
            if (keyFilter.test(listBlobItem.getUri().getPath())) {
                blobItems.add(listBlobItem);
            }
        }

        if (blobItems.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter: %s", blobItems.toString()));
        }

        return blobItems.get(0).getUri().getPath();
    }

    private List<String> getBlobPaths(final Iterable<ListBlobItem> blobItemsIterable, final Predicate<String> keyFilter) {
        return StreamSupport.stream(blobItemsIterable.spliterator(), false)
                            .map(item -> item.getUri().getPath())
                            .filter(keyFilter)
                            .collect(toList());
    }

    @Override
    public void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) throws Exception {
        final AzureRemoteObjectReference azureRemoteObjectReference = (AzureRemoteObjectReference) prefix;
        final Iterable<ListBlobItem> blobItemsIterable = nodeList(azureRemoteObjectReference.getObjectKey());

        for (final ListBlobItem listBlobItem : blobItemsIterable) {
            try {
                consumer.accept(objectKeyToNodeAwareRemoteReference(removeNodePrefix(listBlobItem)));
            } catch (StorageException | URISyntaxException ex) {
                logger.error("Error occurred while trying to consume {}", listBlobItem.getUri().toString(), ex);
                throw ex;
            }
        }
    }

    public void downloadManifestsToDirectory(Path downloadDir) throws Exception {
        FileUtils.createDirectory(downloadDir);
        FileUtils.cleanDirectory(downloadDir.toFile());
        final List<String> manifestKeys = getBlobPaths(list(""), s -> s.contains("manifests"));
        for (String o: manifestKeys) {
            Path manifestPath = Paths.get(o).subpath(1, 6);
            Path destination = downloadDir.resolve(manifestPath);
            downloadFile(destination, objectKeyToRemoteReference(manifestPath));
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
    public void delete(Path objectKey, boolean nodeAware) throws Exception {
        RemoteObjectReference remoteObjectReference;
        if (nodeAware) {
            remoteObjectReference = objectKeyToNodeAwareRemoteReference(objectKey);
        } else {
            remoteObjectReference = objectKeyToRemoteReference(objectKey);
        }
        logger.info("Non dry: " + Paths.get(request.storageLocation.bucket, remoteObjectReference.canonicalPath));
        ((AzureRemoteObjectReference) remoteObjectReference).blob.delete();
    }

    @Override
    public void delete(final Manifest.ManifestReporter.ManifestReport backupToDelete, final RemoveBackupRequest request) throws Exception {
        logger.info("Deleting backup {}", backupToDelete.name);
        if (backupToDelete.reclaimableSpace > 0 && !backupToDelete.getRemovableEntries().isEmpty()) {
            for (final String removableEntry : backupToDelete.getRemovableEntries()) {
                if (!request.dry) {
                    deleteNodeAwareKey(Paths.get(removableEntry));
                } else {
                    logger.info("Dry: " + removableEntry);
                }
            }
        }

        // manifest and topology as the last
        if (!request.dry) {
            //delete in Azure
            deleteNodeAwareKey(backupToDelete.manifest.objectKey);
            //delete in local cache
            localFileRestorer.deleteNodeAwareKey(backupToDelete.manifest.objectKey);
        } else {
            logger.info("Dry: " + backupToDelete.manifest.objectKey);
        }
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

    private Path removeNodePrefix(final ListBlobItem listBlobItem) {
        final String pattern = format("^/%s/%s/%s/%s/",
                                      request.storageLocation.bucket,
                                      request.storageLocation.clusterId,
                                      request.storageLocation.datacenterId,
                                      request.storageLocation.nodeId);

        final Pattern containerPattern = Pattern.compile(pattern);

        return Paths.get(containerPattern.matcher(listBlobItem.getUri().getPath()).replaceFirst(""));
    }

    private Iterable<ListBlobItem> globalList(final Path prefix) {
        return list(prefix.toString());
    }

    private Iterable<ListBlobItem> nodeList(final Path prefix) {
        final String blobPrefix = Paths.get(request.storageLocation.clusterId)
            .resolve(request.storageLocation.datacenterId)
            .resolve(request.storageLocation.nodeId)
            .resolve(prefix).toString();

        return list(blobPrefix);
    }

    private Iterable<ListBlobItem> list(final String prefix) {
        return blobContainer.listBlobs(prefix, true, EnumSet.noneOf(BlobListingDetails.class), null, null);
    }

    @Override
    public void cleanup() {
        // Nothing to cleanup
    }
}
