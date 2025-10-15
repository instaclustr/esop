package com.instaclustr.esop.azure;

import java.io.ByteArrayOutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.azure.AzureModule.BlobServiceClientFactory;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class AzureRestorer extends Restorer {

    private static final Logger logger = LoggerFactory.getLogger(AzureRestorer.class);

    private final BlobContainerClient blobContainerClient;
    private final BlobServiceClient blobServiceClient;

    @AssistedInject
    public AzureRestorer(final BlobServiceClientFactory blobServiceClientFactory,
                         @Assisted final RestoreOperationRequest request) throws Exception {
        super(request);

        blobServiceClient = blobServiceClientFactory.build(request);

        this.blobContainerClient = blobServiceClient.getBlobContainerClient(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureRestorer(final BlobServiceClientFactory blobServiceClientFactory,
                         @Assisted final RestoreCommitLogsOperationRequest request) throws Exception {
        super(request);

        blobServiceClient = blobServiceClientFactory.build(request);

        this.blobContainerClient = blobServiceClient.getBlobContainerClient(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureRestorer(final BlobServiceClientFactory blobServiceClientFactory,
                         @Assisted final ListOperationRequest request,
                         final ObjectMapper objectMapper) throws Exception {
        super(request);

        blobServiceClient = blobServiceClientFactory.build(request);

        this.blobContainerClient = blobServiceClient.getBlobContainerClient(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureRestorer(final BlobServiceClientFactory blobServiceClientFactory,
                         @Assisted final RemoveBackupRequest request,
                         final ObjectMapper objectMapper) throws Exception {
        super(request);

        blobServiceClient = blobServiceClientFactory.build(request);

        this.blobContainerClient = blobServiceClient.getBlobContainerClient(request.storageLocation.bucket);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        String canonicalPath = objectKey.toFile().toString();

        if (canonicalPath.startsWith("/" + this.blobContainerClient.getBlobContainerName() + "/")) {
            canonicalPath = canonicalPath.replaceFirst("/" + this.blobContainerClient.getBlobContainerName() + "/", "");
        }

        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainerClient.getBlobClient(canonicalPath).getBlockBlobClient());
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws BlobStorageException, URISyntaxException {
        final String canonicalPath = resolveNodeAwareRemotePath(objectKey);
        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainerClient.getBlobClient(canonicalPath).getBlockBlobClient());
    }

    @Override
    public String downloadFileToString(final RemoteObjectReference objectReference) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ((AzureRemoteObjectReference) objectReference).blobClient.downloadStream(output);
        return output.toString();
    }

    @Override
    public void downloadFile(final Path localPath, ManifestEntry manifestEntry, final RemoteObjectReference objectReference) throws Exception {
        final BlockBlobClient blob = ((AzureRemoteObjectReference) objectReference).blobClient;
        Files.createDirectories(localPath.getParent());
        blob.downloadToFile(localPath.toAbsolutePath().toString());
    }

    @Override
    public String downloadTopology(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getBlobItemPath(globalList(remotePrefix), keyFilter);
        return downloadFileToString(objectKeyToRemoteReference(Paths.get(blobItemPath)));
    }

    @Override
    public String downloadManifest(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getManifest(nodeList(remotePrefix), keyFilter);
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    @Override
    public String downloadNodeFile(final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {
        final String blobItemPath = getBlobItemPath(nodeList(remotePrefix), keyFilter);
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];
        return downloadFileToString(objectKeyToNodeAwareRemoteReference(remotePrefix.resolve(fileName)));
    }

    private String getManifest(final Iterable<BlobItem> blobItemsIterable, final Predicate<String> keyFilter) {

        final List<BlobItem> manifests = new ArrayList<>();

        for (final BlobItem listBlobItem : blobItemsIterable) {
            if (keyFilter.test(getBlobPathWithContainerName(listBlobItem))) {
                manifests.add(listBlobItem);
            }
        }

        if (manifests.isEmpty()) {
            throw new IllegalStateException("There is no manifest requested found.");
        }

        return Manifest.parseLatestManifest(manifests.stream().map(this::getBlobPathWithContainerName).collect(toList()));
    }

    private String getBlobItemPath(final Iterable<BlobItem> blobItemsIterable, final Predicate<String> keyFilter) {
        final List<BlobItem> blobItems = new ArrayList<>();

        for (final BlobItem listBlobItem : blobItemsIterable) {
            if (keyFilter.test(getBlobPathWithContainerName(listBlobItem))) {
                blobItems.add(listBlobItem);
            }
        }

        if (blobItems.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter: %s", blobItems.toString()));
        }

        return getBlobPathWithContainerName(blobItems.get(0));
    }

    private List<String> getBlobPaths(final Iterable<BlobItem> blobItemsIterable, final Predicate<String> keyFilter) {
        return StreamSupport.stream(blobItemsIterable.spliterator(), false)
                            .map(this::getBlobPathWithContainerName)
                            .filter(keyFilter)
                            .collect(toList());
    }

    // Returns blob path with container name prefixed, e.g. /container-name/path/to/blob
    // New Azure SDK does not provide such method, so we need to build it ourselves
    private String getBlobPathWithContainerName(final BlobItem blobItem) {
        return "/" + blobContainerClient.getBlobContainerName() + "/" + blobItem.getName();
    }

    @Override
    public void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) throws Exception {
        final AzureRemoteObjectReference azureRemoteObjectReference = (AzureRemoteObjectReference) prefix;
        final Iterable<BlobItem> blobItemsIterable = nodeList(azureRemoteObjectReference.getObjectKey());

        for (final BlobItem listBlobItem : blobItemsIterable) {
            try {
                consumer.accept(objectKeyToNodeAwareRemoteReference(removeNodePrefix(listBlobItem)));
            } catch (BlobStorageException | URISyntaxException ex) {
                logger.error("Error occurred while trying to consume {}", getBlobPathWithContainerName(listBlobItem), ex);
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
        try {
            ((AzureRemoteObjectReference) remoteObjectReference).blobClient.delete();
        } catch (BlobStorageException ex) {
            if (ex.getMessage().contains("The specified blob does not exist")) {
                logger.warn("The specified blob does not exist: {}",
                            Paths.get(request.storageLocation.bucket, remoteObjectReference.canonicalPath));
            } else {
                throw ex;
            }
        }
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

    private Path removeNodePrefix(final BlobItem listBlobItem) {
        final String pattern = format("^/%s/%s/%s/%s/",
                                      request.storageLocation.bucket,
                                      request.storageLocation.clusterId,
                                      request.storageLocation.datacenterId,
                                      request.storageLocation.nodeId);

        final Pattern containerPattern = Pattern.compile(pattern);

        return Paths.get(containerPattern.matcher(getBlobPathWithContainerName(listBlobItem)).replaceFirst(""));
    }

    private Iterable<BlobItem> globalList(final Path prefix) {
        return list(prefix.toString());
    }

    private Iterable<BlobItem> nodeList(final Path prefix) {
        final String blobPrefix = Paths.get(request.storageLocation.clusterId)
            .resolve(request.storageLocation.datacenterId)
            .resolve(request.storageLocation.nodeId)
            .resolve(prefix).toString();

        return list(blobPrefix);
    }

    private Iterable<BlobItem> list(final String prefix) {
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
        listBlobsOptions.setPrefix(prefix);

        return blobContainerClient.listBlobs(listBlobsOptions, null);
    }

    @Override
    public void cleanup() {
        // Nothing to cleanup
    }
}
