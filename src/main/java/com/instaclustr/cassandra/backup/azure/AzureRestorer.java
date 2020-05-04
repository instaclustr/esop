package com.instaclustr.cassandra.backup.azure;

import static java.lang.String.format;

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

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.cassandra.backup.impl.RemoteObjectReference;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;
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
                         final ExecutorServiceSupplier executorServiceSupplier,
                         @Assisted final RestoreOperationRequest request) throws Exception {
        super(request, executorServiceSupplier);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureRestorer(final CloudStorageAccountFactory cloudStorageAccountFactory,
                         final ExecutorServiceSupplier executorServiceSupplier,
                         @Assisted final RestoreCommitLogsOperationRequest request) throws Exception {
        super(request, executorServiceSupplier);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws StorageException, URISyntaxException {
        final String canonicalPath = resolveRemotePath(objectKey);
        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainer.getBlockBlobReference(canonicalPath));
    }

    @Override
    public String downloadFileToString(final Path localPath, final RemoteObjectReference objectReference) throws Exception {
        return ((AzureRemoteObjectReference) objectReference).blob.downloadText();
    }

    @Override
    public void downloadFile(final Path localPath, final RemoteObjectReference objectReference) throws Exception {
        final CloudBlockBlob blob = ((AzureRemoteObjectReference) objectReference).blob;
        Files.createDirectories(localPath.getParent());
        blob.downloadToFile(localPath.toAbsolutePath().toString());
    }

    @Override
    public Path downloadFileToDir(final Path destinationDir, final Path remotePrefix, final Predicate<String> keyFilter) throws Exception {

        final Iterable<ListBlobItem> blobItemsIterable = list(remotePrefix);
        final List<ListBlobItem> blobItems = new ArrayList<>();

        for (final ListBlobItem listBlobItem : blobItemsIterable) {
            if (keyFilter.test(listBlobItem.getUri().getPath())) {
                blobItems.add(listBlobItem);
            }
        }

        if (blobItems.size() != 1) {
            throw new IllegalStateException(format("There is not one key which satisfies key filter: %s", blobItems.toString()));
        }

        final String blobItemPath = blobItems.get(0).getUri().getPath();
        final String fileName = blobItemPath.split("/")[blobItemPath.split("/").length - 1];

        final Path destination = destinationDir.resolve(fileName);

        downloadFile(destination, objectKeyToRemoteReference(remotePrefix.resolve(fileName)));

        return destination;
    }

    @Override
    public void consumeFiles(final RemoteObjectReference prefix, final Consumer<RemoteObjectReference> consumer) throws Exception {
        final AzureRemoteObjectReference azureRemoteObjectReference = (AzureRemoteObjectReference) prefix;
        final Iterable<ListBlobItem> blobItemsIterable = list(azureRemoteObjectReference.getObjectKey());

        for (final ListBlobItem listBlobItem : blobItemsIterable) {
            try {
                consumer.accept(objectKeyToRemoteReference(removeNodePrefix(listBlobItem)));
            } catch (StorageException | URISyntaxException ex) {
                logger.error("Error occurred while trying to consume {}", listBlobItem.getUri().toString(), ex);
                throw ex;
            }
        }
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

    private Iterable<ListBlobItem> list(final Path prefix) {
        final String blobPrefix = Paths.get(request.storageLocation.clusterId)
            .resolve(request.storageLocation.datacenterId)
            .resolve(request.storageLocation.nodeId)
            .resolve(prefix).toString();

        return blobContainer.listBlobs(blobPrefix, true, EnumSet.noneOf(BlobListingDetails.class), null, null);
    }

    @Override
    public void cleanup() {
        // Nothing to cleanup
    }
}
