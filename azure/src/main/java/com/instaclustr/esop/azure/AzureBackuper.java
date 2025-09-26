package com.instaclustr.esop.azure;

import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.azure.AzureModule.BlobServiceClientFactory;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;

public class AzureBackuper extends Backuper {

    private static final String DATE_TIME_METADATA_KEY = "LastFreshened";

    private final BlobContainerClient blobContainerClient;

    private final BlobServiceClient blobServiceClient;

    @AssistedInject
    public AzureBackuper(final BlobServiceClientFactory blobServiceClientFactory,
                         @Assisted final BackupOperationRequest request) throws Exception {
        super(request);

        blobServiceClient = blobServiceClientFactory.build(request);
        blobContainerClient = blobServiceClient.getBlobContainerClient(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureBackuper(final BlobServiceClientFactory blobServiceClientFactory,
                         @Assisted final BackupCommitLogsOperationRequest request) throws Exception {
        super(request);

        blobServiceClient = blobServiceClientFactory.build(request);
        blobContainerClient = blobServiceClient.getBlobContainerClient(request.storageLocation.bucket);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        final String canonicalPath = objectKey.toFile().toString();
        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainerClient.getBlobClient(canonicalPath).getBlockBlobClient());
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws Exception {
        final String canonicalPath = resolveNodeAwareRemotePath(objectKey);
        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainerClient.getBlobClient(canonicalPath).getBlockBlobClient());
    }

    @Override
    protected void cleanup() throws Exception {

    }

    @Override
    public FreshenResult freshenRemoteObject(ManifestEntry manifestEntry, final RemoteObjectReference object) throws Exception {
        final BlockBlobClient blob = ((AzureRemoteObjectReference) object).blobClient;

        final Instant now = Instant.now();

        try {
            if (!request.skipRefreshing) {
                Map<String, String>  metadata = blob.getProperties().getMetadata();
                metadata.put(DATE_TIME_METADATA_KEY, now.toString());
                blob.setMetadata(metadata);

                return FreshenResult.FRESHENED;
            } else {
                return blob.exists() ? FreshenResult.FRESHENED : FreshenResult.UPLOAD_REQUIRED;
            }
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() != 404) {
                throw e;
            }

            return FreshenResult.UPLOAD_REQUIRED;
        }
    }

    @Override
    public void uploadFile(final ManifestEntry manifestEntry,
                           final InputStream localFileStream,
                           final RemoteObjectReference objectReference) throws Exception {
        final BlockBlobClient blob = ((AzureRemoteObjectReference) objectReference).blobClient;
        blob.upload(localFileStream, manifestEntry.size, true);
    }

    @Override
    public void uploadText(final String text, final RemoteObjectReference objectReference) throws Exception {
        final BlockBlobClient blob = ((AzureRemoteObjectReference) objectReference).blobClient;
        blob.upload(BinaryData.fromString(text), true);
    }
}
