package com.instaclustr.esop.azure;

import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.esop.impl.backup.Backuper;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

public class AzureBackuper extends Backuper {

    private static final String DATE_TIME_METADATA_KEY = "LastFreshened";

    private final CloudBlobContainer blobContainer;

    private final CloudBlobClient cloudBlobClient;

    private final CloudStorageAccount cloudStorageAccount;

    @AssistedInject
    public AzureBackuper(final CloudStorageAccountFactory cloudStorageAccountFactory,
                         @Assisted final BackupOperationRequest request) throws Exception {
        super(request);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @AssistedInject
    public AzureBackuper(final CloudStorageAccountFactory cloudStorageAccountFactory,
                         @Assisted final BackupCommitLogsOperationRequest request) throws Exception {
        super(request);

        cloudStorageAccount = cloudStorageAccountFactory.build(request);
        cloudBlobClient = cloudStorageAccount.createCloudBlobClient();

        this.blobContainer = cloudBlobClient.getContainerReference(request.storageLocation.bucket);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        final String canonicalPath = objectKey.toFile().toString();
        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainer.getBlockBlobReference(canonicalPath));
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) throws Exception {
        final String canonicalPath = resolveNodeAwareRemotePath(objectKey);
        return new AzureRemoteObjectReference(objectKey, canonicalPath, this.blobContainer.getBlockBlobReference(canonicalPath));
    }

    @Override
    protected void cleanup() throws Exception {

    }

    @Override
    public FreshenResult freshenRemoteObject(final RemoteObjectReference object) throws Exception {
        final CloudBlockBlob blob = ((AzureRemoteObjectReference) object).blob;

        final Instant now = Instant.now();

        try {
            blob.getMetadata().put(DATE_TIME_METADATA_KEY, now.toString());
            blob.uploadMetadata();

            return FreshenResult.FRESHENED;

        } catch (final StorageException e) {
            if (e.getHttpStatusCode() != 404) {
                throw e;
            }

            return FreshenResult.UPLOAD_REQUIRED;
        }
    }

    @Override
    public void uploadFile(final long size,
                           final InputStream localFileStream,
                           final RemoteObjectReference objectReference) throws Exception {
        final CloudBlockBlob blob = ((AzureRemoteObjectReference) objectReference).blob;
        blob.upload(localFileStream, size);
    }

    @Override
    public void uploadText(final String text, final RemoteObjectReference objectReference) throws Exception {
        final CloudBlockBlob blob = ((AzureRemoteObjectReference) objectReference).blob;
        blob.uploadText(text);
    }
}
