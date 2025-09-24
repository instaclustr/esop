package com.instaclustr.esop.gcp;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;

import com.google.common.io.ByteStreams;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.RemoteObjectReference;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;

import static com.google.cloud.storage.Storage.PredefinedAcl.BUCKET_OWNER_FULL_CONTROL;

public class GCPBackuper extends Backuper {

    private final Storage storage;

    @AssistedInject
    public GCPBackuper(final GoogleStorageFactory storageFactory,
                       @Assisted final BackupOperationRequest backupOperationRequest) {
        super(backupOperationRequest);
        this.storage = storageFactory.build();
    }

    @AssistedInject
    public GCPBackuper(final GoogleStorageFactory storageFactory,
                       @Assisted final BackupCommitLogsOperationRequest backupOperationRequest) {
        super(backupOperationRequest);
        this.storage = storageFactory.build();
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        return new GCPRemoteObjectReference(objectKey, objectKey.toString(), request.storageLocation.bucket);
    }

    @Override
    public RemoteObjectReference objectKeyToNodeAwareRemoteReference(final Path objectKey) {
        return new GCPRemoteObjectReference(objectKey, resolveNodeAwareRemotePath(objectKey), request.storageLocation.bucket);
    }

    @Override
    public FreshenResult freshenRemoteObject(ManifestEntry manifestEntry, final RemoteObjectReference object) {
        final BlobId blobId = ((GCPRemoteObjectReference) object).blobId;

        try {
            if (!request.skipRefreshing) {
                final BlobInfo.Builder targetBuilder = BlobInfo.newBuilder(blobId);
                final Storage.CopyRequest.Builder copyBuilder = new Storage.CopyRequest.Builder()
                    .setSource(blobId)
                    .setTarget(targetBuilder.build());
                
                // Only apply ACL if not using uniform bucket-level access
                if (!request.gcpUniformBucketLevelAccess) {
                    copyBuilder.setTarget(targetBuilder.build(), Storage.BlobTargetOption.predefinedAcl(BUCKET_OWNER_FULL_CONTROL));
                }
                
                storage.copy(copyBuilder.build());

                return FreshenResult.FRESHENED;
            } else {
                final Blob blob = storage.get(blobId);
                if (blob == null || !blob.exists()) {
                    return FreshenResult.UPLOAD_REQUIRED;
                } else {
                    return FreshenResult.FRESHENED;
                }
            }
        } catch (final StorageException e) {
            if (e.getCode() != 404) {
                throw e;
            }

            return FreshenResult.UPLOAD_REQUIRED;
        }
    }

    @Override
    public void uploadFile(final ManifestEntry manifestEntry,
                           final InputStream localFileStream,
                           final RemoteObjectReference objectReference) throws Exception {
        final BlobId blobId = ((GCPRemoteObjectReference) objectReference).blobId;

        final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        
        // Only apply ACL if not using uniform bucket-level access
        final WriteChannel outputChannel;
        if (request.gcpUniformBucketLevelAccess) {
            outputChannel = storage.writer(blobInfo);
        } else {
            outputChannel = storage.writer(blobInfo, Storage.BlobWriteOption.predefinedAcl(BUCKET_OWNER_FULL_CONTROL));
        }
        
        try (final ReadableByteChannel inputChannel = Channels.newChannel(localFileStream)) {
            ByteStreams.copy(inputChannel, outputChannel);
        } finally {
            if (outputChannel != null) {
                outputChannel.close();
            }
        }
    }

    @Override
    public void uploadText(final String text, final RemoteObjectReference objectReference) {
        final BlobId blobId = ((GCPRemoteObjectReference) objectReference).blobId;
        final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        final byte[] data = text.getBytes();
        
        // Only apply ACL if not using uniform bucket-level access
        if (request.gcpUniformBucketLevelAccess) {
            storage.create(blobInfo, data);
        } else {
            storage.create(blobInfo, data, Storage.BlobTargetOption.predefinedAcl(BUCKET_OWNER_FULL_CONTROL));
        }
    }

    @Override
    public void cleanup() {
    }
}
