package com.instaclustr.cassandra.backup.gcp;

import static java.lang.String.format;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GCPModuleException;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.BucketService;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;

public class GCPBucketService implements BucketService {

    private final GoogleStorageFactory storageFactory;

    private final Storage storage;

    @AssistedInject
    public GCPBucketService(final GoogleStorageFactory storageFactory,
                            @Assisted final BackupOperationRequest request) {
        this.storageFactory = storageFactory;
        this.storage = storageFactory.build(request);
    }

    @AssistedInject
    public GCPBucketService(final GoogleStorageFactory storageFactory,
                            @Assisted final BackupCommitLogsOperationRequest request) {
        this.storageFactory = storageFactory;
        this.storage = storageFactory.build(request);
    }

    @Override
    public boolean doesExist(final String bucketName) {
        return storage.get(bucketName) != null;
    }

    @Override
    public void create(final String bucketName) {
        try {
            storage.create(BucketInfo.of(bucketName));
        } catch (final StorageException ex) {
            throw new GCPModuleException(format("Unable to create bucket %s", bucketName), ex);
        }
    }

    @Override
    public void delete(final String bucketName) {
        final Bucket bucket = storage.get(bucketName);

        if (bucket != null) {
            for (final Blob blob : bucket.list().iterateAll()) {
                if (!blob.delete()) {
                    throw new GCPModuleException(format("Blob %s was not deleted!", blob.getName()));
                }
            }

            if (!storage.delete(bucket.getName())) {
                throw new GCPModuleException(format("GCP bucket %s was not deleted!", bucket.getName()));
            }
        }
    }

    @Override
    public void close() {
    }
}
