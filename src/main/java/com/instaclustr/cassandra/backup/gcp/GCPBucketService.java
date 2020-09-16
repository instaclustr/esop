package com.instaclustr.cassandra.backup.gcp;

import static java.lang.String.format;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.BucketService;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCPBucketService extends BucketService {

    private static final Logger logger = LoggerFactory.getLogger(GCPBucketService.class);

    private final Storage storage;

    @AssistedInject
    public GCPBucketService(final GoogleStorageFactory storageFactory,
                            @Assisted final BackupOperationRequest request) {
        this.storage = storageFactory.build(request);
    }

    @AssistedInject
    public GCPBucketService(final GoogleStorageFactory storageFactory,
                            @Assisted final BackupCommitLogsOperationRequest request) {
        this.storage = storageFactory.build(request);
    }

    @AssistedInject
    public GCPBucketService(final GoogleStorageFactory storageFactory,
                            @Assisted final RestoreOperationRequest request) {
        this.storage = storageFactory.build(request);
    }

    @AssistedInject
    public GCPBucketService(final GoogleStorageFactory storageFactory,
                            @Assisted final RestoreCommitLogsOperationRequest request) {
        this.storage = storageFactory.build(request);
    }

    @Override
    public boolean doesExist(final String bucketName) {
        return storage.get(bucketName) != null;
    }

    @Override
    public void create(final String bucketName) throws BucketServiceException {
        try {
            if (!doesExist(bucketName)) {
                storage.create(BucketInfo.of(bucketName));
            }
        } catch (final StorageException ex) {
            if (ex.getCode() == 409 && ex.getMessage().contains("You already own this bucket.")) {
                logger.warn(format("Unable to create the bucket %s: %s", bucketName, ex.getMessage()));
            } else {
                throw new BucketServiceException(format("Unable to create the bucket %s", bucketName), ex);
            }
        }
    }

    @Override
    public void delete(final String bucketName) throws BucketServiceException {
        logger.info("Deleting bucket {}", bucketName);
        final Bucket bucket = storage.get(bucketName);

        if (bucket != null) {
            for (final Blob blob : bucket.list().iterateAll()) {
                if (!blob.delete()) {
                    throw new BucketServiceException(format("Blob %s was not deleted!", blob.getName()));
                }
            }

            if (!storage.delete(bucket.getName())) {
                throw new BucketServiceException(format("GCP bucket %s was not deleted!", bucket.getName()));
            }
        }
    }

    @Override
    public void close() {
    }
}
