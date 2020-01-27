package com.instaclustr.cassandra.backup.embedded.gcp;

import static java.lang.String.format;

import java.util.UUID;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;

public abstract class BaseGoogleStorageBackupRestoreTest extends AbstractBackupTest {

    protected static final String BUCKET_NAME = UUID.randomUUID().toString();

    protected abstract BackupOperationRequest getBackupOperationRequest();

    protected abstract String[][] getProgramArguments();

    public abstract GoogleStorageFactory getGoogleStorageFactory();

    public void test() throws Exception {

        final Storage storage = getGoogleStorageFactory().build(getBackupOperationRequest());

        Bucket bucket = null;

        try {
            bucket = storage.create(BucketInfo.of(BUCKET_NAME));

            backupAndRestoreTest(getProgramArguments());
        } finally {
            if (bucket != null) {

                for (final Blob blob : bucket.list().iterateAll()) {
                    if (!blob.delete()) {
                        throw new IllegalStateException(format("Blob %s was not deleted!", blob.getName()));
                    }
                }

                if (!storage.delete(bucket.getName())) {
                    throw new IllegalStateException(format("GCP bucket %s was not deleted!", bucket.getName()));
                }
            }
        }
    }

}
