package com.instaclustr.cassandra.backup.embedded.gcp;

import java.util.UUID;

import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.gcp.GCPBucketService;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;

public abstract class BaseGoogleStorageBackupRestoreTest extends AbstractBackupTest {

    protected static final String BUCKET_NAME = UUID.randomUUID().toString();

    protected abstract BackupOperationRequest getBackupOperationRequest();

    protected abstract String[][] getProgramArguments();

    public abstract GoogleStorageFactory getGoogleStorageFactory();

    public void test() throws Exception {
        try {
            backupAndRestoreTest(getProgramArguments());
        } finally {
            new GCPBucketService(getGoogleStorageFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }

}
