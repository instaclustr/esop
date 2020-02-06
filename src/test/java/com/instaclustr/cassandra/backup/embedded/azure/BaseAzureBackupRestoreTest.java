package com.instaclustr.cassandra.backup.embedded.azure;

import java.util.UUID;

import com.instaclustr.cassandra.backup.azure.AzureBucketService;
import com.instaclustr.cassandra.backup.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;

public abstract class BaseAzureBackupRestoreTest extends AbstractBackupTest {

    protected static final String BUCKET_NAME = UUID.randomUUID().toString();

    protected abstract BackupOperationRequest getBackupOperationRequest();

    protected abstract String[][] getProgramArguments();

    public abstract CloudStorageAccountFactory getStorageAccountFactory();

    public void test() throws Exception {
        try {
            backupAndRestoreTest(getProgramArguments());
        } finally {
            new AzureBucketService(getStorageAccountFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }
}
