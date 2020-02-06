package com.instaclustr.cassandra.backup.embedded.s3;

import java.util.UUID;

import com.instaclustr.cassandra.backup.aws.S3BucketService;
import com.instaclustr.cassandra.backup.aws.S3Module.TransferManagerFactory;
import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;

public abstract class BaseS3BackupRestoreTest extends AbstractBackupTest {

    protected static final String BUCKET_NAME = UUID.randomUUID().toString();

    protected abstract BackupOperationRequest getBackupOperationRequest();

    protected abstract String[][] getProgramArguments();

    public void test() throws Exception {
        try {
            backupAndRestoreTest(getProgramArguments());
        } finally {
            new S3BucketService(getTransferManagerFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }

    public abstract TransferManagerFactory getTransferManagerFactory();
}
