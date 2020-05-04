package com.instaclustr.cassandra.backup.embedded.s3;

import static com.instaclustr.io.FileUtils.deleteDirectory;

import java.nio.file.Paths;

import com.instaclustr.cassandra.backup.aws.S3BucketService;
import com.instaclustr.cassandra.backup.aws.S3Module.TransferManagerFactory;
import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;

public abstract class BaseS3BackupRestoreTest extends AbstractBackupTest {

    protected abstract BackupOperationRequest getBackupOperationRequest();

    @Override
    protected String getStorageLocation() {
        return "s3://" + BUCKET_NAME + "/cluster/datacenter1/node1";
    }

    public void inPlaceTest(final String[][] programArguments) throws Exception {
        try {
            inPlaceBackupRestoreTest(programArguments);
        } finally {
            new S3BucketService(getTransferManagerFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    public void liveCassandraTest(final String[][] programArguments) throws Exception {
        try {
            liveBackupRestoreTest(programArguments);
        } finally {
            new S3BucketService(getTransferManagerFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    public abstract TransferManagerFactory getTransferManagerFactory();
}
