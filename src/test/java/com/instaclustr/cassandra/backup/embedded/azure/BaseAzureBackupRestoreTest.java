package com.instaclustr.cassandra.backup.embedded.azure;

import static com.instaclustr.io.FileUtils.deleteDirectory;

import java.nio.file.Paths;

import com.instaclustr.cassandra.backup.azure.AzureBucketService;
import com.instaclustr.cassandra.backup.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;

public abstract class BaseAzureBackupRestoreTest extends AbstractBackupTest {

    protected abstract BackupOperationRequest getBackupOperationRequest();

    public abstract CloudStorageAccountFactory getStorageAccountFactory();

    @Override
    protected String getStorageLocation() {
        return "azure://" + BUCKET_NAME + "/cluster/datacenter1/node1";
    }

    public void inPlaceTest(final String[][] programArguments) throws Exception {
        try {
            inPlaceBackupRestoreTest(programArguments);
        } finally {
            new AzureBucketService(getStorageAccountFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    public void liveCassandraTest(final String[][] programArguments) throws Exception {
        try {
            liveBackupRestoreTest(programArguments);
        } finally {
            new AzureBucketService(getStorageAccountFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }
}
