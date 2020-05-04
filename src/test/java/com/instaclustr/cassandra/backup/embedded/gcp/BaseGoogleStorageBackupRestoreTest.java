package com.instaclustr.cassandra.backup.embedded.gcp;

import static com.instaclustr.io.FileUtils.deleteDirectory;

import java.nio.file.Paths;

import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.gcp.GCPBucketService;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;

public abstract class BaseGoogleStorageBackupRestoreTest extends AbstractBackupTest {

    protected abstract BackupOperationRequest getBackupOperationRequest();

    public abstract GoogleStorageFactory getGoogleStorageFactory();

    @Override
    protected String getStorageLocation() {
        return "gcp://" + BUCKET_NAME + "/cluster/datacenter1/node1";
    }

    public void inPlaceTest(final String[][] programArguments) throws Exception {
        try {
            inPlaceBackupRestoreTest(programArguments);
        } finally {
            new GCPBucketService(getGoogleStorageFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    public void liveCassandraTest(final String[][] programArguments) throws Exception {
        try {
            liveBackupRestoreTest(programArguments);
        } finally {
            new GCPBucketService(getGoogleStorageFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }
}
