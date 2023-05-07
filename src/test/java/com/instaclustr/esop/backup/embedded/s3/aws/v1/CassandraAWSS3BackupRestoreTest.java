package com.instaclustr.esop.backup.embedded.s3.aws.v1;

import com.google.inject.Inject;
import com.instaclustr.esop.backup.embedded.s3.aws.BaseAWSS3BackupRestoreTest;
import com.instaclustr.esop.impl.BucketService.BucketServiceException;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.aws.S3BucketService;
import com.instaclustr.esop.s3.aws.S3Module;
import com.instaclustr.esop.s3.aws.S3Module.S3TransferManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
        "s3Test",
        "cloudTest",
})
public class CassandraAWSS3BackupRestoreTest extends BaseAWSS3BackupRestoreTest
{
    @Inject
    public S3TransferManagerFactory transferManagerFactory;

    @BeforeMethod
    public void setup() throws Exception {
        inject(new S3Module());
        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    @Override
    public void deleteBucket() throws BucketServiceException {
        new S3BucketService(transferManagerFactory, getBackupOperationRequest()).delete(BUCKET_NAME);
    }

    @Override
    protected BackupOperationRequest getBackupOperationRequest() {
        return new BackupOperationRequest();
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments(CASSANDRA_VERSION));
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments(CASSANDRA_4_VERSION), CASSANDRA_4_VERSION);
    }

    @Test
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments(CASSANDRA_VERSION), CASSANDRA_VERSION);
    }
}
