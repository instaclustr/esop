package com.instaclustr.esop.backup.embedded.s3.aws.v2;

import com.google.inject.Inject;
import com.instaclustr.esop.backup.embedded.s3.aws.BaseAWSS3BackupRestoreTest;
import com.instaclustr.esop.impl.BucketService.BucketServiceException;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.aws_v2.S3BucketService;
import com.instaclustr.esop.s3.aws_v2.S3V2Module;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_KMS_KEY_ID_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.TEST_ESOP_AWS_KMS_WRAPPING_KEY;

@Test(groups = {
        "s3Test",
        "cloudTest",
})
public class CassandraAWSS3BackupRestoreTest extends BaseAWSS3BackupRestoreTest {
    @Inject
    public S3Clients s3Clients;

    @BeforeMethod
    public void setup() throws Exception {
        inject(new S3V2Module());
        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    @Override
    public void deleteBucket() throws BucketServiceException {
        new S3BucketService(s3Clients).delete(BUCKET_NAME);
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

    @Test
    public void testInPlaceBackupRestoreEncrypted() throws Exception {
        runWithEncryption(new ThrowingRunnable() {
            @Override
            public void run() throws Exception
            {
                inPlaceTest(inPlaceArguments(CASSANDRA_VERSION));
            }
        });
    }

    @Test
    public void testImportingBackupAndRestoreEncrypted() throws Exception {
        runWithEncryption(new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                liveCassandraTest(importArguments(CASSANDRA_4_VERSION), CASSANDRA_4_VERSION);
            }
        });
    }

    @Test
    public void testHardlinkingBackupAndRestoreEncrypted() throws Exception {
        runWithEncryption(new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                liveCassandraTest(hardlinkingArguments(CASSANDRA_VERSION), CASSANDRA_VERSION);
            }
        });
    }

    private void runWithEncryption(ThrowingRunnable test) throws Exception {
        System.setProperty(AWS_KMS_KEY_ID_PROPERTY, System.getProperty(TEST_ESOP_AWS_KMS_WRAPPING_KEY));
        if (System.getProperty(AWS_KMS_KEY_ID_PROPERTY) == null)
            throw new SkipException("Cannot continue as " + AWS_KMS_KEY_ID_PROPERTY + " is not set!");

        try {
            test.run();
        } finally {
            System.clearProperty(AWS_KMS_KEY_ID_PROPERTY);
        }
    }

    private abstract static class ThrowingRunnable {
        public abstract void run() throws Exception;
    }
}
