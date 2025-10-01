package com.instaclustr.esop.backup.embedded.s3.aws.v2;

import com.instaclustr.esop.backup.embedded.s3.aws.BaseAWSS3BackupRestoreTest;
import com.instaclustr.esop.impl.BucketService.BucketServiceException;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.aws_v2.S3Module;
import com.instaclustr.esop.s3.v2.BaseS3BucketService;
import com.instaclustr.esop.s3.v2.S3ClientsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.testng.SkipException;

import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_KMS_KEY_ID_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.TEST_ESOP_AWS_KMS_WRAPPING_KEY;

public class CassandraAWSS3BackupRestoreTest extends BaseAWSS3BackupRestoreTest {

    @Before
    public void setup() {
        inject(new S3Module());
        init();
    }

    @Override
    protected String protocol() {
        return "s3://";
    }

    @After
    public void teardown() throws Exception {
        destroy();
    }

    @Override
    @Ignore
    public void deleteBucket() throws BucketServiceException {
        new BaseS3BucketService(new S3ClientsFactory().build(new S3ConfigurationResolver())).delete(BUCKET_NAME);
    }

    @Override
    protected BackupOperationRequest getBackupOperationRequest() {
        return new BackupOperationRequest();
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments());
    }

    @Test
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments());
    }

    @Test
    @Ignore
    public void testInPlaceBackupRestoreEncrypted() throws Exception {
        runWithEncryption(new ThrowingRunnable() {
            @Override
            public void run() throws Exception
            {
                inPlaceTest(inPlaceArguments());
            }
        });
    }

    @Test
    @Ignore
    public void testImportingBackupAndRestoreEncrypted() throws Exception {
        runWithEncryption(new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                liveCassandraTest(importArguments());
            }
        });
    }

    @Test
    @Ignore
    public void testHardlinkingBackupAndRestoreEncrypted() throws Exception {
        runWithEncryption(new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                liveCassandraTest(hardlinkingArguments());
            }
        });
    }

    private void runWithEncryption(ThrowingRunnable test) throws Exception {
        String kmsKeyId = System.getProperty(TEST_ESOP_AWS_KMS_WRAPPING_KEY);
        if (kmsKeyId == null)
            throw new SkipException("Cannot continue as " + TEST_ESOP_AWS_KMS_WRAPPING_KEY + " is not set!");

        System.setProperty(AWS_KMS_KEY_ID_PROPERTY, kmsKeyId);

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
