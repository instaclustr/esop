package com.instaclustr.esop.backup.embedded.s3.aws.v2;

import com.instaclustr.esop.backup.embedded.s3.aws.AbstractS3UploadDownloadTest;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.aws_v2.S3Backuper;
import com.instaclustr.esop.s3.aws_v2.S3Restorer;
import org.junit.Test;

import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_KMS_KEY_ID_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.TEST_ESOP_AWS_KMS_WRAPPING_KEY;
import static org.junit.Assume.assumeTrue;

public class UploadDownloadEncryptedTest extends AbstractS3UploadDownloadTest {
    @Test
    public void testEncrypted() throws Exception {
        RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
        BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
        BucketService s3BucketService = null;

        String kmsKeyId = System.getProperty(TEST_ESOP_AWS_KMS_WRAPPING_KEY);
        assumeTrue("Cannot continue as " + TEST_ESOP_AWS_KMS_WRAPPING_KEY + " is not set!", kmsKeyId != null);
        System.setProperty(AWS_KMS_KEY_ID_PROPERTY, System.getProperty(TEST_ESOP_AWS_KMS_WRAPPING_KEY));

        try {
            backupOperationRequest.kmsKeyId = System.getProperty(AWS_KMS_KEY_ID_PROPERTY);

            restoreOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");
            backupOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");

            s3Backuper = new S3Backuper(backupOperationRequest);
            s3Restorer = new S3Restorer(restoreOperationRequest);
            s3BucketService = s3Backuper.s3BucketService;
            s3BucketService.create(BUCKET_NAME);
            s3Clients = s3Backuper.s3Clients;

            testExecution();
        } finally {
            if (s3BucketService != null) {
                s3BucketService.delete(BUCKET_NAME);
            }
        }
    }
}
