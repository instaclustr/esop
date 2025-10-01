package com.instaclustr.esop.backup.embedded.s3.aws.v2;

import com.instaclustr.esop.backup.embedded.s3.aws.AbstractS3UploadDownloadTest;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.aws_v2.S3Backuper;
import com.instaclustr.esop.s3.aws_v2.S3Restorer;
import org.junit.Test;

public class UploadDownloadUnencryptedTest extends AbstractS3UploadDownloadTest {

    @Test
    public void testUnencrypted() throws Exception {
        RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
        BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
        BucketService s3BucketService = null;

        try {
            restoreOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");
            backupOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");
            backupOperationRequest.kmsKeyId = null;

            s3Backuper = new S3Backuper(backupOperationRequest);
            s3Restorer = new S3Restorer(restoreOperationRequest);
            s3BucketService = s3Backuper.s3BucketService;
            s3BucketService.create(BUCKET_NAME);
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
