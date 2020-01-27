package com.instaclustr.cassandra.backup.embedded.s3;

import java.util.UUID;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.instaclustr.cassandra.backup.aws.S3Module.TransferManagerFactory;
import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import org.testng.annotations.Test;

public abstract class BaseS3BackupRestoreTest extends AbstractBackupTest {

    protected static final String BUCKET_NAME = UUID.randomUUID().toString();

    protected abstract BackupOperationRequest getBackupOperationRequest();

    protected abstract String[][] getProgramArguments();

    public void test() throws Exception {
        final TransferManager transferManager = getTransferManagerFactory().build(getBackupOperationRequest());

        Bucket bucket = null;

        try {
            bucket = transferManager.getAmazonS3Client().createBucket(new CreateBucketRequest(BUCKET_NAME));

            backupAndRestoreTest(getProgramArguments());
        } finally {
            if (bucket != null) {

                ObjectListing objectListing = transferManager.getAmazonS3Client().listObjects(BUCKET_NAME);

                delete(transferManager.getAmazonS3Client(), objectListing);

                while (objectListing.isTruncated()) {
                    objectListing = transferManager.getAmazonS3Client().listNextBatchOfObjects(objectListing);
                    delete(transferManager.getAmazonS3Client(), objectListing);
                }

                transferManager.getAmazonS3Client().deleteBucket(new DeleteBucketRequest(bucket.getName()));
            }
        }
    }

    private void delete(final AmazonS3 s3Client, final ObjectListing objectListing) {
        for (final S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            s3Client.deleteObject(BUCKET_NAME, summary.getKey());
        }
    }

    public abstract TransferManagerFactory getTransferManagerFactory();
}
