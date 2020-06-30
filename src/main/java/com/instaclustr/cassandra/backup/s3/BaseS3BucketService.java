package com.instaclustr.cassandra.backup.s3;

import static java.lang.String.format;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.instaclustr.cassandra.backup.impl.BucketService;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseS3BucketService implements BucketService {

    private static final Logger logger = LoggerFactory.getLogger(BaseS3BucketService.class);

    private final TransferManager transferManager;

    public BaseS3BucketService(final TransferManagerFactory transferManagerFactory,
                               final BackupOperationRequest request) {
        this.transferManager = transferManagerFactory.build(request);
    }

    public BaseS3BucketService(final TransferManagerFactory transferManagerFactory,
                               final BackupCommitLogsOperationRequest request) {
        this.transferManager = transferManagerFactory.build(request);
    }


    @Override
    public boolean doesExist(final String bucketName) {
        return transferManager.getAmazonS3Client().listBuckets().stream().anyMatch(bucket -> bucket.getName().equals(bucketName));
    }

    @Override
    public void create(final String bucketName) {
        if (!doesExist(bucketName)) {
            try {
                transferManager.getAmazonS3Client().createBucket(bucketName);
            } catch (final AmazonS3Exception ex) {
                if (ex.getStatusCode() == 409 && "BucketAlreadyOwnedByYou".equals(ex.getErrorCode())) {
                    logger.warn(ex.getErrorMessage());
                } else {
                    throw new S3ModuleException(format("Unable to create bucket %s", bucketName), ex);
                }
            }
        }
    }

    @Override
    public void delete(final String bucketName) {
        if (!doesExist(bucketName)) {
            logger.info("Bucket was not deleted as it does not exist.");
            return;
        }

        ObjectListing objectListing = transferManager.getAmazonS3Client().listObjects(bucketName);

        delete(transferManager.getAmazonS3Client(), objectListing, bucketName);

        while (objectListing.isTruncated()) {
            objectListing = transferManager.getAmazonS3Client().listNextBatchOfObjects(objectListing);
            delete(transferManager.getAmazonS3Client(), objectListing, bucketName);
        }

        transferManager.getAmazonS3Client().deleteBucket(bucketName);
    }

    @Override
    public void close() {
        try {
            transferManager.shutdownNow();
        } catch (final Exception ex) {
            logger.error("Unable to shutdown TransferManager!", ex);
        }
    }

    private void delete(final AmazonS3 s3Client, final ObjectListing objectListing, final String bucketName) {
        for (final S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            s3Client.deleteObject(bucketName, summary.getKey());
        }
    }
}
