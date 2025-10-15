package com.instaclustr.esop.s3.v2;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import static java.lang.String.format;

public class BaseS3BucketService extends BucketService {

    private static final Logger logger = LoggerFactory.getLogger(BaseS3BucketService.class);

    private final S3Clients s3Clients;

    public BaseS3BucketService(final S3Clients s3Clients) {
        this.s3Clients = s3Clients;
    }

    @Override
    public boolean doesExist(final String bucketName) throws BucketServiceException {
        try
        {
            ListBucketsResponse listBucketsResponse = s3Clients.getClient().listBuckets(ListBucketsRequest.builder().build());
            return listBucketsResponse.buckets().stream().filter(bucket -> bucket.name().equals(bucketName)).count() == 1;
        } catch (Exception ex) {
            throw new BucketServiceException(format("Unable to determine if the bucket %s exists.", bucketName), ex);
        }
    }

    @Override
    public void create(final String bucketName) throws BucketServiceException {
        try {
            if (!doesExist(bucketName)) {
                try {
                    s3Clients.getNonEncryptingClient().createBucket(CreateBucketRequest.builder()
                                                                                       .bucket(bucketName)
                                                                                       .build());
                    s3Clients.getNonEncryptingClient()
                             .waiter()
                             .waitUntilBucketExists(HeadBucketRequest.builder()
                                                                     .bucket(bucketName)
                                                                     .build(),
                                                    WaiterOverrideConfiguration.builder()
                                                                               .backoffStrategy(BackoffStrategy.defaultStrategy(RetryMode.STANDARD))
                                                                               .waitTimeout(Duration.of(1, ChronoUnit.MINUTES)).build());
                } catch (final Exception ex) {
                    if (ex instanceof BucketAlreadyExistsException || ex instanceof BucketAlreadyOwnedByYouException) {
                        logger.warn(ex.getMessage());
                    } else {
                        throw ex;
                    }
                }
            }
        } catch (final Exception ex) {
            throw new BucketServiceException(format("Unable to create the bucket %s", bucketName), ex);
        }
    }

    @Override
    public void delete(final String bucketName) throws BucketServiceException {
        try {
            logger.info("Deleting bucket {}", bucketName);
            if (!doesExist(bucketName)) {
                logger.info("Bucket was not deleted as it does not exist.");
                return;
            }

            // To delete a bucket, all the objects in the bucket must be deleted first.
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                                                                            .bucket(bucketName)
                                                                            .build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3Clients.getClient().listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    DeleteObjectRequest request = DeleteObjectRequest.builder()
                                                                     .bucket(bucketName)
                                                                     .key(s3Object.key())
                                                                     .build();
                    s3Clients.getClient().deleteObject(request);
                }
            } while (listObjectsV2Response.isTruncated());

            s3Clients.getClient().deleteBucket(DeleteBucketRequest.builder()
                                                                  .bucket(bucketName)
                                                                  .build());

            s3Clients.getClient()
                     .waiter()
                     .waitUntilBucketNotExists(HeadBucketRequest.builder()
                                                                .bucket(bucketName)
                                                                .build());
        } catch (final Exception ex) {
            throw new BucketServiceException(format("Unable to delete the bucket %s", bucketName), ex);
        }
    }

    @Override
    public void close() {
        try {
            s3Clients.close();
        } catch (final Exception ex) {
            logger.error("Unable to close S3Clients!", ex);
        }
    }
}
