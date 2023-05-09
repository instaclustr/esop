package com.instaclustr.esop.s3.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.S3ConfigurationResolver;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Object;

import static java.lang.String.format;

public class BaseS3BucketService extends BucketService {

    private static final Logger logger = LoggerFactory.getLogger(BaseS3BucketService.class);

    private final S3Clients s3Clients;

    public BaseS3BucketService(final S3ClientsFactory s3ClientsFactory,
                               final S3ConfigurationResolver configurationResolver,
                               final BackupOperationRequest request) {
        this.s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    public BaseS3BucketService(final S3ClientsFactory s3ClientsFactory,
                               final S3ConfigurationResolver configurationResolver,
                               final BackupCommitLogsOperationRequest request) {
        this.s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    public BaseS3BucketService(final S3ClientsFactory s3ClientsFactory,
                               final S3ConfigurationResolver configurationResolver,
                               final RestoreOperationRequest request) {
        this.s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    public BaseS3BucketService(final S3ClientsFactory s3ClientsFactory,
                               final S3ConfigurationResolver configurationResolver,
                               final RestoreCommitLogsOperationRequest request) {
        this.s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    public BaseS3BucketService(final S3ClientsFactory s3ClientsFactory,
                               final S3ConfigurationResolver configurationResolver,
                               final ListOperationRequest request) {
        this.s3Clients = s3ClientsFactory.build(request, configurationResolver);
    }

    @Override
    public boolean doesExist(final String bucketName) throws BucketServiceException {
        try
        {
            s3Clients.getClient().headBucket(HeadBucketRequest.builder()
                                                              .bucket(bucketName)
                                                              .build())
                     .get();
            return true;
        } catch (final NoSuchBucketException ex) {
            return false;
        } catch (final Exception ex) {
            throw new BucketServiceException(format("Unable to determine if the bucket %s exists.", bucketName), ex);
        }
    }

    @Override
    public void create(final String bucketName) throws BucketServiceException {
        try {
            if (!doesExist(bucketName)) {
                try {
                    s3Clients.getClient().createBucket(CreateBucketRequest.builder()
                                                                          .bucket(bucketName)
                                                                          .build()).get();
                    s3Clients.getClient().waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build()).get();
                } catch (final BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ex) {
                    logger.warn(ex.getMessage());
                }
            }
        } catch (final BucketServiceException ex) {
            throw ex;
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
                listObjectsV2Response = s3Clients.getClient().listObjectsV2(listObjectsV2Request).get();
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    DeleteObjectRequest request = DeleteObjectRequest.builder()
                                                                     .bucket(bucketName)
                                                                     .key(s3Object.key())
                                                                     .build();
                    s3Clients.getClient().deleteObject(request).get();
                }
            } while (listObjectsV2Response.isTruncated());

            s3Clients.getClient().deleteBucket(DeleteBucketRequest.builder()
                                                                  .bucket(bucketName)
                                                                  .build()).get();

            s3Clients.getClient()
                     .waiter()
                     .waitUntilBucketNotExists(HeadBucketRequest.builder()
                                                                .bucket(bucketName)
                                                                .build())
                     .get();
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
