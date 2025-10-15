package com.instaclustr.esop.azure;

import java.net.URISyntaxException;
import java.util.stream.StreamSupport;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.azure.AzureModule.BlobServiceClientFactory;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public class AzureBucketService extends BucketService {

    private static final Logger logger = LoggerFactory.getLogger(AzureBucketService.class);

    private final BlobServiceClient blobServiceClient;

    @AssistedInject
    public AzureBucketService(final BlobServiceClientFactory blobServiceClientFactory,
                              @Assisted final BackupOperationRequest request) throws URISyntaxException {
        this.blobServiceClient = blobServiceClientFactory.build(request);
    }

    @AssistedInject
    public AzureBucketService(final BlobServiceClientFactory blobServiceClientFactory,
                              @Assisted final BackupCommitLogsOperationRequest request) throws URISyntaxException {
        this.blobServiceClient = blobServiceClientFactory.build(request);
    }

    @AssistedInject
    public AzureBucketService(final BlobServiceClientFactory blobServiceClientFactory,
                              @Assisted final RestoreOperationRequest request) throws URISyntaxException {
        this.blobServiceClient = blobServiceClientFactory.build(request);
    }

    @AssistedInject
    public AzureBucketService(final BlobServiceClientFactory blobServiceClientFactory,
                              @Assisted final RestoreCommitLogsOperationRequest request) throws URISyntaxException {
        this.blobServiceClient = blobServiceClientFactory.build(request);
    }

    @AssistedInject
    public AzureBucketService(final BlobServiceClientFactory blobServiceClientFactory,
                              @Assisted final ListOperationRequest request) throws URISyntaxException {
        this.blobServiceClient = blobServiceClientFactory.build(request);
    }

    @Override
    public boolean doesExist(final String bucketName) throws BucketServiceException {
        try {
            return blobServiceClient.getBlobContainerClient(bucketName).exists();
        } catch (BlobStorageException ex) {
            throw new BucketServiceException(format("Unable to determine if the bucket %s exists.", bucketName), ex);
        }
    }

    @Override
    public void create(final String bucketName) throws BucketServiceException {

        while (true) {
            try {
                // Default access type is Private
                blobServiceClient.getBlobContainerClient(bucketName).createIfNotExists();
                break;
            } catch (BlobStorageException ex) {
                if (ex.getResponse().getStatusCode() == 409
                    && ex.getServiceMessage().contains("The specified container is being deleted. Try operation later.")) {
                    try {
                        logger.info("Bucket to create {} is being deleted, we are going to wait 5s and check again.", bucketName);
                        Thread.sleep(5000);
                    } catch (final Exception ex2) {
                        ex2.printStackTrace();
                    }
                } else {
                    throw new BucketServiceException(format("Unable to create a bucket %s", bucketName), ex);
                }
            }
        }
    }

    @Override
    public void delete(final String bucketName) throws BucketServiceException {
        try {
            logger.info("Deleting bucket {}", bucketName);
            blobServiceClient.getBlobContainerClient(bucketName).deleteIfExists();

            // waiting until it is really deleted
            while (true) {

                final Iterable<BlobContainerItem> iterable = blobServiceClient.listBlobContainers();

                if (StreamSupport.stream(iterable.spliterator(), false).noneMatch(container -> container.getName().equals(bucketName))) {
                    break;
                }

                try {
                    logger.info("Waiting until bucket {} is truly deleted.", bucketName);
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (final Exception ex) {
            throw new BucketServiceException(format("Unable to delete the bucket %s", bucketName), ex);
        }
    }

    @Override
    public void close() {
    }
}
