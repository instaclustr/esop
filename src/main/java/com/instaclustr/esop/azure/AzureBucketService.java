package com.instaclustr.esop.azure;

import static java.lang.String.format;

import java.net.URISyntaxException;
import java.util.stream.StreamSupport;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.esop.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBucketService extends BucketService {

    private static final Logger logger = LoggerFactory.getLogger(AzureBucketService.class);

    private final CloudStorageAccount cloudStorageAccount;

    private final CloudBlobClient cloudBlobClient;

    @AssistedInject
    public AzureBucketService(final CloudStorageAccountFactory accountFactory,
                              @Assisted final BackupOperationRequest request) throws URISyntaxException {
        this.cloudStorageAccount = accountFactory.build(request);
        this.cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
    }

    @AssistedInject
    public AzureBucketService(final CloudStorageAccountFactory accountFactory,
                              @Assisted final BackupCommitLogsOperationRequest request) throws URISyntaxException {
        this.cloudStorageAccount = accountFactory.build(request);
        this.cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
    }

    @AssistedInject
    public AzureBucketService(final CloudStorageAccountFactory accountFactory,
                              @Assisted final RestoreOperationRequest request) throws URISyntaxException {
        this.cloudStorageAccount = accountFactory.build(request);
        this.cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
    }

    @AssistedInject
    public AzureBucketService(final CloudStorageAccountFactory accountFactory,
                              @Assisted final RestoreCommitLogsOperationRequest request) throws URISyntaxException {
        this.cloudStorageAccount = accountFactory.build(request);
        this.cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
    }

    @AssistedInject
    public AzureBucketService(final CloudStorageAccountFactory accountFactory,
                              @Assisted final ListOperationRequest request) throws URISyntaxException {
        this.cloudStorageAccount = accountFactory.build(request);
        this.cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
    }

    @Override
    public boolean doesExist(final String bucketName) throws BucketServiceException {
        try {
            return cloudBlobClient.getContainerReference(bucketName).exists();
        } catch (URISyntaxException | StorageException ex) {
            throw new BucketServiceException(format("Unable to determine if the bucket %s exists.", bucketName), ex);
        }
    }

    @Override
    public void create(final String bucketName) throws BucketServiceException {

        while (true) {
            try {
                cloudBlobClient.getContainerReference(bucketName)
                               .createIfNotExists(BlobContainerPublicAccessType.OFF,
                                                  new BlobRequestOptions(),
                                                  new OperationContext());

                break;
            } catch (URISyntaxException ex) {
                throw new BucketServiceException(format("Unable to create a bucket %s", bucketName), ex);
            } catch (StorageException ex) {
                if (ex.getHttpStatusCode() == 409
                    && ex.getExtendedErrorInformation().getErrorMessage().contains("The specified container is being deleted. Try operation later.")) {
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
            cloudBlobClient.getContainerReference(bucketName).deleteIfExists();

            // waiting until it is really deleted
            while (true) {

                final Iterable<CloudBlobContainer> iterable = cloudBlobClient.listContainers();

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
