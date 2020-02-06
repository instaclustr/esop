package com.instaclustr.cassandra.backup.azure;

import static java.lang.String.format;

import java.net.URISyntaxException;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.instaclustr.cassandra.backup.azure.AzureModule.AzureModuleException;
import com.instaclustr.cassandra.backup.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.cassandra.backup.impl.BucketService;
import com.instaclustr.cassandra.backup.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;

public class AzureBucketService implements BucketService {

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

    @Override
    public boolean doesExist(final String bucketName) {
        try {
            return cloudBlobClient.getContainerReference(bucketName).exists();
        } catch (URISyntaxException | StorageException ex) {
            throw new AzureModuleException(format("Unable to determine if bucket %s exists!", bucketName), ex);
        }
    }

    @Override
    public void create(final String bucketName) {
        try {
            cloudBlobClient.getContainerReference(bucketName)
                .createIfNotExists(BlobContainerPublicAccessType.OFF,
                                   new BlobRequestOptions(),
                                   new OperationContext());
        } catch (URISyntaxException | StorageException ex) {
            throw new IllegalStateException(format("Unable to create a bucket %s", bucketName), ex);
        }
    }

    @Override
    public void delete(final String bucketName) {
        try {
            cloudBlobClient.getContainerReference(bucketName).deleteIfExists();
        } catch (URISyntaxException | StorageException ex) {
            throw new AzureModuleException(format("Unable to delete bucket %s", bucketName), ex);
        }
    }

    @Override
    public void close() {
    }
}
