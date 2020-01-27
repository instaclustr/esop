package com.instaclustr.cassandra.backup.embedded.azure;

import static java.lang.String.format;

import java.util.UUID;

import com.instaclustr.cassandra.backup.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public abstract class BaseAzureBackupRestoreTest extends AbstractBackupTest {

    protected static final String BUCKET_NAME = UUID.randomUUID().toString();

    protected abstract BackupOperationRequest getBackupOperationRequest();

    protected abstract String[][] getProgramArguments();

    public abstract CloudStorageAccountFactory getStorageAccountFactory();

    public void test() throws Exception {
        final CloudStorageAccount storageAccount = getStorageAccountFactory().build(getBackupOperationRequest());
        final CloudBlobClient cloudBlobClient = storageAccount.createCloudBlobClient();
        CloudBlobContainer cloudBlobContainer = null;

        try {
            cloudBlobContainer = cloudBlobClient.getContainerReference(BUCKET_NAME);

            if (!cloudBlobContainer.createIfNotExists(BlobContainerPublicAccessType.OFF, new BlobRequestOptions(), new OperationContext())) {
                throw new IllegalStateException(format("Unable to create blob container %s", BUCKET_NAME));
            }

            cloudBlobContainer = cloudBlobClient.getContainerReference(BUCKET_NAME);

            backupAndRestoreTest(getProgramArguments());
        } finally {
            if (cloudBlobContainer != null) {
                cloudBlobContainer.deleteIfExists();
            }
        }
    }
}
