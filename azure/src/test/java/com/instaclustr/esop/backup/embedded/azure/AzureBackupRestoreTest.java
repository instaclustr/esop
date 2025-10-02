package com.instaclustr.esop.backup.embedded.azure;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.inject.Inject;
import com.instaclustr.esop.azure.AzureBackuper;
import com.instaclustr.esop.azure.AzureBucketService;
import com.instaclustr.esop.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.esop.azure.AzureRestorer;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("azure-test")
@Tag("cloud-test")
public class AzureBackupRestoreTest extends BaseAzureBackupRestoreTest {

    @Inject
    public CloudStorageAccountFactory cloudStorageAccountFactory;

    @BeforeEach
    public void setup() {
        inject();
        init();
    }

    @AfterEach
    public void teardown() throws Exception {
        destroy();
    }

    @Override
    public CloudStorageAccountFactory getStorageAccountFactory() {
        return cloudStorageAccountFactory;
    }

    protected BackupOperationRequest getBackupOperationRequest() {
        return new BackupOperationRequest();
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments());
    }

    @Test
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments());
    }

    @Test
    public void testDownload() throws Exception {
        AzureBucketService azureBucketService = new AzureBucketService(cloudStorageAccountFactory, getBackupOperationRequest());

        Path tmp = Files.createTempDirectory("tmp");
        tmp.toFile().deleteOnExit();

        try {
            azureBucketService.create(AbstractBackupTest.BUCKET_NAME);

            CloudBlobClient cloudBlobClient = cloudStorageAccountFactory.build(getBackupOperationRequest()).createCloudBlobClient();

            CloudBlobContainer container = cloudBlobClient.getContainerReference(AbstractBackupTest.BUCKET_NAME);

            CloudBlockBlob blob1 = container.getBlockBlobReference("cluster/dc/node/manifests/snapshot-name-" + AbstractBackupTest.BUCKET_NAME);
            blob1.uploadText("hello");

            CloudBlockBlob blob2 = container.getBlockBlobReference("snapshot/in/dir/name-" + AbstractBackupTest.BUCKET_NAME);
            blob2.uploadText("hello world");

            final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
            restoreOperationRequest.storageLocation = new StorageLocation("azure://" + AbstractBackupTest.BUCKET_NAME + "/cluster/dc/node");

            final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
            backupOperationRequest.storageLocation = new StorageLocation("azure://" + AbstractBackupTest.BUCKET_NAME + "/cluster/dc/node");

            final AzureRestorer azureRestorer = new AzureRestorer(cloudStorageAccountFactory, restoreOperationRequest);
            final AzureBackuper azureBackuper = new AzureBackuper(cloudStorageAccountFactory, backupOperationRequest);

            // 2

            final String content = azureRestorer.downloadNodeFile(Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            assertEquals("hello", content);

            // 3

            final String content2 = azureRestorer.downloadTopology(Paths.get("snapshot/in/dir"), s -> s.endsWith("name-" + AbstractBackupTest.BUCKET_NAME));
            assertEquals("hello world", content2);

            // 4

            azureRestorer.downloadFile(tmp.resolve("some-file"), azureRestorer.objectKeyToRemoteReference(Paths.get("snapshot/in/dir/name-" + AbstractBackupTest.BUCKET_NAME)));

            assertTrue(Files.exists(tmp.resolve("some-file")));
            assertEquals("hello world", new String(Files.readAllBytes(tmp.resolve("some-file"))));

            // backup

            azureBackuper.uploadText("hello world", azureBackuper.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));
            String text = azureRestorer.downloadFileToString(azureBackuper.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));

            assertEquals("hello world", text);

            String topology = azureRestorer.downloadTopology(Paths.get("topology/some-file-in"), fileName -> fileName.contains("topology/some-file-in"));

            assertEquals("hello world", topology);
        } finally {
            azureBucketService.delete(AbstractBackupTest.BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
            Files.deleteIfExists(tmp.resolve("some-file"));
        }
    }
}
