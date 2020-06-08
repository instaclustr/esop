package com.instaclustr.cassandra.backup.embedded.azure;

import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.azure.AzureBucketService;
import com.instaclustr.cassandra.backup.azure.AzureModule;
import com.instaclustr.cassandra.backup.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.cassandra.backup.azure.AzureRestorer;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;
import com.instaclustr.threading.Executors.FixedTasksExecutorSupplier;
import com.instaclustr.threading.ExecutorsModule;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
    "cloudTest",
    "azureTest",
})
public class AzureBackupRestoreTest extends BaseAzureBackupRestoreTest {

    @Inject
    public CloudStorageAccountFactory cloudStorageAccountFactory;

    @BeforeMethod
    public void setup() throws Exception {

        final List<Module> modules = new ArrayList<Module>()
        {{
            add(new KubernetesApiModule());
            add(new AzureModule());
            add(new ExecutorsModule());
        }};

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
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

        try {
            azureBucketService.create(BUCKET_NAME);

            CloudBlobClient cloudBlobClient = cloudStorageAccountFactory.build(getBackupOperationRequest()).createCloudBlobClient();

            CloudBlobContainer container = cloudBlobClient.getContainerReference(BUCKET_NAME);

            CloudBlockBlob blob1 = container.getBlockBlobReference("cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME);
            blob1.uploadText("hello");

            CloudBlockBlob blob2 = container.getBlockBlobReference("snapshot/in/dir/name-" + BUCKET_NAME);
            blob2.uploadText("hello world");

            final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
            restoreOperationRequest.storageLocation = new StorageLocation("azure://" + BUCKET_NAME + "/cluster/dc/node");

            final AzureRestorer azureRestorer = new AzureRestorer(cloudStorageAccountFactory, new FixedTasksExecutorSupplier(), restoreOperationRequest);

            // 1

            final Path downloadedFile = azureRestorer.downloadNodeFileToDir(Paths.get("/tmp"), Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            assertTrue(Files.exists(downloadedFile));

            // 2

            final String content = azureRestorer.downloadNodeFileToString(Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            Assert.assertEquals("hello", content);

            // 3

            final String content2 = azureRestorer.downloadFileToString(Paths.get("snapshot/in/dir"), s -> s.endsWith("name-" + BUCKET_NAME));
            Assert.assertEquals("hello world", content2);

            // 4

            azureRestorer.downloadFile(Paths.get("/tmp/some-file"), azureRestorer.objectKeyToRemoteReference(Paths.get("snapshot/in/dir/name-" + BUCKET_NAME)));

            Assert.assertTrue(Files.exists(Paths.get("/tmp/some-file")));
            Assert.assertEquals("hello world", new String(Files.readAllBytes(Paths.get("/tmp/some-file"))));
        } finally {
            azureBucketService.delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
            Files.deleteIfExists(Paths.get("/tmp/some-file"));
        }
    }
}
