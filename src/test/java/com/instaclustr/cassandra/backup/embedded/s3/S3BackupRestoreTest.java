package com.instaclustr.cassandra.backup.embedded.s3;

import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.s3.aws.S3BucketService;
import com.instaclustr.cassandra.backup.s3.aws.S3Module;
import com.instaclustr.cassandra.backup.s3.aws.S3Module.S3TransferManagerFactory;
import com.instaclustr.cassandra.backup.s3.aws.S3Restorer;
import com.instaclustr.kubernetes.KubernetesApiModule;
import com.instaclustr.threading.Executors.FixedTasksExecutorSupplier;
import com.instaclustr.threading.ExecutorsModule;
import io.kubernetes.client.ApiException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
    "s3Test",
    "cloudTest",
})
public class S3BackupRestoreTest extends BaseS3BackupRestoreTest {

    @Inject
    public S3TransferManagerFactory transferManagerFactory;

    @BeforeMethod
    public void setup() throws ApiException, IOException {

        final List<Module> modules = new ArrayList<Module>() {{
            add(new KubernetesApiModule());
            add(new S3Module());
            add(new ExecutorsModule());
        }};

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() throws ApiException {
        destroy();
    }

    @Override
    public S3TransferManagerFactory getTransferManagerFactory() {
        return transferManagerFactory;
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
        S3BucketService s3BucketService = new S3BucketService(getTransferManagerFactory(), getBackupOperationRequest());

        try {
            s3BucketService.create(BUCKET_NAME);

            AmazonS3 amazonS3Client = getTransferManagerFactory().build(getBackupOperationRequest()).getAmazonS3Client();

            amazonS3Client.putObject(BUCKET_NAME, "cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME, "hello");
            amazonS3Client.putObject(BUCKET_NAME, "snapshot/in/dir/my-name-" + BUCKET_NAME, "hello world");

            amazonS3Client.listObjects(BUCKET_NAME).getObjectSummaries().forEach(summary -> logger.info(summary.getKey()));

            final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
            restoreOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");

            final S3Restorer s3Restorer = new S3Restorer(getTransferManagerFactory(), new FixedTasksExecutorSupplier(), restoreOperationRequest);

            // 1

            final Path downloadedFile = s3Restorer.downloadNodeFileToDir(Paths.get("/tmp"), Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            assertTrue(Files.exists(downloadedFile));

            // 2

            final String content = s3Restorer.downloadNodeFileToString(Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            Assert.assertEquals("hello", content);

            // 3

            final String content2 = s3Restorer.downloadFileToString(Paths.get("snapshot/in/dir"), s -> s.endsWith("my-name-" + BUCKET_NAME));
            Assert.assertEquals("hello world", content2);

            // 4

            s3Restorer.downloadFile(Paths.get("/tmp/some-file"), s3Restorer.objectKeyToRemoteReference(Paths.get("snapshot/in/dir/my-name-" + BUCKET_NAME)));

            Assert.assertTrue(Files.exists(Paths.get("/tmp/some-file")));
            Assert.assertEquals("hello world", new String(Files.readAllBytes(Paths.get("/tmp/some-file"))));
        } finally {
            s3BucketService.delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
            Files.deleteIfExists(Paths.get("/tmp/some-file"));
        }
    }
}
