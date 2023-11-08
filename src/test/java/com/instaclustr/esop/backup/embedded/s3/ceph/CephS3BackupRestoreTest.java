package com.instaclustr.esop.backup.embedded.s3.ceph;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.services.s3.AmazonS3;
import com.google.inject.Inject;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.ceph.CephBackuper;
import com.instaclustr.esop.s3.ceph.CephBucketService;
import com.instaclustr.esop.s3.ceph.CephModule.CephS3TransferManagerFactory;
import com.instaclustr.esop.s3.ceph.CephRestorer;
import io.kubernetes.client.ApiException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.instaclustr.io.FileUtils.deleteDirectory;

@Test(groups = {
    "cephTest",
})
public class CephS3BackupRestoreTest extends BaseCephS3BackupRestoreTest {

    @Inject
    public CephS3TransferManagerFactory transferManagerFactory;

    @BeforeMethod
    public void setup() throws ApiException, IOException {
        inject();
        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    @Override
    public CephS3TransferManagerFactory getTransferManagerFactory() {
        return transferManagerFactory;
    }

    protected BackupOperationRequest getBackupOperationRequest() {
        return new BackupOperationRequest();
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments(CASSANDRA_VERSION));
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments(CASSANDRA_4_VERSION), CASSANDRA_4_VERSION);
    }

    @Test
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments(CASSANDRA_VERSION), CASSANDRA_VERSION);
    }

    @Test
    public void testDownload() throws Exception {

        CephS3TransferManagerFactory factory = getTransferManagerFactory();

        CephBucketService s3BucketService = new CephBucketService(factory, getBackupOperationRequest());

        Path tmp = Files.createTempDirectory("tmp");
        tmp.toFile().deleteOnExit();

        try {
            s3BucketService.create(BUCKET_NAME);

            AmazonS3 amazonS3Client = factory.build(getBackupOperationRequest()).getAmazonS3Client();

            amazonS3Client.putObject(BUCKET_NAME, "cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME, "hello");
            amazonS3Client.putObject(BUCKET_NAME, "snapshot/in/dir/my-name-" + BUCKET_NAME, "hello world");

            amazonS3Client.listObjects(BUCKET_NAME).getObjectSummaries().forEach(summary -> logger.info(summary.getKey()));

            final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
            restoreOperationRequest.storageLocation = new StorageLocation("ceph://" + BUCKET_NAME + "/cluster/dc/node");

            final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
            backupOperationRequest.storageLocation = new StorageLocation("ceph://" + BUCKET_NAME + "/cluster/dc/node");

            final CephRestorer s3Restorer = new CephRestorer(factory, restoreOperationRequest);
            final CephBackuper s3Backuper = new CephBackuper(factory, backupOperationRequest);

            // 2

            final String content = s3Restorer.downloadNodeFile(Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            Assert.assertEquals("hello", content);

            // 3

            final String content2 = s3Restorer.downloadTopology(Paths.get("snapshot/in/dir"), s -> s.endsWith("my-name-" + BUCKET_NAME));
            Assert.assertEquals("hello world", content2);

            // 4

            s3Restorer.downloadFile(tmp.resolve("some-file"), s3Restorer.objectKeyToRemoteReference(Paths.get("snapshot/in/dir/my-name-" + BUCKET_NAME)));

            Assert.assertTrue(Files.exists(tmp.resolve("some-file")));
            Assert.assertEquals("hello world", new String(Files.readAllBytes(tmp.resolve("some-file"))));

            // backup

            s3Backuper.uploadText("hello world", s3Backuper.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));
            String text = s3Restorer.downloadFileToString(s3Restorer.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));

            Assert.assertEquals("hello world", text);
        } finally {
            s3BucketService.delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
            Files.deleteIfExists(tmp.resolve("some-file"));
        }
    }
}
