package com.instaclustr.esop.backup.embedded.s3.aws;

import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.services.s3.AmazonS3;
import com.google.inject.Inject;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.aws.S3Backuper;
import com.instaclustr.esop.s3.aws.S3BucketService;
import com.instaclustr.esop.s3.aws.S3Module.S3TransferManagerFactory;
import com.instaclustr.esop.s3.aws.S3Restorer;
import io.kubernetes.client.ApiException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
        "s3Test",
        "cloudTest",
})
public class AWSS3BackupRestoreTest extends BaseAWSS3BackupRestoreTest {

    @Inject
    public S3TransferManagerFactory transferManagerFactory;

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
    protected String getStorageLocation() {
        return "s3://" + BUCKET_NAME + "/cluster/datacenter1/node1";
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

        S3TransferManagerFactory factory = getTransferManagerFactory();

        S3BucketService s3BucketService = new S3BucketService(factory, getBackupOperationRequest());

        try {
            s3BucketService.create(BUCKET_NAME);

            AmazonS3 amazonS3Client = factory.build(getBackupOperationRequest()).getAmazonS3Client();

            amazonS3Client.putObject(BUCKET_NAME, "cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME, "hello");
            amazonS3Client.putObject(BUCKET_NAME, "snapshot/in/dir/my-name-" + BUCKET_NAME, "hello world");

            amazonS3Client.listObjects(BUCKET_NAME).getObjectSummaries().forEach(summary -> logger.info(summary.getKey()));

            final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
            restoreOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");

            final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
            backupOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");

            final S3Restorer s3Restorer = new S3Restorer(factory, restoreOperationRequest);
            final S3Backuper s3Backuper = new S3Backuper(factory, backupOperationRequest);

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

            // backup

            s3Backuper.uploadText("hello world", s3Backuper.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));
            String text = s3Restorer.downloadFileToString(s3Restorer.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));

            Assert.assertEquals("hello world", text);
        } finally {
            s3BucketService.delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
            Files.deleteIfExists(Paths.get("/tmp/some-file"));
        }
    }
}
