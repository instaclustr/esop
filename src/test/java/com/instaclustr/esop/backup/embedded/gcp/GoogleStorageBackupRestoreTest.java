package com.instaclustr.esop.backup.embedded.gcp;

import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.inject.Inject;
import com.instaclustr.esop.gcp.GCPBackuper;
import com.instaclustr.esop.gcp.GCPBucketService;
import com.instaclustr.esop.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.esop.gcp.GCPRestorer;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Test(groups = {
    "googleTest",
    "cloudTest",
})
@Ignore
public class GoogleStorageBackupRestoreTest extends BaseGoogleStorageBackupRestoreTest {

    @Inject
    public GoogleStorageFactory googleStorageFactory;

    @BeforeMethod
    public void setup() throws Exception {
        inject();
        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    protected BackupOperationRequest getBackupOperationRequest() {
        return new BackupOperationRequest();
    }

    @Override
    public GoogleStorageFactory getGoogleStorageFactory() {
        return googleStorageFactory;
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments(), CASSANDRA_4_VERSION);
    }

    @Test
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments(), CASSANDRA_VERSION);
    }

    @Test
    public void testDownload() throws Exception {
        GCPBucketService gcpBucketService = new GCPBucketService(googleStorageFactory, getBackupOperationRequest());

        try {
            gcpBucketService.create(BUCKET_NAME);

            Storage storage = googleStorageFactory.build(getBackupOperationRequest());
            Bucket bucket = storage.get(BUCKET_NAME);

            bucket.create("cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME, "hello".getBytes());
            bucket.create("snapshot/in/dir/name-" + BUCKET_NAME, "hello world".getBytes());

            final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
            restoreOperationRequest.storageLocation = new StorageLocation("gcp://" + BUCKET_NAME + "/cluster/dc/node");

            final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
            backupOperationRequest.storageLocation = new StorageLocation("gcp://" + BUCKET_NAME + "/cluster/dc/node");

            final GCPRestorer gcpRestorer = new GCPRestorer(googleStorageFactory, restoreOperationRequest);
            final GCPBackuper gcpBackuper = new GCPBackuper(googleStorageFactory, backupOperationRequest);

            // 1

            final Path downloadedFile = gcpRestorer.downloadNodeFileToDir(Paths.get("/tmp"), Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            assertTrue(Files.exists(downloadedFile));

            // 2

            final String content = gcpRestorer.downloadNodeFileToString(Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            Assert.assertEquals("hello", content);

            // 3

//            final String content2 = gcpRestorer.downloadFileToString(Paths.get("snapshot/in/dir"), s -> s.endsWith("name-" + BUCKET_NAME));
//            Assert.assertEquals("hello world", content2);

            // 4

            gcpRestorer.downloadFile(Paths.get("/tmp/some-file"), gcpRestorer.objectKeyToRemoteReference(Paths.get("snapshot/in/dir/name-" + BUCKET_NAME)));

            Assert.assertTrue(Files.exists(Paths.get("/tmp/some-file")));
            Assert.assertEquals("hello world", new String(Files.readAllBytes(Paths.get("/tmp/some-file"))));

            // backup

            gcpBackuper.uploadText("hello world", gcpBackuper.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));
            String topology = gcpRestorer.downloadFileToString(Paths.get("topology/some-file-in"), fileName -> fileName.contains("topology/some-file-in"));
            Assert.assertEquals("hello world", topology);
        } finally {
            gcpBucketService.delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
            Files.deleteIfExists(Paths.get("/tmp/some-file"));
        }
    }
}
