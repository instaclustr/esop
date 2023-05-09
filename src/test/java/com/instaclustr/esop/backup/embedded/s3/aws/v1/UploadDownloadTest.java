package com.instaclustr.esop.backup.embedded.s3.aws.v1;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.inject.Inject;
import com.instaclustr.esop.backup.embedded.s3.aws.AbstractS3UploadDownloadTest;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.aws.S3Backuper;
import com.instaclustr.esop.s3.aws.S3BucketService;
import com.instaclustr.esop.s3.aws.S3Module;
import com.instaclustr.esop.s3.aws.S3Module.S3TransferManagerFactory;
import com.instaclustr.esop.s3.aws.S3Restorer;
import com.instaclustr.esop.s3.v1.S3RemoteObjectReference;
import com.instaclustr.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.testng.Assert.assertTrue;

public class UploadDownloadTest extends AbstractS3UploadDownloadTest
{
    @Inject
    public S3TransferManagerFactory transferManagerFactory;

    @BeforeMethod
    public void setup() throws Exception {
        inject(new S3Module());
        init();
    }

    @Test
    public void testDownloadUpload() throws Exception {

        S3TransferManagerFactory factory = transferManagerFactory;

        S3BucketService s3BucketService = new S3BucketService(transferManagerFactory, new BackupOperationRequest());

        Path tmp = Files.createTempDirectory("tmp");
        tmp.toFile().deleteOnExit();

        try {
            s3BucketService.create(BUCKET_NAME);

            AmazonS3 amazonS3Client = factory.build(new BackupOperationRequest()).getAmazonS3Client();

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

            final Path downloadedFile = s3Restorer.downloadNodeFileToDir(tmp, Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            assertTrue(Files.exists(downloadedFile));

            // 2

            final String content = s3Restorer.downloadNodeFileToString(Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
            Assert.assertEquals("hello", content);

            // 3

            final String content2 = s3Restorer.downloadFileToString(Paths.get("snapshot/in/dir"), s -> s.endsWith("my-name-" + BUCKET_NAME));
            Assert.assertEquals("hello world", content2);

            // 4

            s3Restorer.downloadFile(tmp.resolve("some-file"), s3Restorer.objectKeyToRemoteReference(Paths.get("snapshot/in/dir/my-name-" + BUCKET_NAME)));

            Assert.assertTrue(Files.exists(tmp.resolve("some-file")));
            Assert.assertEquals("hello world", new String(Files.readAllBytes(tmp.resolve("some-file"))));

            // backup upload text

            // these "encrypted" method are not always encrypting, that depends on aws kms key id set or not for a given test
            s3Backuper.uploadEncryptedText("hello world", s3Backuper.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));
            String text = s3Restorer.downloadFileToString(s3Restorer.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));

            Assert.assertEquals("hello world", text);

            // backup upload file

            Path tempFile = Files.createTempFile("esop", ".txt");
            Files.write(tempFile, "hello world".getBytes(StandardCharsets.UTF_8));

            try (FileInputStream fis = new FileInputStream(tempFile.toFile())) {
                s3Backuper.uploadEncryptedFile(Files.size(tempFile), fis, new S3RemoteObjectReference(tempFile, tempFile.toString()));
            }
        } finally {
            new S3BucketService(transferManagerFactory, new BackupOperationRequest()).delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
            Files.deleteIfExists(tmp.resolve("some-file"));
        }
    }
}
