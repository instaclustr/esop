package com.instaclustr.esop.backup.embedded.s3.aws.v2;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.inject.Inject;
import com.instaclustr.esop.backup.embedded.s3.aws.AbstractS3UploadDownloadTest;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.aws_v2.S3Backuper;
import com.instaclustr.esop.s3.aws_v2.S3BucketService;
import com.instaclustr.esop.s3.aws_v2.S3Restorer;
import com.instaclustr.esop.s3.aws_v2.S3V2Module;
import com.instaclustr.esop.s3.v1.S3RemoteObjectReference;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_KMS_KEY_ID_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.TEST_ESOP_AWS_KMS_WRAPPING_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static software.amazon.awssdk.core.async.AsyncRequestBody.fromBytes;

public class UploadDownloadTest extends AbstractS3UploadDownloadTest {
    @Inject
    private S3Clients s3Clients;
    private S3BucketService s3BucketService;
    private Path tmp;

    @BeforeMethod
    public void setup() throws Exception {
        tmp = Files.createTempDirectory("tmp");
        Files.deleteIfExists(tmp.resolve("some-file"));
        inject(new S3V2Module());
        s3BucketService = new S3BucketService(s3Clients);
        s3BucketService.create(BUCKET_NAME);
    }

    @AfterMethod
    public void teardown() throws Exception {
        System.clearProperty(AWS_KMS_KEY_ID_PROPERTY);
        s3BucketService.delete(BUCKET_NAME);
        s3Clients.close();
    }

    @Test
    public void testEncrypted() throws Exception {
        System.setProperty(AWS_KMS_KEY_ID_PROPERTY, System.getProperty(TEST_ESOP_AWS_KMS_WRAPPING_KEY));
        if (System.getProperty(AWS_KMS_KEY_ID_PROPERTY) == null)
            throw new SkipException("Cannot continue as " + AWS_KMS_KEY_ID_PROPERTY + " is not set!");

        try {
            testExecution();
        }
        finally {
            System.clearProperty(AWS_KMS_KEY_ID_PROPERTY);
        }
    }

    @Test
    public void testUnencrypted() throws Exception {
        testExecution();
    }

    private void testExecution() throws Exception {
        Path tmp = Files.createTempDirectory("tmp");
        tmp.toFile().deleteOnExit();

        S3AsyncClient s3Client = s3Clients.getClient();
        s3Client.putObject(PutObjectRequest.builder()
                                           .bucket(BUCKET_NAME)
                                           .key("cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME)
                                           .build(),
                           fromBytes("hello".getBytes(UTF_8))).get();

        s3Client.putObject(PutObjectRequest.builder()
                                           .bucket(BUCKET_NAME)
                                           .key("snapshot/in/dir/my-name-" + BUCKET_NAME)
                                           .build(),
                           fromBytes("hello world".getBytes(UTF_8))).get();

        s3Client.waiter().waitUntilObjectExists(HeadObjectRequest.builder()
                                                                 .bucket(BUCKET_NAME)
                                                                 .key("cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME)
                                                                 .build()).get();
        s3Client.waiter().waitUntilObjectExists(HeadObjectRequest.builder()
                                                                 .bucket(BUCKET_NAME)
                                                                 .key("snapshot/in/dir/my-name-" + BUCKET_NAME)
                                                                 .build()).get();

        s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET_NAME).build())
                .get()
                .contents()
                .forEach(object -> logger.info(object.key()));

        final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
        restoreOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");

        final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
        backupOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");

        S3Backuper s3Backuper = new S3Backuper(s3Clients, backupOperationRequest);
        S3Restorer s3Restorer = new S3Restorer(s3Clients, restoreOperationRequest);

        // 1

        final Path downloadedFile = s3Restorer.downloadNodeFileToDir(tmp, Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
        assertTrue(Files.exists(downloadedFile));

        // 2

        final String content = s3Restorer.downloadNodeFileToString(Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
        assertEquals("hello", content);

        // 3

        final String content2 = s3Restorer.downloadFileToString(Paths.get("snapshot/in/dir"), s -> s.endsWith("my-name-" + BUCKET_NAME));
        assertEquals("hello world", content2);

        // 4

        s3Restorer.downloadFile(tmp.resolve("some-file"), s3Restorer.objectKeyToRemoteReference(Paths.get("snapshot/in/dir/my-name-" + BUCKET_NAME)));

        assertTrue(Files.exists(tmp.resolve("some-file")));
        assertEquals("hello world", new String(Files.readAllBytes(tmp.resolve("some-file"))));

        // backup

        s3Backuper.uploadText("hello world", s3Backuper.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));
        String text = s3Restorer.downloadFileToString(s3Restorer.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));

        assertEquals("hello world", text);

        // backup upload file

        Path tempFile = Files.createTempFile("esop", ".txt");
        Files.write(tempFile, "hello world".getBytes(UTF_8));

        try (FileInputStream fis = new FileInputStream(tempFile.toFile())) {
            s3Backuper.uploadEncryptedFile(Files.size(tempFile), fis, new S3RemoteObjectReference(tempFile, tempFile.toString()));
        }
    }
}
