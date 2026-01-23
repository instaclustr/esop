package com.instaclustr.esop.backup.embedded.s3.aws.v2;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.instaclustr.esop.backup.embedded.s3.aws.AbstractS3UploadDownloadTest;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.s3.S3RemoteObjectReference;
import com.instaclustr.esop.s3.aws_v2.S3Backuper;
import com.instaclustr.esop.s3.aws_v2.S3BucketService;
import com.instaclustr.esop.s3.aws_v2.S3Restorer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.encryption.s3.CommitmentPolicy;
import software.amazon.encryption.s3.S3EncryptionClient;
import software.amazon.encryption.s3.algorithms.AlgorithmSuite;

import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_KMS_KEY_ID_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.TEST_ESOP_AWS_KMS_WRAPPING_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag("s3-test")
@Tag("cloud-test")
public class AwsS3EncryptionLibBackwardCompatibilityTest extends AbstractS3UploadDownloadTest {
    private static final String OBJECT_KEY_1 = "test-object-1";
    private static final String OBJECT_KEY_2 = "test-object-2";
    private static final String TEST_DATA = "some-test-data";

    private S3Client v3Client;
    private S3Backuper backuper;
    private S3Restorer restorer;

    @AfterAll
    static void clear() throws Exception {
        deleteTestBucket();
        System.clearProperty(AWS_KMS_KEY_ID_PROPERTY);
    }

    @AfterEach
    void closeClients() throws Exception {
        if (v3Client != null) {
            v3Client.close();
        }
        if (s3Backuper != null) {
            backuper.close();
        }
        if (s3Restorer != null) {
            restorer.close();
        }
    }

    @Test
        // This tests ensure that data encrypted with v3 behavior client can be decrypted with v4 client and vice versa.
        // So, if we had a data encrypted with v3 client before migrating to esop 4.1.0+, we can still read it after migration,
        // and we can also write new data with v4 client that can be read back with v3 behavior client.
    void v3Tov4BackwardCompatibilityTest() throws Exception {
        String kmsKeyId = System.getProperty(TEST_ESOP_AWS_KMS_WRAPPING_KEY);
        assumeTrue(kmsKeyId != null, "Cannot continue as " + TEST_ESOP_AWS_KMS_WRAPPING_KEY + " is not set!");
        System.setProperty(AWS_KMS_KEY_ID_PROPERTY, kmsKeyId);

        BackupOperationRequest backupRequest = new BackupOperationRequest();
        backupRequest.kmsKeyId = kmsKeyId;
        backupRequest.storageLocation = storageLocation();

        createTestBucket();

        // v3 behavior client
        // This settings are default for v3.6.0 lib version.
        v3Client = S3EncryptionClient.builderV4()
                .wrappedClient(S3Client.create())
                .kmsKeyId(kmsKeyId)
                .enableDelayedAuthenticationMode(true)
                .commitmentPolicy(CommitmentPolicy.FORBID_ENCRYPT_ALLOW_DECRYPT)
                .encryptionAlgorithm(AlgorithmSuite.ALG_AES_256_GCM_IV12_TAG16_NO_KDF)
                .build();

        v3Client.putObject(PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(OBJECT_KEY_1)
                        .build(),
                RequestBody.fromString(TEST_DATA));
        v3Client.waiter().waitUntilObjectExists(HeadObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(OBJECT_KEY_1)
                .build());


        RestoreOperationRequest restoreRequest = new RestoreOperationRequest();
        backupRequest.kmsKeyId = kmsKeyId;
        restoreRequest.storageLocation = storageLocation();

        // It will resolve aws credentials and region from environment automatically
        backuper = new S3Backuper(backupRequest);
        restorer = new S3Restorer(restoreRequest);

        // Downloads data that was encrypted by v4 client with v3 behavior.
        // This is the best we can do to test backward compatibility.
        String downloadedData = restorer.downloadFileToString(new S3RemoteObjectReference(null, OBJECT_KEY_1), true);

        assertEquals(TEST_DATA, downloadedData, "Downloaded data by v4 client should match uploaded data by v3 client");

        // Creating new test file to upload with v4 client
        Path testFile = Files.createFile(tempDir.resolve("some-file"));
        Files.write(testFile, TEST_DATA.getBytes(StandardCharsets.UTF_8));
        ManifestEntry manifestEntry = new ManifestEntry(Path.of(BUCKET_NAME, OBJECT_KEY_2), testFile, ManifestEntry.Type.FILE, null, null);

        // Uploading with v4 client and downloading with v3 behavior client
        backuper.uploadEncryptedFile(manifestEntry, new FileInputStream(testFile.toFile()), new S3RemoteObjectReference(null, OBJECT_KEY_2));
        String downloadedData2 = v3Client.getObjectAsBytes(builder -> builder.bucket(BUCKET_NAME).key(OBJECT_KEY_2)).asUtf8String();

        assertEquals(TEST_DATA, downloadedData2, "Downloaded data v3 client should match uploaded data by v4 client");
    }

    private static StorageLocation storageLocation() {
        return new StorageLocation("s3://" + BUCKET_NAME);
    }

    private static void createTestBucket() throws Exception {
        BackupOperationRequest backupRequest = new BackupOperationRequest();
        backupRequest.storageLocation = storageLocation();
        BucketService service = new S3BucketService(backupRequest);
        service.create(BUCKET_NAME);
        service.close();
    }

    private static void deleteTestBucket() throws Exception {
        BackupOperationRequest backupRequest = new BackupOperationRequest();
        backupRequest.storageLocation = storageLocation();
        BucketService service = new S3BucketService(backupRequest);
        service.delete(BUCKET_NAME);
        service.close();
    }
}
