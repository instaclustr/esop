package com.instaclustr.esop.backup.embedded.s3.aws;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.KeyspaceTable;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.s3.S3RemoteObjectReference;
import com.instaclustr.esop.s3.aws_v2.S3Backuper;
import com.instaclustr.esop.s3.aws_v2.S3Module;
import com.instaclustr.esop.s3.aws_v2.S3Restorer;
import com.instaclustr.esop.s3.v2.S3ClientsFactory;
import org.junit.After;
import org.junit.Before;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_KMS_KEY_ID_PROPERTY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractS3UploadDownloadTest extends AbstractBackupTest {
    @Override
    protected String protocol() {
        return "s3://";
    }

    protected S3Backuper s3Backuper;
    protected S3Restorer s3Restorer;
    protected S3ClientsFactory.S3Clients s3Clients;

    protected Path tempDir;

    public void inject(AbstractModule s3Module) {
        final List<Module> modules = new ArrayList<Module>() {{
            add(s3Module);
        }};

        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);
    }

    @Before
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory("tmp");
        Files.deleteIfExists(tempDir.resolve("some-file"));
        inject(new S3Module());
    }

    @After
    public void teardown() throws Exception {
        System.clearProperty(AWS_KMS_KEY_ID_PROPERTY);
        if (s3Clients != null)
            s3Clients.close();
    }

    protected void testExecution() throws Exception {
        Path tmp = Files.createTempDirectory("tmp");
        tmp.toFile().deleteOnExit();

        S3Client s3Client = s3Backuper.s3Clients.getNonEncryptingClient();

        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key("cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME)
                        .build(),
                RequestBody.fromBytes("hello".getBytes(UTF_8)));

        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key("snapshot/in/dir/my-name-" + BUCKET_NAME)
                        .build(),
                RequestBody.fromBytes("hello world".getBytes(UTF_8)));

        s3Client.waiter().waitUntilObjectExists(HeadObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME)
                .build());
        s3Client.waiter().waitUntilObjectExists(HeadObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("snapshot/in/dir/my-name-" + BUCKET_NAME)
                .build());

        s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET_NAME).build())
                .contents()
                .forEach(object -> logger.info(object.key()));

        // 2

        final String content = s3Restorer.downloadNodeFile(Paths.get("manifests"), s -> s.contains("manifests/snapshot-name"));
        assertEquals("hello", content);

        // 3

        final String content2 = s3Restorer.downloadTopology(Paths.get("snapshot/in/dir"), s -> s.endsWith("my-name-" + BUCKET_NAME));
        assertEquals("hello world", content2);

        // 4

        s3Restorer.downloadFile(tmp.resolve("some-file"), s3Restorer.objectKeyToRemoteReference(Paths.get("snapshot/in/dir/my-name-" + BUCKET_NAME)));

        assertTrue(Files.exists(tmp.resolve("some-file")));
        assertEquals("hello world", new String(Files.readAllBytes(tmp.resolve("some-file"))));

        // backup

        s3Backuper.uploadText("hello world", s3Backuper.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")));
        String text = s3Restorer.downloadFileToString(s3Restorer.objectKeyToRemoteReference(Paths.get("topology/some-file-in-here.txt")), false);

        assertEquals("hello world", text);

        // backup upload file

        Path tempFile = Files.createTempFile("esop", ".txt");
        Files.write(tempFile, "hello world".getBytes(UTF_8));

        try (FileInputStream fis = new FileInputStream(tempFile.toFile())) {
            ManifestEntry manifestEntry = new ManifestEntry(tempFile,
                    tempFile,
                    ManifestEntry.Type.FILE,
                    Files.size(tempFile),
                    new KeyspaceTable("ks", "tb"),
                    "123",
                    null);
            s3Backuper.uploadEncryptedFile(manifestEntry, fis, new S3RemoteObjectReference(tempFile, tempFile.toString()));
        }

        s3Backuper.freshenRemoteObject(null, new S3RemoteObjectReference(tempFile, tempFile.toString() + "abc"));
    }
}
