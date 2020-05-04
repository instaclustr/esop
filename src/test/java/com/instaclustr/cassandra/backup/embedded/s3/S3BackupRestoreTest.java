package com.instaclustr.cassandra.backup.embedded.s3;

import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.aws.S3BucketService;
import com.instaclustr.cassandra.backup.aws.S3Module;
import com.instaclustr.cassandra.backup.aws.S3Module.TransferManagerFactory;
import com.instaclustr.cassandra.backup.aws.S3Restorer;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;
import com.instaclustr.threading.Executors.FixedTasksExecutorSupplier;
import com.instaclustr.threading.ExecutorsModule;
import io.kubernetes.client.ApiException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Test(groups = {
    "s3Test",
    "cloudTest",
})
public class S3BackupRestoreTest extends BaseS3BackupRestoreTest {

    @Inject
    public TransferManagerFactory transferManagerFactory;

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
    public TransferManagerFactory getTransferManagerFactory() {
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
    @Ignore
    public void testDownloadOfRemoteManifest() throws Exception {
        S3BucketService s3BucketService = new S3BucketService(getTransferManagerFactory(), getBackupOperationRequest());

        try {
            s3BucketService.create(BUCKET_NAME);

            AmazonS3 amazonS3Client = getTransferManagerFactory().build(getBackupOperationRequest()).getAmazonS3Client();

            amazonS3Client.putObject(BUCKET_NAME, "cluster/dc/node/manifests/snapshot-name-" + BUCKET_NAME, "hello");

            Thread.sleep(5000);

            ObjectListing objectListing = amazonS3Client.listObjects(BUCKET_NAME);

            objectListing.getObjectSummaries().forEach(summary -> System.out.println(summary.getKey()));

            RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();
            restoreOperationRequest.storageLocation = new StorageLocation("s3://" + BUCKET_NAME + "/cluster/dc/node");

            S3Restorer s3Restorer = new S3Restorer(getTransferManagerFactory(), new FixedTasksExecutorSupplier(), restoreOperationRequest);

            final Path downloadedFile = s3Restorer.downloadFileToDir(Paths.get("/tmp"), Paths.get("manifests"), new Predicate<String>() {
                @Override
                public boolean test(final String s) {
                    return s.contains("manifests/snapshot-name");
                }
            });

            assertTrue(Files.exists(downloadedFile));
        } finally {
            s3BucketService.delete(BUCKET_NAME);
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }
}
