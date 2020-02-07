package com.instaclustr.cassandra.backup.embedded.s3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.aws.S3Module;
import com.instaclustr.cassandra.backup.aws.S3Module.TransferManagerFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;
import com.instaclustr.threading.ExecutorsModule;
import io.kubernetes.client.ApiException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
    "s3Test",
    "cloudTest",
})
public class S3BackupRestoreTest extends BaseS3BackupRestoreTest {

    final String[] backupArgs = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=s3://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data"
    };

    final String[] backupArgsWithSnapshotName = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=s3://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--snapshot-tag=stefansnapshot",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data"
    };

    final String[] restoreArgs = new String[]{
        "restore",
        "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
        "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
        "--snapshot-tag=stefansnapshot",
        "--storage-location=s3://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--update-cassandra-yaml=true",
    };

    final String[] commitlogBackupArgs = new String[]{
        "commitlog-backup",
        "--storage-location=s3://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data"
    };

    final String[] commitlogRestoreArgs = new String[]{
        "commitlog-restore",
        "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
        "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
        "--storage-location=s3://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--commitlog-download-dir=" + target("commitlog_download_dir"),
    };

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

    protected String[][] getProgramArguments() {
        return new String[][]{
            backupArgs,
            backupArgsWithSnapshotName,
            commitlogBackupArgs,
            restoreArgs,
            commitlogRestoreArgs
        };
    }

    @Test
    public void testBackupAndRestore() throws Exception {
        test();
    }
}
