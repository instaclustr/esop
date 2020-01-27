package com.instaclustr.cassandra.backup.embedded.s3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.aws.S3Module;
import com.instaclustr.cassandra.backup.aws.S3Module.TransferManagerFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;
import com.instaclustr.kubernetes.KubernetesService;
import com.instaclustr.threading.ExecutorsModule;
import io.kubernetes.client.ApiException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
    "s3Test",
    "k8sTest"
})
public class KubernetesS3BackupRestoreTest extends BaseS3BackupRestoreTest {

    private static final String AWS_S3_SIDECAR_SECRET_NAME = "test-amazon-s3-sidecar-secret";

    final String[] backupArgs = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=s3://" + BUCKET_NAME + "/test-dc/1",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
        "--k8s-backup-secret-name=" + AWS_S3_SIDECAR_SECRET_NAME
    };

    final String[] backupArgsWithSnapshotName = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=s3://" + BUCKET_NAME + "/test-dc/1",
        "--snapshot-tag=stefansnapshot",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
        "--k8s-backup-secret-name=" + AWS_S3_SIDECAR_SECRET_NAME
    };

    final String[] restoreArgs = new String[]{
        "restore",
        "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
        "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
        "--snapshot-tag=stefansnapshot",
        "--storage-location=s3://" + BUCKET_NAME + "/test-dc/1",
        "--update-cassandra-yaml=true",
        "--k8s-backup-secret-name=" + AWS_S3_SIDECAR_SECRET_NAME
    };

    final String[] commitlogBackupArgs = new String[]{
        "commitlog-backup",
        "--storage-location=s3://" + BUCKET_NAME + "/test-dc/1",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
        "--k8s-backup-secret-name=" + AWS_S3_SIDECAR_SECRET_NAME
    };

    final String[] commitlogRestoreArgs = new String[]{
        "commitlog-restore",
        "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
        "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
        "--storage-location=s3://" + BUCKET_NAME + "/test-dc/1",
        "--commitlog-download-dir=" + target("commitlog_download_dir"),
        "--k8s-backup-secret-name=" + AWS_S3_SIDECAR_SECRET_NAME
    };

    @Inject
    public KubernetesService kubernetesService;

    @Inject
    public TransferManagerFactory transferManagerFactory;

    @BeforeMethod
    public void setup() throws ApiException {

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
    protected void init() throws ApiException {
        System.setProperty("kubernetes.client", "true");

        kubernetesService.createSecret(AWS_S3_SIDECAR_SECRET_NAME, new HashMap<String, String>() {{
            put("awssecretaccesskey", System.getProperty("awssecretaccesskey"));
            put("awsaccesskeyid", System.getProperty("awsaccesskeyid"));
        }});
    }

    @Override
    protected void destroy() throws ApiException {
        kubernetesService.deleteSecret(AWS_S3_SIDECAR_SECRET_NAME);
        System.setProperty("kubernetes.client", "false");
    }

    @Override
    protected BackupOperationRequest getBackupOperationRequest() {

        final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();

        backupOperationRequest.k8sBackupSecretName = AWS_S3_SIDECAR_SECRET_NAME;

        return backupOperationRequest;
    }

    @Override
    protected String[][] getProgramArguments() {
        return new String[][]{
            backupArgs,
            backupArgsWithSnapshotName,
            commitlogBackupArgs,
            restoreArgs,
            commitlogRestoreArgs
        };
    }

    @Override
    public TransferManagerFactory getTransferManagerFactory() {
        return transferManagerFactory;
    }

    @Test
    public void testBackupAndRestore() throws Exception {
        test();
    }
}
