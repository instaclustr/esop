package com.instaclustr.cassandra.backup.embedded.gcp;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.gcp.GCPModule;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;
import com.instaclustr.threading.ExecutorsModule;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
    "googleTest",
    "cloudTest",
})
public class GoogleStorageBackupRestoreTest extends BaseGoogleStorageBackupRestoreTest {

    final String[] backupArgs = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=gcp://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
        "--entities=system_schema,test,test2" // keyspaces
    };

    final String[] backupArgsWithSnapshotName = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=gcp://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--snapshot-tag=stefansnapshot",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
        "--entities=system_schema,test,test2" // keyspaces
    };

    final String[] restoreArgs = new String[]{
        "restore",
        "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
        "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
        "--snapshot-tag=stefansnapshot",
        "--storage-location=gcp://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--update-cassandra-yaml=true",
        "--entities=system_schema,test,test2"
    };

    final String[] commitlogBackupArgs = new String[]{
        "commitlog-backup",
        "--storage-location=gcp://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data"
    };

    final String[] commitlogRestoreArgs = new String[]{
        "commitlog-restore",
        "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
        "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
        "--storage-location=gcp://" + BUCKET_NAME + "/cluster/test-dc/1",
        "--commitlog-download-dir=" + target("commitlog_download_dir"),
    };

    @Inject
    public GoogleStorageFactory googleStorageFactory;

    @BeforeMethod
    public void setup() throws Exception {

        final List<Module> modules = new ArrayList<Module>() {{
            add(new KubernetesApiModule());
            add(new GCPModule());
            add(new ExecutorsModule());
        }};

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
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

    @Override
    public GoogleStorageFactory getGoogleStorageFactory() {
        return googleStorageFactory;
    }

    @Test
    public void testBackupAndRestore() throws Exception {
        test();
    }
}
