package com.instaclustr.cassandra.backup.embedded.local;

import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.local.LocalFileRestorer;
import com.instaclustr.io.FileUtils;
import com.instaclustr.threading.Executors.FixedTasksExecutorSupplier;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

public class LocalBackupTest extends AbstractBackupTest {

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceBackupRestoreTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveBackupRestoreTest(importArguments());
    }

    @Test
    public void testHardlinksBackupAndRestore() throws Exception {
        liveBackupRestoreTest(hardlinkingArguments());
    }

    @Test
    @Ignore
    public void testDownloadOfRemoteManifest() throws Exception {
        try {
            RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();

            FileUtils.createDirectory(Paths.get(target("backup1") + "/cluster/test-dc/1/manifests").toAbsolutePath());

            restoreOperationRequest.storageLocation = new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/1");

            Files.write(Paths.get("target/backup1/cluster/test-dc/1/manifests/snapshot-name-" + UUID.randomUUID().toString()).toAbsolutePath(),
                        "hello".getBytes(),
                        StandardOpenOption.CREATE_NEW
            );

            LocalFileRestorer localFileRestorer = new LocalFileRestorer(new FixedTasksExecutorSupplier(), restoreOperationRequest);

            final Path downloadedFile = localFileRestorer.downloadFileToDir(Paths.get("/tmp"), Paths.get("manifests"), s -> s.startsWith("snapshot-name-"));

            assertTrue(Files.exists(downloadedFile));
        } finally {
            deleteDirectory(Paths.get(target("backup1")));
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    @Override
    protected String getStorageLocation() {
        return "file://" + target("backup1") + "/cluster/datacenter1/node1";
    }
}
