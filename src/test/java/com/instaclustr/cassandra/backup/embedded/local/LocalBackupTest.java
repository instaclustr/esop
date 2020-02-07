package com.instaclustr.cassandra.backup.embedded.local;

import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import org.testng.annotations.Test;

public class LocalBackupTest extends AbstractBackupTest {

    final String[] backupArgs = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=file://" + target("backup1") + "/cluster/test-dc/1",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data"
    };

    final String[] backupArgsWithSnapshotName = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=file://" + target("backup1") + "/cluster/test-dc/1",
        "--snapshot-tag=stefansnapshot",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data"
    };

    final String[] restoreArgs = new String[]{
        "restore",
        "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
        "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
        "--snapshot-tag=stefansnapshot",
        "--storage-location=file://" + target("backup1") + "/cluster/test-dc/1",
        "--update-cassandra-yaml=true",
    };

    final String[] commitlogBackupArgs = new String[]{
        "commitlog-backup",
        "--storage-location=file://" + target("backup1") + "/cluster/test-dc/1",
        "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data"
    };

    final String[] commitlogRestoreArgs = new String[]{
        "commitlog-restore",
        "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
        "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
        "--storage-location=file://" + target("backup1") + "/cluster/test-dc/1",
        "--commitlog-download-dir=" + target("commitlog_download_dir"),
    };

    final String[][] localBackupRestore = new String[][]{
        backupArgs,
        backupArgsWithSnapshotName,
        commitlogBackupArgs,
        restoreArgs,
        commitlogRestoreArgs
    };


    @Test
    public void testBackupAndRestore() throws Exception {
        backupAndRestoreTest(localBackupRestore);
    }
}
