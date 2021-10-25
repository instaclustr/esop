package com.instaclustr.esop.backup.embedded;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIMEUUID;
import static com.datastax.oss.driver.api.core.uuid.Uuids.timeBased;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.google.common.collect.ImmutableMap.of;
import static com.instaclustr.esop.backup.embedded.TestEntity.DATE;
import static com.instaclustr.esop.backup.embedded.TestEntity.ID;
import static com.instaclustr.esop.backup.embedded.TestEntity.KEYSPACE;
import static com.instaclustr.esop.backup.embedded.TestEntity.NAME;
import static com.instaclustr.esop.backup.embedded.TestEntity.TABLE;
import static com.instaclustr.esop.backup.embedded.TestEntity2.KEYSPACE_2;
import static com.instaclustr.esop.backup.embedded.TestEntity2.TABLE_2;
import static com.instaclustr.esop.backup.embedded.TestEntity3.KEYSPACE_3;
import static com.instaclustr.esop.backup.embedded.TestEntity3.TABLE_3;
import static com.instaclustr.io.FileUtils.deleteDirectory;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Version;
import com.github.nosan.embedded.cassandra.WorkingDirectoryCustomizer;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.instaclustr.cassandra.CassandraModule;
import com.instaclustr.esop.cli.Esop;
import com.instaclustr.esop.impl.backup.BackupModules.BackupModule;
import com.instaclustr.esop.impl.backup.BackupModules.UploadingModule;
import com.instaclustr.esop.impl.hash.HashModule;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.esop.impl.list.ListModule;
import com.instaclustr.esop.impl.remove.RemoveBackupModule;
import com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType;
import com.instaclustr.esop.impl.restore.RestoreModules.DownloadingModule;
import com.instaclustr.esop.impl.restore.RestoreModules.RestorationStrategyModule;
import com.instaclustr.esop.impl.restore.RestoreModules.RestoreModule;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.OperationsModule;
import com.instaclustr.threading.ExecutorsModule;
import io.kubernetes.client.ApiException;
import jmx.org.apache.cassandra.CassandraJMXConnectionInfo;
import jmx.org.apache.cassandra.service.CassandraJMXServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBackupTest {

    public List<AbstractModule> defaultModules = new ArrayList<AbstractModule>() {{
        add(new ExecutorsModule());
        add(new BackupModule());
        add(new RestoreModule());
        add(new UploadingModule());
        add(new DownloadingModule());
        add(new RestorationStrategyModule());
        add(new ListModule());
        add(new RemoveBackupModule());
        add(new HashModule(new HashSpec()));

        try {
            add(new CassandraModule());
        } catch (final Exception ignored) {

        }
        add(new OperationsModule());
    }};

    protected static final Logger logger = LoggerFactory.getLogger(AbstractBackupTest.class);

    public static final String CASSANDRA_VERSION = System.getProperty("cassandra3.version", "3.11.11");

    public static final String CASSANDRA_4_VERSION = System.getProperty("cassandra4.version", "4.0-rc2");

    // This is number of rows we inserted into Cassandra DB in total
    // we backed up first 6 rows. For the last two rows, they are stored in commit logs.
    // The backup procedure backs up also commit logs including remaining 2 rows
    // so commit log restoration procedure restores them too
    protected static int NUMBER_OF_INSERTED_ROWS = 8;

    // This is number of rows we expect to see in database after commitlog restoration
    // We omitted one row here, on purpose, to demonstrate point in time restoration
    protected static int NUMBER_OF_ROWS_AFTER_RESTORATION = 7;

    protected final Path target = new File("target").toPath().toAbsolutePath();
    protected final Path cassandraDir = new File("target/cassandra").toPath().toAbsolutePath();
    protected final Path cassandraDataDir = new File("target/cassandra/data/data").toPath().toAbsolutePath();
    protected final Path cassandraRestoredDir = new File("target/cassandra-restored").toPath().toAbsolutePath();
    protected final Path cassandraRestoredConfigDir = new File("target/cassandra-restored/conf").toPath().toAbsolutePath();

    ////////// ARGUMENTS

    protected static final String BUCKET_NAME = UUID.randomUUID().toString();

    protected static final String SIDECAR_SECRET_NAME = "test-sidecar-secret";

    private String systemKeyspace(final String cassandraVersion) {
        if (cassandraVersion.startsWith("2")) {
            return "system";
        } else {
            return "system_schema";
        }
    }

    protected String[][] inPlaceArguments(final String cassandraVersion) {

        final String snapshotName = UUID.randomUUID().toString();

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket",
            "--skip-refreshing"
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName,
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket",
            "--skip-refreshing"
        };

        // one more backup to have there manifests with same snapshot name so the latest wins
        final String[] backupArgsWithSnapshotName2 = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName,
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket",
            "--skip-refreshing"
        };

        // COMMIT LOGS BACKUP

        final String[] commitlogBackupArgs = new String[]{
            "commitlog-backup",
            "--storage-location=" + getStorageLocation(),
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            //"--online"
        };

        // RESTORE

        final String[] restoreArgs = new String[]{
            "restore",
            "--data-directory=" + cassandraRestoredDir.toAbsolutePath() + "/data",
            "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath(),
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--restore-system-keyspace",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2",
            // this will import systema_schema, normally, it wont happen without setting --restore-system-keyspace
            // that would import all of them which is not always what we really want as other system tables
            // would be regenerated, only schema should be as it was.
            "--restore-into-new-cluster",
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--restoration-strategy-type=IN_PLACE"
        };

        // COMMIT LOG RESTORE

        final String[] commitlogRestoreArgs = new String[]{
            "commitlog-restore",
            "--data-directory=" + cassandraRestoredDir.toAbsolutePath() + "/data",
            "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath(),
            "--storage-location=" + getStorageLocation(),
            "--commitlog-download-dir=" + target("commitlog_download_dir"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        return new String[][]{
            backupArgs,
            backupArgsWithSnapshotName,
            backupArgsWithSnapshotName2,
            commitlogBackupArgs,
            restoreArgs,
            commitlogRestoreArgs
        };
    }

    protected String[][] importArguments(final String cassandraVersion) {

        final String snapshotName1 = "snapshot1";
        final String snapshotName2 = "snapshot2";

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName1,
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket"
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName2,
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket"
        };

        // RESTORE

        final String[] restoreArgsPhase1 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=download", /// DOWNLOAD
            //"--import-source-dir=" + target("downloaded"),
            "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase2 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase3 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=import", // IMPORT
            "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase4 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=cleanup", // CLEANUP
            "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        return new String[][]{
            backupArgs,
            backupArgsWithSnapshotName,
            restoreArgsPhase1,
            restoreArgsPhase2,
            restoreArgsPhase3,
            restoreArgsPhase4,
        };
    }

    protected String[][] importArgumentsRenamedTable(final String cassandraVersion, final RestorationStrategyType strategyType, final boolean crossKeyspaceRename) {

        final String snapshotName1 = "snapshot1";
        final String snapshotName2 = "snapshot2";

        String rename;

        if (crossKeyspaceRename) {
            rename = "--rename=test2.test2=test.test";
        } else {
            rename = "--rename=test2.test2=test2.test3";
        }

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName1,
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket"
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName2,
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket"
        };

        // RESTORE

        final String[] restoreArgsPhase1 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test2.test2",
            "--restoration-strategy-type=" + strategyType.toValue().toLowerCase(),
            "--restoration-phase-type=download", /// DOWNLOAD
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            //// !!! Renaming for test2 to test3, test3 will be same as test2 and test2 will not be touched
            rename
        };

        final String[] restoreArgsPhase2 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test2.test2",
            "--restoration-strategy-type=" + strategyType.toValue().toLowerCase(),
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            //// !!! Renaming for test2 to test3, test3 will be same as test2 and test2 will not be touched
            rename
        };

        final String[] restoreArgsPhase3 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test2.test2",
            "--restoration-strategy-type=" + strategyType.toValue().toLowerCase(),
            "--restoration-phase-type=import",
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            //// !!! Renaming for test2 to test3, test3 will be same as test2 and test2 will not be touched
            rename
        };

        final String[] restoreArgsPhase4 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test2.test2",
            "--restoration-strategy-type=" + strategyType.toValue().toLowerCase(),
            "--restoration-phase-type=cleanup", // CLEANUP
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            //// !!! Renaming for test2 to test3, test3 will be same as test2 and test2 will not be touched
            rename
        };

        return new String[][]{
            backupArgs,
            backupArgsWithSnapshotName,
            restoreArgsPhase1,
            restoreArgsPhase2,
            restoreArgsPhase3,
            restoreArgsPhase4,
        };
    }

    protected String[][] hardlinkingArguments(final String cassandraVersion) {

        final String snapshotName = UUID.randomUUID().toString();

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket"
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName,
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket"
        };

        final String[] backupArgs2WithSnapshotName = new String[]{
                "backup",
                "--jmx-service", "127.0.0.1:7199",
                "--storage-location=" + getStorageLocationForAnotherCluster(),
                "--snapshot-tag=" + snapshotName,
                "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
                "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
                "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
                "--create-missing-bucket"
        };

        // RESTORE

        final String[] downloadPhase = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=download", /// DOWNLOAD
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] truncatePhase = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] importPhase = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=import", // IMPORT
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] cleanupPhase = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=cleanup", // CLEANUP
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        return new String[][]{
            backupArgs,
            backupArgsWithSnapshotName,
            downloadPhase,
            truncatePhase,
            importPhase,
            cleanupPhase,
            backupArgs2WithSnapshotName
        };
    }

    protected String[][] restoreByImportingIntoDifferentSchemaArguments(final String cassandraVersion,
                                                                        final RestorationStrategyType type) {

        String snapshotName1 = "snapshot1";
        String snapshotName2 = "snapshot2";

        // BACKUP 1

        // test,test2
        final String[] backupArgs1 = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName1,
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket"
        };

        // adding third table into backup, test3 in addition to test and test2
        final String[] backupArgs2 = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName2,
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2,test3", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
            "--create-missing-bucket"
        };

        // RESTORE

        // specifying system_schema in entities does nothing as restore logic will filter out system keyspaces in import and hardlink strategy
        // it is here just for testing purposes

        final String[] restoreArgsPhase1 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName1, // !!! important, we are restoring into the FIRST snapshot
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // here we want to restore only to test and test2
            "--restoration-strategy-type=" + type.toString(),
            "--restoration-phase-type=download", /// DOWNLOAD
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase2 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName1, // !!! important, we are restoring into the FIRST snapshot
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // here we want to restore only to test and test2 which were not altered (test3 was)
            "--restoration-strategy-type=" + type,
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase3 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName1, // !!! important, we are restoring into the FIRST snapshot
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // here we want to restore only to test and test2
            "--restoration-strategy-type=" + type,
            "--restoration-phase-type=import", // IMPORT
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase4 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath() + "/data",
            "--snapshot-tag=" + snapshotName1, // !!! important, we are restoring into the FIRST snapshot
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(cassandraVersion) + ",test,test2", // here we want to restore only to test and test2
            "--restoration-strategy-type=" + type,
            "--restoration-phase-type=cleanup", // CLEANUP
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        return new String[][]{
            backupArgs1,
            backupArgs2,
            restoreArgsPhase1,
            restoreArgsPhase2,
            restoreArgsPhase3,
            restoreArgsPhase4
        };
    }

    protected String getStorageLocation() {
        return protocol() + BUCKET_NAME + "/cluster/datacenter1/node1";
    }

    protected String getStorageLocationForAnotherCluster() {
        return protocol() + BUCKET_NAME + "/cluster2/datacenter1/node1";
    }

    protected abstract String protocol();

    public void inPlaceBackupRestoreTest(final String[][] arguments) throws Exception {

        Cassandra cassandra = null;

        try {
            cassandra = getCassandra(cassandraDir, CASSANDRA_VERSION);
            cassandra.start();

            List<Long> insertionTimes;

            try (CqlSession session = CqlSession.builder().build()) {
                insertionTimes = populateDatabaseWithBackup(session, arguments);
                assertEquals(insertionTimes.size(), NUMBER_OF_INSERTED_ROWS);
            }

            logger.info("Executing backup of commit logs {}", asList(arguments[3]));
            Esop.mainWithoutExit(arguments[3]);

            cassandra.stop();

            // RESTORE VERIFICATION
            cassandra = getCassandra(cassandraRestoredDir, CASSANDRA_VERSION, (workingDirectory, version) -> {
                Files.createDirectory(workingDirectory.resolve("data"));
                restoreOnStoppedNode(insertionTimes, arguments);
            });

            cassandra.start();

            waitForCql();

            try (CqlSession session = CqlSession.builder().build()) {
                dumpTable(session, KEYSPACE, TABLE, NUMBER_OF_ROWS_AFTER_RESTORATION);
            }
        } finally {
            if (cassandra != null) {
                cassandra.stop();
            }
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
            FileUtils.deleteDirectory(cassandraDir);
            FileUtils.deleteDirectory(cassandraRestoredDir);
        }
    }

    public void liveBackupRestoreTestRenamedEntities(final String[][] arguments,
                                                     final String cassandraVersion,
                                                     int rounds,
                                                     boolean crossKeyspaceRestore) throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, cassandraVersion);
        cassandra.start();

        waitForCql();

        try (CqlSession session = CqlSession.builder().build()) {

            createTable(session, KEYSPACE, TABLE);
            createTable(session, KEYSPACE_2, TABLE_2);
            createTable(session, KEYSPACE_2, TABLE_3);

            insertAndCallBackupCLI(2, session, arguments[0]); // stefansnapshot-1
            insertAndCallBackupCLI(2, session, arguments[1]); // stefansnapshot-2

            // first round

            for (int i = 1; i < rounds + 1; ++i) {

                // each phase is executed twice here to check that phases are idempotent / repeatable

                logger.info("Round " + i + " - Executing the first restoration phase - download {}", asList(arguments[2]));
                Esop.mainWithoutExit(arguments[2]);

                logger.info("Round " + i + " - Executing the first restoration phase - download {}", asList(arguments[2]));
                Esop.mainWithoutExit(arguments[2]);

                logger.info("Round " + i + " - Executing the second restoration phase - truncate {}", asList(arguments[3]));
                Esop.mainWithoutExit(arguments[3]);

                logger.info("Round " + i + " - Executing the second restoration phase - truncate {}", asList(arguments[3]));
                Esop.mainWithoutExit(arguments[3]);

                //
                // this will import backup-ed table2 into table3
                //
                logger.info("Round " + i + " - Executing the third restoration phase - import {}", asList(arguments[4]));
                Esop.mainWithoutExit(arguments[4]);

                if (!cassandraVersion.startsWith("4")) {
                    // second round would not pass for 4 because import deletes files in download
                    logger.info("Round " + i + " - Executing the third restoration phase for the second time - import {}", asList(arguments[4]));
                    Esop.mainWithoutExit(arguments[4]);
                }

                logger.info("Round " + i + " - Executing the fourth restoration phase - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);

                logger.info("Round " + i + " - Executing the fourth restoration phase - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);

                // we expect 4 records to be there as 2 were there before the first backup and the second 2 before the second backup
                dumpTable(session, KEYSPACE, TABLE, 4);
                dumpTable(session, KEYSPACE_2, TABLE_2, 4);
                // here we expect that table3 is same as table2
                if (!crossKeyspaceRestore) {
                    dumpTable(session, KEYSPACE_2, TABLE_3, 4);
                }
            }
        } finally {
            cassandra.stop();
            FileUtils.deleteDirectory(cassandraDir);
            deleteDirectory(Paths.get(target("backup1")));
        }
    }

    public void liveBackupRestoreTest(final String[][] arguments, final String cassandraVersion, int rounds) throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, cassandraVersion);
        cassandra.start();

        waitForCql();

        try (CqlSession session = CqlSession.builder().build()) {

            createTable(session, KEYSPACE, TABLE);
            createTable(session, KEYSPACE_2, TABLE_2);

            insertAndCallBackupCLI(2, session, arguments[0]); // stefansnapshot-1
            insertAndCallBackupCLI(2, session, arguments[1]); // stefansnapshot-2

            // first round

            for (int i = 1; i < rounds + 1; ++i) {

                // each phase is executed twice here to check that phases are idempotent / repeatable

                logger.info("Round " + i + " - Executing the first restoration phase - download {}", asList(arguments[2]));
                Esop.mainWithoutExit(arguments[2]);
                logger.info("Round " + i + " - Executing the first restoration phase for the second time - download {}", asList(arguments[2]));
                Esop.mainWithoutExit(arguments[2]);

                logger.info("Round " + i + " - Executing the second restoration phase - truncate {}", asList(arguments[3]));
                Esop.mainWithoutExit(arguments[3]);
                logger.info("Round " + i + " - Executing the second restoration phase for the second time - truncate {}", asList(arguments[3]));
                Esop.mainWithoutExit(arguments[3]);

                logger.info("Round " + i + " - Executing the third restoration phase - import {}", asList(arguments[4]));

                Esop.mainWithoutExit(arguments[4]);

                if (!cassandraVersion.startsWith("4")) {
                    // second round would not pass for 4 because import deletes files in download
                    logger.info("Round " + i + " - Executing the third restoration phase for the second time - import {}", asList(arguments[4]));
                    Esop.mainWithoutExit(arguments[4]);
                }

                logger.info("Round " + i + " - Executing the fourth restoration phase - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);
                logger.info("Round " + i + " - Executing the fourth restoration phase for the second time - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);

                // we expect 4 records to be there as 2 were there before the first backup and the second 2 before the second backup
                dumpTable(session, KEYSPACE, TABLE, 4);
                dumpTable(session, KEYSPACE_2, TABLE_2, 4);
            }
        } finally {
            cassandra.stop();
            FileUtils.deleteDirectory(cassandraDir);
            deleteDirectory(Paths.get(target("backup1")));
        }
    }

    public void liveBackupRestoreTest(final String[][] arguments, final String cassandraVersion) throws Exception {
        liveBackupRestoreTest(arguments, cassandraVersion, 1);
    }

    private void waitUntilSchemaChanged(final String firstSchemaVersion) {
        await().pollInterval(3, SECONDS).timeout(1, MINUTES).until(() -> {
            String secondSchemaVersion = new CassandraSchemaVersion(new CassandraJMXServiceImpl(new CassandraJMXConnectionInfo())).act();
            // wait until schemas are NOT same
            return !firstSchemaVersion.equals(secondSchemaVersion);
        });
    }

    public void liveBackupWithRestoreOnDifferentTableSchema(final String[][] arguments,
                                                            final String cassandraVersion,
                                                            final boolean tableAddition) throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, cassandraVersion);
        cassandra.start();

        waitForCql();

        try (CqlSession session = CqlSession.builder().build()) {
            createTable(session, KEYSPACE, TABLE);
            createTable(session, KEYSPACE_2, TABLE_2);

            // after this, table and table2 will contain 2 rows each
            insert(2, session, new ArrayList<String[]>() {{
                add(new String[]{KEYSPACE, TABLE});
                add(new String[]{KEYSPACE_2, TABLE_2});
            }});

            // first backup
            Esop.mainWithoutExit(arguments[0]);

            String firstSchemaVersion = new CassandraSchemaVersion(new CassandraJMXServiceImpl(new CassandraJMXConnectionInfo())).act();

            // create third schema, by this way, Cassandra schema will change
            createTable(session, KEYSPACE_3, TABLE_3);
            waitUntilSchemaChanged(firstSchemaVersion);

            // after this, table and table2 will contain 4 rows each
            // and table3 just 2
            insert(2, session, new ArrayList<String[]>() {{
                add(new String[]{KEYSPACE, TABLE});
                add(new String[]{KEYSPACE_2, TABLE_2});
                add(new String[]{KEYSPACE_3, TABLE_3});
            }});

            // second backup
            Esop.mainWithoutExit(arguments[1]);

            if (tableAddition) {
                addColumnToTable(session, KEYSPACE, TABLE, "newColumn", TEXT);
            } else {
                removeColumnFromTable(session, KEYSPACE, TABLE, TestEntity.NAME);
            }

            // restore into the first snapshot where table1 was without newly added column
            // we effectively restored SSTables on different schema so we expect that values in the new column will be "null"

            // download
            Esop.mainWithoutExit(arguments[2]);

            // truncate
            Esop.mainWithoutExit(arguments[3]);

            // after truncating, we see that we have truncated just two tables which were
            // in snapshot from snapshot1, not the third one
            dumpTable(session, KEYSPACE, TABLE, 0);
            dumpTable(session, KEYSPACE_2, TABLE_2, 0);
            dumpTable(session, KEYSPACE_3, TABLE_3, 2);

            // import
            Esop.mainWithoutExit(arguments[4]);

            // cleanup
            Esop.mainWithoutExit(arguments[5]);

            // verify

            // here we check that table1 and table2 contains 2 rows each (as we restored it from the first snapshot) and table 3 will contain still 2

            dumpTable(session, KEYSPACE, TABLE, 2);
            dumpTable(session, KEYSPACE_2, TABLE_2, 2);
            dumpTable(session, KEYSPACE_3, TABLE_3, 2);
        } finally {
            cassandra.stop();
            FileUtils.deleteDirectory(cassandraDir);
            deleteDirectory(Paths.get(target("backup1")));
        }
    }

    public void liveBackupWithRestoreOnDifferentSchema(final String[][] arguments, final String cassandraVersion) throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, cassandraVersion);
        cassandra.start();

        waitForCql();

        try (CqlSession session = CqlSession.builder().build()) {
            createTable(session, KEYSPACE, TABLE);
            createTable(session, KEYSPACE_2, TABLE_2);

            // after this, table and table2 will contain 2 rows each
            insert(2, session, new ArrayList<String[]>() {{
                add(new String[]{KEYSPACE, TABLE});
                add(new String[]{KEYSPACE_2, TABLE_2});
            }});

            // first backup
            Esop.mainWithoutExit(arguments[0]);

            String firstSchemaVersion = new CassandraSchemaVersion(new CassandraJMXServiceImpl(new CassandraJMXConnectionInfo())).act();

            // create third schema, by this way, Cassandra schema will change
            createTable(session, KEYSPACE_3, TABLE_3);

            waitUntilSchemaChanged(firstSchemaVersion);

            // after this, table and table2 will contain 4 rows each
            // and table3 just 2
            insert(2, session, new ArrayList<String[]>() {{
                add(new String[]{KEYSPACE, TABLE});
                add(new String[]{KEYSPACE_2, TABLE_2});
                add(new String[]{KEYSPACE_3, TABLE_3});
            }});

            // second backup
            Esop.mainWithoutExit(arguments[1]);

            // here we want to restore into first snapshot even though schema has changed, because we added the third table
            // (keep in mind we have not changed schema of any table, we changed Cassandra schema as such)

            // restore into the first backup

            // download
            Esop.mainWithoutExit(arguments[2]);

            // truncate
            Esop.mainWithoutExit(arguments[3]);

            // after truncating, we see that we have truncated just two tables which were
            // in snapshot from snapshot1, not the third one
            dumpTable(session, KEYSPACE, TABLE, 0);
            dumpTable(session, KEYSPACE_2, TABLE_2, 0);
            dumpTable(session, KEYSPACE_3, TABLE_3, 2);

            // import
            Esop.mainWithoutExit(arguments[4]);

            // cleanup
            Esop.mainWithoutExit(arguments[5]);

            // verify

            // here we check that table1 and table2 contains 2 rows each (as we restored it from the first snapshot) and table 3 will contain still 2

            dumpTable(session, KEYSPACE, TABLE, 2);
            dumpTable(session, KEYSPACE_2, TABLE_2, 2);
            dumpTable(session, KEYSPACE_3, TABLE_3, 2);
        } finally {
            cassandra.stop();
            FileUtils.deleteDirectory(cassandraDir);
            deleteDirectory(Paths.get(target("backup1")));
        }
    }

    protected List<Long> populateDatabaseWithBackup(final CqlSession session, final String[][] arguments) {

        assert arguments != null;

        createTable(session, KEYSPACE, TABLE);
        createTable(session, KEYSPACE_2, TABLE_2);

        // this will invoke backup 2 times, each time generating 2 records and taking a snapshot and backup
        List<Long> times = range(0, 2)
                .mapToObj(i -> insertAndCallBackupCLI(2, session, arguments[0]))
                .flatMap(Collection::stream).collect(toList());

        // insert two more records, snapshot with predefined name and back it up too
        List<Long> times2 = insertAndCallBackupCLI(2, session, arguments[1]);

        // insert two more rows but do not back them up, this simulates that they are written in commit logs
        // so even we have not backed it up, once they are replayed, they will all be there
        List<Long> times3 = insert(2, session);

        return new ArrayList<Long>() {{
            addAll(times);
            addAll(times2);
            addAll(times3);
        }};
    }

    protected List<Long> populateDatabase(final CqlSession session) {
        createTable(session, KEYSPACE, TABLE);
        createTable(session, KEYSPACE_2, TABLE_2);

        // this will invoke backup 2 times, each time generating 2 records and taking a snapshot and backup
        List<Long> times = range(0, 2)
                .mapToObj(i -> insert(2, session))
                .flatMap(Collection::stream).collect(toList());

        List<Long> times2 = insert(2, session);

        // insert two more rows but do not back them up, this simulates that they are written in commit logs
        // so even we have not backed it up, once they are replayed, they will all be there
        List<Long> times3 = insert(2, session);

        return new ArrayList<Long>() {{
            addAll(times);
            addAll(times2);
            addAll(times3);
        }};
    }

    protected Cassandra getCassandra(final Path cassandraHome, final String version) throws Exception {
        return getCassandra(cassandraHome, version, null);
    }

    protected Cassandra getCassandra(final Path cassandraHome, final String version, final WorkingDirectoryCustomizer customizer) throws Exception {
        FileUtils.createDirectory(cassandraHome);

        CassandraBuilder builder = new CassandraBuilder();
        builder.version(Version.parse(version));
        builder.jvmOptions("-Dcassandra.ring_delay_ms=1000", "-Xms1g", "-Xmx1g");
        builder.workingDirectory(() -> cassandraHome);

        if (customizer != null) {
            builder.workingDirectoryCustomizers(customizer);
        }

        if (version.startsWith("2")) {
            FileUtils.createDirectory(cassandraHome.resolve("data").resolve("data"));
            builder.addConfigProperties(new HashMap<String, String[]>() {{
                put("data_file_directories", new String[]{
                        cassandraHome.resolve("data").resolve("data").toAbsolutePath().toString()
                });
            }});
        }

        return builder.build();
    }

    protected void init() throws ApiException, IOException {

    }

    protected void destroy() throws Exception {
        FileUtils.cleanDirectory(Paths.get(target(".esop")));
    }

    protected List<Long> insertAndCallBackupCLI(int records, CqlSession cqlSession, String[] backupArgs) {
        List<Long> executionTimes = insert(records, cqlSession);

        logger.info("Executing backup with arguments {}", asList(backupArgs));
        Esop.main(backupArgs, false);

        return executionTimes;
    }

    protected List<Long> insert(int records, CqlSession cqlSession, List<String[]> entities) {
        return range(0, records).mapToObj(i -> {
            for (String[] keyspaceTable : entities) {
                cqlSession.execute(insertInto(keyspaceTable[0], keyspaceTable[1])
                                       .value(ID, literal("1"))
                                       .value(DATE, literal(timeBased()))
                                       .value(NAME, literal("stefan1"))
                                       .build());
            }

            Uninterruptibles.sleepUninterruptibly(2, SECONDS);

            return System.currentTimeMillis();
        }).collect(toList());
    }

    protected List<Long> insert(int records, CqlSession cqlSession) {
        return insert(records, cqlSession, new ArrayList<String[]>() {{
            add(new String[]{KEYSPACE, TABLE});
            add(new String[]{KEYSPACE_2, TABLE_2});
        }});
    }

    protected void addColumnToTable(CqlSession session, String keyspace, String table, String column, DataType dataType) {
        session.execute(SchemaBuilder.alterTable(keyspace, table).addColumn(column, dataType).build());
    }

    protected void removeColumnFromTable(CqlSession session, String keyspace, String table, String column) {
        session.execute(SchemaBuilder.alterTable(keyspace, table).dropColumn(column).build());
    }

    protected void createTable(CqlSession session, String keyspace, String table) {
        session.execute(SchemaBuilder.createKeyspace(keyspace)
                            .ifNotExists()
                            .withNetworkTopologyStrategy(of("datacenter1", 1))
                            .build());

        Uninterruptibles.sleepUninterruptibly(2, SECONDS);

        session.execute(SchemaBuilder.createTable(keyspace, table)
                            .ifNotExists()
                            .withPartitionKey(ID, TEXT)
                            .withClusteringColumn(DATE, TIMEUUID)
                            .withColumn(NAME, TEXT)
                            .build());
    }

    protected String target(final String path) {
        return target.resolve(path).toAbsolutePath().toString();
    }

    protected void restoreOnStoppedNode(List<Long> insertionTimes, String[][] arguments) {

        // RESTORE

        logger.info("Executing restore on stopped node with arguments {}", asList(arguments[4]));

        Esop.mainWithoutExit(arguments[4]);

        // restore of commit logs on top of done restore

        List<String> commitlogRestoreArgsAsList = new ArrayList<>(asList(arguments[5]));

        // here we want to point-in-time restoration, so out of 8 records in total, (2x2 + 2 + 2) where last two
        // were not backed up, we take into account all first and second set of records (6 in total)
        // plus the first record from the last batch of 2 records which were not backed up but the idea is that they
        // are in commit logs so we simulate we restored records from the beginning until the end but the last record

        // from very early start
        commitlogRestoreArgsAsList.add("--timestamp-start=" + (insertionTimes.get(0) - 10000));

        // til the end but the last record
        commitlogRestoreArgsAsList.add("--timestamp-end=" + (insertionTimes.get(insertionTimes.size() - 2) + 1));

        logger.info("Executing restore of commit logs with arguments {}", asList(commitlogRestoreArgsAsList));

        Esop.main(commitlogRestoreArgsAsList.toArray(new String[0]), false);
    }

    protected void waitForCql() {
        await()
            .pollInterval(10, SECONDS)
            .pollInSameThread()
            .timeout(1, MINUTES)
            .until(() -> {
                try (final CqlSession ignored = CqlSession.builder().build()) {
                    return true;
                } catch (Exception ex) {
                    return false;
                }
            });
    }


    protected void dumpTable(final CqlSession session,
                             final String keyspace,
                             final String table,
                             int expectedLength) {
        List<Row> rows = session.execute(selectFrom(keyspace, table).all().build()).all();

        logger.info(format("Dumping %s.%s", keyspace, table));

        for (Row row : rows) {

            Date date = new Date(requireNonNull(row.getUuid(DATE)).timestamp() / 10000L - 12219292800000L);

            if (row.getColumnDefinitions().contains(NAME)) {
                logger.info(format("id: %s, date: %s, name: %s", row.getString(ID), date, row.getString(NAME)));
            } else {
                logger.info(format("id: %s, date: %s, name: not present in table", row.getString(ID), date));
            }
        }

        if (rows.isEmpty()) {
            logger.info(format("Table %s.%s is empty.", keyspace, table));
        }

        assertEquals(rows.size(), expectedLength);
    }
}
