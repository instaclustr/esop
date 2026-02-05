package com.instaclustr.esop.backup.embedded;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
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
import jmx.org.apache.cassandra.CassandraJMXConnectionInfo;
import jmx.org.apache.cassandra.service.CassandraJMXServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractBackupTest {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractBackupTest.class);

    public String getCassandraVersion() {
        return CASSANDRA_5_VERSION;
    }

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

    public static final String CASSANDRA_5_VERSION = System.getProperty("cassandra5.version", "5.0.6");

    public static final String CASSANDRA_4_VERSION = System.getProperty("cassandra4.version", "4.1.10");

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

    private String systemKeyspace(final String cassandraVersion) {
        if (cassandraVersion.startsWith("2")) {
            return "system";
        } else {
            return "system_schema";
        }
    }

    protected String[][] inPlaceArguments() {

        final String snapshotName = UUID.randomUUID().toString();

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket"
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName,
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket"
        };

        // one more backup to have there manifests with same snapshot name so the latest wins
        final String[] backupArgsWithSnapshotName2 = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName,
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket",
        };

        // COMMIT LOGS BACKUP

        final String[] commitlogBackupArgs = new String[]{
            "commitlog-backup",
            "--storage-location=" + getStorageLocation(),
            "--commit-log-dir=" + cassandraDir.toAbsolutePath() + "/data/commitlog",
            //"--online"
        };

        // RESTORE

        final String[] restoreArgs = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraRestoredDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraRestoredDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraRestoredDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraRestoredDir.toAbsolutePath() + "/data/data3",
            "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath(),
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--restore-system-keyspace",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2",
            // this will import systema_schema, normally, it wont happen without setting --restore-system-keyspace
            // that would import all of them which is not always what we really want as other system tables
            // would be regenerated, only schema should be as it was.
            "--restore-into-new-cluster",
            "--restoration-strategy-type=IN_PLACE"
        };

        // COMMIT LOG RESTORE

        final String[] commitlogRestoreArgs = new String[]{
            "commitlog-restore",
            "--commit-log-dir=" + cassandraRestoredDir.toAbsolutePath() + "/data/commitlog",
            "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath(),
            "--storage-location=" + getStorageLocation(),
            "--commitlog-download-dir=" + target("commitlog_download_dir"),
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

    protected String[][] importArguments() {

        final String snapshotName1 = "snapshot1";
        final String snapshotName2 = "snapshot2";

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName1,
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket"
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName2,
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket"
        };

        // RESTORE

        final String[] restoreArgsPhase1 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=download", /// DOWNLOAD
            //"--import-source-dir=" + target("downloaded"),
            "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
        };

        final String[] restoreArgsPhase2 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
        };

        final String[] restoreArgsPhase3 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=import", // IMPORT
            "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
        };

        final String[] restoreArgsPhase4 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=cleanup", // CLEANUP
            "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
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

    protected String[][] importArgumentsWhenEmptyKeyspace() {

        final String snapshotName1 = "snapshot1";
        final String snapshotName2 = "snapshot2";

        // BACKUP

        final String[] backupArgs = new String[]{
                "backup",
                "--jmx-service", "127.0.0.1:7199",
                "--storage-location=" + getStorageLocation(),
                "--snapshot-tag=" + snapshotName1,
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data2",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data3",
                "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2,some_ks", // keyspaces
                "--create-missing-bucket"
        };

        final String[] backupArgsWithSnapshotName = new String[]{
                "backup",
                "--jmx-service", "127.0.0.1:7199",
                "--storage-location=" + getStorageLocation(),
                "--snapshot-tag=" + snapshotName2,
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data2",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data3",
                "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2,some_ks", // keyspaces
                "--create-missing-bucket"
        };

        // RESTORE

        final String[] restoreArgsPhase1 = new String[]{
                "restore",
                "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data2",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data3",
                "--snapshot-tag=" + snapshotName2,
                "--storage-location=" + getStorageLocation(),
                "--update-cassandra-yaml=true",
                "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2,some_ks",
                "--restoration-strategy-type=import",
                "--restoration-phase-type=download", /// DOWNLOAD
                //"--import-source-dir=" + target("downloaded"),
                "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
        };

        final String[] restoreArgsPhase2 = new String[]{
                "restore",
                "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data2",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data3",
                "--snapshot-tag=" + snapshotName2,
                "--storage-location=" + getStorageLocation(),
                "--update-cassandra-yaml=true",
                "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2,some_ks",
                "--restoration-strategy-type=import",
                "--restoration-phase-type=truncate", // TRUNCATE
                "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
        };

        final String[] restoreArgsPhase3 = new String[]{
                "restore",
                "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data2",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data3",
                "--snapshot-tag=" + snapshotName2,
                "--storage-location=" + getStorageLocation(),
                "--update-cassandra-yaml=true",
                "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2,some_ks",
                "--restoration-strategy-type=import",
                "--restoration-phase-type=import", // IMPORT
                "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
        };

        final String[] restoreArgsPhase4 = new String[]{
                "restore",
                "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data2",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data3",
                "--snapshot-tag=" + snapshotName2,
                "--storage-location=" + getStorageLocation(),
                "--update-cassandra-yaml=true",
                "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2,some_ks",
                "--restoration-strategy-type=import",
                "--restoration-phase-type=cleanup", // CLEANUP
                "--import-source-dir=" + cassandraDir.toAbsolutePath() + "/data/downloads",
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

    protected String[][] importArgumentsRenamedTable(final RestorationStrategyType strategyType, final boolean crossKeyspaceRename) {

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
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket"
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName2,
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket"
        };

        // RESTORE

        final String[] restoreArgsPhase1 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test2.test2",
            "--restoration-strategy-type=" + strategyType.toValue().toLowerCase(),
            "--restoration-phase-type=download", /// DOWNLOAD
            "--import-source-dir=" + target("downloaded"),
            //// !!! Renaming for test2 to test3, test3 will be same as test2 and test2 will not be touched
            rename
        };

        final String[] restoreArgsPhase2 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test2.test2",
            "--restoration-strategy-type=" + strategyType.toValue().toLowerCase(),
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + target("downloaded"),
            //// !!! Renaming for test2 to test3, test3 will be same as test2 and test2 will not be touched
            rename
        };

        final String[] restoreArgsPhase3 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test2.test2",
            "--restoration-strategy-type=" + strategyType.toValue().toLowerCase(),
            "--restoration-phase-type=import",
            "--import-source-dir=" + target("downloaded"),
            //// !!! Renaming for test2 to test3, test3 will be same as test2 and test2 will not be touched
            rename
        };

        final String[] restoreArgsPhase4 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName2,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test2.test2",
            "--restoration-strategy-type=" + strategyType.toValue().toLowerCase(),
            "--restoration-phase-type=cleanup", // CLEANUP
            "--import-source-dir=" + target("downloaded"),
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

    protected String[][] hardlinkingArguments() {

        final String snapshotName = UUID.randomUUID().toString();

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket"
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName,
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket"
        };

        final String[] backupArgs2WithSnapshotName = new String[]{
                "backup",
                "--jmx-service", "127.0.0.1:7199",
                "--storage-location=" + getStorageLocationForAnotherCluster(),
                "--snapshot-tag=" + snapshotName,
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data2",
                "--data-dir",
                cassandraDir.toAbsolutePath() + "/data/data3",
                "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
                "--create-missing-bucket"
        };

        // RESTORE

        final String[] downloadPhase = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=download", /// DOWNLOAD
            "--import-source-dir=" + target("downloaded"),
        };

        final String[] truncatePhase = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + target("downloaded"),
        };

        final String[] importPhase = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=import", // IMPORT
            "--import-source-dir=" + target("downloaded"),
        };

        final String[] cleanupPhase = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=cleanup", // CLEANUP
            "--import-source-dir=" + target("downloaded"),
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

    protected String[][] restoreByImportingIntoDifferentSchemaArguments(final RestorationStrategyType type) {

        String snapshotName1 = "snapshot1";
        String snapshotName2 = "snapshot2";

        // BACKUP 1

        // test,test2
        final String[] backupArgs1 = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName1,
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // keyspaces
            "--create-missing-bucket"
        };

        // adding third table into backup, test3 in addition to test and test2
        final String[] backupArgs2 = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName2,
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2,test3", // keyspaces
            "--create-missing-bucket"
        };

        // RESTORE

        // specifying system_schema in entities does nothing as restore logic will filter out system keyspaces in import and hardlink strategy
        // it is here just for testing purposes

        final String[] restoreArgsPhase1 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName1, // !!! important, we are restoring into the FIRST snapshot
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // here we want to restore only to test and test2
            "--restoration-strategy-type=" + type.toString(),
            "--restoration-phase-type=download", /// DOWNLOAD
            "--import-source-dir=" + target("downloaded"),
        };

        final String[] restoreArgsPhase2 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName1, // !!! important, we are restoring into the FIRST snapshot
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // here we want to restore only to test and test2 which were not altered (test3 was)
            "--restoration-strategy-type=" + type,
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + target("downloaded"),
        };

        final String[] restoreArgsPhase3 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName1, // !!! important, we are restoring into the FIRST snapshot
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // here we want to restore only to test and test2
            "--restoration-strategy-type=" + type,
            "--restoration-phase-type=import", // IMPORT
            "--import-source-dir=" + target("downloaded"),
        };

        final String[] restoreArgsPhase4 = new String[]{
            "restore",
            "--cassandra-dir=" + cassandraDir.toAbsolutePath() + "/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data2",
            "--data-dir",
            cassandraDir.toAbsolutePath() + "/data/data3",
            "--snapshot-tag=" + snapshotName1, // !!! important, we are restoring into the FIRST snapshot
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=" + systemKeyspace(getCassandraVersion()) + ",test,test2", // here we want to restore only to test and test2
            "--restoration-strategy-type=" + type,
            "--restoration-phase-type=cleanup", // CLEANUP
            "--import-source-dir=" + target("downloaded"),
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
            cassandra = getCassandra(cassandraDir, getCassandraVersion());
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
            cassandra = getCassandra(cassandraRestoredDir, getCassandraVersion(), (workingDirectory, version) -> {
                try {
                    FileUtils.createDirectory(workingDirectory.resolve("data").resolve("data"));
                    FileUtils.createDirectory(workingDirectory.resolve("data").resolve("data2"));
                    FileUtils.createDirectory(workingDirectory.resolve("data").resolve("data3"));
                } catch (final Exception ex) {
                    // ignore
                }
                restoreOnStoppedNode(insertionTimes, arguments);
            });

            cassandra.start();

            waitForCql();

            try (CqlSession session = CqlSession.builder().build()) {
                assertRowCount(session, KEYSPACE, TABLE, NUMBER_OF_ROWS_AFTER_RESTORATION);
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
                                                     int rounds,
                                                     boolean crossKeyspaceRestore) throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, getCassandraVersion());
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

                if (!getCassandraVersion().startsWith("4") && !getCassandraVersion().startsWith("5")) {
                    // second round would not pass for 4 because import deletes files in download
                    logger.info("Round " + i + " - Executing the third restoration phase for the second time - import {}", asList(arguments[4]));
                    Esop.mainWithoutExit(arguments[4]);
                }

                logger.info("Round " + i + " - Executing the fourth restoration phase - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);

                logger.info("Round " + i + " - Executing the fourth restoration phase - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);

                // we expect 4 records to be there as 2 were there before the first backup and the second 2 before the second backup
                assertRowCount(session, KEYSPACE, TABLE, 4);
                assertRowCount(session, KEYSPACE_2, TABLE_2, 4);
                // here we expect that table3 is same as table2
                if (!crossKeyspaceRestore) {
                    assertRowCount(session, KEYSPACE_2, TABLE_3, 4);
                }
            }
        } finally {
            cassandra.stop();
            FileUtils.deleteDirectory(cassandraDir);
            deleteDirectory(Paths.get(target("backup1")));
        }
    }

    public void liveBackupRestoreTest(final String[][] arguments, int rounds) throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, getCassandraVersion());
        cassandra.start();

        waitForCql();

        try (CqlSession session = CqlSession.builder().build()) {

            createTable(session, KEYSPACE, TABLE);
            createTable(session, KEYSPACE_2, TABLE_2);
            //createIndex(session, KEYSPACE, TABLE);

            insertAndCallBackupCLI(2, session, arguments[0]); // stefansnapshot-1
            insertAndCallBackupCLI(2, session, arguments[1]); // stefansnapshot-2

            assertRowCount(session, KEYSPACE, TABLE, 4);
            assertRowCount(session, KEYSPACE_2, TABLE_2, 4);

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

                // second round would not pass for 4 because import deletes files in download
                logger.info("Round " + i + " - Executing the third restoration phase for the second time - import {}", asList(arguments[4]));
                Esop.mainWithoutExit(arguments[4]);

                logger.info("Round " + i + " - Executing the fourth restoration phase - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);
                logger.info("Round " + i + " - Executing the fourth restoration phase for the second time - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);

                // we expect 4 records to be there as 2 were there before the first backup and the second 2 before the second backup
                assertRowCount(session, KEYSPACE, TABLE, 4);
                assertRowCount(session, KEYSPACE_2, TABLE_2, 4);
            }
        } finally {
            cassandra.stop();
            FileUtils.deleteDirectory(cassandraDir);
            deleteDirectory(Paths.get(target("backup1")));
        }
    }

    public void liveBackupRestoreWithEmptyKeyspace(final String[][] arguments, int rounds) throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, getCassandraVersion());
        cassandra.start();

        waitForCql();

        try (CqlSession session = CqlSession.builder().build()) {

            createTable(session, KEYSPACE, TABLE);
            createTable(session, KEYSPACE_2, TABLE_2);
            createKeyspace(session, "some_ks");

            insertAndCallBackupCLI(2, session, arguments[0]); // stefansnapshot-1
            insertAndCallBackupCLI(2, session, arguments[1]); // stefansnapshot-2

            assertRowCount(session, KEYSPACE, TABLE, 4);
            assertRowCount(session, KEYSPACE_2, TABLE_2, 4);

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

                // second round would not pass for 4 because import deletes files in download
                logger.info("Round " + i + " - Executing the third restoration phase for the second time - import {}", asList(arguments[4]));
                Esop.mainWithoutExit(arguments[4]);

                logger.info("Round " + i + " - Executing the fourth restoration phase - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);
                logger.info("Round " + i + " - Executing the fourth restoration phase for the second time - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);

                // we expect 4 records to be there as 2 were there before the first backup and the second 2 before the second backup
                assertRowCount(session, KEYSPACE, TABLE, 4);
                assertRowCount(session, KEYSPACE_2, TABLE_2, 4);
            }
        } finally {
            cassandra.stop();
            FileUtils.deleteDirectory(cassandraDir);
            deleteDirectory(Paths.get(target("backup1")));
        }
    }

    public void liveBackupRestoreTest(final String[][] arguments) throws Exception {
        liveBackupRestoreTest(arguments, 1);
    }

    private void waitUntilSchemaChanged(final String firstSchemaVersion) {
        await().pollInterval(3, SECONDS).timeout(1, MINUTES).until(() -> {
            String secondSchemaVersion = new CassandraSchemaVersion(new CassandraJMXServiceImpl(new CassandraJMXConnectionInfo())).act();
            // wait until schemas are NOT same
            return !firstSchemaVersion.equals(secondSchemaVersion);
        });
    }

    public void liveBackupWithRestoreOnDifferentTableSchema(final String[][] arguments, final boolean tableAddition) throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, getCassandraVersion());
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
            assertRowCount(session, KEYSPACE, TABLE, 0);
            assertRowCount(session, KEYSPACE_2, TABLE_2, 0);
            assertRowCount(session, KEYSPACE_3, TABLE_3, 2);

            // import
            Esop.mainWithoutExit(arguments[4]);

            // cleanup
            Esop.mainWithoutExit(arguments[5]);

            // verify

            // here we check that table1 and table2 contains 2 rows each (as we restored it from the first snapshot) and table 3 will contain still 2

            assertRowCount(session, KEYSPACE, TABLE, 2);
            assertRowCount(session, KEYSPACE_2, TABLE_2, 2);
            assertRowCount(session, KEYSPACE_3, TABLE_3, 2);
        } finally {
            cassandra.stop();
            FileUtils.deleteDirectory(cassandraDir);
            deleteDirectory(Paths.get(target("backup1")));
        }
    }

    public void liveBackupWithRestoreOnDifferentSchema(final String[][] arguments) throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, getCassandraVersion());
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
            assertRowCount(session, KEYSPACE, TABLE, 0);
            assertRowCount(session, KEYSPACE_2, TABLE_2, 0);
            assertRowCount(session, KEYSPACE_3, TABLE_3, 2);

            // import
            Esop.mainWithoutExit(arguments[4]);

            // cleanup
            Esop.mainWithoutExit(arguments[5]);

            // verify

            // here we check that table1 and table2 contains 2 rows each (as we restored it from the first snapshot) and table 3 will contain still 2

            assertRowCount(session, KEYSPACE, TABLE, 2);
            assertRowCount(session, KEYSPACE_2, TABLE_2, 2);
            assertRowCount(session, KEYSPACE_3, TABLE_3, 2);
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
        createIndex(session, KEYSPACE, TABLE);

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

        Version parsedVersion = Version.parse(version);
        CassandraBuilder builder = new CassandraBuilder();
        builder.version(parsedVersion);
        builder.jvmOptions("-Dcassandra.ring_delay_ms=1000", "-Xms1g", "-Xmx2g");
        builder.workingDirectory(() -> cassandraHome);


        Map<String, Object> config = new HashMap<String, Object>() {{
                put("data_file_directories", new ArrayList<String>() {{
                    add(cassandraHome.toAbsolutePath() + "/data/data");
                    add(cassandraHome.toAbsolutePath() + "/data/data2");
                    add(cassandraHome.toAbsolutePath() + "/data/data3");
                }});
            }};

        if (parsedVersion.getMajor() == 5) {
            config.put("sstable", new HashMap<>() {{
                put("selected_format", System.getProperty("sstable.format", "big"));
            }});
            config.put("storage_compatibility_mode", "NONE");
            config.put("uuid_sstable_identifiers_enabled", "true");
        }

        builder.addConfigProperties(config);

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

    protected void init() {

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
                                           .value(ID, literal(UUID.randomUUID().toString()))
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
        createKeyspace(session, keyspace);
        session.execute(SchemaBuilder.createTable(keyspace, table)
                                .ifNotExists()
                                .withPartitionKey(ID, TEXT)
                                .withClusteringColumn(DATE, TIMEUUID)
                                .withColumn(NAME, TEXT)
                                .build());
    }

    protected void createKeyspace(CqlSession session, String keyspace) {
        session.execute(SchemaBuilder.createKeyspace(keyspace)
                                .ifNotExists()
                                .withNetworkTopologyStrategy(of("datacenter1", 1))
                                .build());

        Uninterruptibles.sleepUninterruptibly(2, SECONDS);
    }

    protected void createIndex(CqlSession session, String keyspace, String table) {
        createTable(session, keyspace, table);
        Uninterruptibles.sleepUninterruptibly(2, SECONDS);
        session.execute(SchemaBuilder.createIndex(table + "_idx")
                                .ifNotExists()
                                .onTable(keyspace, table)
                                .andColumn("name")
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


    protected void assertRowCount(final CqlSession session,
                                  final String keyspace,
                                  final String table,
                                  int expectedLength) {

        long count = session.execute(selectFrom(keyspace, table).countAll().asCql()).one().getLong("count");
        assertEquals(expectedLength, count);
    }
}
