package com.instaclustr.cassandra.backup.embedded;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIMEUUID;
import static com.datastax.oss.driver.api.core.uuid.Uuids.timeBased;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static com.google.common.collect.ImmutableMap.of;
import static com.instaclustr.cassandra.backup.embedded.TestEntity.DATE;
import static com.instaclustr.cassandra.backup.embedded.TestEntity.ID;
import static com.instaclustr.cassandra.backup.embedded.TestEntity.KEYSPACE;
import static com.instaclustr.cassandra.backup.embedded.TestEntity.NAME;
import static com.instaclustr.cassandra.backup.embedded.TestEntity.TABLE;
import static com.instaclustr.cassandra.backup.embedded.TestEntity2.KEYSPACE_2;
import static com.instaclustr.cassandra.backup.embedded.TestEntity2.TABLE_2;
import static com.instaclustr.io.FileUtils.cleanDirectory;
import static com.instaclustr.io.FileUtils.deleteDirectory;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.github.nosan.embedded.cassandra.EmbeddedCassandraFactory;
import com.github.nosan.embedded.cassandra.api.Cassandra;
import com.github.nosan.embedded.cassandra.api.Version;
import com.github.nosan.embedded.cassandra.artifact.Artifact;
import com.github.nosan.embedded.cassandra.artifact.DefaultArtifact;
import com.instaclustr.cassandra.backup.cli.BackupRestoreCLI;
import io.kubernetes.client.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBackupTest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractBackupTest.class);

    private static final String CASSANDRA_VERSION = System.getProperty("backup.tests.cassandra.version", "3.11.6");

    // This is number of rows we inserted into Cassandra DB in total
    // we backed up first 6 rows. For the last two rows, they are stored in commit logs.
    // The backup procedure backs up also commit logs including remaining 2 rows
    // so commit log restoration procedure restores them too
    static int NUMBER_OF_INSERTED_ROWS = 8;

    // This is number of rows we expect to see in database after commitlog restoration
    // We omitted one row here, on purpose, to demonstrate point in time restoration
    static int NUMBER_OF_ROWS_AFTER_RESTORATION = 7;

    // inserted rows without these in commitlogs
    static int NUMBER_OF_FLUSHED_ROWS = 6;

    static Artifact CASSANDRA_ARTIFACT = Artifact.ofVersion(Version.of(CASSANDRA_VERSION));

    protected final Path target = new File("target").toPath().toAbsolutePath();
    protected final Path cassandraDir = new File("target/cassandra").toPath().toAbsolutePath();
    protected final Path cassandraRestoredDir = new File("target/cassandra-restored").toPath().toAbsolutePath();
    protected final Path cassandraRestoredConfigDir = new File("target/cassandra-restored/conf").toPath().toAbsolutePath();

    ////////// ARGUMENTS

    protected static final String BUCKET_NAME = UUID.randomUUID().toString();

    protected static final String SIDECAR_SECRET_NAME = "test-sidecar-secret";

    protected String[][] inPlaceArguments() {

        final String snapshotName = UUID.randomUUID().toString();

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--entities=system_schema,test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName,
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--entities=system_schema,test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        // RESTORE

        final String[] restoreArgs = new String[]{
            "restore",
            "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
            "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=system_schema,test,test2",
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        // COMMIT LOGS

        final String[] commitlogBackupArgs = new String[]{
            "commitlog-backup",
            "--storage-location=" + getStorageLocation(),
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] commitlogRestoreArgs = new String[]{
            "commitlog-restore",
            "--data-directory=" + cassandraRestoredDir.toAbsolutePath().toString() + "/data",
            "--config-directory=" + cassandraRestoredConfigDir.toAbsolutePath().toString(),
            "--storage-location=" + getStorageLocation(),
            "--commitlog-download-dir=" + target("commitlog_download_dir"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        return new String[][]{
            backupArgs,
            backupArgsWithSnapshotName,
            commitlogBackupArgs,
            restoreArgs,
            commitlogRestoreArgs
        };
    }

    protected String[][] importArguments() {

        final String snapshotName = UUID.randomUUID().toString();

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--entities=system_schema,test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName,
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--entities=system_schema,test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        // RESTORE

        final String[] restoreArgsPhase1 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=system_schema,test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=download", /// DOWNLOAD
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase2 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=system_schema,test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase3 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=system_schema,test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=import", // IMPORT
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase4 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=system_schema,test,test2",
            "--restoration-strategy-type=import",
            "--restoration-phase-type=cleanup", // CLEANUP
            "--import-source-dir=" + target("downloaded"),
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

    protected String[][] hardlinkingArguments() {

        final String snapshotName = UUID.randomUUID().toString();

        // BACKUP

        final String[] backupArgs = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--entities=system_schema,test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] backupArgsWithSnapshotName = new String[]{
            "backup",
            "--jmx-service", "127.0.0.1:7199",
            "--storage-location=" + getStorageLocation(),
            "--snapshot-tag=" + snapshotName,
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--entities=system_schema,test,test2", // keyspaces
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        // RESTORE

        final String[] restoreArgsPhase1 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=download", /// DOWNLOAD
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase2 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=truncate", // TRUNCATE
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase3 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
            "--snapshot-tag=" + snapshotName,
            "--storage-location=" + getStorageLocation(),
            "--update-cassandra-yaml=true",
            "--entities=test,test2",
            "--restoration-strategy-type=hardlinks",
            "--restoration-phase-type=import", // IMPORT
            "--import-source-dir=" + target("downloaded"),
            "--k8s-secret-name=" + SIDECAR_SECRET_NAME,
        };

        final String[] restoreArgsPhase4 = new String[]{
            "restore",
            "--data-directory=" + cassandraDir.toAbsolutePath().toString() + "/data",
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
            restoreArgsPhase1,
            restoreArgsPhase2,
            restoreArgsPhase3,
            restoreArgsPhase4,
        };

    }

    protected abstract String getStorageLocation();

    public void inPlaceBackupRestoreTest(final String[][] arguments) throws Exception {
        try {
            List<Long> insertionTimes = backup(arguments);
            restoreOnStoppedNode(insertionTimes, arguments);
        } catch (final Exception ex) {
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    public void liveBackupRestoreTest(final String[][] arguments) throws Exception {
        EmbeddedCassandraFactory cassandraFactory = new EmbeddedCassandraFactory();
        cassandraFactory.setWorkingDirectory(cassandraDir);
        cassandraFactory.setArtifact(Artifact.ofVersion(Version.of("4.0-alpha4")));
        cassandraFactory.getJvmOptions().add("-Xmx2g");
        cassandraFactory.getJvmOptions().add("-Xms2g");

        Cassandra cassandra = cassandraFactory.create();

        cassandra.start();

        try (CqlSession session = CqlSession.builder().build()) {

            // keyspace, table

            session.execute(createKeyspace(KEYSPACE)
                                .ifNotExists()
                                .withNetworkTopologyStrategy(of("datacenter1", 1))
                                .build());

            session.execute(createTable(KEYSPACE, TABLE)
                                .ifNotExists()
                                .withPartitionKey(ID, TEXT)
                                .withClusteringColumn(DATE, TIMEUUID)
                                .withColumn(NAME, TEXT)
                                .build());

            // keyspace2, table2

            session.execute(createKeyspace(KEYSPACE_2)
                                .ifNotExists()
                                .withNetworkTopologyStrategy(of("datacenter1", 1))
                                .build());

            session.execute(createTable(KEYSPACE_2, TABLE_2)
                                .ifNotExists()
                                .withPartitionKey(ID, TEXT)
                                .withClusteringColumn(DATE, TIMEUUID)
                                .withColumn(NAME, TEXT)
                                .build());

            insertAndBackup(2, session, arguments[0]); // stefansnapshot-1
            insertAndBackup(2, session, arguments[1]); // stefansnapshot-2

            logger.info("Executing the first restoration phase - download {}", asList(arguments[2]));
            BackupRestoreCLI.mainWithoutExit(arguments[2]);

            logger.info("Executing the second restoration phase - truncate {}", asList(arguments[3]));
            BackupRestoreCLI.mainWithoutExit(arguments[3]);

            logger.info("Executing the third restoration phase - import {}", asList(arguments[4]));
            BackupRestoreCLI.mainWithoutExit(arguments[4]);

            logger.info("Executing the fourth restoration phase - cleanup {}", asList(arguments[5]));
            BackupRestoreCLI.mainWithoutExit(arguments[5]);

            // we expect 4 records to be there as 2 were there before the first backup and the second 2 before the second backup
            dumpTable(KEYSPACE, TABLE, 4);
            dumpTable(KEYSPACE_2, TABLE_2, 4);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            cassandra.stop();
            deleteDirectory(Paths.get(target("backup1")));
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    public List<Long> backup(final String[][] arguments) throws Exception {
        // BACKUP

        EmbeddedCassandraFactory cassandraToBackupFactory = new EmbeddedCassandraFactory();
        cassandraToBackupFactory.setWorkingDirectory(cassandraDir);
        cassandraToBackupFactory.setArtifact(CASSANDRA_ARTIFACT);
        cassandraToBackupFactory.getJvmOptions().add("-Xmx2g");
        cassandraToBackupFactory.getJvmOptions().add("-Xms2g");
        Cassandra cassandraToBackup = cassandraToBackupFactory.create();

        cassandraToBackup.start();

        List<Long> insertionTimes = new ArrayList<>();

        try (CqlSession session = CqlSession.builder().build()) {

            // keyspace, table

            session.execute(createKeyspace(KEYSPACE)
                                .ifNotExists()
                                .withNetworkTopologyStrategy(of("datacenter1", 1))
                                .build());

            session.execute(createTable(KEYSPACE, TABLE)
                                .ifNotExists()
                                .withPartitionKey(ID, TEXT)
                                .withClusteringColumn(DATE, TIMEUUID)
                                .withColumn(NAME, TEXT)
                                .build());

            // keyspace2, table2

            session.execute(createKeyspace(TestEntity2.KEYSPACE_2)
                                .ifNotExists()
                                .withNetworkTopologyStrategy(of("datacenter1", 1))
                                .build());

            session.execute(createTable(TestEntity2.KEYSPACE_2, TestEntity2.TABLE_2)
                                .ifNotExists()
                                .withPartitionKey(ID, TEXT)
                                .withClusteringColumn(DATE, TIMEUUID)
                                .withColumn(NAME, TEXT)
                                .build());

            // this will invoke backup 2 times, each time generating 2 records and taking a snapshot and backup
            List<Long> times = range(0, 2)
                .mapToObj(i -> insertAndBackup(2, session, arguments[0]))
                .flatMap(Collection::stream).collect(toList());

            // insert two more records, snapshot with predefined name and back it up too
            List<Long> times2 = insertAndBackup(2, session, arguments[1]);

            // insert two more rows but do not back them up, this simulates that they are written in commit logs
            // so even we have not backed it up, once they are replayed, they will all be there
            List<Long> times3 = insert(2, session);

            insertionTimes.addAll(times);
            insertionTimes.addAll(times2);
            insertionTimes.addAll(times3);

            assertEquals(insertionTimes.size(), NUMBER_OF_INSERTED_ROWS);

            logger.info("Executing backup of commit logs {}", asList(arguments[2]));

            BackupRestoreCLI.mainWithoutExit(arguments[2]);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            copyCassandra(cassandraDir, cassandraRestoredDir);
            cassandraToBackup.stop();
        }

        return insertionTimes;
    }

    public void restoreOnStoppedNode(List<Long> insertionTimes, String[][] arguments) {

        // RESTORE

        logger.info("Executing restore on stopped node with arguments {}", asList(arguments[3]));

        BackupRestoreCLI.mainWithoutExit(arguments[3]);

        // restore of commit logs on top of done restore

        List<String> commitlogRestoreArgsAsList = new ArrayList<>(asList(arguments[4]));

        // here we want to point-in-time restoration, so out of 8 records in total, (2x2 + 2 + 2) where last two
        // were not backed up, we take into account all first and second set of records (6 in total)
        // plus the first record from the last batch of 2 records which were not backed up but the idea is that they
        // are in commit logs so we simulate we restored records from the beginning until the end but the last record

        // from very early start
        commitlogRestoreArgsAsList.add("--timestamp-start=" + (insertionTimes.get(0) - 10000));

        // til the end but the last record
        commitlogRestoreArgsAsList.add("--timestamp-end=" + (insertionTimes.get(insertionTimes.size() - 2) + 1));

        logger.info("Executing restore of commit logs with arguments {}", asList(commitlogRestoreArgsAsList));

        BackupRestoreCLI.main(commitlogRestoreArgsAsList.toArray(new String[0]), false);

        // RESTORE VERIFICATION

        EmbeddedCassandraFactory cassandraToRestoreFactory = new EmbeddedCassandraFactory();
        cassandraToRestoreFactory.setWorkingDirectory(cassandraRestoredDir);
        cassandraToRestoreFactory.getJvmOptions().add("-Xmx2g");
        cassandraToRestoreFactory.getJvmOptions().add("-Xms2g");
        cassandraToRestoreFactory.setArtifact(new DefaultArtifact(Version.of(CASSANDRA_VERSION), cassandraRestoredDir));
        Cassandra cassandraToRestore = cassandraToRestoreFactory.create();

        cassandraToRestore.start();

        waitForCql();

        try {
            dumpTable(KEYSPACE, TABLE, NUMBER_OF_ROWS_AFTER_RESTORATION);
        } finally {
            cassandraToRestore.stop();
        }
    }

    // helpers

    protected List<Long> insertAndBackup(int records, CqlSession cqlSession, String[] backupArgs) {
        List<Long> executionTimes = insert(records, cqlSession);

        logger.info("Executing backup with arguments {}", asList(backupArgs));
        BackupRestoreCLI.main(backupArgs, false);

        return executionTimes;
    }

    protected List<Long> insert(int records, CqlSession cqlSession) {
        return range(0, records).mapToObj(i -> insert(cqlSession)).collect(toList());
    }

    protected long insert(CqlSession session) {

        try {
            session.execute(insertInto(KEYSPACE, TABLE)
                                .value(ID, literal("1"))
                                .value(DATE, literal(timeBased()))
                                .value(NAME, literal("stefan1"))
                                .build());

            session.execute(insertInto(TestEntity2.KEYSPACE_2, TestEntity2.TABLE_2)
                                .value(ID, literal("1"))
                                .value(DATE, literal(timeBased()))
                                .value(NAME, literal("stefan1"))
                                .build());

            Thread.sleep(2000);
        } catch (InterruptedException ex) {
            logger.error("Exception while sleeping!");
        }

        return System.currentTimeMillis();
    }

    protected String target(final String path) {
        return target.resolve(path).toAbsolutePath().toString();
    }

    private Date uuidToDate(final UUID uuid) {
        return new Date(uuid.timestamp() / 10000L - 12219292800000L);
    }

    private void copyCassandra(Path source, Path target) throws IOException {
        org.apache.commons.io.FileUtils.copyDirectory(source.toFile(), target.toFile());
        cleanDirectory(target.resolve("data"));
    }

    protected void init() throws ApiException, IOException {

    }

    protected void destroy() throws ApiException {

    }

    protected void waitForCql() {
        await().until(() -> {
            try (CqlSession cqlSession = CqlSession.builder().build()) {
                return true;
            } catch (Exception ex) {
                return false;
            }
        });
    }

    protected void dumpTable(final String keyspace, final String table, int expectedLength) {
        try (CqlSession session = CqlSession.builder().build()) {
            List<Row> rows = session.execute(selectFrom(keyspace, table).all().build()).all();

            for (Row row : rows) {
                logger.info(format("id: %s, date: %s, name: %s", row.getString(ID), uuidToDate(requireNonNull(row.getUuid(DATE))), row.getString(NAME)));
            }

            assertEquals(rows.size(), expectedLength);
        }
    }
}
