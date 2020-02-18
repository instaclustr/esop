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
import static com.instaclustr.io.FileUtils.cleanDirectory;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
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
import com.github.nosan.embedded.cassandra.api.connection.CqlSessionCassandraConnectionFactory;
import com.github.nosan.embedded.cassandra.artifact.Artifact;
import com.github.nosan.embedded.cassandra.artifact.DefaultArtifact;
import com.instaclustr.cassandra.backup.cli.BackupRestoreCLI;
import io.kubernetes.client.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractBackupTest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractBackupTest.class);

    private static final String CASSANDRA_VERSION = "3.11.6";

    // This is number of rows we inserted into Cassandra DB in total
    // we backed up first 6 rows. For the last two rows, they are stored in commit logs.
    // The backup procedure backs up also commit logs including remaining 2 rows
    // so commit log restoration procedure restores them too
    static int NUMBER_OF_INSERTED_ROWS = 8;

    // This is number of rows we expect to see in database after commitlog restoration
    // We omitted one row here, on purpose, to demonstrate point in time restoration
    static int NUMBER_OF_ROWS_AFTER_RESTORATION = 7;

    static Artifact CASSANDRA_ARTIFACT = Artifact.ofVersion(Version.of(CASSANDRA_VERSION));

    protected final Path target = new File("target").toPath().toAbsolutePath();
    protected final Path cassandraDir = new File("target/cassandra").toPath().toAbsolutePath();
    protected final Path cassandraRestoredDir = new File("target/cassandra-restored").toPath().toAbsolutePath();
    protected final Path cassandraRestoredConfigDir = new File("target/cassandra-restored/conf").toPath().toAbsolutePath();

    public void backupAndRestoreTest(final String[][] arguments) throws Exception {
        // BACKUP

        EmbeddedCassandraFactory cassandraToBackupFactory = new EmbeddedCassandraFactory();
        cassandraToBackupFactory.setWorkingDirectory(cassandraDir);
        cassandraToBackupFactory.setArtifact(CASSANDRA_ARTIFACT);
        cassandraToBackupFactory.getJvmOptions().add("-Xmx1g");
        cassandraToBackupFactory.getJvmOptions().add("-Xms1g");
        Cassandra cassandraToBackup = cassandraToBackupFactory.create();

        cassandraToBackup.start();

        List<Long> insertionTimes = new ArrayList<>();

        try (CqlSession session = new CqlSessionCassandraConnectionFactory().create(cassandraToBackup).getConnection()) {
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
        } finally {
            copyCassandra(cassandraDir, cassandraRestoredDir);
            cassandraToBackup.stop();
        }

        // RESTORE

        logger.info("Executing restore with arguments {}", asList(arguments[3]));

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
        cassandraToRestoreFactory.getJvmOptions().add("-Xmx1g");
        cassandraToRestoreFactory.getJvmOptions().add("-Xms1g");
        cassandraToRestoreFactory.setArtifact(new DefaultArtifact(Version.of(CASSANDRA_VERSION), cassandraRestoredDir));
        Cassandra cassandraToRestore = cassandraToRestoreFactory.create();

        cassandraToRestore.start();

        try (CqlSession session = new CqlSessionCassandraConnectionFactory().create(cassandraToRestore).getConnection()) {
            List<Row> rows = session.execute(selectFrom(KEYSPACE, TABLE).all().build()).all();

            for (Row row : rows) {
                logger.info(format("id: %s, date: %s, name: %s", row.getString(ID), uuidToDate(requireNonNull(row.getUuid(DATE))), row.getString(NAME)));
            }

            assertEquals(rows.size(), NUMBER_OF_ROWS_AFTER_RESTORATION);
        } finally {
            cassandraToRestore.stop();
        }
    }

    // helpers

    private List<Long> insertAndBackup(int records, CqlSession cqlSession, String[] backupArgs) {
        List<Long> executionTimes = insert(records, cqlSession);

        logger.info("Executing backup with arguments {}", asList(backupArgs));
        BackupRestoreCLI.main(backupArgs, false);

        return executionTimes;
    }

    private List<Long> insert(int records, CqlSession cqlSession) {
        return range(0, records).mapToObj(i -> insert(cqlSession)).collect(toList());
    }

    private long insert(CqlSession session) {

        try {
            session.execute(insertInto(KEYSPACE, TABLE)
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
}
