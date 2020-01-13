package com.instaclustr.cassandra.backup.real;

import static com.instaclustr.cassandra.backup.real.TestEntity.KEYSPACE;
import static com.instaclustr.cassandra.backup.real.TestEntity.TABLE;
import static com.instaclustr.io.FileUtils.copy;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.instaclustr.cassandra.backup.cli.BackupRestoreCLI;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The point of this test is to backup and restore but restore verification
 * is done in {@link CassandraRestoreVerificationTest} because we have to
 * spin up a "restored Cassandra node". Cassandra is started as part of
 * Maven lifecycle and it is not possible to start two of them so
 * the idea is to run second profile (cassandra-restore-verification) which will
 * connect to Cassandra we already restored by this test.
 */
@Test(groups = {"cassandra-backup-restore"})
public class CassandraBackupTest extends AbstractBackupRestoreTest {

    final String[] backupArgs = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=file://" + target("backup1") + "/test-dc/1",
        "--data-directory=" + cassandraDir
    };

    final String[] backupArgsWithSnapshotName = new String[]{
        "backup",
        "--jmx-service", "127.0.0.1:7199",
        "--storage-location=file://" + target("backup1") + "/test-dc/1",
        "--snapshot-tag=stefansnapshot",
        "--data-directory=" + cassandraDir
    };

    final String[] restoreArgs = new String[]{
        "restore",
        "--data-directory=" + cassandraRestoredDir,
        "--config-directory=" + cassandraRestoredConfigDir,
        "--snapshot-tag=stefansnapshot",
        "--storage-location=file://" + target("backup1") + "/test-dc/1",
        "--update-cassandra-yaml=true",
    };

    final String[] commitlogBackupArgs = new String[]{
        "commitlog-backup",
        "--storage-location=file://" + target("backup1") + "/test-dc/1",
        "--data-directory=" + cassandraDir
    };

    final String[] commitlogRestoreArgs = new String[]{
        "commitlog-restore",
        "--data-directory=" + cassandraRestoredDir,
        "--config-directory=" + cassandraRestoredConfigDir,
        "--storage-location=file://" + target("backup1") + "/test-dc/1",
        "--commitlog-download-dir=" + target("commitlog_download_dir"),
    };

    @BeforeClass
    public void setup() {
        injectMembers();

        cleanup();

        cluster = initCluster();

        session = cluster.connect();

        createSchema(session);

        mappingManager = new MappingManager(session);

        testEntityMapper = mappingManager.mapper(TestEntity.class);
    }

    @AfterClass
    public void teardown() {
        deleteSchema(session);

        if (session != null) {
            session.close();
        }

        if (cluster != null) {
            cluster.close();
        }
    }

    @Override
    protected void createSchema(final Session session) {
        final KeyspaceOptions keyspaceOptions = SchemaBuilder.createKeyspace(KEYSPACE)
            .ifNotExists()
            .with()
            .replication(new HashMap<String, Object>() {{
                put("class", "NetworkTopologyStrategy");
                put("datacenter1", 1);
            }}).durableWrites(true);

        session.execute(keyspaceOptions);

        final Create create = SchemaBuilder.createTable(KEYSPACE, TABLE)
            .ifNotExists()
            .addPartitionKey("id", DataType.cint())
            .addClusteringColumn("date", DataType.timeuuid())
            .addColumn("name", DataType.text());

        session.execute(create);
    }

    @Override
    protected void deleteSchema(final Session session) {
        session.execute(new SimpleStatement("DROP KEYSPACE IF EXISTS " + KEYSPACE));
    }

    @Override
    protected void cleanup() {
        deleteResources("backup1", "lock");
    }

    @Test
    public void testBackup() throws Exception {

        logger.info("Executing backup with arguments {}", asList(backupArgs));

        // this will invoke backup 2 times, each time generating 2 records and taking a snapshot and backup
        final List<Long> insertionTimes = range(0, 2).mapToObj(i -> insertAndBackup(2, backupArgs)).flatMap(Collection::stream).collect(toList());
        // insert two more records, snapshot with predefined name and back it up too
        final List<Long> insertionTimes2 = insertAndBackup(2, backupArgsWithSnapshotName);
        // insert two more rows but do not back them up, this simulates that they are written in commit logs
        // so even we have not backed it up, once they are replayed, they will all be there
        final List<Long> insertionTimes3 = insertWithoutBackup(2);

        insertionTimes.addAll(insertionTimes2);
        insertionTimes.addAll(insertionTimes3);

        assertEquals(insertionTimes.size(), Constants.NUMBER_OF_INSERTED_ROWS);

        // then restore to dir structure where second Cassandra will start by another profile (-Pcassandra-restore-verification)
        // by time -Pcassandra-restore is run, it will be already restored as it will pick up that directory,
        // we just have to test that it restored the records inserted correctly

        createCassandraDirStructure(cassandraRestoredDir);

        copy(resourcePath("cassandra-restored.yaml"), Paths.get(cassandraRestoredDir).resolve("conf/cassandra.yaml"));

        logger.info("Executing backup of commit logs {}", asList(commitlogBackupArgs));

        BackupRestoreCLI.mainWithoutExit(commitlogBackupArgs);

        logger.info("Executing restore with arguments {}", asList(restoreArgs));

        BackupRestoreCLI.mainWithoutExit(restoreArgs);

        // restore of commit logs on top of done restore

        final List<String> commitlogRestoreArgsAsList = new ArrayList<>(asList(commitlogRestoreArgs));

        // here we want to point-in-time restoration, so out of 8 records in total, (2x2 + 2 + 2) where last two
        // were not backed up, we take into account all first and second set of records (6 in total)
        // plus the first record from the last batch of 2 records which were not backed up but the idea is that they
        // are in commit logs so we simulate we restored records from the beginning until the end but the last record

        // from very early start
        commitlogRestoreArgsAsList.add("--timestamp-start=" + (insertionTimes.get(0) - 10000));

        // til the end but the last record
        commitlogRestoreArgsAsList.add("--timestamp-end=" + (insertionTimes.get(insertionTimes.size() - 2) + 1));

        logger.info("Executing restore of commitlogs with arguments {}", asList(commitlogRestoreArgsAsList));
        copy(resourceFile("cassandra-env.sh"), targetFile("cassandra-restored/conf/cassandra-env.sh"));

        BackupRestoreCLI.main(commitlogRestoreArgsAsList.toArray(new String[0]), false);
    }

    protected List<Long> insertWithoutBackup(int records) {
        return range(0, records).mapToObj(i -> insert(1)).collect(toList());
    }

    protected List<Long> insertAndBackup(int records, String[] backupArgs) {
        final List<Long> executionTimes = range(0, records).mapToObj(i -> insert(1)).collect(toList());

        logger.info("Executing backup with arguments {}", asList(backupArgs));
        BackupRestoreCLI.main(backupArgs, false);

        return executionTimes;
    }
}
