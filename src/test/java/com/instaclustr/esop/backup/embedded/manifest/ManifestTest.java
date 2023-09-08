package com.instaclustr.esop.backup.embedded.manifest;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Version;
import com.github.nosan.embedded.cassandra.WorkingDirectoryDestroyer;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.instaclustr.cassandra.CassandraModule;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.Snapshots;
import com.instaclustr.esop.impl.Snapshots.Snapshot;
import com.instaclustr.esop.impl.Snapshots.Snapshot.Keyspace;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.coordination.ClearSnapshotOperation;
import com.instaclustr.esop.impl.backup.coordination.ClearSnapshotOperation.ClearSnapshotOperationRequest;
import com.instaclustr.esop.impl.backup.coordination.TakeSnapshotOperation;
import com.instaclustr.esop.impl.backup.coordination.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import com.instaclustr.esop.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.esop.impl.interaction.CassandraTokens;
import com.instaclustr.io.FileUtils;
import com.instaclustr.jackson.JacksonModule;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.Operation.State;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;
import com.instaclustr.threading.ExecutorsModule;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIMEUUID;
import static com.datastax.oss.driver.api.core.uuid.Uuids.timeBased;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.google.common.collect.ImmutableMap.of;
import static com.instaclustr.esop.backup.embedded.TestEntity.DATE;
import static com.instaclustr.esop.backup.embedded.TestEntity.ID;
import static com.instaclustr.esop.backup.embedded.TestEntity.NAME;
import static com.instaclustr.esop.impl.Manifest.getLocalManifestPath;
import static com.instaclustr.esop.impl.Manifest.getManifestAsManifestEntry;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ManifestTest {

    private static final Logger logger = LoggerFactory.getLogger(ManifestTest.class);

    private final Path cassandraDir = new File("target/cassandra-manifest-test").toPath().toAbsolutePath();
    private final Path cassandraDataDir = cassandraDir.resolve("data/data").toAbsolutePath();

    private Cassandra cassandra;
    private CqlSession session;

    @Inject
    private Provider<CassandraVersion> cassandraVersionProvider;

    @Inject
    private CassandraJMXService jmx;

    @Inject
    private ExecutorServiceSupplier executorServiceSupplier;

    @Inject
    private ObjectMapper objectMapper;

    private OperationsService operationsService;

    @Test
    public void testEmptySnapshot() throws Exception {
        Snapshots snapshots = Snapshots.parse(cassandraDir);

        assertNotNull(snapshots);
        assertTrue(snapshots.isEmpty());
        assertEquals(0, snapshots.size());

        assertFalse(snapshots.get("whatever").isPresent());
    }

    private void waitUntilSchemaChange(String actual) {
        Awaitility.await().pollInterval(5, SECONDS).timeout(1, MINUTES).until(() -> !actual.equals(new CassandraSchemaVersion(jmx).act()));
    }

    private Manifest[] prepareDatabase() throws Exception {

        // first table
        createTable("ks1", "ks1t1");
        createTable("ks1", "ks1t2");
        disableAutocompaction("ks1");

        insertDataIntoTable("ks1", "ks1t1");
        flush("ks1");
        insertDataIntoTable("ks1", "ks1t1");
        flush("ks1");
        insertDataIntoTable("ks1", "ks1t2");
        flush("ks1");
        insertDataIntoTable("ks1", "ks1t2");
        flush("ks1");

        Manifest manifest1 = getManifest("snapshot1");

        // ks2t1

        createTable("ks2", "ks2t1");
        disableAutocompaction("ks2");
        insertDataIntoTable("ks2", "ks2t1");
        flush("ks2");
        insertDataIntoTable("ks2", "ks2t1");
        flush("ks2");

        Manifest manifest2 = getManifest("snapshot2");

        // ks2t2

        createTable("ks2", "ks2t2");
        disableAutocompaction("ks2");
        insertDataIntoTable("ks2", "ks2t2");
        flush("ks2");
        insertDataIntoTable("ks2", "ks2t2");
        flush("ks2");

        Manifest manifest3 = getManifest("snapshot3");

        // ADDED COLUMN

        execute("ALTER TABLE ks2.ks2t2 ADD something text");

        insertDataIntoTable("ks2", "ks2t2");
        flush("ks2");
        insertDataIntoTable("ks2", "ks2t2");
        flush("ks2");

        Manifest manifest4 = getManifest("snapshot4");

        System.out.println(Manifest.write(manifest4, objectMapper));

        return new Manifest[]{
            manifest1,
            manifest2,
            manifest3,
            manifest4
        };
    }

    private Manifest getManifest(String snapshotName) throws Exception {
        waitForOperation(new TakeSnapshotOperation(jmx, new TakeSnapshotOperationRequest(DatabaseEntities.empty(), snapshotName), cassandraVersionProvider));

        Snapshots snapshot = Snapshots.parse(cassandraDataDir);
        Manifest manifest = new Manifest(snapshot.get(snapshotName).get());
        manifest.setSchemaVersion(new CassandraSchemaVersion(jmx).act());
        manifest.setTokens(new CassandraTokens(jmx).act());

        Thread.sleep(5000);

        return manifest;
    }

    @Test
    public void testManifestMethods() throws Exception {

        Manifest[] manifests = prepareDatabase();

        try {
            // schemas across snapshots are not same
            assertNotEquals(manifests[0].getSchemaVersion(), manifests[1].getSchemaVersion());
            assertNotEquals(manifests[1].getSchemaVersion(), manifests[2].getSchemaVersion());
            assertNotEquals(manifests[0].getSchemaVersion(), manifests[2].getSchemaVersion());

            // however, this is true, because tables which are present in both are same
            assertTrue(manifests[0].getSnapshot().hasSameSchemas(manifests[1].getSnapshot()));
            assertTrue(manifests[0].getSnapshot().hasSameSchemas(manifests[2].getSnapshot()));
            assertTrue(manifests[1].getSnapshot().hasSameSchemas(manifests[2].getSnapshot()));

            // second and third snapshot are "same" on schemas even third snapshot has one table more
            // but it is important that same tables are same
            assertTrue(manifests[0].getSnapshot().getKeyspace("ks1").get().hasSameSchema(manifests[1].getSnapshot().getKeyspace("ks1").get()));
            assertTrue(manifests[1].getSnapshot().getKeyspace("ks2").get().hasSameSchema(manifests[2].getSnapshot().getKeyspace("ks2").get()));

            // however, we have altered a schema of k2t2

            assertFalse(manifests[2].getSnapshot().hasSameSchemas(manifests[3].getSnapshot()));
            assertFalse(manifests[2].getSnapshot().getKeyspace("ks2").get().hasSameSchema(manifests[3].getSnapshot().getKeyspace("ks2").get()));

            Keyspace ks2BeforeAlter = manifests[2].getSnapshot().getKeyspace("ks2").get();
            Keyspace ks2AfterAlter = manifests[3].getSnapshot().getKeyspace("ks2").get();

            // TODO - get entities with different schemas
            List<String> tablesWithDifferentSchemas = ks2BeforeAlter.getTablesWithDifferentSchemas(ks2AfterAlter);

            assertFalse(tablesWithDifferentSchemas.isEmpty());
            assertEquals(1, tablesWithDifferentSchemas.size());
            assertEquals("ks2t2", tablesWithDifferentSchemas.get(0));
        } finally {
            try {
                for (Manifest manifest : manifests) {
                    waitForOperation(new ClearSnapshotOperation(jmx, new ClearSnapshotOperationRequest(manifest.getSnapshot().getName())));
                }
            } catch (final Exception ex) {
                logger.error("Unable to clear snapshots", ex);
            }
        }
    }

    @Test
    public void testJsonManifest() throws Exception {
        try {

            final List<String> tokens = new CassandraTokens(jmx).act();

            DatabaseEntities databaseEntities = DatabaseEntities.empty();

            // first table

            createTable("ks1", "ks1t1");
            disableAutocompaction("ks1");

            // #1 insert and flush & take snapshot

            insertDataIntoTable("ks1", "ks1t1");
            flush("ks1");
            insertDataIntoTable("ks1", "ks1t1");
            flush("ks1");

            waitForOperation(new TakeSnapshotOperation(jmx, new TakeSnapshotOperationRequest(databaseEntities, "snapshot1"), cassandraVersionProvider));

            // #2 insert and flush & take snapshot

            insertDataIntoTable("ks1", "ks1t1");
            flush("ks1");
            insertDataIntoTable("ks1", "ks1t1");
            flush("ks1");

            waitForOperation(new TakeSnapshotOperation(jmx, new TakeSnapshotOperationRequest(databaseEntities, "snapshot2"), cassandraVersionProvider));

            // second table

            createTable("ks2", "ks2t1");
            disableAutocompaction("ks2");

            // #1 insert and flush & take snapshot

            insertDataIntoTable("ks2", "ks2t1");
            flush("ks2");
            insertDataIntoTable("ks2", "ks2t1");
            flush("ks2");

            waitForOperation(new TakeSnapshotOperation(jmx, new TakeSnapshotOperationRequest(databaseEntities, "snapshot3"), cassandraVersionProvider));

            // parse

            final Snapshots snapshots = Snapshots.parse(cassandraDataDir);

            assertNotNull(snapshots);
            assertFalse(snapshots.isEmpty());
            assertEquals(3, snapshots.size());
            assertTrue(snapshots.get("snapshot1").isPresent());
            assertTrue(snapshots.get("snapshot2").isPresent());
            assertTrue(snapshots.get("snapshot3").isPresent());

            Manifest manifest = new Manifest(snapshots.get("snapshot3").get());

            // manifest itself, but it wont be serialised
            final Path localManifestPath = getLocalManifestPath("snapshot1");
            manifest.setManifest(getManifestAsManifestEntry(localManifestPath, new BackupOperationRequest()));

            // tokens
            manifest.setTokens(tokens);

            final String schemaVersion = new CassandraSchemaVersion(jmx).act();
            manifest.setSchemaVersion(schemaVersion);

            String writtenManifestAsJson = Manifest.write(manifest, objectMapper);

            logger.info(writtenManifestAsJson);

            assertNotNull(writtenManifestAsJson);

            Snapshot snapshot3 = snapshots.get("snapshot3").get();

            Optional<Keyspace> ks2 = snapshot3.getKeyspace("ks2");

            assertTrue(ks2.isPresent());
            assertTrue(ks2.get().containsTable("ks2t1"));

            List<ManifestEntry> ks2t1 = ks2.get().getManifestEntries("ks2t1");

            assertFalse(ks2t1.isEmpty());

            Manifest readManifest = Manifest.read(writtenManifestAsJson, objectMapper);
            assertNotNull(readManifest);

            HashMultimap<String, String> ksAndTables = readManifest.getSnapshot().getKeyspacesAndTables();
            // also system
            assertTrue(ksAndTables.size() > 2);
            assertTrue(ksAndTables.containsEntry("ks1", "ks1t1"));
            assertTrue(ksAndTables.containsEntry("ks2", "ks2t1"));

            HashMultimap<String, String> ksAndTablesWithoutSystem = readManifest.getSnapshot().getKeyspacesAndTables(false);
            assertEquals(2, ksAndTablesWithoutSystem.size());
            assertTrue(ksAndTables.containsEntry("ks1", "ks1t1"));
            assertTrue(ksAndTables.containsEntry("ks2", "ks2t1"));

            snapshots.clear();

            assertTrue(snapshots.isEmpty());
            assertEquals(0, snapshots.size());
        } finally {
            try {
                waitForOperation(new ClearSnapshotOperation(jmx, new ClearSnapshotOperationRequest("snapshot1")));
                waitForOperation(new ClearSnapshotOperation(jmx, new ClearSnapshotOperationRequest("snapshot2")));
                waitForOperation(new ClearSnapshotOperation(jmx, new ClearSnapshotOperationRequest("snapshot3")));
            } catch (final Exception ex) {
                logger.error("Unable to clear snapshots", ex);
            }
        }

    }

    @BeforeMethod
    public void setup() throws Exception {

        final List<Module> modules = new ArrayList<Module>() {{
            add(new ExecutorsModule());
            add(new CassandraModule());
            add(new JacksonModule());
        }};

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        operationsService = new OperationsService(executorServiceSupplier.get());

        cassandra = getCassandra();
        cassandra.start();
        waitForCql();

        session = CqlSession.builder().build();
    }

    private Cassandra getCassandra() {
        return new CassandraBuilder()
            .version(Version.parse(System.getProperty("cassandra3.version", "3.11.14")))
            .jvmOptions("-Xmx1g", "-Xms1g", "-Dcassandra.ring_delay_ms=1000")
            .workingDirectory(() -> cassandraDir)
            .workingDirectoryDestroyer(WorkingDirectoryDestroyer.deleteAll())
            .build();
    }

    @AfterMethod
    public void afterTest() throws Exception {
        cassandra.stop();
        session.close();
        FileUtils.deleteDirectory(cassandraDir);
    }

    // helpers

    private void waitForOperation(final Operation<?> operation) {
        operationsService.submitOperation(operation);
        await().timeout(1, MINUTES).until(() -> {
            Optional<Operation<?>> op = operationsService.operation(operation.id);
            return op.isPresent() && op.get().state == State.COMPLETED;
        });
    }

    private void flush(String keyspace, String... tables) throws Exception {
        jmx.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Void>() {
            @Override
            public Void apply(final StorageServiceMBean object) throws Exception {
                object.forceKeyspaceFlush(keyspace, tables);
                return null;
            }
        });
    }

    private void createTable(String keyspace, String table) throws Exception {

        String currentVersion = new CassandraSchemaVersion(jmx).act();

        Thread.sleep(3000);

        session.execute(createKeyspace(keyspace)
                            .ifNotExists()
                            .withNetworkTopologyStrategy(of("datacenter1", 1))
                            .build());

        Thread.sleep(5000);

        session.execute(SchemaBuilder.createTable(keyspace, table)
                            .ifNotExists()
                            .withPartitionKey(ID, TEXT)
                            .withClusteringColumn(DATE, TIMEUUID)
                            .withColumn(NAME, TEXT)
                            .build());

        waitUntilSchemaChange(currentVersion);
    }

    private void execute(final String query) {
        session.execute(query);
    }

    private void insertDataIntoTable(String keyspace, String table) {
        session.execute(insertInto(keyspace, table)
                            .value(ID, literal("1"))
                            .value(DATE, literal(timeBased()))
                            .value(NAME, literal("stefan1"))
                            .build());

    }

    private void disableAutocompaction(final String keyspace) throws Exception {
        jmx.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Void>() {
            @Override
            public Void apply(final StorageServiceMBean object) throws Exception {
                object.disableAutoCompaction(keyspace);
                return null;
            }
        });
    }

    private void waitForCql() {
        await()
            .pollInterval(10, TimeUnit.SECONDS)
            .pollInSameThread()
            .timeout(1, TimeUnit.MINUTES)
            .until(() -> {
                try (final CqlSession cqlSession = CqlSession.builder().build();) {
                    return true;
                } catch (final Exception ex) {
                    return false;
                }
            });
    }
}
