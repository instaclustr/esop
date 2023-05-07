package com.instaclustr.esop.backup.embedded.local;

import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.IMPORT;
import static com.instaclustr.io.FileUtils.deleteDirectory;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.impl.AbstractTracker.Session;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.Snapshots;
import com.instaclustr.esop.impl.Snapshots.Snapshot;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.StorageLocation.StorageLocationTypeConverter;
import com.instaclustr.esop.impl.backup.BackupOperation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.impl.backup.UploadTracker;
import com.instaclustr.esop.impl.backup.UploadTracker.UploadUnit;
import com.instaclustr.esop.impl.backup.coordination.ClearSnapshotOperation;
import com.instaclustr.esop.impl.backup.coordination.ClearSnapshotOperation.ClearSnapshotOperationRequest;
import com.instaclustr.esop.impl.backup.coordination.TakeSnapshotOperation;
import com.instaclustr.esop.impl.backup.coordination.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.local.LocalFileBackuper;
import com.instaclustr.esop.local.LocalFileModule;
import com.instaclustr.esop.local.LocalFileRestorer;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.threading.Executors;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
    "localTest",
})
public class LocalBackupTest extends AbstractBackupTest {

    @Inject
    private Optional<OperationCoordinator<BackupOperationRequest>> operationCoordinator;

    @Inject
    private Provider<CassandraVersion> cassandraVersionProvider;

    @Inject
    private CassandraJMXService jmxService;

    @Inject
    private OperationsService operationsService;

    @Override
    protected String protocol() {
        return "file://";
    }

    @BeforeMethod
    public void setup() throws Exception {

        final List<Module> modules = new ArrayList<Module>() {{
            add(new LocalFileModule());
        }};

        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceBackupRestoreTest(inPlaceArguments(CASSANDRA_VERSION));
    }

    @Test
    public void testImportingBackupAndRestoreRenameEntitiesCrossKeyspaces() throws Exception {
        liveBackupRestoreTestRenamedEntities(importArgumentsRenamedTable(CASSANDRA_4_VERSION, IMPORT, true), CASSANDRA_4_VERSION, 2, true);
    }

    @Test
    public void testImportingHardlinksBackupAndRestoreRenameEntitiesCrossKeyspaces() throws Exception {
        liveBackupRestoreTestRenamedEntities(importArgumentsRenamedTable(CASSANDRA_VERSION, HARDLINKS, true), CASSANDRA_VERSION, 2, true);
    }

    @Test
    public void testImportingBackupAndRestoreRenameEntities() throws Exception {
        liveBackupRestoreTestRenamedEntities(importArgumentsRenamedTable(CASSANDRA_4_VERSION, IMPORT, false), CASSANDRA_4_VERSION, 2, false);
    }

    @Test
    public void testImportingHardlinksBackupAndRestoreRenameEntities() throws Exception {
        liveBackupRestoreTestRenamedEntities(importArgumentsRenamedTable(CASSANDRA_VERSION, HARDLINKS, false), CASSANDRA_VERSION, 2, false);
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveBackupRestoreTest(importArguments(CASSANDRA_4_VERSION), CASSANDRA_4_VERSION, 2);
    }

    @Test
    public void testHardlinksBackupAndRestore() throws Exception {
        liveBackupRestoreTest(hardlinkingArguments(CASSANDRA_VERSION), CASSANDRA_VERSION, 2);
    }

    @Test
    public void testImportingOnDifferentSchema() throws Exception {
        liveBackupWithRestoreOnDifferentSchema(restoreByImportingIntoDifferentSchemaArguments(CASSANDRA_4_VERSION, IMPORT), CASSANDRA_4_VERSION);
    }

    @Test
    public void testHardlinksOnDifferentSchema() throws Exception {
        liveBackupWithRestoreOnDifferentSchema(restoreByImportingIntoDifferentSchemaArguments(CASSANDRA_VERSION, HARDLINKS), CASSANDRA_VERSION);
    }

    @Test
    public void testImportingOnDifferentTableSchemaAddColumn() throws Exception {
        liveBackupWithRestoreOnDifferentTableSchema(restoreByImportingIntoDifferentSchemaArguments(CASSANDRA_4_VERSION, IMPORT), CASSANDRA_4_VERSION, true);
    }

    @Test
    public void testHardlinksOnDifferentTableSchemaAddColumn() throws Exception {
        liveBackupWithRestoreOnDifferentTableSchema(restoreByImportingIntoDifferentSchemaArguments(CASSANDRA_VERSION, HARDLINKS), CASSANDRA_VERSION, true);
    }

    @Test
    public void testImportingOnDifferentTableSchemaDropColumn() throws Exception {
        liveBackupWithRestoreOnDifferentTableSchema(restoreByImportingIntoDifferentSchemaArguments(CASSANDRA_4_VERSION, IMPORT), CASSANDRA_4_VERSION, false);
    }

    @Test
    public void testHardlinksOnDifferentTableSchemaDropColumn() throws Exception {
        liveBackupWithRestoreOnDifferentTableSchema(restoreByImportingIntoDifferentSchemaArguments(CASSANDRA_VERSION, HARDLINKS), CASSANDRA_VERSION, false);
    }

    @Test
    public void testDownload() throws Exception {
        try {
            RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();

            FileUtils.createDirectory(Paths.get(target("backup1") + "/cluster/test-dc/1/manifests").toAbsolutePath());

            restoreOperationRequest.storageLocation = new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/1");

            Files.write(Paths.get("target/backup1/cluster/test-dc/1/manifests/snapshot-name-" + UUID.randomUUID().toString()).toAbsolutePath(),
                        "hello".getBytes(),
                        StandardOpenOption.CREATE_NEW
            );

        } finally {
            deleteDirectory(Paths.get(target("backup1")));
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    @Override
    protected String getStorageLocation() {
        return "file://" + target("backup1") + "/cluster/datacenter1/node1";
    }
    @Override
    protected String getStorageLocationForAnotherCluster() {
        return "file://" + target("backup1") + "/cluster2/datacenter1/node1";
    }

    @Test
    public void testUploadTracker() throws Exception {

        final String snapshotName = UUID.randomUUID().toString();
        final String snapshotName2 = UUID.randomUUID().toString();

        List<Path> dataDirs = Arrays.asList(
                cassandraDir.toAbsolutePath().resolve("data").resolve("data"),
                cassandraDir.toAbsolutePath().resolve("data").resolve("data2"),
                cassandraDir.toAbsolutePath().resolve("data").resolve("data3")
        );

        final BackupOperationRequest backupOperationRequest = getBackupOperationRequestForTracker(snapshotName, "test,test2", dataDirs);
        final BackupOperationRequest backupOperationRequest2 = getBackupOperationRequestForTracker(snapshotName2, "test", dataDirs);

        UploadTracker uploadTracker = null;

        Cassandra cassandra = null;

        try {
            cassandra = getCassandra(cassandraDir, CASSANDRA_VERSION);
            cassandra.start();

            try (CqlSession session = CqlSession.builder().build()) {
                assertEquals(populateDatabase(session).size(), NUMBER_OF_INSERTED_ROWS);
            }

            final AtomicBoolean wait = new AtomicBoolean(true);

            final ListeningExecutorService finisher = new Executors.FixedTasksExecutorSupplier().get(10);

            uploadTracker = new UploadTracker(finisher, operationsService, new HashSpec()) {
                // override for testing purposes
                @Override
                public UploadUnit constructUnitToSubmit(final Backuper backuper,
                                                        final ManifestEntry manifestEntry,
                                                        final AtomicBoolean shouldCancel,
                                                        final String snapshotTag,
                                                        final HashSpec hashSpec) {
                    return new TestingUploadUnit(wait, backuper, manifestEntry, shouldCancel, snapshotTag, hashSpec);
                }
            };

            final LocalFileBackuper backuper = new LocalFileBackuper(backupOperationRequest);

            new TakeSnapshotOperation(jmxService,
                                      new TakeSnapshotOperationRequest(backupOperationRequest.entities,
                                                                       backupOperationRequest.snapshotTag),
                                      cassandraVersionProvider).run();

            new TakeSnapshotOperation(jmxService,
                                      new TakeSnapshotOperationRequest(backupOperationRequest2.entities,
                                                                       backupOperationRequest2.snapshotTag),
                                      cassandraVersionProvider).run();

            final Snapshots snapshots = Snapshots.parse(dataDirs);
            final Optional<Snapshot> snapshot = snapshots.get(backupOperationRequest.snapshotTag);
            final Optional<Snapshot> snapshot2 = snapshots.get(backupOperationRequest2.snapshotTag);

            assert snapshot.isPresent();
            assert snapshot2.isPresent();

            Set<String> providers = Stream.of("file").collect(Collectors.toSet());

            final BackupOperation backupOperation = new BackupOperation(operationCoordinator, providers, backupOperationRequest);
            final BackupOperation backupOperation2 = new BackupOperation(operationCoordinator, providers, backupOperationRequest2);

            final List<ManifestEntry> manifestEntries = Manifest.from(snapshot.get()).getManifestEntries();
            final List<ManifestEntry> manifestEntries2 = Manifest.from(snapshot2.get()).getManifestEntries();

            Session<UploadUnit> session = uploadTracker.submit(backuper,
                                                               backupOperation,
                                                               manifestEntries,
                                                               backupOperation.request.snapshotTag,
                                                               backupOperation.request.concurrentConnections);

            final int submittedUnits1 = uploadTracker.submittedUnits.intValue();
            Assert.assertEquals(manifestEntries.size(), submittedUnits1);

            final Session<UploadUnit> session2 = uploadTracker.submit(backuper,
                                                                      backupOperation2,
                                                                      manifestEntries2,
                                                                      backupOperation.request.snapshotTag,
                                                                      backupOperation.request.concurrentConnections);

            final int submittedUnits2 = uploadTracker.submittedUnits.intValue();

            // even we submitted the second session, it does not change the number of units because session2
            // wants to upload "test" but it is already going to be uploaded by session1
            // we have effectively submitted only what should be submitted, no duplicates
            // so it is as if "test" from session2 was not submitted at all

            Assert.assertEquals(submittedUnits1, submittedUnits2);
            Assert.assertEquals(manifestEntries.size(), uploadTracker.submittedUnits.intValue());

            // however we have submitted two sessions in total
            Assert.assertEquals(2, uploadTracker.submittedSessions.intValue());

            // lets upload it now
            wait.set(false);

            session.waitUntilConsideredFinished();
            session2.waitUntilConsideredFinished();

            Assert.assertTrue(session.isConsideredFinished());
            Assert.assertTrue(session.isSuccessful());
            Assert.assertTrue(session.getFailedUnits().isEmpty());

            Assert.assertEquals(uploadTracker.submittedUnits.intValue(), session.getUnits().size());

            Assert.assertTrue(session2.isConsideredFinished());
            Assert.assertTrue(session2.isSuccessful());
            Assert.assertTrue(session2.getFailedUnits().isEmpty());

            Assert.assertTrue(submittedUnits2 > session2.getUnits().size());

            for (final UploadUnit uploadUnit : session2.getUnits()) {
                Assert.assertTrue(session.getUnits().contains(uploadUnit));
            }

            Assert.assertTrue(uploadTracker.getUnits().isEmpty());

            uploadTracker.removeSession(session);
            uploadTracker.removeSession(session2);

            Assert.assertTrue(session.getUnits().isEmpty());
            Assert.assertTrue(session2.getUnits().isEmpty());
        } catch (final Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            new ClearSnapshotOperation(jmxService, new ClearSnapshotOperationRequest(backupOperationRequest.snapshotTag)).run();
            if (cassandra != null) {
                cassandra.stop();
            }
            uploadTracker.stopAsync();
            uploadTracker.awaitTerminated(1, MINUTES);
            uploadTracker.stopAsync();
            uploadTracker.awaitTerminated(1, MINUTES);
            FileUtils.deleteDirectory(Paths.get(target(backupOperationRequest.storageLocation.bucket)));
        }
    }

    private BackupOperationRequest getBackupOperationRequestForTracker(final String snapshotName,
                                                                       final String entities,
                                                                       final List<Path> dataDirs) throws Exception {
        return new BackupOperationRequest(
            "backup",
            new StorageLocationTypeConverter().convert(getStorageLocation()),
            null,
            null,
            null,
            null,
            DatabaseEntities.parse(entities),
            snapshotName,
            null,
            null,
            false,
            null,
            null, // timeout
            false,
            false,
            false,
            null,
            false,
            null, // proxy settings
            null, // retry
            false, // skipRefreshing
            dataDirs,
            null
        );
    }

    private static class TestingUploadUnit extends UploadUnit {

        private final AtomicBoolean wait;

        private final Random random = new Random();

        public TestingUploadUnit(final AtomicBoolean wait,
                                 final Backuper backuper,
                                 final ManifestEntry manifestEntry,
                                 final AtomicBoolean shouldCancel,
                                 final String snapshotTag,
                                 final HashSpec hashSpec) {
            super(backuper, manifestEntry, shouldCancel, snapshotTag, hashSpec);
            this.wait = wait;
        }

        @Override
        public Void call() {
            while (wait.get()) {
                Uninterruptibles.sleepUninterruptibly(random.nextInt(10) + 1, TimeUnit.SECONDS);
            }

            return super.call();
        }
    }
}
