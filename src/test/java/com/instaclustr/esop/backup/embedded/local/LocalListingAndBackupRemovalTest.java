package com.instaclustr.esop.backup.embedded.local;

import static com.instaclustr.esop.backup.embedded.TestEntity.KEYSPACE;
import static com.instaclustr.esop.backup.embedded.TestEntity.TABLE;
import static com.instaclustr.esop.backup.embedded.TestEntity2.KEYSPACE_2;
import static com.instaclustr.esop.backup.embedded.TestEntity2.TABLE_2;
import static com.instaclustr.io.FileUtils.deleteDirectory;
import static java.nio.file.Files.createTempFile;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.cli.Esop;
import com.instaclustr.esop.impl.Manifest.AllManifestsReport;
import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.local.LocalFileModule;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.OperationsService;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
    "localTest",
})
public class LocalListingAndBackupRemovalTest extends AbstractBackupTest {

    @Inject
    private Optional<OperationCoordinator<BackupOperationRequest>> operationCoordinator;

    @Inject
    private Provider<CassandraVersion> cassandraVersionProvider;

    @Inject
    private CassandraJMXService jmxService;

    @Inject
    private OperationsService operationsService;

    @Inject
    private ObjectMapper objectMapper;

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

    @Override
    protected String getStorageLocation() {
        return "file://" + target("backup1") + "/cluster/datacenter1/node1";
    }

    @Test
    public void testHardlinksBackupAndRestore() throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, CASSANDRA_VERSION);
        cassandra.start();

        waitForCql();

        String[][] arguments = hardlinkingArguments(CASSANDRA_VERSION);

        try (CqlSession session = CqlSession.builder().build()) {

            createTable(session, KEYSPACE, TABLE);
            createTable(session, KEYSPACE_2, TABLE_2);

            insertAndCallBackupCLI(2, session, arguments[0]); // stefansnapshot-1
            insertAndCallBackupCLI(2, session, arguments[1]); // stefansnapshot-2

            try {
                logger.info("Executing the first restoration phase - download {}", asList(arguments[2]));
                Esop.mainWithoutExit(arguments[2]);
                logger.info("Executing the second restoration phase - truncate {}", asList(arguments[3]));
                Esop.mainWithoutExit(arguments[3]);
                logger.info("Executing the third restoration phase - import {}", asList(arguments[4]));
                Esop.mainWithoutExit(arguments[4]);
                logger.info("Executing the fourth restoration phase - cleanup {}", asList(arguments[5]));
                Esop.mainWithoutExit(arguments[5]);

                // we expect 4 records to be there as 2 were there before the first backup and the second 2 before the second backup
                dumpTable(session, KEYSPACE, TABLE, 4);
                dumpTable(session, KEYSPACE_2, TABLE_2, 4);

                // listing

                final Path jsonComplexFile = createTempFile("esop-backup-json-complex", null);
                final Path jsonSimpleFile = createTempFile("esop-backup-json-simple", null);
                final Path tableComplexFile = createTempFile("esop-backup-table-complex", null);
                final Path tableSimpleFile = createTempFile("esop-backup-table-simple", null);

                final String[] jsonComplex = new String[]{
                    "list",
                    "--storage-location=" + getStorageLocation(),
                    "--skip-node-resolution",
                    "--human-units",
                    "--json",
                    "--to-file=" + jsonComplexFile.toAbsolutePath().toString()
                };

                final String[] jsonSimple = new String[]{
                    "list",
                    "--storage-location=" + getStorageLocation(),
                    "--skip-node-resolution",
                    "--human-units",
                    "--json",
                    "--simple-format",
                    "--to-file=" + jsonSimpleFile.toAbsolutePath().toString()
                };

                final String[] tableComplex = new String[]{
                    "list",
                    "--storage-location=" + getStorageLocation(),
                    "--skip-node-resolution",
                    "--human-units",
                    "--to-file=" + tableComplexFile.toAbsolutePath().toString()
                };

                final String[] tableSimple = new String[]{
                    "list",
                    "--storage-location=" + getStorageLocation(),
                    "--skip-node-resolution",
                    "--human-units",
                    "--simple-format",
                    "--to-file=" + tableSimpleFile.toAbsolutePath().toString()
                };

                logger.info("Executing listing of json complex format: " + asList(jsonComplex));
                Esop.mainWithoutExit(jsonComplex);
                logger.info("Executing listing of json simple format: " + asList(jsonSimple));
                Esop.mainWithoutExit(jsonSimple);
                logger.info("Executing listing of table complex format: " + asList(tableComplex));
                Esop.mainWithoutExit(tableComplex);
                logger.info("Executing listing of table simple format: " + asList(tableSimple));
                Esop.mainWithoutExit(tableSimple);

                AllManifestsReport report = objectMapper.readValue(Files.readAllBytes(jsonComplexFile), AllManifestsReport.class);

                Optional<ManifestReport> oldest = report.getOldest();
                Optional<ManifestReport> latest = report.getLatest();

                if (!oldest.isPresent()) {
                    Assert.fail("Not found the oldest report!");
                }

                if (!latest.isPresent()) {
                    Assert.fail("Not found the latest report!");
                }

                final String oldestBackupName = oldest.get().name;
                final String latestBackupName = latest.get().name;

                final String[] delete1 = new String[]{
                    "remove-backup",
                    "--storage-location=" + getStorageLocation(),
                    "--skip-node-resolution",
                    "--backup-name=" + oldestBackupName
                };

                logger.info("Executing the first delete - {}", asList(delete1));

                // delete oldest
                Esop.mainWithoutExit(delete1);

                final String[] delete2 = new String[]{
                    "remove-backup",
                    "--storage-location=" + getStorageLocation(),
                    "--skip-node-resolution",
                    "--backup-name=" + latestBackupName
                };

                logger.info("Executing the second delete - {}", asList(delete2));

                // delete latest
                Esop.mainWithoutExit(delete2);

                // we basically deleted everything by deleting both backups
                assertEquals(Files.list(Paths.get(getStorageLocation().replaceAll("file://", ""), "data")).count(), 0);
                assertEquals(Files.list(Paths.get(getStorageLocation().replaceAll("file://", ""), "manifests")).count(), 0);
            } finally {
                cassandra.stop();
            }
        }

        FileUtils.deleteDirectory(cassandraDir);
        deleteDirectory(Paths.get(target("backup1")));
    }
}
