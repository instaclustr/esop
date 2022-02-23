package com.instaclustr.esop.backup.embedded;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.google.inject.*;
import com.google.inject.Module;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.cli.Esop;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.OperationsService;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static com.instaclustr.esop.backup.embedded.TestEntity.KEYSPACE;
import static com.instaclustr.esop.backup.embedded.TestEntity.TABLE;
import static com.instaclustr.esop.backup.embedded.TestEntity2.KEYSPACE_2;
import static com.instaclustr.esop.backup.embedded.TestEntity2.TABLE_2;
import static java.nio.file.Files.createTempFile;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public abstract class BaseListingRemovalTest extends AbstractBackupTest {

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

        List<Module> modules = getModules();
        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    protected abstract List<Module> getModules();

    protected abstract String protocol();

    @Test
    public void testListingAndBackup() throws Exception {
        Cassandra cassandra = getCassandra(cassandraDir, CASSANDRA_VERSION);
        cassandra.start();

        waitForCql();

        String[][] arguments = hardlinkingArguments(CASSANDRA_VERSION);

        try (CqlSession session = CqlSession.builder().build()) {

            createTable(session, KEYSPACE, TABLE);
            createTable(session, KEYSPACE_2, TABLE_2);

            insertAndCallBackupCLI(2, session, arguments[0]); // stefansnapshot-1
            insertAndCallBackupCLI(2, session, arguments[1]); // stefansnapshot-2
            insertAndCallBackupCLI(2, session, arguments[6]); // stefansnapshot-2 in different storage location


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
                dumpTableAndAssertRowCount(session, KEYSPACE, TABLE, 4);
                dumpTableAndAssertRowCount(session, KEYSPACE_2, TABLE_2, 4);

                // listing

                final Path jsonComplexFile = createTempFile("esop-backup-json-complex", null);
                final Path jsonSimpleFile = createTempFile("esop-backup-json-simple", null);
                final Path tableComplexFile = createTempFile("esop-backup-table-complex", null);
                final Path tableSimpleFile = createTempFile("esop-backup-table-simple", null);

                final String[] jsonComplex = new String[]{
                        "list",
                        "--storage-location=" + getStorageLocation(),
                        "--human-units",
                        "--json",
                        "--to-file=" + jsonComplexFile.toAbsolutePath(),
                        "--cache-dir=" + target(".esop")
                };

                final String[] jsonSimple = new String[]{
                        "list",
                        "--storage-location=" + getStorageLocation(),
                        "--human-units",
                        "--json",
                        "--simple-format",
                        "--to-file=" + jsonSimpleFile.toAbsolutePath(),
                        "--cache-dir=" + target(".esop")
                };

                final String[] tableComplex = new String[]{
                        "list",
                        "--storage-location=" + getStorageLocation(),
                        "--human-units",
                        "--to-file=" + tableComplexFile.toAbsolutePath(),
                        "--cache-dir=" + target(".esop")
                };

                final String[] tableSimple = new String[]{
                        "list",
                        "--storage-location=" + getStorageLocation(),
                        "--human-units",
                        "--simple-format",
                        "--to-file=" + tableSimpleFile.toAbsolutePath(),
                        "--cache-dir=" + target(".esop")
                };

                logger.info("Executing listing of json complex format: " + asList(jsonComplex));
                Esop.mainWithoutExit(jsonComplex);
                logger.info("Executing listing of json simple format: " + asList(jsonSimple));
                Esop.mainWithoutExit(jsonSimple);
                logger.info("Executing listing of table complex format: " + asList(tableComplex));
                Esop.mainWithoutExit(tableComplex);
                logger.info("Executing listing of table simple format: " + asList(tableSimple));
                Esop.mainWithoutExit(tableSimple);

                Manifest.AllManifestsReport report = objectMapper.readValue(Files.readAllBytes(jsonComplexFile), Manifest.AllManifestsReport.class);

                Optional<Manifest.ManifestReporter.ManifestReport> oldest = report.getOldest();
                Optional<Manifest.ManifestReporter.ManifestReport> latest = report.getLatest();

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
                        "--backup-name=" + oldestBackupName,
                        "--cache-dir=" + target(".esop")
                };

                logger.info("Executing the first delete - {}", asList(delete1));

                // delete oldest
                Esop.mainWithoutExit(delete1);

                final String[] delete2 = new String[]{
                        "remove-backup",
                        "--storage-location=" + getStorageLocation(),
                        "--backup-name=" + latestBackupName,
                        "--cache-dir=" + target(".esop")
                };

                logger.info("Executing the second delete - {}", asList(delete2));

                // delete latest
                Esop.mainWithoutExit(delete2);

                // we basically deleted everything in the first storage location by deleting first two backups
                //assertEquals(Files.list(Paths.get(getStorageLocation().replaceAll(protocol(), ""), "data")).count(), 0);
                String s = getStorageLocation().replaceAll(protocol(), "");
                Path manifests = Paths.get(target(".esop")).resolve(s).resolve("manifests");
                assertEquals(0, Files.list(manifests).count());
            } finally {
                cassandra.stop();
            }
        }
    }
}
