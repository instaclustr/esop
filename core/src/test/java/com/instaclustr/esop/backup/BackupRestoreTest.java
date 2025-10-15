package com.instaclustr.esop.backup;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.SSTableUtils;
import com.instaclustr.esop.impl.hash.HashSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BackupRestoreTest {

    public static final CassandraVersion THREE = CassandraVersion.parse("3.0.0");

    private final static String sha1Hash = "3a1bd6900872256303b1ed036881cd35f5b670ce";
    private final static String testSnapshotName = "testSnapshot";
    private final Long independentChecksum = 2973505342L;

    private final static List<TestFileConfig> versionsToTest = ImmutableList.of(
        new TestFileConfig(sha1Hash, THREE)
    );


    private static final Map<String, Path> tempDirs = new LinkedHashMap<>();

    @BeforeAll
    public static void setup() throws IOException {
        for (TestFileConfig testFileConfig : versionsToTest) {
            Path containerTempRoot = Files.createTempDirectory(testFileConfig.cassandraVersion.toString());
            Path containerBackupRoot = Files.createTempDirectory(testFileConfig.cassandraVersion.toString());
            BackupRestoreTestUtils.createTempDirectories(containerTempRoot, BackupRestoreTestUtils.cleanableDirs);
            BackupRestoreTestUtils.createTempDirectories(containerTempRoot, BackupRestoreTestUtils.uncleanableDirs);
            tempDirs.put(testFileConfig.cassandraVersion.toString(), containerTempRoot);
            tempDirs.put(testFileConfig.cassandraVersion.toString() + "-backup-location", containerBackupRoot);

        }
        BackupRestoreTestUtils.resetDirectories(versionsToTest, tempDirs, testSnapshotName);
    }

    @Test
    // Check that we are checksumming properly
    public void testCalculateDigest() throws Exception {
        for (TestFileConfig testFileConfig : versionsToTest) {
            final String keyspace = "keyspace1";
            final String table1 = "table1";
            final Path table1Path = tempDirs.get(testFileConfig.cassandraVersion.toString()).resolve("data/" + keyspace + "/" + table1);
            final Path path = table1Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table1)));
            final String checksum = SSTableUtils.calculateChecksum(path);
            assertEquals(checksum, String.valueOf(independentChecksum));
        }
    }

    @BeforeEach
    public void hardResetTestDirs() throws IOException, URISyntaxException {
        cleanUp();
        setup();
    }

    @Test
    // Test that the manifest is correctly constructed, includes expected files and generates checksum if necessary
    @Disabled
    public void testSSTableLister() throws Exception {
        hardResetTestDirs(); //TODO not sure why this doesn't recreate things fully given its called before each test
        for (TestFileConfig testFileConfig : versionsToTest) {
            Path backupRoot = Paths.get("/backupRoot/keyspace1");

            final String keyspace = "keyspace1";
            final String table1 = "table1";
            final Path table1Path = tempDirs.get(testFileConfig.cassandraVersion.toString()).resolve("data/" + keyspace + "/" + table1);
            Map<String, List<ManifestEntry>> sstables = SSTableUtils.getSSTables(keyspace, table1, table1Path, backupRoot.resolve(table1Path.getFileName()), new HashSpec());

            final String table2 = "table2";
            final Path table2Path = tempDirs.get(testFileConfig.cassandraVersion.toString()).resolve("data/" + keyspace + "/" + table2);
            sstables.putAll(SSTableUtils.getSSTables(keyspace, table2, table2Path, backupRoot.resolve(table2Path.getFileName()), new HashSpec()));

            Map<Path, Path> manifestMap = new HashMap<>();
            for (ManifestEntry e : sstables.values().stream().flatMap(Collection::stream).collect(Collectors.toList())) {
                manifestMap.put(e.localFile, e.objectKey);
            }

            if (CassandraVersion.isTwoZero(testFileConfig.cassandraVersion)) {
                // table1 is un-compressed so should have written out a sha1 digest
                final Path localPath1 = table1Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table1)));

                assertEquals(manifestMap.get(localPath1),
                        backupRoot.resolve(String.format("%s/1-%s/%s-1-big-Data.db", table1, sha1Hash, testFileConfig.getSstablePrefix(keyspace, table1))));

                final Path localPath2 = table1Path.resolve(String.format("%s-3-big-Index.db", testFileConfig.getSstablePrefix(keyspace, table1)));
                final String checksum2 = SSTableUtils.calculateChecksum(localPath2);

                assertEquals(manifestMap.get(localPath2),
                        backupRoot.resolve(String.format("%s/3-%s/%s-3-big-Index.db", table1, checksum2, testFileConfig.getSstablePrefix(keyspace, table1))));

                final Path localPath3 = table2Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table2)));
                final String checksum3 = SSTableUtils.calculateChecksum(localPath3);

                assertEquals(manifestMap.get(localPath3),
                        backupRoot.resolve(String.format("%s/1-%s/%s-1-big-Data.db", table2, checksum3, testFileConfig.getSstablePrefix(keyspace, table2))));

                assertNull(manifestMap.get(table2Path.resolve(String.format("%s-3-big-Index.db", testFileConfig.getSstablePrefix(keyspace, table2)))));
            } else {
                Path resolve = table1Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table1)));

                assertEquals(manifestMap.get(resolve),
                        backupRoot.resolve(String.format("%s/1-1000000000/%s-1-big-Data.db", table1, testFileConfig.getSstablePrefix(keyspace, table1))));

                // Cassandra doesn't create CRC32 file for 2.0.x
                assertEquals(manifestMap.get(table1Path.resolve(String.format("%s-2-big-Digest.crc32", testFileConfig.getSstablePrefix(keyspace, table1)))),
                        backupRoot.resolve(String.format("%s/2-1000000000/%s-2-big-Digest.crc32", table1, testFileConfig.getSstablePrefix(keyspace, table1))));

                assertEquals(manifestMap.get(table1Path.resolve(String.format("%s-3-big-Index.db", testFileConfig.getSstablePrefix(keyspace, table1)))),
                        backupRoot.resolve(String.format("%s/3-1000000000/%s-3-big-Index.db", table1, testFileConfig.getSstablePrefix(keyspace, table1))));

                assertEquals(manifestMap.get(table2Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table2)))),
                        backupRoot.resolve(String.format("%s/1-1000000000/%s-1-big-Data.db", table2, testFileConfig.getSstablePrefix(keyspace, table2))));

                assertEquals(manifestMap.get(table2Path.resolve(String.format("%s-2-big-Digest.crc32", testFileConfig.getSstablePrefix(keyspace, table2)))),
                        backupRoot.resolve(String.format("%s/2-1000000000/%s-2-big-Digest.crc32", table2, testFileConfig.getSstablePrefix(keyspace, table2))));

                assertNull(manifestMap.get(table2Path.resolve(String.format("%s-3-big-Index.db", testFileConfig.getSstablePrefix(keyspace, table2)))));
            }

            assertNull(manifestMap.get(table1Path.resolve("manifest.json")));
            assertNull(manifestMap.get(table1Path.resolve("backups")));
            assertNull(manifestMap.get(table1Path.resolve("snapshots")));
        }
    }



    @AfterEach
    public void cleanUp() throws IOException {
        BackupRestoreTestUtils.deleteTempDirectories(tempDirs);
    }
}