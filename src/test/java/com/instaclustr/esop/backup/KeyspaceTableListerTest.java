package com.instaclustr.esop.backup;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.instaclustr.esop.impl.CassandraData.KeyspaceTableLister;
import com.instaclustr.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyspaceTableListerTest {

    private static final Logger logger = LoggerFactory.getLogger(KeyspaceTableLister.class);

    @Test
    public void testKeyspaceTableLister() throws Exception {
        Path cassandrDir = Files.createTempDirectory("keyspace-table-lister");

        // in total 20000 files in 1000 tables in 100 keyspaces

        for (int i = 0; i < 90; i++) {
            Files.createDirectory(cassandrDir.resolve("ks" + i));
            for (int j = 0; j < 10; j++) {
                Files.createDirectory(cassandrDir.resolve("ks" + i + "/tb-" + j));
                for (int k = 0; k < 20; k++) {
                    Files.createFile(cassandrDir.resolve("ks" + i + "/tb-" + j + "/file" + k));
                }
            }
        }

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                Files.createDirectory(cassandrDir.resolve("ks" + i + "/tb-" + j + "/snapshots"));
                Files.createDirectory(cassandrDir.resolve("ks" + i + "/tb-" + j + "/snapshots/dropped-3424324"));
            }
        }

        // keyspaces from no. 90 - 99 will have all tables dropped which renders them dropped too

        for (int i = 90; i < 99; i++) {
            Files.createDirectory(cassandrDir.resolve("ks" + i));
            for (int j = 0; j < 5; j++) {
                Files.createDirectory(cassandrDir.resolve("ks" + i + "/tb-" + j));
                Files.createDirectory(cassandrDir.resolve("ks" + i + "/tb-" + j + "/snapshots"));
                Files.createDirectory(cassandrDir.resolve("ks" + i + "/tb-" + j + "/snapshots/dropped-3424324"));
            }
        }

        KeyspaceTableLister lister = new KeyspaceTableLister(cassandrDir);
        Files.walkFileTree(cassandrDir, lister);

        lister.removeDroppedKeyspaces();

        Map<Path, List<Path>> dataDirs = lister.getDataDirs();
        Assert.assertFalse(dataDirs.isEmpty());

        Assert.assertEquals(90, dataDirs.size());

        // 25 tables are dropped and 10 keyspaces, each having 10 tables, have all tables dropped: 1000 - 100 - 25 = 875
        Assert.assertEquals(875, dataDirs.values().stream().map(List::size).reduce(0, Integer::sum).intValue());

        FileUtils.deleteDirectory(cassandrDir);
    }
}
