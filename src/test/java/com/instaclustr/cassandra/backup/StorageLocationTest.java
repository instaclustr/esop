package com.instaclustr.cassandra.backup;

import static org.testng.Assert.assertEquals;

import com.instaclustr.cassandra.backup.impl.StorageLocation;
import org.testng.annotations.Test;

public class StorageLocationTest {

    @Test
    public void storageLocationTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/node");

        assertEquals(storageLocation.storageProvider, "gcp");
        assertEquals(storageLocation.bucket, "bucket");
        assertEquals(storageLocation.clusterId, "cluster");
        assertEquals(storageLocation.datacenterId, "dc");
        assertEquals(storageLocation.nodeId, "node");

        storageLocation.validate();

        StorageLocation fileLocation = new StorageLocation("file:///some/path/bucket/cluster/dc/node");

        assertEquals(fileLocation.storageProvider, "file");
        assertEquals(fileLocation.fileBackupDirectory.toString(), "/some/path");
        assertEquals(fileLocation.bucket, "bucket");
        assertEquals(fileLocation.clusterId, "cluster");
        assertEquals(fileLocation.datacenterId, "dc");
        assertEquals(fileLocation.nodeId, "node");

        fileLocation.validate();
    }
}
