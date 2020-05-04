package com.instaclustr.cassandra.backup;

import static org.testng.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.instaclustr.cassandra.backup.impl.StorageLocation;
import org.testng.annotations.Test;

public class StorageLocationTest {

    public static class Clone implements Cloneable {

        public Path myPath;

        public Clone(final Path myPath) {
            this.myPath = myPath;
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

    @Test
    public void updateStorageLocationDatacenterTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/node");

        StorageLocation changedNode = StorageLocation.updateNodeId(storageLocation, "node2");
        StorageLocation changedDc = StorageLocation.updateDatacenter(changedNode, "dc2");

        assertEquals(changedDc.datacenterId, "dc2");
        assertEquals(changedDc.nodeId, "node2");
    }

    @Test
    public void updateStorageLocationTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/global");

        StorageLocation updatedLocation = StorageLocation.updateNodeId(storageLocation, "node2");

        assertEquals(updatedLocation.nodeId, "node2");
    }

    @Test
    public void storageLocationTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/node");

        storageLocation.validate();

        assertEquals(storageLocation.storageProvider, "gcp");
        assertEquals(storageLocation.bucket, "bucket");
        assertEquals(storageLocation.clusterId, "cluster");
        assertEquals(storageLocation.datacenterId, "dc");
        assertEquals(storageLocation.nodeId, "node");
    }

    @Test
    public void fileLocationTest() {
        StorageLocation fileLocation = new StorageLocation("file:///some/path/bucket/cluster/dc/node");

        fileLocation.validate();

        assertEquals(fileLocation.storageProvider, "file");
        assertEquals(fileLocation.fileBackupDirectory.toString(), "/some/path");
        assertEquals(fileLocation.bucket, "bucket");
        assertEquals(fileLocation.clusterId, "cluster");
        assertEquals(fileLocation.datacenterId, "dc");
        assertEquals(fileLocation.nodeId, "node");
    }

    @Test
    public void fileLocationTest2() {
        StorageLocation fileLocation = new StorageLocation("file:///tmp/a/b/c/d/");

        fileLocation.validate();

        assertEquals(fileLocation.storageProvider, "file");
        assertEquals(fileLocation.fileBackupDirectory.toString(), "/tmp");
        assertEquals(fileLocation.bucket, "a");
        assertEquals(fileLocation.clusterId, "b");
        assertEquals(fileLocation.datacenterId, "c");
        assertEquals(fileLocation.nodeId, "d");
    }

    @Test
    public void fileLocationTest3() {
        StorageLocation fileLocation = new StorageLocation("file:///a/b/c/d/");

        fileLocation.validate();

        assertEquals(fileLocation.storageProvider, "file");
        assertEquals(fileLocation.fileBackupDirectory.toString(), Paths.get("").toAbsolutePath().toString());
        assertEquals(fileLocation.bucket, "a");
        assertEquals(fileLocation.clusterId, "b");
        assertEquals(fileLocation.datacenterId, "c");
        assertEquals(fileLocation.nodeId, "d");
    }
}
