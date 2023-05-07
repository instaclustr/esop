package com.instaclustr.esop.backup;

import java.nio.file.Paths;

import com.instaclustr.esop.impl.StorageLocation;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class StorageLocationTest {

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

    @Test
    public void globalLocationTest() {
        StorageLocation globalLocation = new StorageLocation("oracle://my-bucket");

        globalLocation.validate();

        assertEquals(globalLocation.storageProvider, "oracle");
        assertEquals(globalLocation.bucket, "my-bucket");
        assertNull(globalLocation.clusterId);
        assertNull(globalLocation.datacenterId);
        assertNull(globalLocation.nodeId);
        assertTrue(globalLocation.cloudLocation);
        assertTrue(globalLocation.globalRequest);
    }

    @Test
    public void updateGlobalLocationTest() {
        StorageLocation globalLocation = new StorageLocation("oracle://my-bucket");

        StorageLocation updated = StorageLocation.update(globalLocation, "clusterName", "datacenterId", "nodeId");

        assertEquals(updated.storageProvider, "oracle");
        assertEquals(updated.bucket, "my-bucket");
        assertEquals(updated.clusterId, "clusterName");
        assertEquals(updated.datacenterId, "datacenterId");
        assertEquals(updated.nodeId, "nodeId");
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void updateNodeIdLocationTest() {
        StorageLocation location = new StorageLocation("oracle://my-bucket/clusterName/datacenterId/nodeId");

        StorageLocation updated = StorageLocation.updateNodeId(location, "nodeId2");

        assertEquals(updated.storageProvider, "oracle");
        assertEquals(updated.bucket, "my-bucket");
        assertEquals(updated.clusterId, "clusterName");
        assertEquals(updated.datacenterId, "datacenterId");
        assertEquals(updated.nodeId, "nodeId2");
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void updateDatacenterIdLocationTest() {
        StorageLocation location = new StorageLocation("oracle://my-bucket/clusterName/datacenterId/nodeId");

        StorageLocation updated = StorageLocation.updateDatacenter(location, "datacenterId2");

        assertEquals(updated.storageProvider, "oracle");
        assertEquals(updated.bucket, "my-bucket");
        assertEquals(updated.clusterId, "clusterName");
        assertEquals(updated.datacenterId, "datacenterId2");
        assertEquals(updated.nodeId, "nodeId");
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void updateClusterNameLocationTest() {
        StorageLocation location = new StorageLocation("oracle://my-bucket/clusterName/datacenterId/nodeId");

        StorageLocation updated = StorageLocation.updateClusterName(location, "clusterName2");

        assertEquals(updated.storageProvider, "oracle");
        assertEquals(updated.bucket, "my-bucket");
        assertEquals(updated.clusterId, "clusterName2");
        assertEquals(updated.datacenterId, "datacenterId");
        assertEquals(updated.nodeId, "nodeId");
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }
}
