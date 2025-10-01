package com.instaclustr.esop.backup;

import java.nio.file.Paths;

import com.instaclustr.esop.impl.StorageLocation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StorageLocationTest {

    @Test
    public void updateStorageLocationDatacenterTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/node");

        StorageLocation changedNode = StorageLocation.updateNodeId(storageLocation, "node2");
        StorageLocation changedDc = StorageLocation.updateDatacenter(changedNode, "dc2");

        assertEquals("dc2", changedDc.datacenterId);
        assertEquals( "node2", changedDc.nodeId);
    }

    @Test
    public void updateStorageLocationTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/global");

        StorageLocation updatedLocation = StorageLocation.updateNodeId(storageLocation, "node2");

        assertEquals("node2", updatedLocation.nodeId);
    }

    @Test
    public void storageLocationTest() {
        StorageLocation storageLocation = new StorageLocation("gcp://bucket/cluster/dc/node");

        storageLocation.validate();

        assertEquals("gcp", storageLocation.storageProvider);
        assertEquals("bucket", storageLocation.bucket);
        assertEquals("cluster", storageLocation.clusterId);
        assertEquals("dc", storageLocation.datacenterId);
        assertEquals("node", storageLocation.nodeId);
    }

    @Test
    public void fileLocationTest() {
        StorageLocation fileLocation = new StorageLocation("file:///some/path/bucket/cluster/dc/node");

        fileLocation.validate();

        assertEquals("file", fileLocation.storageProvider);
        assertEquals("/some/path", fileLocation.fileBackupDirectory.toString());
        assertEquals("bucket", fileLocation.bucket);
        assertEquals("cluster", fileLocation.clusterId);
        assertEquals("dc", fileLocation.datacenterId);
        assertEquals("node", fileLocation.nodeId);
    }

    @Test
    public void fileLocationTest2() {
        StorageLocation fileLocation = new StorageLocation("file:///tmp/a/b/c/d/");

        fileLocation.validate();

        assertEquals("file", fileLocation.storageProvider);
        assertEquals("/tmp", fileLocation.fileBackupDirectory.toString());
        assertEquals("a", fileLocation.bucket);
        assertEquals("b", fileLocation.clusterId);
        assertEquals("c", fileLocation.datacenterId);
        assertEquals("d", fileLocation.nodeId);
    }

    @Test
    public void fileLocationTest3() {
        StorageLocation fileLocation = new StorageLocation("file:///a/b/c/d/");

        fileLocation.validate();

        assertEquals("file", fileLocation.storageProvider);
        assertEquals(Paths.get("").toAbsolutePath().toString(), fileLocation.fileBackupDirectory.toString());
        assertEquals("a", fileLocation.bucket);
        assertEquals("b", fileLocation.clusterId);
        assertEquals("c", fileLocation.datacenterId);
        assertEquals("d", fileLocation.nodeId);
    }

    @Test
    public void globalLocationTest() {
        StorageLocation globalLocation = new StorageLocation("oracle://my-bucket");

        globalLocation.validate();

        assertEquals("oracle", globalLocation.storageProvider);
        assertEquals("my-bucket", globalLocation.bucket);
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

        assertEquals("oracle", updated.storageProvider);
        assertEquals("my-bucket", updated.bucket);
        assertEquals("clusterName", updated.clusterId);
        assertEquals("datacenterId", updated.datacenterId);
        assertEquals("nodeId", updated.nodeId);
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void updateNodeIdLocationTest() {
        StorageLocation location = new StorageLocation("oracle://my-bucket/clusterName/datacenterId/nodeId");

        StorageLocation updated = StorageLocation.updateNodeId(location, "nodeId2");

        assertEquals("oracle", updated.storageProvider);
        assertEquals("my-bucket", updated.bucket);
        assertEquals("clusterName", updated.clusterId);
        assertEquals("datacenterId", updated.datacenterId);
        assertEquals("nodeId2", updated.nodeId);
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void updateDatacenterIdLocationTest() {
        StorageLocation location = new StorageLocation("oracle://my-bucket/clusterName/datacenterId/nodeId");

        StorageLocation updated = StorageLocation.updateDatacenter(location, "datacenterId2");

        assertEquals("oracle", updated.storageProvider);
        assertEquals("my-bucket", updated.bucket);
        assertEquals("clusterName", updated.clusterId);
        assertEquals("datacenterId2", updated.datacenterId);
        assertEquals("nodeId", updated.nodeId);
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }

    @Test
    public void updateClusterNameLocationTest() {
        StorageLocation location = new StorageLocation("oracle://my-bucket/clusterName/datacenterId/nodeId");

        StorageLocation updated = StorageLocation.updateClusterName(location, "clusterName2");

        assertEquals("oracle", updated.storageProvider);
        assertEquals("my-bucket", updated.bucket);
        assertEquals("clusterName2", updated.clusterId);
        assertEquals("datacenterId", updated.datacenterId);
        assertEquals("nodeId", updated.nodeId);
        assertTrue(updated.cloudLocation);
        assertFalse(updated.globalRequest);
    }
}
