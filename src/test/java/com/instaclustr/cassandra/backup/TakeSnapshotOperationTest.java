package com.instaclustr.cassandra.backup;

import static org.testng.Assert.assertEquals;

import com.instaclustr.cassandra.backup.impl.backup.BackupOperation.TakeSnapshotOperation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperation.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TakeSnapshotOperationTest {

    @Test
    public void testParsingOfNoEntities() {
        Assert.assertTrue(getOperation(null).parseEntities().isEmpty());
    }

    @Test
    public void testParsingKeyspaceEntities() {
        assertEquals(getOperation("ks1,ks2,ks3").parseEntities().size(), 3);
        assertEquals(getOperation("ks1,ks2,ks3,").parseEntities().size(), 3);
    }

    @Test
    public void testParsingKeyspaceWithColumnFamilyEntities() {
        assertEquals(getOperation("ks1.cf1,ks2.cf1,ks3.cf1").parseEntities().size(), 3);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testMixedEntities() {
        getOperation("ks1,ks2.cf1,ks3").parseEntities();
    }

    private TakeSnapshotOperation getOperation(String entities) {
        TakeSnapshotOperationRequest request = new TakeSnapshotOperationRequest(entities, "tag");
        return new TakeSnapshotOperation(null, request);
    }
}
