package com.instaclustr.esop.backup.embedded;

import com.instaclustr.esop.impl.Snapshots;
import org.junit.Assert;
import org.junit.Test;

public class SnapshotTest {

    @Test
    public void snapshotTagContainsTimestamp() {
        Assert.assertTrue(Snapshots.snapshotContainsTimestamp("snapshot-some-uuid-1234567"));
        Assert.assertFalse(Snapshots.snapshotContainsTimestamp("snapshot-some-uuid"));
        Assert.assertFalse(Snapshots.snapshotContainsTimestamp("snapshot"));
        Assert.assertFalse(Snapshots.snapshotContainsTimestamp("-123456"));
        Assert.assertFalse(Snapshots.snapshotContainsTimestamp("-123456-"));
        Assert.assertFalse(Snapshots.snapshotContainsTimestamp("123456-"));
    }

}
