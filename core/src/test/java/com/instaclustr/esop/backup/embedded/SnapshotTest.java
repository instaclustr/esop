package com.instaclustr.esop.backup.embedded;

import com.instaclustr.esop.impl.Snapshots;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SnapshotTest {

    @Test
    public void snapshotTagContainsTimestamp() {
        Assertions.assertTrue(Snapshots.snapshotContainsTimestamp("snapshot-some-uuid-1234567"));
        Assertions.assertFalse(Snapshots.snapshotContainsTimestamp("snapshot-some-uuid"));
        Assertions.assertFalse(Snapshots.snapshotContainsTimestamp("snapshot"));
        Assertions.assertFalse(Snapshots.snapshotContainsTimestamp("-123456"));
        Assertions.assertFalse(Snapshots.snapshotContainsTimestamp("-123456-"));
        Assertions.assertFalse(Snapshots.snapshotContainsTimestamp("123456-"));
    }

}
