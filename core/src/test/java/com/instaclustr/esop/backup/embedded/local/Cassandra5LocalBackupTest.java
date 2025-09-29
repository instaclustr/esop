package com.instaclustr.esop.backup.embedded.local;

import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import org.junit.jupiter.api.Tag;

@Tag("local-test")
@Tag("cassandra5")
public class Cassandra5LocalBackupTest extends AbstractLocalBackupTest {
    @Override
    public String getCassandraVersion() {
        return AbstractBackupTest.CASSANDRA_5_VERSION;
    }
}
