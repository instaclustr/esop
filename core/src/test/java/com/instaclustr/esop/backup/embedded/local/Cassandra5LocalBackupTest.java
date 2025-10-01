package com.instaclustr.esop.backup.embedded.local;

import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;

@Tags({
        @Tag("localTest"),
        @Tag("cassandra5")
})
public class Cassandra5LocalBackupTest extends AbstractLocalBackupTest {
    @Override
    public String getCassandraVersion() {
        return AbstractBackupTest.CASSANDRA_5_VERSION;
    }
}
