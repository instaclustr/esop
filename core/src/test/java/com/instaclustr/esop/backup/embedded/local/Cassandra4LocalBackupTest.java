package com.instaclustr.esop.backup.embedded.local;

import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;

@Tags({
        @Tag("localTest"),
        @Tag("cassandra4")
})
public class Cassandra4LocalBackupTest extends AbstractLocalBackupTest
{
    @Override
    public String getCassandraVersion() {
        return AbstractBackupTest.CASSANDRA_4_VERSION;
    }
}
