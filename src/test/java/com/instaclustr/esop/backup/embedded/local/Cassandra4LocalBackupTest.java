package com.instaclustr.esop.backup.embedded.local;

import org.testng.annotations.Test;

@Test(groups = {
"localTest", "cassandra4"
})
public class Cassandra4LocalBackupTest extends AbstractLocalBackupTest
{
    @Override
    public String getCassandraVersion() {
        return CASSANDRA_4_VERSION;
    }
}
