package com.instaclustr.esop.backup.embedded.local;

import org.testng.annotations.Test;

@Test(groups = {
"localTest", "cassandra5"
})
public class Cassandra5LocalBackupTest extends AbstractLocalBackupTest
{
    @Override
    public String getCassandraVersion() {
        return CASSANDRA_5_VERSION;
    }
}
