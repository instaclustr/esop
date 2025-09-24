package com.instaclustr.esop.backup.embedded.local;

import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import org.testng.annotations.Test;

@Test(groups = {
"localTest", "cassandra5"
})
public class Cassandra5LocalBackupTest extends AbstractLocalBackupTest
{
    @Override
    public String getCassandraVersion() {
        return AbstractBackupTest.CASSANDRA_5_VERSION;
    }
}
