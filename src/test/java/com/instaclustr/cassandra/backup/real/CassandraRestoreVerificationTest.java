package com.instaclustr.cassandra.backup.real;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.instaclustr.cassandra.backup.real.TestEntity.DATE;
import static com.instaclustr.cassandra.backup.real.TestEntity.ID;
import static com.instaclustr.cassandra.backup.real.TestEntity.KEYSPACE;
import static com.instaclustr.cassandra.backup.real.TestEntity.NAME;
import static com.instaclustr.cassandra.backup.real.TestEntity.TABLE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * This profile is meant to be run after "cassandra-backup-restore" Maven profile.
 */
@Test(groups = {"cassandra-restore-verification"})
public class CassandraRestoreVerificationTest extends AbstractBackupRestoreTest {

    @BeforeTest
    public void setup() {
        injectMembers();

        cluster = initCluster();

        session = cluster.connect();

        createSchema(session);

        disableAutoCompaction(KEYSPACE);
    }

    @AfterTest
    public void teardown() {
        if (session != null) {
            session.close();
        }

        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void testRestore() {
        final List<Row> rows = session.execute(select().all().from(KEYSPACE, TABLE)).all();

        for (final Row row : rows) {
            logger.info(format("id: %s, date: %s, name: %s", row.getInt(ID), uuidToDate(row.getUUID(DATE)), row.getString(NAME)));
        }

        assertEquals(rows.size(), Constants.NUMBER_OF_ROWS_AFTER_RESTORATION);
    }

    @Override
    protected void createSchema(Session session) {
        // schema is already created by restore
    }

    @Override
    protected void deleteSchema(Session session) {
        // nothing to delete
    }

    @Override
    protected void cleanup() {
        // nothing to cleanup before tests as we are going to test what is there
    }
}
