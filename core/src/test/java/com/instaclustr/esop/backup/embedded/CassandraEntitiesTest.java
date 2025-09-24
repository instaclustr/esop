package com.instaclustr.esop.backup.embedded;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.impl.CassandraData;
import com.instaclustr.esop.local.LocalFileModule;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class CassandraEntitiesTest extends AbstractBackupTest {

    @Inject
    private CassandraJMXService jmxService;

    private Cassandra cassandra;
    private CqlSession session;

    @BeforeMethod
    public void setup() throws Throwable {

        cassandra = getCassandra(cassandraDir, getCassandraVersion());
        cassandra.start();

        waitForCql();

        session = CqlSession.builder().build();

        final List<Module> modules = new ArrayList<Module>() {{
            add(new LocalFileModule());
        }};

        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() {
        cassandra.stop();
        session.close();
    }

    @Override
    protected String protocol() {
        return "file://";
    }

    @Test
    private void test() throws Throwable {

        session.execute("CREATE KEYSPACE IF NOT EXISTS test1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

        CassandraData parse = CassandraData.parse(jmxService);

        System.out.println(parse);
    }
}
