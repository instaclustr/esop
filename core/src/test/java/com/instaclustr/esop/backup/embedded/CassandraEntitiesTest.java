package com.instaclustr.esop.backup.embedded;

import java.util.ArrayList;
import java.util.List;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.impl.CassandraData;
import com.instaclustr.esop.local.LocalFileModule;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CassandraEntitiesTest extends AbstractBackupTest {

    @Inject
    private CassandraJMXService jmxService;

    private Cassandra cassandra;
    private CqlSession session;

    @BeforeEach
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

    @AfterEach
    public void teardown() {
        cassandra.stop();
        session.close();
    }

    @Override
    protected String protocol() {
        return "file://";
    }

    @Test
    public void test() throws Throwable {

        session.execute("CREATE KEYSPACE IF NOT EXISTS test1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

        CassandraData parse = CassandraData.parse(jmxService);

        System.out.println(parse);
    }
}
