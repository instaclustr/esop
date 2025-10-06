package com.instaclustr.cassandra;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.CassandraJMXConnectionInfo;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.CassandraJMXServiceImpl;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraModule extends AbstractModule {

    private final CassandraJMXConnectionInfo jmxConnectionInfo;

    public CassandraModule() throws Exception {
        this(new CassandraJMXConnectionInfo());
    }

    public CassandraModule(final CassandraJMXConnectionInfo jmxConnectionInfo) {
        this.jmxConnectionInfo = jmxConnectionInfo;
    }

    @Singleton
    @Provides
    CassandraJMXConnectionInfo provideJmxConnectionInfo() {
        return jmxConnectionInfo;
    }

    @Singleton
    @Provides
    CassandraJMXService provideCassandraJMXService(final CassandraJMXConnectionInfo jmxConnectionInfo) {
        return new CassandraJMXServiceImpl(jmxConnectionInfo);
    }

    @Provides
    CassandraVersion provideCassandraVersion(final CassandraJMXService cassandraJMXService) throws Exception {
        return CassandraVersion.parse(cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, String>() {
            @Override
            public String apply(StorageServiceMBean object) {
                return object.getReleaseVersion();
            }
        }));
    }
}
