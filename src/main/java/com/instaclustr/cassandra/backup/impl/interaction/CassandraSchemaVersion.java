package com.instaclustr.cassandra.backup.impl.interaction;

import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraSchemaVersion implements CassandraInteraction<String> {

    private final CassandraJMXService cassandraJMXService;

    public CassandraSchemaVersion(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    public String act() throws Exception {
        return cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, String>() {
            @Override
            public String apply(final StorageServiceMBean ssMBean) throws Exception {
                return ssMBean.getSchemaVersion();
            }
        });
    }
}
