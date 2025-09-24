package com.instaclustr.esop.impl.interaction;

import com.instaclustr.cassandra.CassandraInteraction;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraMyEndpoint implements CassandraInteraction<String> {

    private final CassandraJMXService cassandraJMXService;

    public CassandraMyEndpoint(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    public String act() throws Exception {
        return cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, String>() {
            @Override
            public String apply(final StorageServiceMBean object) throws Exception {
                return object.getLocalHostId();
            }
        });
    }
}
