package com.instaclustr.cassandra.topology;

import com.instaclustr.cassandra.CassandraInteraction;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraClusterName implements CassandraInteraction<String> {

    private final CassandraJMXService cassandraJMXService;

    public CassandraClusterName(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    public String act() throws Exception {
        return cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, String>() {
            @Override
            public String apply(final StorageServiceMBean object) throws Exception {
                return object.getClusterName().replaceAll("\\s+", "-");
            }
        });
    }
}
