package com.instaclustr.esop.impl.interaction;

import java.util.List;

import com.instaclustr.cassandra.CassandraInteraction;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraKeyspaces implements CassandraInteraction<List<String>> {

    private final CassandraJMXService cassandraJMXService;

    public CassandraKeyspaces(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    public List<String> act() throws Exception {
        return cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, List<String>>() {
            @Override
            public List<String> apply(final StorageServiceMBean object) throws Exception {
                return object.getKeyspaces();
            }
        });
    }

    public static boolean containsKeyspace(final CassandraJMXService cassandraJMXService,
                                           final String keyspace) throws Exception {
        return new CassandraKeyspaces(cassandraJMXService).act().contains(keyspace);
    }
}
