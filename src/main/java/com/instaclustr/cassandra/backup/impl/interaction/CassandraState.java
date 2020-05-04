package com.instaclustr.cassandra.backup.impl.interaction;

import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraState implements CassandraInteraction<Boolean> {

    private final CassandraJMXService cassandraJMXService;

    private final String stateToExpect;

    public CassandraState(final CassandraJMXService cassandraJMXService) {
        this(cassandraJMXService, "NORMAL");
    }

    public CassandraState(final CassandraJMXService cassandraJMXService, final String stateToExpect) {
        this.cassandraJMXService = cassandraJMXService;
        this.stateToExpect = stateToExpect;
    }

    @Override
    public Boolean act() throws Exception {
        return cassandraJMXService.doWithCassandra3StorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Boolean>() {
            @Override
            public Boolean apply(final StorageServiceMBean ssBean) {
                return stateToExpect.equals(ssBean.getOperationMode());
            }
        });
    }
}
