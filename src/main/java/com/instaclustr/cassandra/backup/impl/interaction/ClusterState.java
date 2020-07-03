package com.instaclustr.cassandra.backup.impl.interaction;

import com.instaclustr.cassandra.CassandraInteraction;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class ClusterState implements CassandraInteraction<Boolean> {

    private final CassandraJMXService cassandraJMXService;

    public ClusterState(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    public Boolean act() throws Exception {
        return cassandraJMXService.doWithCassandra3StorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Boolean>() {
            @Override
            public Boolean apply(final StorageServiceMBean ssBean) {
                boolean noJoiningNodes = ssBean.getJoiningNodes().isEmpty();
                boolean noLeavingNodes = ssBean.getLeavingNodes().isEmpty();
                boolean noMovingNodes = ssBean.getMovingNodes().isEmpty();
                boolean noUnreachableNodes = ssBean.getUnreachableNodes().isEmpty();

                return noJoiningNodes && noLeavingNodes && noMovingNodes && noUnreachableNodes;
            }
        });
    }
}
