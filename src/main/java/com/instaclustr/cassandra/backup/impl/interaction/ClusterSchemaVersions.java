package com.instaclustr.cassandra.backup.impl.interaction;

import java.util.List;
import java.util.Map;

import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.CassandraObjectNames.V3;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageProxyMBean;

public class ClusterSchemaVersions implements CassandraInteraction<Map<String, List<String>>> {

    private final CassandraJMXService cassandraJMXService;

    public ClusterSchemaVersions(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    public Map<String, List<String>> act() throws Exception {
        return cassandraJMXService.doWithMBean(new FunctionWithEx<StorageProxyMBean, Map<String, List<String>>>() {
            @Override
            public Map<String, List<String>> apply(final StorageProxyMBean storageProxyMBean) throws Exception {
                return storageProxyMBean.getSchemaVersions();
            }
        }, StorageProxyMBean.class, V3.STORAGE_PROXY_MBEAN_NAME);
    }
}
