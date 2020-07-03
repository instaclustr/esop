package com.instaclustr.cassandra.backup.impl.interaction;

import java.util.Collections;
import java.util.List;

import com.instaclustr.cassandra.CassandraInteraction;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraTokens implements CassandraInteraction<List<String>> {

    private final CassandraJMXService cassandraJMXService;

    public CassandraTokens(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    @Override
    public List<String> act() throws Exception {
        final List<String> tokensOfNode = cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, List<String>>() {
            @Override
            public List<String> apply(final StorageServiceMBean ssMBean) {
                return ssMBean.getTokens();
            }
        });

        Collections.sort(tokensOfNode);

        return tokensOfNode;
    }
}
