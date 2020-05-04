package com.instaclustr.cassandra.backup.impl.interaction;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.CassandraObjectNames.V3;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.EndpointSnitchInfoMBean;

public class CassandraEndpointDC implements CassandraInteraction<Map<InetAddress, String>> {

    private final CassandraJMXService cassandraJMXService;
    private final Collection<InetAddress> endpoints = new ArrayList<>();

    public CassandraEndpointDC(final CassandraJMXService cassandraJMXService,
                               final InetAddress endpoint) {
        this(cassandraJMXService, Collections.singletonList(endpoint));
    }

    public CassandraEndpointDC(final CassandraJMXService cassandraJMXService,
                               final Collection<InetAddress> endpoints) {
        this.cassandraJMXService = cassandraJMXService;
        this.endpoints.addAll(endpoints);
    }

    @Override
    public Map<InetAddress, String> act() throws Exception {

        final Map<InetAddress, String> endpointDCMap = new HashMap<>();

        for (final InetAddress endpoint : endpoints) {
            final String dc = cassandraJMXService.doWithMBean(new FunctionWithEx<EndpointSnitchInfoMBean, String>() {
                @Override
                public String apply(final EndpointSnitchInfoMBean mbean) throws Exception {
                    return mbean.getDatacenter(endpoint.getHostAddress());
                }
            }, EndpointSnitchInfoMBean.class, V3.ENDPOINT_SNITCH_INFO_MBEAN_NAME);

            endpointDCMap.put(endpoint, dc);
        }

        return endpointDCMap;
    }
}
