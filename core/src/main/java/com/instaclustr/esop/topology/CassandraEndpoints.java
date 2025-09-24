package com.instaclustr.esop.topology;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.google.common.base.Strings;

import com.instaclustr.cassandra.CassandraInteraction;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraEndpoints implements CassandraInteraction<Map<InetAddress, UUID>> {

    private final CassandraJMXService cassandraJMXService;
    private final String dc;


    public CassandraEndpoints(final CassandraJMXService cassandraJMXService, final String dc) {
        this.cassandraJMXService = cassandraJMXService;
        this.dc = dc;
    }

    public CassandraEndpoints(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
        this.dc = null;
    }

    @Override
    public Map<InetAddress, UUID> act() throws Exception {
        final Map<InetAddress, UUID> endpointToHostIdMap = getEndpointToHostIdMap();

        if (Strings.isNullOrEmpty(dc)) {
            return endpointToHostIdMap;
        }

        final Map<InetAddress, UUID> hostsInDC = new HashMap<>();

        for (final Entry<InetAddress, UUID> entry : endpointToHostIdMap.entrySet()) {
            final Map<InetAddress, String> endpointDC = new CassandraEndpointDC(cassandraJMXService, entry.getKey()).act();
            if (dc.equals(endpointDC.get(entry.getKey()))) {
                hostsInDC.put(entry.getKey(), entry.getValue());
            }
        }

        return hostsInDC;
    }

    private Map<InetAddress, UUID> getEndpointToHostIdMap() throws Exception {
        final Map<String, String> endpointToHostIdMap = cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Map<String, String>>() {
            @Override
            public Map<String, String> apply(final StorageServiceMBean ssMBean) throws Exception {
                return ssMBean.getEndpointToHostId();
            }
        });

        return transform(endpointToHostIdMap);
    }

    private Map<InetAddress, UUID> transform(final Map<String, String> ipAddresses) throws Exception {

        final Map<InetAddress, UUID> transformed = new HashMap<>();

        for (final Entry<String, String> entry : ipAddresses.entrySet()) {
            transformed.put(InetAddress.getByName(entry.getKey()), UUID.fromString(entry.getValue()));
        }

        return transformed;
    }
}
